package merkletrie

import (
	"bytes"
	"errors"
	"runtime"
	"sync"

	"github.com/lbryio/lbcd/chaincfg/chainhash"
	"github.com/lbryio/lbcd/claimtrie/node"
)

type MerkleTrie interface {
	SetRoot(h *chainhash.Hash) error
	Update(name []byte, h *chainhash.Hash, restoreChildren bool)
	MerkleHash() *chainhash.Hash
	MerkleHashAllClaims() *chainhash.Hash
	Flush() error
	MerklePath(name []byte) []HashSidePair
}

type RamTrie struct {
	collapsedTrie
	bufs *sync.Pool
}

func NewRamTrie() *RamTrie {
	return &RamTrie{
		bufs: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		collapsedTrie: collapsedTrie{Root: &collapsedVertex{merkleHash: EmptyTrieHash}},
	}
}

var ErrFullRebuildRequired = errors.New("a full rebuild is required")

func (rt *RamTrie) SetRoot(h *chainhash.Hash) error {
	if rt.Root.merkleHash.IsEqual(h) {
		runtime.GC()
		return nil
	}

	// should technically clear the old trie first, but this is abused for partial rebuilds so don't
	return ErrFullRebuildRequired
}

func (rt *RamTrie) Update(name []byte, h *chainhash.Hash, _ bool) {
	if h == nil {
		rt.Erase(name)
	} else {
		_, n := rt.InsertOrFind(name)
		n.claimHash = h
	}
}

func (rt *RamTrie) MerkleHash() *chainhash.Hash {
	if h := rt.merkleHash(rt.Root); h == nil {
		return EmptyTrieHash
	}
	return rt.Root.merkleHash
}

func (rt *RamTrie) merkleHash(v *collapsedVertex) *chainhash.Hash {
	if v.merkleHash != nil {
		return v.merkleHash
	}

	b := rt.bufs.Get().(*bytes.Buffer)
	defer rt.bufs.Put(b)
	b.Reset()

	for _, ch := range v.children {
		h := rt.merkleHash(ch)              // h is a pointer; don't destroy its data
		b.WriteByte(ch.key[0])              // nolint : errchk
		b.Write(rt.completeHash(h, ch.key)) // nolint : errchk
	}

	if v.claimHash != nil {
		b.Write(v.claimHash[:])
	}

	if b.Len() > 0 {
		h := chainhash.DoubleHashH(b.Bytes())
		v.merkleHash = &h
	}

	return v.merkleHash
}

func (rt *RamTrie) completeHash(h *chainhash.Hash, childKey KeyType) []byte {
	var data [chainhash.HashSize + 1]byte
	copy(data[1:], h[:])
	for i := len(childKey) - 1; i > 0; i-- {
		data[0] = childKey[i]
		copy(data[1:], chainhash.DoubleHashB(data[:]))
	}
	return data[1:]
}

func (rt *RamTrie) MerkleHashAllClaims() *chainhash.Hash {
	if h := rt.merkleHashAllClaims(rt.Root); h == nil {
		return EmptyTrieHash
	}
	return rt.Root.merkleHash
}

func (rt *RamTrie) merkleHashAllClaims(v *collapsedVertex) *chainhash.Hash {
	if v.merkleHash != nil {
		return v.merkleHash
	}

	childHash, hasChildren := rt.computeChildHash(v)

	claimHash := NoClaimsHash
	if v.claimHash != nil {
		claimHash = v.claimHash
	} else if !hasChildren {
		return nil
	}

	v.merkleHash = node.HashMerkleBranches(childHash, claimHash)
	return v.merkleHash
}

func (rt *RamTrie) computeChildHash(v *collapsedVertex) (*chainhash.Hash, bool) {
	childHashes := make([]*chainhash.Hash, 0, len(v.children))
	for _, ch := range v.children {
		h := rt.merkleHashAllClaims(ch)
		childHashes = append(childHashes, h)
	}
	childHash := NoChildrenHash
	if len(childHashes) > 0 {
		// this shouldn't be referencing node; where else can we put this merkle root func?
		childHash = node.ComputeMerkleRoot(childHashes)
	}
	return childHash, len(childHashes) > 0
}

func (rt *RamTrie) Flush() error {
	return nil
}

type HashSidePair struct {
	Right bool
	Hash  *chainhash.Hash
}

func (rt *RamTrie) MerklePath(name []byte) []HashSidePair {

	// algorithm:
	// for each node in the path to key:
	//   get all the childHashes for that node and the index of our path
	//   get all the claimHashes for that node as well
	//   if we're at the end of the path:
	//      push(true, root(childHashes))
	//      push all of merklePath(claimHashes, bid)
	//   else
	//      push(false, root(claimHashes)
	//      push all of merklePath(childHashes, child index)

	var results []HashSidePair

	indexes, path := rt.FindPath(name)
	for i := 0; i < len(indexes); i++ {
		if i == len(indexes)-1 {
			childHash, _ := rt.computeChildHash(path[i])
			results = append(results, HashSidePair{Right: true, Hash: childHash})
			// letting the caller append the claim hashes at present (needs better code organization)
		} else {
			ch := path[i].claimHash
			if ch == nil {
				ch = NoClaimsHash
			}
			results = append(results, HashSidePair{Right: false, Hash: ch})
			childHashes := make([]*chainhash.Hash, 0, len(path[i].children))
			for j := range path[i].children {
				childHashes = append(childHashes, path[i].children[j].merkleHash)
			}
			if len(childHashes) > 0 {
				partials := node.ComputeMerklePath(childHashes, indexes[i+1])
				for i := len(partials) - 1; i >= 0; i-- {
					results = append(results, HashSidePair{Right: ((indexes[i+1] >> i) & 1) > 0, Hash: partials[i]})
				}
			}
		}
	}
	return results
}
