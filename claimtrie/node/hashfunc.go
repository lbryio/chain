package node

import (
	"crypto/sha256"
	"encoding/binary"
	"strconv"

	"github.com/lbryio/lbcd/chaincfg/chainhash"
	"github.com/lbryio/lbcd/wire"
)

func HashMerkleBranches(left *chainhash.Hash, right *chainhash.Hash) *chainhash.Hash {
	// Concatenate the left and right nodes.
	var hash [chainhash.HashSize * 2]byte
	copy(hash[:chainhash.HashSize], left[:])
	copy(hash[chainhash.HashSize:], right[:])

	newHash := chainhash.DoubleHashH(hash[:])
	return &newHash
}

func ComputeMerkleRoot(hashes []*chainhash.Hash) *chainhash.Hash {
	if len(hashes) <= 0 {
		return nil
	}
	for len(hashes) > 1 {
		if (len(hashes) & 1) > 0 { // odd count
			hashes = append(hashes, hashes[len(hashes)-1])
		}
		for i := 0; i < len(hashes); i += 2 { // TODO: parallelize this loop (or use a lib that does it)
			hashes[i>>1] = HashMerkleBranches(hashes[i], hashes[i+1])
		}
		hashes = hashes[:len(hashes)>>1]
	}
	return hashes[0]
}

func calculateNodeHash(op wire.OutPoint, takeover int32) *chainhash.Hash {

	txHash := chainhash.DoubleHashH(op.Hash[:])

	nOut := []byte(strconv.Itoa(int(op.Index)))
	nOutHash := chainhash.DoubleHashH(nOut)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(takeover))
	heightHash := chainhash.DoubleHashH(buf)

	h := make([]byte, 0, sha256.Size*3)
	h = append(h, txHash[:]...)
	h = append(h, nOutHash[:]...)
	h = append(h, heightHash[:]...)

	hh := chainhash.DoubleHashH(h)

	return &hh
}

func ComputeMerklePath(hashes []*chainhash.Hash, idx int) []*chainhash.Hash {
	count := 0
	matchlevel := -1
	matchh := false
	var h *chainhash.Hash
	var res []*chainhash.Hash
	var inner [32]*chainhash.Hash // old code had 32; dunno if it's big enough for all scenarios

	iterateInner := func(level int) int {
		for ; (count & (1 << level)) == 0; level++ {
			ihash := inner[level]
			if matchh {
				res = append(res, ihash)
			} else if matchlevel == level {
				res = append(res, h)
				matchh = true
			}
			h = HashMerkleBranches(ihash, h)
		}
		return level
	}

	for count < len(hashes) {
		h = hashes[count]
		matchh = count == idx
		count++
		level := iterateInner(0)
		// Store the resulting hash at inner position level.
		inner[level] = h
		if matchh {
			matchlevel = level
		}
	}

	level := 0
	for (count & (1 << level)) == 0 {
		level++
	}

	h = inner[level]
	matchh = matchlevel == level

	for count != (1 << level) {
		// If we reach this point, h is an inner value that is not the top.
		if matchh {
			res = append(res, h)
		}
		h = HashMerkleBranches(h, h)
		// Increment count to the value it would have if two entries at this
		count += 1 << level
		level++
		level = iterateInner(level)
	}
	return res
}
