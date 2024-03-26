package node

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/lbryio/lbcd/chaincfg/chainhash"
	"github.com/lbryio/lbcd/claimtrie/change"
	"github.com/lbryio/lbcd/claimtrie/param"
)

type HashV2Manager struct {
	Manager
}

type HashV3Manager struct {
	Manager
}

func (nm *HashV2Manager) computeClaimHashes(name []byte) (*chainhash.Hash, int32) {

	n, err := nm.NodeAt(nm.Height(), name)
	if err != nil || n == nil {
		return nil, 0
	}

	n.SortClaimsByBid()
	claimHashes := make([]*chainhash.Hash, 0, len(n.Claims))
	for _, c := range n.Claims {
		if c.Status == Activated { // TODO: unit test this line
			claimHashes = append(claimHashes, calculateNodeHash(c.OutPoint, n.TakenOverAt))
		}
	}
	if len(claimHashes) > 0 {
		return ComputeMerkleRoot(claimHashes), n.NextUpdate(nm.Height())
	}
	return nil, n.NextUpdate(nm.Height())
}

func (nm *HashV2Manager) Hash(name []byte) (*chainhash.Hash, int32) {

	if nm.Height() >= param.ActiveParams.AllClaimsInMerkleForkHeight {
		return nm.computeClaimHashes(name)
	}

	return nm.Manager.Hash(name)
}

func (nm *HashV3Manager) AppendChange(chg change.Change) {
	if nm.Height() >= param.ActiveParams.GrandForkHeight && len(chg.Name) == 0 {
		return
	}
	nm.Manager.AppendChange(chg)
}

func ComputeBidSeqNameHash(name []byte, c *Claim, bid, takeover int32) (*chainhash.Hash, error) {

	s := sha256.New()

	s.Write(c.OutPoint.Hash[:])

	var temp [4]byte
	binary.BigEndian.PutUint32(temp[:], c.OutPoint.Index)
	s.Write(temp[:])

	binary.BigEndian.PutUint32(temp[:], uint32(bid))
	s.Write(temp[:])

	binary.BigEndian.PutUint32(temp[:], uint32(c.Sequence))
	s.Write(temp[:])

	binary.BigEndian.PutUint32(temp[:], uint32(takeover))
	s.Write(temp[:])

	s.Write(name)

	var m [sha256.Size]byte
	return chainhash.NewHash(s.Sum(m[:0]))
}

func (nm *HashV3Manager) bidSeqNameHash(name []byte) (*chainhash.Hash, int32) {
	n, err := nm.NodeAt(nm.Height(), name)
	if err != nil || n == nil {
		return nil, 0
	}

	claimHashes := ComputeClaimHashes(name, n)
	if len(claimHashes) > 0 {
		return ComputeMerkleRoot(claimHashes), n.NextUpdate(nm.Height())
	}
	return nil, n.NextUpdate(nm.Height())
}

func ComputeClaimHashes(name []byte, n *Node) []*chainhash.Hash {
	n.SortClaimsByBid()
	claimHashes := make([]*chainhash.Hash, 0, len(n.Claims))
	for i, c := range n.Claims {
		if c.Status == Activated {
			h, _ := ComputeBidSeqNameHash(name, c, int32(i), n.TakenOverAt)
			claimHashes = append(claimHashes, h)
		}
	}
	return claimHashes
}

func (nm *HashV3Manager) Hash(name []byte) (*chainhash.Hash, int32) {

	if nm.Height() >= param.ActiveParams.GrandForkHeight {
		if len(name) == 0 {
			return nil, 0 // empty name's claims are not included in the hash
		}
		return nm.bidSeqNameHash(name)
	}

	return nm.Manager.Hash(name)
}
