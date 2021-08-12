package node

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lbryio/chain/claimtrie/change"
	"github.com/lbryio/chain/claimtrie/param"
)

type HashV2Manager struct {
	Manager
}

type HashV3Manager struct {
	Manager
}

func (nm *HashV2Manager) claimHashes(name []byte) (*chainhash.Hash, int32) {

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
		return nm.claimHashes(name)
	}

	return nm.Manager.Hash(name)
}

func (nm *HashV3Manager) AppendChange(chg change.Change) {
	if nm.Height() >= param.ActiveParams.GrandForkHeight && len(chg.Name) == 0 {
		return
	}
	nm.Manager.AppendChange(chg)
}

func (nm *HashV3Manager) Hash(name []byte) (*chainhash.Hash, int32) {

	if nm.Height() >= param.ActiveParams.GrandForkHeight {
		if len(name) == 0 {
			return nil, 0 // empty name's claims are not included in the hash
		}
		// return nm.detailHash()
	}

	return nm.Manager.Hash(name)
}
