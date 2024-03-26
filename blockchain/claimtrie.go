package blockchain

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/lbryio/lbcd/chaincfg/chainhash"
	"github.com/lbryio/lbcd/txscript"
	"github.com/lbryio/lbcd/wire"
	btcutil "github.com/lbryio/lbcutil"

	"github.com/lbryio/lbcd/claimtrie"
	"github.com/lbryio/lbcd/claimtrie/change"
	"github.com/lbryio/lbcd/claimtrie/merkletrie"
	"github.com/lbryio/lbcd/claimtrie/node"
	"github.com/lbryio/lbcd/claimtrie/normalization"
	"github.com/lbryio/lbcd/claimtrie/param"
)

func (b *BlockChain) SetClaimtrieHeader(block *btcutil.Block, view *UtxoViewpoint) error {
	b.chainLock.Lock()
	defer b.chainLock.Unlock()

	err := b.ParseClaimScripts(block, nil, view, false)
	if err != nil {
		return errors.Wrapf(err, "in parse claim scripts")
	}

	block.MsgBlock().Header.ClaimTrie = *b.claimTrie.MerkleHash()
	err = b.claimTrie.ResetHeight(b.claimTrie.Height() - 1)

	return errors.Wrapf(err, "in reset height")
}

func (b *BlockChain) ParseClaimScripts(block *btcutil.Block, bn *blockNode, view *UtxoViewpoint, shouldFlush bool) error {
	ht := block.Height()

	for _, tx := range block.Transactions() {
		h := handler{ht, tx, view, map[string][]byte{}}
		if err := h.handleTxIns(b.claimTrie); err != nil {
			return err
		}
		if err := h.handleTxOuts(b.claimTrie); err != nil {
			return err
		}
	}

	err := b.claimTrie.AppendBlock(bn == nil)
	if err != nil {
		return errors.Wrapf(err, "in append block")
	}

	if shouldFlush {
		b.claimTrie.FlushToDisk()
	}

	hash := b.claimTrie.MerkleHash()
	if bn != nil && bn.claimTrie != *hash {
		// undo our AppendBlock call as we've decided that our interpretation of the block data is incorrect,
		// or that the person who made the block assembled the pieces incorrectly.
		_ = b.claimTrie.ResetHeight(b.claimTrie.Height() - 1)
		return errors.Errorf("height: %d, computed hash: %s != header's ClaimTrie: %s", ht, *hash, bn.claimTrie)
	}
	return nil
}

type handler struct {
	ht    int32
	tx    *btcutil.Tx
	view  *UtxoViewpoint
	spent map[string][]byte
}

func (h *handler) handleTxIns(ct *claimtrie.ClaimTrie) error {
	if IsCoinBase(h.tx) {
		return nil
	}
	for _, txIn := range h.tx.MsgTx().TxIn {
		op := txIn.PreviousOutPoint
		e := h.view.LookupEntry(op)
		if e == nil {
			return errors.Errorf("missing input in view for %s", op.String())
		}
		cs, err := txscript.ExtractClaimScript(e.pkScript)
		if txscript.IsErrorCode(err, txscript.ErrNotClaimScript) {
			continue
		}
		if err != nil {
			return err
		}

		var id change.ClaimID
		name := cs.Name // name of the previous one (that we're now spending)

		switch cs.Opcode {
		case txscript.OP_CLAIMNAME: // OP code from previous transaction
			id = change.NewClaimID(op) // claimID of the previous item now being spent
			h.spent[id.Key()] = normalization.NormalizeIfNecessary(name, ct.Height())
			err = ct.SpendClaim(name, op, id)
		case txscript.OP_UPDATECLAIM:
			copy(id[:], cs.ClaimID)
			h.spent[id.Key()] = normalization.NormalizeIfNecessary(name, ct.Height())
			err = ct.SpendClaim(name, op, id)
		case txscript.OP_SUPPORTCLAIM:
			copy(id[:], cs.ClaimID)
			err = ct.SpendSupport(name, op, id)
		}
		if err != nil {
			return errors.Wrapf(err, "handleTxIns")
		}
	}
	return nil
}

func (h *handler) handleTxOuts(ct *claimtrie.ClaimTrie) error {
	for i, txOut := range h.tx.MsgTx().TxOut {
		op := *wire.NewOutPoint(h.tx.Hash(), uint32(i))
		cs, err := txscript.ExtractClaimScript(txOut.PkScript)
		if txscript.IsErrorCode(err, txscript.ErrNotClaimScript) {
			continue
		}
		if err != nil {
			return err
		}

		var id change.ClaimID
		name := cs.Name
		amt := txOut.Value

		switch cs.Opcode {
		case txscript.OP_CLAIMNAME:
			id = change.NewClaimID(op)
			err = ct.AddClaim(name, op, id, amt)
		case txscript.OP_SUPPORTCLAIM:
			copy(id[:], cs.ClaimID)
			err = ct.AddSupport(name, op, amt, id)
		case txscript.OP_UPDATECLAIM:
			// old code wouldn't run the update if name or claimID didn't match existing data
			// that was a safety feature, but it should have rejected the transaction instead
			// TODO: reject transactions with invalid update commands
			copy(id[:], cs.ClaimID)
			normName := normalization.NormalizeIfNecessary(name, ct.Height())
			if !bytes.Equal(h.spent[id.Key()], normName) {
				node.LogOnce(fmt.Sprintf("Invalid update operation: name or ID mismatch at %d for: %s, %s",
					ct.Height(), normName, id.String()))
				continue
			}

			delete(h.spent, id.Key())
			err = ct.UpdateClaim(name, op, amt, id)
		}
		if err != nil {
			return errors.Wrapf(err, "handleTxOuts")
		}
	}
	return nil
}

func (b *BlockChain) GetNamesChangedInBlock(height int32) ([]string, error) {
	b.chainLock.RLock()
	defer b.chainLock.RUnlock()

	return b.claimTrie.NamesChangedInBlock(height)
}

func (b *BlockChain) GetClaimsForName(height int32, name string) (string, *node.Node, error) {

	normalizedName := normalization.NormalizeIfNecessary([]byte(name), height)

	b.chainLock.RLock()
	defer b.chainLock.RUnlock()

	n, err := b.claimTrie.NodeAt(height, normalizedName)
	if err != nil {
		return string(normalizedName), nil, err
	}

	if n == nil {
		return string(normalizedName), nil, fmt.Errorf("name does not exist at height %d: %s", height, name)
	}

	n.SortClaimsByBid()
	return string(normalizedName), n, nil
}

func (b *BlockChain) GetProofForName(name, id string, bid, seq int) (chainhash.Hash, int32, *node.Claim, int32, int32, string, []merkletrie.HashSidePair, error) {
	// results: block hash, height, claim, bid, takeover, name, pairs, err

	b.chainLock.RLock()
	defer b.chainLock.RUnlock()

	tip := b.bestChain.Tip()

	normalizedName := normalization.NormalizeIfNecessary([]byte(name), tip.height)

	if tip.height < param.ActiveParams.GrandForkHeight {
		err := errors.Errorf("Unable to generate proofs for claims before height %d",
			param.ActiveParams.GrandForkHeight)
		return tip.hash, tip.height, nil, 0, 0, string(normalizedName), nil, err
	}

	n, err := b.claimTrie.NodeAt(tip.height, normalizedName)
	if n == nil && err == nil {
		err = errors.Errorf("Unable to locate a claim with name %s at height %d", normalizedName, tip.height)
	}
	if err != nil {
		return tip.hash, tip.height, nil, 0, 0, string(normalizedName), nil, err
	}

	// now find the desired claim
	n.SortClaimsByBid()
	var claim *node.Claim
	for i, c := range n.Claims {
		if c.Status != node.Activated {
			continue
		}
		if bid >= 0 && i == bid {
			claim = c
			bid = i
			break
		}
		if seq >= 0 && int(c.Sequence) == seq {
			claim = c
			bid = i
			break
		}
		if len(id) > 0 && strings.HasPrefix(c.ClaimID.String(), id) {
			claim = c
			bid = i
			break
		}
	}
	if claim == nil {
		if bid >= 0 {
			err = errors.Errorf("Unable to locate a claim named %s with bid %d at height %d", normalizedName, bid, tip.height)
		}
		if seq >= 0 {
			err = errors.Errorf("Unable to locate a claim named %s with sequence %d at height %d", normalizedName, seq, tip.height)
		}
		if len(id) > 0 {
			err = errors.Errorf("Unable to locate a claim named %s with ID %s at height %d", normalizedName, id, tip.height)
		}
		return tip.hash, tip.height, nil, 0, 0, string(normalizedName), nil, err
	}

	pairs := b.claimTrie.MerklePath(normalizedName, n, bid)

	return tip.hash, tip.height, claim, int32(bid), n.TakenOverAt, string(normalizedName), pairs, nil
}
