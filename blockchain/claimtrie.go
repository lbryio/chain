package blockchain

import (
	"fmt"

	"github.com/lbryio/claimtrie"
	"github.com/lbryio/claimtrie/change"
	"github.com/lbryio/claimtrie/claim"

	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"

	"github.com/pkg/errors"
)

// CheckClaimScripts extracts ClaimScripts from transactions, if any, and updates ClaimTrie accordingly.
func (b *BlockChain) CheckClaimScripts(block *btcutil.Block, node *blockNode, view *UtxoViewpoint) error {
	ht := block.Height()
	for _, tx := range block.Transactions() {
		h := handler{ht, tx, view, map[string]bool{}}
		if err := h.handleTxIns(b.claimTrie); err != nil {
			return err
		}
		if err := h.handleTxOuts(b.claimTrie); err != nil {
			return err
		}
	}
	b.claimTrie.Commit(claim.Height(ht))
	hash := b.claimTrie.MerkleHash()
	if node.claimTrie != *hash {
		return fmt.Errorf("expect: %s, got: %s", *hash, node.claimTrie)
	}
	return nil
}

type handler struct {
	ht    int32
	tx    *btcutil.Tx
	view  *UtxoViewpoint
	spent map[string]bool
}

func (h *handler) handleTxIns(ct *claimtrie.ClaimTrie) error {
	if IsCoinBase(h.tx) {
		return nil
	}
	for _, txIn := range h.tx.MsgTx().TxIn {
		op := txIn.PreviousOutPoint
		e := h.view.LookupEntry(op)
		cs, err := txscript.DecodeClaimScript(e.pkScript)
		if err == txscript.ErrNotClaimScript {
			continue
		} else if err != nil {
			return err
		}
		chg := &change.Change{
			Height: claim.Height(h.ht),
			Name:   string(cs.Name()),
			OP:     claim.OutPoint{OutPoint: op},
			Amt:    claim.Amount(e.Amount()),
			Value:  cs.Value(),
		}

		switch cs.Opcode() {
		case txscript.OP_CLAIMNAME:
			chg.Cmd = change.SpendClaim
			chg.ID = claim.NewID(chg.OP)
			h.spent[chg.ID.String()] = true
			err = ct.SpendClaim(chg.Name, chg.OP)
		case txscript.OP_UPDATECLAIM:
			chg.Cmd = change.SpendClaim
			copy(chg.ID[:], cs.ClaimID())
			h.spent[chg.ID.String()] = true
			err = ct.SpendClaim(chg.Name, chg.OP)
		case txscript.OP_SUPPORTCLAIM:
			chg.Cmd = change.SpendSupport
			copy(chg.ID[:], cs.ClaimID())
			err = ct.SpendSupport(chg.Name, chg.OP)
		}
		if err != nil {
			return errors.Wrapf(err, "handleTxIns: %s", chg)
		}
	}
	return nil
}

func (h *handler) handleTxOuts(ct *claimtrie.ClaimTrie) error {
	for i, txOut := range h.tx.MsgTx().TxOut {
		op := wire.NewOutPoint(h.tx.Hash(), uint32(i))
		cs, err := txscript.DecodeClaimScript(txOut.PkScript)
		if err == txscript.ErrNotClaimScript {
			continue
		} else if err != nil {
			return err
		}
		chg := &change.Change{
			Height: claim.Height(h.ht),
			Name:   string(cs.Name()),
			OP:     claim.OutPoint{OutPoint: *op},
			Amt:    claim.Amount(txOut.Value),
			Value:  cs.Value(),
		}

		switch cs.Opcode() {
		case txscript.OP_CLAIMNAME:
			chg.Cmd = change.AddClaim
			chg.ID = claim.NewID(chg.OP)
			err = ct.AddClaim(chg.Name, chg.OP, chg.Amt, chg.Value)
		case txscript.OP_SUPPORTCLAIM:
			chg.Cmd = change.AddSupport
			copy(chg.ID[:], cs.ClaimID())
			err = ct.AddSupport(chg.Name, chg.OP, chg.Amt, chg.ID)
		case txscript.OP_UPDATECLAIM:
			chg.Cmd = change.UpdateClaim
			copy(chg.ID[:], cs.ClaimID())
			if !h.spent[chg.ID.String()] {
				continue
			}
			delete(h.spent, chg.ID.String())
			err = ct.UpdateClaim(chg.Name, chg.OP, chg.Amt, chg.ID, chg.Value)
		}
		if err != nil {
			return errors.Wrapf(err, "handleTxOuts: %s", chg)
		}
	}
	return nil
}
