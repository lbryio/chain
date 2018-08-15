package main

import (
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/chaincfg/chainhash"

	"github.com/btcsuite/btcutil"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/lbryio/claimtrie/claim"
)

var (
	errNoError = errors.New("no error")
)

func amountToLBC(amt claim.Amount) string {
	sign := ""
	if amt < 0 {
		sign = "-"
		amt = -amt
	}
	quotient := amt / btcutil.SatoshiPerBitcoin
	remainder := amt % btcutil.SatoshiPerBitcoin
	return fmt.Sprintf("%s%d.%08d", sign, quotient, remainder)
}

// handleGetClaimsInTrie returns all claims in the name trie.
func handleGetClaimsInTrie(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	res := btcjson.GetClaimsInTrieResult{}
	fn := func(n *claim.Node) bool {
		e := btcjson.ClaimsInTrieEntry{
			Name:   n.Name(),
			Claims: []btcjson.ClaimsInTrieDetail{},
		}
		for _, c := range n.Claims() {
			clm := btcjson.ClaimsInTrieDetail{
				ClaimID: c.ID.String(),
				TxID:    c.OutPoint.Hash.String(),
				N:       c.OutPoint.Index,
				Amount:  amountToLBC(c.Amt),
				Height:  c.Accepted,
				Value:   string(c.Value),
			}
			e.Claims = append(e.Claims, clm)
		}
		res = append(res, e)
		return false
	}
	s.cfg.Chain.ClaimTrie().NodeMgr().Visit(fn)
	return res, nil
}

// handleGetClaimTrie returns the entire name trie.
func handleGetClaimTrie(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	return nil, nil
}

// handleGetValueForName returns the value associated with a name, if one exists.
func handleGetValueForName(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	name := cmd.(*btcjson.GetValueForNameCmd).Name
	ct := s.cfg.Chain.ClaimTrie()
	n := ct.NodeMgr().NodeAt(name, ct.Height())
	if n == nil || n.BestClaim() == nil {
		return btcjson.GetValueForNameResult{}, nil
	}
	c := n.BestClaim()

	return btcjson.GetValueForNameResult{
		Value:           string(c.Value),
		ClaimID:         c.ID.String(),
		TxID:            c.OutPoint.Hash.String(),
		N:               c.OutPoint.Index,
		Amount:          c.Amt,
		EffectiveAmount: c.EffAmt,
		Height:          c.Accepted,
	}, nil
}

// handleGetClaimsForName returns all claims and supports for a name.
func handleGetClaimsForName(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	name := cmd.(*btcjson.GetClaimsForNameCmd).Name
	res := btcjson.GetClaimsForNameResult{}
	ct := s.cfg.Chain.ClaimTrie()
	n := ct.NodeMgr().NodeAt(name, ct.Height())
	if n == nil {
		return res, nil
	}

	matched := map[claim.OutPoint]bool{}
	for _, c := range n.Claims() {
		cfn := btcjson.ClaimForName{
			ClaimID:         c.ID.String(),
			TxID:            c.OutPoint.Hash.String(),
			N:               c.OutPoint.Index,
			Height:          c.Accepted,
			ValidHeight:     c.ActiveAt,
			Amount:          c.Amt,
			EffectiveAmount: c.EffAmt,
			Supports:        []btcjson.SupportOfClaim{},
		}
		for _, s := range n.Supports() {
			if s.ID != c.ID {
				continue
			}
			sup := btcjson.SupportOfClaim{
				TxID:        s.OutPoint.Hash.String(),
				N:           s.OutPoint.Index,
				Height:      s.Accepted,
				ValidHeight: s.ActiveAt,
				Amount:      s.Amt,
			}
			cfn.Supports = append(cfn.Supports, sup)
			matched[s.OutPoint] = true
		}

		res.Claims = append(res.Claims, cfn)
	}
	// Initialize as empty slice instead of nil.
	res.UnmatchedSupports = []btcjson.SupportOfClaim{}
	for _, s := range n.Supports() {
		if matched[s.OutPoint] {
			continue
		}
		sup := btcjson.SupportOfClaim{
			TxID:        s.OutPoint.Hash.String(),
			N:           s.OutPoint.Index,
			Height:      s.Accepted,
			ValidHeight: s.ActiveAt,
			Amount:      s.Amt,
		}
		res.UnmatchedSupports = append(res.UnmatchedSupports, sup)
	}
	res.LastTakeoverHeight = n.Tookover()

	return res, nil
}

// handleGetTotalClaimedNames returns the total number of names that have been successfully claimed, and therefore exist in the trie
func handleGetTotalClaimedNames(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	return s.cfg.Chain.ClaimTrie().NodeMgr().Size(), nil
}

// handleGetTotalClaims returns the total number of active claims in the trie.
func handleGetTotalClaims(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	cnt := 0
	fn := func(n *claim.Node) bool {
		cnt += len(n.Claims())
		return false
	}
	s.cfg.Chain.ClaimTrie().NodeMgr().Visit(fn)
	return int64(cnt), nil
}

// handleGetTotalValueOfClaims returns the total value of the claims in the trie.
func handleGetTotalValueOfClaims(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	var amt claim.Amount
	fn := func(n *claim.Node) bool {
		for _, c := range n.Claims() {
			amt += c.Amt
		}
		return false
	}
	if cmd.(*btcjson.GetTotalValueOfClaimsCmd).ControllingOnly {
		fn = func(n *claim.Node) bool {
			if n.BestClaim() != nil {
				amt += n.BestClaim().Amt
			}
			return false
		}
	}
	s.cfg.Chain.ClaimTrie().NodeMgr().Visit(fn)
	return amt, nil
}

// handleGetClaimsForTx returns any claims or supports found in a transaction.
func handleGetClaimsForTx(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	h, err := chainhash.NewHashFromStr(cmd.(*btcjson.GetClaimsForTxCmd).TxID)
	if err != nil {
		return nil, &btcjson.RPCError{
			Code:    btcjson.ErrRPCInvalidParameter,
			Message: err.Error(),
		}
	}
	ht := claim.Height(s.cfg.Chain.BestSnapshot().Height)
	res := btcjson.GetClaimsForTxResult{}
	fn := func(n *claim.Node) bool {
		for _, c := range n.Claims() {
			if c.OutPoint.Hash != *h {
				continue
			}
			e := btcjson.ClaimsForTxEntry{
				N:             c.OutPoint.Index,
				Type:          "claim",
				Name:          n.Name(),
				Value:         string(c.Value),
				Depth:         ht - c.Accepted,
				InClaimTrie:   claim.IsActiveAt(c, ht),
				InQueue:       true,
				BlocksToValid: c.ActiveAt - ht,
			}
			if n.BestClaim() != nil && n.BestClaim().OutPoint == c.OutPoint {
				e.IsControlling = true
			}
			if e.BlocksToValid <= 0 {
				e.InQueue = false
				e.BlocksToValid = 0
			}
			res = append(res, e)
		}
		for _, c := range n.Supports() {
			if c.OutPoint.Hash != *h {
				continue
			}
			e := btcjson.ClaimsForTxEntry{
				N:             c.OutPoint.Index,
				Type:          "support",
				Name:          n.Name(),
				SupportedID:   c.ID.String(),
				SupportedNOut: c.OutPoint.Index,
				Depth:         ht - c.Accepted,
				InSupportMap:  claim.IsActiveAt(c, ht),
				InQueue:       true,
				BlocksToValid: c.ActiveAt - ht,
			}
			if e.BlocksToValid <= 0 {
				e.InQueue = false
				e.BlocksToValid = 0
			}
			res = append(res, e)
		}
		return false
	}
	s.cfg.Chain.ClaimTrie().NodeMgr().Visit(fn)
	return res, nil
}

// getnameproof
// Return the cryptographic proof that a name maps to a value or doesn't.
// Arguments:
// 1. "name"                    (string) the name to get a proof for
// 2. "blockhash"               (string, optional) the hash of the block which is the basis of the proof.
//                                                 If none is given, the latest block will be used.
// Result:
// {
//   "nodes" : [                (array of object) full nodes (i.e. those which lead to the requested name)
//     "children" : [           (array of object) the children of this node
//       "child" : {            (object) a child node, either leaf or reference to a full node
//         "character" : "char" (string) the character which leads from the parent to this child node
//         "nodeHash" :  "hash" (string, if exists) the hash of the node if this is a leaf node
//         }
//       ]
//     "valueHash"               (string, if exists) the hash of this node's value, if it has one.
//                                                   If this is the requested name this will not exist whether the node has a value or not.
//     ]
//   "txhash" : "hash"       (string, if exists) the txid of the claim which controls this name, if there is one.
//   "nOut" : n,             (numeric) the nOut of the claim which controls this name, if there is one.
//   "last takeover height"  (numeric) the most recent height at which the value of a name changed other than through an update to the winning bid
//   }
// }

// handleGetNameProof returns the cryptographic proof that a name maps to a value or doesn't.
func handleGetNameProof(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	return nil, nil
}

// handleGetClaimByID returns a claim by ID.
func handleGetClaimByID(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	c := cmd.(*btcjson.GetClaimByIDCmd)

	id, err := claim.NewIDFromString(c.ID)
	if err != nil {
		return nil, &btcjson.RPCError{
			Code:    btcjson.ErrRPCInvalidParameter,
			Message: err.Error(),
		}
	}

	var clm *claim.Claim
	var node *claim.Node
	fn := func(n *claim.Node) bool {
		if clm = claim.Find(claim.ByID(id), n.Claims()); clm != nil {
			node = n
			return true
		}
		return false
	}
	s.cfg.Chain.ClaimTrie().NodeMgr().Visit(fn)
	if node == nil {
		return btcjson.EmptyResult{}, nil
	}

	res := btcjson.GetClaimByIDResult{
		Name:        node.Name(),
		Value:       string(clm.Value),
		ClaimID:     id.String(),
		TxID:        clm.OutPoint.Hash.String(),
		N:           clm.OutPoint.Index,
		Amount:      clm.Amt,
		EffAmount:   clm.EffAmt,
		Supports:    []btcjson.ClaimByIDSupport{},
		Height:      clm.Accepted,
		ValidHeight: clm.ActiveAt,
	}
	for _, s := range node.Supports() {
		if s.ID != id {
			continue
		}
		sup := btcjson.ClaimByIDSupport{
			TxID:        s.OutPoint.Hash.String(),
			N:           s.OutPoint.Index,
			Height:      s.Accepted,
			ValidHeight: s.ActiveAt,
			Amount:      s.Amt,
		}
		res.Supports = append(res.Supports, sup)
	}

	return res, nil
}
