package txscript

import (
	"fmt"

	"github.com/lbryio/lbrycrd.go/wire"
)

const (
	// MinFeePerNameclaimChar is the minimum claim fee per character in the name of an OP_CLAIM_NAME
	// command that must be attached to transactions for it to be accepted into the memory pool.
	// Rationale: current implementation of the claim trie uses more memory for longer name claims
	// due to the fact that each chracater is assigned a trie node regardless of whether it contains
	// any claims or not. In the future, we can switch to a radix tree implementation where empty
	// nodes do not take up any memory and the minimum fee can be priced on a per claim basis.
	MinFeePerNameclaimChar int64 = 200000

	// MaxClaimScriptSize is the max claim script size in bytes, not including the script pubkey part of the script.
	MaxClaimScriptSize = 8192

	// MaxClaimNameSize is the max claim name size in bytes, for all claim trie transactions.
	MaxClaimNameSize = 255
)

var (
	// ErrNotClaimScript is returned when the script does not have a ClaimScript Opcode.
	ErrNotClaimScript = fmt.Errorf("not a claim scrpit")

	// ErrInvalidClaimScript is returned when a script has a ClaimScript Opcode,
	// but does not conform to the format.
	ErrInvalidClaimScript = fmt.Errorf("invalid claim scrpit")
)

// DecodeClaimScript extracts and returns ClaimScript from a public key script.
// ErrNotClaimScript is returned if the public key script is not prefixed by a Claim Script.
func DecodeClaimScript(script []byte) (*ClaimScript, error) {
	op := script[0]
	if op != OP_CLAIMNAME && op != OP_SUPPORTCLAIM && op != OP_UPDATECLAIM {
		return nil, ErrNotClaimScript
	}
	pops, err := parseScript(script)
	if err != nil {
		return nil, err
	}
	if isClaimName(pops) || isSupportClaim(pops) || isUpdateClaim(pops) {
		cs := &ClaimScript{op: op, pops: pops}
		if cs.Size() > MaxClaimScriptSize {
			log.Infof("claim script of %d bytes is larger than %d", cs.Size(), MaxClaimScriptSize)
			return nil, ErrInvalidClaimScript
		}
		return cs, nil
	}
	return nil, ErrInvalidClaimScript
}

// ClaimScript implement Claim Operations for the LBRY Blockchain.
// Currently, it can be one of the following forms.
// OP_CLAIMNAME    <Name> <Value>           OP_2DROP OP_DROP <P2PKH>
// OP_SUPPORTCLAIM <Name> <ClaimID>         OP_2DROP OP_DROP <P2PKH>
// OP_UPDATECLAIM  <Name> <ClaimID> <Value> OP_2DROP OP_2DROP <P2PKH>
type ClaimScript struct {
	op   byte
	pops []parsedOpcode
}

// Opcode returns Opcode of ClaimScript.
func (cs *ClaimScript) Opcode() byte {
	return cs.op
}

// Name returns Name of ClaimScript.
func (cs *ClaimScript) Name() []byte {
	return cs.pops[1].data
}

// ClaimID returns ClaimID of ClaimScript.
func (cs *ClaimScript) ClaimID() []byte {
	if cs.op == OP_CLAIMNAME {
		return nil
	}
	return cs.pops[2].data
}

// Value returns Value of ClaimScript.
func (cs *ClaimScript) Value() []byte {
	if cs.pops[0].opcode.value == OP_CLAIMNAME {
		return cs.pops[2].data
	}
	return cs.pops[3].data
}

// Size returns Size of ClaimScript.
func (cs *ClaimScript) Size() int {
	ops := 5
	if cs.pops[0].opcode.value == OP_UPDATECLAIM {
		ops++
	}
	size := 0
	for _, op := range cs.pops[:ops] {
		if op.opcode.length > 0 {
			size += op.opcode.length
			continue
		}
		size += 1 - op.opcode.length + len(op.data)
	}
	return size
}

// StripClaimScriptPrefix strips ClaimScript, if exists, from a public key script.
func StripClaimScriptPrefix(script []byte) []byte {
	cs, err := DecodeClaimScript(script)
	if err != nil {
		return script
	}
	return script[cs.Size():]
}

// ClaimScriptSize returns size of the claim script minus the script pubkey part.
func ClaimScriptSize(script []byte) int {
	cs, err := DecodeClaimScript(script)
	if err != nil {
		return len(script)
	}
	return cs.Size()
}

// ClaimNameSize returns size of the name in a claim script or 0 if script is not a claimetrie transaction.
func ClaimNameSize(script []byte) int {
	cs, err := DecodeClaimScript(script)
	if err != nil {
		return 0
	}
	return len(cs.Name())
}

// CalcMinClaimTrieFee calculates the minimum fee (mempool rule) required for transaction.
func CalcMinClaimTrieFee(tx *wire.MsgTx, minFeePerNameClaimChar int64) int64 {
	var minFee int64
	for _, txOut := range tx.TxOut {
		minFee += int64(ClaimNameSize(txOut.PkScript))
	}
	return minFee * minFeePerNameClaimChar
}

func isClaimName(pops []parsedOpcode) bool {
	return len(pops) > 5 &&
		pops[0].opcode.value == OP_CLAIMNAME &&
		// canonicalPush(pops[1]) &&
		len(pops[1].data) <= MaxClaimNameSize &&
		// canonicalPush(pops[2]) &&
		pops[3].opcode.value == OP_2DROP &&
		pops[4].opcode.value == OP_DROP
}

func isSupportClaim(pops []parsedOpcode) bool {
	return len(pops) > 5 &&
		pops[0].opcode.value == OP_SUPPORTCLAIM &&
		// canonicalPush(pops[1]) &&
		len(pops[1].data) <= MaxClaimNameSize &&
		// canonicalPush(pops[2]) &&
		len(pops[2].data) == 160/8 &&
		pops[3].opcode.value == OP_2DROP &&
		pops[4].opcode.value == OP_DROP
}

func isUpdateClaim(pops []parsedOpcode) bool {
	return len(pops) > 6 &&
		pops[0].opcode.value == OP_UPDATECLAIM &&
		// canonicalPush(pops[1]) &&
		len(pops[1].data) <= MaxClaimNameSize &&
		// canonicalPush(pops[2]) &&
		len(pops[2].data) == 160/8 &&
		// canonicalPush(pops[3]) &&
		pops[4].opcode.value == OP_2DROP &&
		pops[5].opcode.value == OP_2DROP
}
