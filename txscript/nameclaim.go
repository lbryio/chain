package txscript

import (
	"bytes"
	"fmt"
	"unicode/utf8"

	"github.com/lbryio/lbcd/wire"
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
	ErrNotClaimScript = fmt.Errorf("not a claim script")

	// ErrInvalidClaimScript is returned when a script has a ClaimScript Opcode,
	// but does not conform to the format.
	ErrInvalidClaimScript = fmt.Errorf("invalid claim script")
)

// ClaimNameScript ...
func ClaimNameScript(name string, value string) ([]byte, error) {
	return NewScriptBuilder().AddOp(OP_CLAIMNAME).AddData([]byte(name)).AddData([]byte(value)).
		AddOp(OP_2DROP).AddOp(OP_DROP).AddOp(OP_TRUE).Script()
}

// SupportClaimScript ...
func SupportClaimScript(name string, claimID []byte, value []byte) ([]byte, error) {
	builder := NewScriptBuilder().AddOp(OP_SUPPORTCLAIM).AddData([]byte(name)).AddData(claimID)
	if len(value) > 0 {
		return builder.addData(value).AddOp(OP_2DROP).AddOp(OP_2DROP).AddOp(OP_TRUE).Script()
	}
	return builder.AddOp(OP_2DROP).AddOp(OP_DROP).AddOp(OP_TRUE).Script()
}

// UpdateClaimScript ...
func UpdateClaimScript(name string, claimID []byte, value string) ([]byte, error) {
	return NewScriptBuilder().AddOp(OP_UPDATECLAIM).AddData([]byte(name)).AddData(claimID).AddData([]byte(value)).
		AddOp(OP_2DROP).AddOp(OP_2DROP).AddOp(OP_TRUE).Script()
}

// DecodeClaimScript ...
func DecodeClaimScript(script []byte) (*ClaimScript, error) {
	if len(script) == 0 {
		return nil, ErrNotClaimScript
	}
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

// ClaimScript ...
// OP_CLAIMNAME    <Name> <Value>           OP_2DROP OP_DROP <P2PKH>
// OP_SUPPORTCLAIM <Name> <ClaimID>         OP_2DROP OP_DROP <P2PKH>
// OP_UPDATECLAIM  <Name> <ClaimID> <Value> OP_2DROP OP_2DROP <P2PKH>
type ClaimScript struct {
	op   byte
	pops []parsedOpcode
}

// Opcode ...
func (cs *ClaimScript) Opcode() byte {
	return cs.op
}

// Name ...
func (cs *ClaimScript) Name() []byte {
	return cs.pops[1].data
}

// ClaimID ...
func (cs *ClaimScript) ClaimID() []byte {
	if cs.op == OP_CLAIMNAME {
		return nil
	}
	return cs.pops[2].data
}

// Value ...
func (cs *ClaimScript) Value() []byte {
	if cs.pops[0].opcode.value == OP_CLAIMNAME {
		return cs.pops[2].data
	}
	return cs.pops[3].data
}

// Size ...
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

// StripClaimScriptPrefix ...
func StripClaimScriptPrefix(script []byte) []byte {
	cs, err := DecodeClaimScript(script)
	if err != nil {
		return script
	}
	return script[cs.Size():]
}

// claimNameSize returns size of the name in a claim script or 0 if script is not a claimtrie transaction.
func claimNameSize(script []byte) int {
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
		// TODO maybe: lbrycrd ignored transactions that weren't OP_CLAIMNAME
		minFee += int64(claimNameSize(txOut.PkScript))
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
	prefixed := len(pops) > 5 &&
		pops[0].opcode.value == OP_SUPPORTCLAIM &&
		// canonicalPush(pops[1]) &&
		len(pops[1].data) <= MaxClaimNameSize &&
		// canonicalPush(pops[2]) &&
		len(pops[2].data) == 160/8

	if prefixed && pops[3].opcode.value == OP_2DROP && pops[4].opcode.value == OP_DROP {
		return true
	}
	if prefixed && pops[4].opcode.value == OP_2DROP && pops[5].opcode.value == OP_2DROP {
		return len(pops[3].data) > 0 // is this robust enough?
	}
	return false
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

const illegalChars = "=&#:*$%?/;\\\b\n\t\r\x00"

func AllClaimsAreSane(script []byte, enforceSoftFork bool) error {
	cs, err := DecodeClaimScript(script)
	if err != ErrNotClaimScript {
		if err != nil {
			return fmt.Errorf("invalid claim script: %s", err.Error())
		}
		if cs.Size() > MaxClaimScriptSize {
			return fmt.Errorf("claimscript exceeds max size of %v", MaxClaimScriptSize)
		}
		if len(cs.Name()) > MaxClaimNameSize {
			return fmt.Errorf("claim name exceeds max size of %v", MaxClaimNameSize)
		}
		if enforceSoftFork {
			if !utf8.Valid(cs.Name()) {
				return fmt.Errorf("claim name is not valid UTF-8")
			}
			if bytes.ContainsAny(cs.Name(), illegalChars) {
				return fmt.Errorf("claim name has illegal chars; it should not contain any of these: %s", illegalChars)
			}
		}
	}
	return nil
}
