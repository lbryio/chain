// Copyright (c) 2014-2017 The btcsuite developers
// Copyright (c) 2015-2017 The Decred developers
// Copyright (c) 2018-2018 The LBRY developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package btcjson

// GetClaimsInTrieCmd defines the getclaimsintrie JSON-RPC command.
type GetClaimsInTrieCmd struct{}

// NewGetClaimsInTrieCmd returns a new instance which can be used to issue a getclaimsintrie JSON-RPC command.
func NewGetClaimsInTrieCmd() *GetClaimsInTrieCmd {
	return &GetClaimsInTrieCmd{}
}

// GetClaimTrieCmd defines the getclaimtrie JSON-RPC command.
type GetClaimTrieCmd struct{}

// NewGetClaimTrieCmd returns a new instance which can be used to issue a getclaimtrie JSON-RPC command.
func NewGetClaimTrieCmd() *GetClaimTrieCmd {
	return &GetClaimTrieCmd{}
}

// GetValueForNameCmd defines the getvalueforname JSON-RPC command.
type GetValueForNameCmd struct {
	Name string
}

// NewGetValueForNameCmd returns a new instance which can be used to issue a getvalueforname JSON-RPC command.
func NewGetValueForNameCmd() *GetValueForNameCmd {
	return &GetValueForNameCmd{}
}

// GetClaimsForNameCmd defines the getclaimsforname JSON-RPC command.
type GetClaimsForNameCmd struct {
	Name string
}

// NewGetClaimsForNameCmd returns a new instance which can be used to issue a getclaimsforname JSON-RPC command.
func NewGetClaimsForNameCmd() *GetClaimsForNameCmd {
	return &GetClaimsForNameCmd{}
}

// GetTotalClaimedNamesCmd defines the gettotalclaimednames JSON-RPC command.
type GetTotalClaimedNamesCmd struct{}

// NewGetTotalClaimedNamesCmd returns a new instance which can be used to issue a gettotalclaimednames JSON-RPC command.
func NewGetTotalClaimedNamesCmd() *GetTotalClaimedNamesCmd {
	return &GetTotalClaimedNamesCmd{}
}

// GetTotalClaimsCmd defines the gettotalclaims JSON-RPC command.
type GetTotalClaimsCmd struct{}

// NewGetTotalClaimsCmd returns a new instance which can be used to issue a gettotalclaims JSON-RPC command.
func NewGetTotalClaimsCmd() *GetTotalClaimsCmd {
	return &GetTotalClaimsCmd{}
}

// GetTotalValueOfClaimsCmd defines the gettotalvalueofclaims JSON-RPC command.
type GetTotalValueOfClaimsCmd struct {
	ControllingOnly bool `json:"controlling_only"`
}

// NewGetTotalValueOfClaimsCmd returns a new instance which can be used to issue a gettotalvalueofclaims JSON-RPC command.
func NewGetTotalValueOfClaimsCmd() *GetTotalValueOfClaimsCmd {
	return &GetTotalValueOfClaimsCmd{}
}

// GetClaimsForTxCmd defines the getclaimsfortx JSON-RPC command.
type GetClaimsForTxCmd struct {
	TxID string
}

// NewGetClaimsForTxCmd returns a new instance which can be used to issue a getclaimsfortx JSON-RPC command.
func NewGetClaimsForTxCmd() *GetClaimsForTxCmd {
	return &GetClaimsForTxCmd{}
}

// GetNameProofCmd defines the getnameproof JSON-RPC command.
type GetNameProofCmd struct {
	BlockHash string
}

// NewGetNameProofCmd returns a new instance which can be used to issue a getnameproof JSON-RPC command.
func NewGetNameProofCmd() *GetNameProofCmd {
	return &GetNameProofCmd{}
}

// GetClaimByIDCmd defines the getclaimbyid JSON-RPC command.
type GetClaimByIDCmd struct {
	ID string
}

// NewGetClaimByIDCmd returns a new instance which can be used to issue a getclaimbyid JSON-RPC command.
func NewGetClaimByIDCmd() *GetClaimByIDCmd {
	return &GetClaimByIDCmd{}
}

func init() {
	// No special flags for commands in this file.
	flags := UsageFlag(0)

	MustRegisterCmd("getclaimsintrie", (*GetClaimsInTrieCmd)(nil), flags)
	MustRegisterCmd("getclaimtrie", (*GetClaimTrieCmd)(nil), flags)
	MustRegisterCmd("getvalueforname", (*GetValueForNameCmd)(nil), flags)
	MustRegisterCmd("getclaimsforname", (*GetClaimsForNameCmd)(nil), flags)
	MustRegisterCmd("gettotalclaimednames", (*GetTotalClaimedNamesCmd)(nil), flags)
	MustRegisterCmd("gettotalclaims", (*GetTotalClaimsCmd)(nil), flags)
	MustRegisterCmd("gettotalvalueofclaims", (*GetTotalValueOfClaimsCmd)(nil), flags)
	MustRegisterCmd("getclaimsfortx", (*GetClaimsForTxCmd)(nil), flags)
	MustRegisterCmd("getnameproof", (*GetNameProofCmd)(nil), flags)
	MustRegisterCmd("getclaimbyid", (*GetClaimByIDCmd)(nil), flags)
}
