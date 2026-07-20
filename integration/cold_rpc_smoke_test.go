// Copyright (c) 2025 The btcd developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package integration

import (
	"strings"
	"testing"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/integration/rpctest"
)

// TestColdRPCSmoke drives a real praxisd regtest node past --witness-buffer
// with txindex+addrindex, then asserts the cold-tier RPC contract:
//   - getblock verbosity=0 refuses cold hex (no witness_excised carrier)
//   - getblock verbosity=1 sets witness_excised
//   - getrawtransaction verbose sets witness_excised for a cold-height tx
//   - searchrawtransactions still resolves the mining address
func TestColdRPCSmoke(t *testing.T) {
	const buffer = 8
	extra := []string{
		"--txindex",
		"--addrindex",
		"--witness-buffer=8",
	}
	r, err := rpctest.New(&chaincfg.RegressionNetParams, nil, extra, "/tmp/praxisd")
	if err != nil {
		t.Fatalf("New harness: %v", err)
	}
	if err := r.SetUp(true, 0); err != nil {
		t.Fatalf("SetUp: %v", err)
	}
	defer r.TearDown()

	// Mine well past the buffer so early blocks age out to cold.
	const tipHeight = 24
	if _, err := r.Client.Generate(tipHeight); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	count, err := r.Client.GetBlockCount()
	if err != nil {
		t.Fatalf("GetBlockCount: %v", err)
	}
	if count != tipHeight {
		t.Fatalf("tip height=%d, want %d", count, tipHeight)
	}

	// Height 1 should be cold: tip - buffer = 16, so ages 1..16 are cold.
	coldHeight := int64(1)
	if tipHeight-buffer < coldHeight {
		t.Fatal("test tip too shallow for cold height 1")
	}
	coldHash, err := r.Client.GetBlockHash(coldHeight)
	if err != nil {
		t.Fatalf("GetBlockHash(%d): %v", coldHeight, err)
	}

	// Tip stays hot.
	tipHash, err := r.Client.GetBestBlockHash()
	if err != nil {
		t.Fatalf("GetBestBlockHash: %v", err)
	}
	tipVerbose, err := r.Client.GetBlockVerbose(tipHash)
	if err != nil {
		t.Fatalf("GetBlockVerbose tip: %v", err)
	}
	if tipVerbose.WitnessExcised {
		t.Fatal("tip unexpectedly witness_excised")
	}

	// getblock verbosity=0 on cold must refuse (stripped hex has no flag).
	_, err = r.Client.GetBlock(coldHash)
	if err == nil {
		t.Fatal("GetBlock (verbosity 0) on cold hash unexpectedly succeeded")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, "excised") && !strings.Contains(errStr, "witness") {
		t.Fatalf("GetBlock cold error missing excision hint: %v", err)
	}
	t.Logf("getblock v0 cold refused: %v", err)

	// getblock verbosity=1 must set witness_excised.
	coldVerbose, err := r.Client.GetBlockVerbose(coldHash)
	if err != nil {
		t.Fatalf("GetBlockVerbose cold: %v", err)
	}
	if !coldVerbose.WitnessExcised {
		t.Fatal("cold getblock v1 missing witness_excised")
	}
	t.Logf("getblock v1 cold: witness_excised=%v size=%d",
		coldVerbose.WitnessExcised, coldVerbose.Size)

	// Coinbase of the cold block via getrawtransaction verbose.
	if len(coldVerbose.Tx) == 0 {
		t.Fatal("cold block has no txids")
	}
	rawTx, err := r.Client.GetRawTransactionVerbose(mustHash(t, coldVerbose.Tx[0]))
	if err != nil {
		t.Fatalf("GetRawTransactionVerbose: %v", err)
	}
	if !rawTx.WitnessExcised {
		t.Fatal("cold getrawtransaction missing witness_excised")
	}
	t.Logf("getrawtransaction cold coinbase: witness_excised=%v", rawTx.WitnessExcised)

	// searchrawtransactions for the mining address must still return results.
	addr := r.MiningAddr()
	results, err := r.Client.SearchRawTransactionsVerbose(addr, 0, 10, true, false, nil)
	if err != nil {
		t.Fatalf("SearchRawTransactionsVerbose: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("searchrawtransactions returned no results for mining addr")
	}
	foundExcised := false
	for _, res := range results {
		if res.WitnessExcised {
			foundExcised = true
			break
		}
	}
	if !foundExcised {
		t.Fatal("expected at least one searchrawtransactions hit with witness_excised")
	}
	t.Logf("searchrawtransactions: %d hits, witnessed excised among them", len(results))
}

func mustHash(t *testing.T, s string) *chainhash.Hash {
	t.Helper()
	h, err := chainhash.NewHashFromStr(s)
	if err != nil {
		t.Fatalf("NewHashFromStr(%q): %v", s, err)
	}
	return h
}
