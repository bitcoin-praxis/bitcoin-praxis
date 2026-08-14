// Copyright (c) 2025 The btcd developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"testing"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

func TestShouldParallelValidate(t *testing.T) {
	// Build blocks with a controlled non-coinbase input count so the adaptive
	// selector's cost estimate can be exercised.
	light := mkBlockWithInputs(0)
	heavy := mkBlockWithInputs(parallelValidateMinSigs)
	under := mkBlockWithInputs(parallelValidateMinSigs - 1)

	cases := []struct {
		name  string
		ibd   bool
		flags blockchain.BehaviorFlags
		block *btcutil.Block
		want  bool
	}{
		{"ibd light (serial verify)", true, blockchain.BFNone, light, false},
		{"ibd just-under threshold", true, blockchain.BFNone, under, false},
		{"ibd at threshold (parallel)", true, blockchain.BFNone, heavy, true},
		{"ibd fastadd skips", true, blockchain.BFFastAdd, heavy, false},
		{"non-ibd serial", false, blockchain.BFNone, heavy, false},
		{"non-ibd fastadd", false, blockchain.BFFastAdd, heavy, false},
	}
	for _, c := range cases {
		if got := shouldParallelValidate(c.ibd, c.flags, c.block); got != c.want {
			t.Fatalf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestShouldEnqueueIBD(t *testing.T) {
	cases := []struct {
		name  string
		ibd   bool
		flags blockchain.BehaviorFlags
		want  bool
	}{
		{"ibd full-validation", true, blockchain.BFNone, true},
		{"ibd light still enqueued", true, blockchain.BFNone, true},
		{"ibd fastadd not enqueued", true, blockchain.BFFastAdd, false},
		{"tip-sync not enqueued", false, blockchain.BFNone, false},
	}
	for _, c := range cases {
		if got := shouldEnqueueIBD(c.ibd, c.flags); got != c.want {
			t.Fatalf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

// mkBlockWithInputs builds a block whose non-coinbase transactions have the
// given total number of inputs. Only the input count matters for the
// adaptive selector's cost estimate, so the scripts/outputs are dummy.
func mkBlockWithInputs(nonCoinbaseInputs int) *btcutil.Block {
	cb := wire.NewMsgTx(wire.TxVersion)
	cb.AddTxIn(wire.NewTxIn(&wire.OutPoint{Index: wire.MaxPrevOutIndex}, nil, nil))
	cb.AddTxOut(wire.NewTxOut(0, nil))

	txs := []*wire.MsgTx{cb}
	if nonCoinbaseInputs > 0 {
		tx := wire.NewMsgTx(wire.TxVersion)
		for i := 0; i < nonCoinbaseInputs; i++ {
			tx.AddTxIn(wire.NewTxIn(&wire.OutPoint{Index: uint32(i)}, nil, nil))
		}
		tx.AddTxOut(wire.NewTxOut(0, nil))
		txs = append(txs, tx)
	}
	msgBlock := &wire.MsgBlock{Transactions: txs}
	return btcutil.NewBlock(msgBlock)
}

func TestParallelValidatePoolSize(t *testing.T) {
	n := parallelValidatePoolSize()
	if n < 2 || n > maxParallelValidateWindow {
		t.Fatalf("pool size %d out of range [2,%d]", n, maxParallelValidateWindow)
	}
}

func TestScriptWorkersPerBlockFullBudgetPerBlock(t *testing.T) {
	// Multi-block windows must keep the full serial budget per block so
	// aggregate script work can fill idle cores (sharing one NumCPU*3 pie
	// matched serial and left ~⅓ of an 8-core box idle on dense IBD).
	if got := scriptWorkersPerBlock(8, 8); got != 24 {
		t.Fatalf("8 cpu / batch 8: got %d want 24", got)
	}
	if got := scriptWorkersPerBlock(8, 1); got != 24 {
		t.Fatalf("8 cpu / batch 1: got %d want 24", got)
	}
	if got := scriptWorkersPerBlock(4, 8); got != 12 {
		t.Fatalf("4 cpu / batch 8: got %d want 12", got)
	}
	if got := scriptWorkersPerBlock(16, 8); got != 48 {
		t.Fatalf("16 cpu / batch 8: got %d want 48", got)
	}
}

func TestFindPendingExtendingOOO(t *testing.T) {
	sm := &SyncManager{
		pendingValidate: make(map[chainhash.Hash]*pendingIBDBlock),
	}

	var parent, child, unrelated chainhash.Hash
	parent[0] = 1
	child[0] = 2
	unrelated[0] = 9

	mk := func(prev chainhash.Hash) *pendingIBDBlock {
		msg := wire.NewMsgBlock(&wire.BlockHeader{PrevBlock: prev})
		block := btcutil.NewBlock(msg)
		return &pendingIBDBlock{bmsg: &blockMsg{block: block}}
	}

	childPending := mk(parent)
	sm.pendingValidate[*childPending.bmsg.block.Hash()] = childPending
	unrelPending := mk(unrelated)
	sm.pendingValidate[*unrelPending.bmsg.block.Hash()] = unrelPending

	got := sm.findPendingExtending(parent)
	if got == nil {
		t.Fatal("expected pending block extending parent")
	}
	if got.bmsg.block.MsgBlock().Header.PrevBlock != parent {
		t.Fatalf("wrong prev hash")
	}
	if sm.findPendingExtending(child) != nil {
		t.Fatal("no block should extend child hash")
	}
}
