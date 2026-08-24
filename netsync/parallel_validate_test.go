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

func TestLightThenHeavyFlushOrder(t *testing.T) {
	light := &pendingIBDBlock{bmsg: &blockMsg{block: mkBlockWithInputs(0)}}
	heavy := &pendingIBDBlock{bmsg: &blockMsg{block: mkBlockWithInputs(parallelValidateMinSigs)}}
	heavy2 := &pendingIBDBlock{bmsg: &blockMsg{block: mkBlockWithInputs(parallelValidateMinSigs + 8)}}

	if !shouldEnqueueIBD(true, blockchain.BFNone) {
		t.Fatal("non-fast-add IBD bodies must enqueue")
	}

	// Heavy H+2 arrived first and is queued behind light H+1. Flush must
	// serial-commit the light tip-extender and leave the heavy for the
	// next iteration — not drop it or skip the light.
	kind, batch := nextIBDFlush(true, []*pendingIBDBlock{light, heavy, heavy2}, 32)
	if kind != ibdFlushSerial || len(batch) != 1 || batch[0] != light {
		t.Fatalf("light-then-heavy: want serial light, got kind=%d n=%d", kind, len(batch))
	}
	kind, batch = nextIBDFlush(true, []*pendingIBDBlock{heavy, heavy2}, 32)
	if kind != ibdFlushPipeline || len(batch) != 2 || batch[0] != heavy || batch[1] != heavy2 {
		t.Fatalf("after light: want pipeline [heavy,heavy2], got kind=%d n=%d", kind, len(batch))
	}

	// Dense run stops before a later light so that light is not pulled into
	// the pipeline window.
	kind, batch = nextIBDFlush(true, []*pendingIBDBlock{heavy, light, heavy2}, 32)
	if kind != ibdFlushPipeline || len(batch) != 1 || batch[0] != heavy {
		t.Fatalf("heavy-then-light: want pipeline [heavy], got kind=%d n=%d", kind, len(batch))
	}

	kind, batch = nextIBDFlush(true, nil, 32)
	if kind != ibdFlushNone || batch != nil {
		t.Fatalf("empty: want none, got kind=%d n=%d", kind, len(batch))
	}
}

func TestPendingKeepRangeIncludesReorgPrefix(t *testing.T) {
	// Linear IBD: fork is the current tip; keep from tip+1.
	start, maxHeight := pendingKeepRange(15, 30, 8)
	if start != 16 || maxHeight != 23 {
		t.Fatalf("linear: start=%d max=%d, want 16,23", start, maxHeight)
	}

	// Reorg: shorter tip at 15, longer headers at 30, fork at 10.
	// Bodies 11-15 are at or below tip and must stay queued.
	start, maxHeight = pendingKeepRange(10, 30, 224)
	if start != 11 {
		t.Fatalf("reorg start=%d, want 11 (fork+1, not tip+1)", start)
	}
	if maxHeight != 30 {
		t.Fatalf("reorg max=%d, want 30", maxHeight)
	}

	start, maxHeight = pendingKeepRange(0, 100, 10)
	if start != 1 || maxHeight != 10 {
		t.Fatalf("genesis: start=%d max=%d, want 1,10", start, maxHeight)
	}

	start, maxHeight = pendingKeepRange(30, 30, 224)
	if start <= 30 && start <= maxHeight {
		t.Fatalf("caught up: start=%d max=%d, want empty range", start, maxHeight)
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
