// Copyright (c) 2025 The btcd developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// TestSoftConnectAndVerifyExtendsTip soft-connects a generated block onto the
// tip view, verifies scripts, then commits with BFNoScriptCheck.
func TestSoftConnectAndVerifyExtendsTip(t *testing.T) {
	chain, tearDown, err := chainSetup("softconnect", &chaincfg.MainNetParams)
	if err != nil {
		t.Fatalf("chainSetup: %v", err)
	}
	defer tearDown()

	blocks, err := loadBlocks("blk_0_to_14131.dat")
	if err != nil {
		t.Fatalf("loadBlocks: %v", err)
	}
	const warm = 50
	if len(blocks) <= warm+1 {
		t.Fatalf("need >%d blocks, got %d", warm+1, len(blocks))
	}
	for i := 1; i <= warm; i++ {
		_, _, err := chain.ProcessBlock(blocks[i], BFNone)
		if err != nil {
			t.Fatalf("ProcessBlock[%d]: %v", i, err)
		}
	}

	next := blocks[warm+1]
	view := chain.NewTipUtxoView()
	res, err := chain.SoftConnectNext(next, view)
	if err != nil {
		t.Fatalf("SoftConnectNext: %v", err)
	}
	if res.Height != int32(warm+1) {
		t.Fatalf("height=%d, want %d", res.Height, warm+1)
	}
	if err := chain.VerifyBlockScripts(res); err != nil {
		t.Fatalf("VerifyBlockScripts: %v", err)
	}

	isMain, isOrphan, err := chain.ProcessBlock(next, BFNoScriptCheck)
	if err != nil {
		t.Fatalf("ProcessBlock BFNoScriptCheck: %v", err)
	}
	if !isMain || isOrphan {
		t.Fatalf("isMain=%v isOrphan=%v", isMain, isOrphan)
	}
	if tip := chain.BestSnapshot(); tip.Height != int32(warm+1) {
		t.Fatalf("tip height=%d, want %d", tip.Height, warm+1)
	}
}

// TestSoftConnectWindowParallelScripts soft-connects several tip extensions on
// one view, verifies scripts concurrently, then commits with BFFastAdd.
func TestSoftConnectWindowParallelScripts(t *testing.T) {
	chain, tearDown, err := chainSetup("softwindow", &chaincfg.MainNetParams)
	if err != nil {
		t.Fatalf("chainSetup: %v", err)
	}
	defer tearDown()

	blocks, err := loadBlocks("blk_0_to_14131.dat")
	if err != nil {
		t.Fatalf("loadBlocks: %v", err)
	}
	const warm = 40
	const window = 4
	if len(blocks) <= warm+window {
		t.Fatalf("need >%d blocks", warm+window)
	}
	for i := 1; i <= warm; i++ {
		if _, _, err := chain.ProcessBlock(blocks[i], BFNone); err != nil {
			t.Fatalf("ProcessBlock[%d]: %v", i, err)
		}
	}

	view := chain.NewTipUtxoView()
	results := make([]*SoftConnectResult, window)
	var prev *SoftConnectResult
	for i := 0; i < window; i++ {
		res, err := chain.SoftConnectNextAfter(blocks[warm+1+i], view, prev)
		if err != nil {
			t.Fatalf("SoftConnectNextAfter[%d]: %v", i, err)
		}
		results[i] = res
		prev = res
	}

	SetScriptCheckWorkers(1)
	defer SetScriptCheckWorkers(0)

	errCh := make(chan error, window)
	for _, res := range results {
		go func(res *SoftConnectResult) {
			errCh <- chain.VerifyBlockScripts(res)
		}(res)
	}
	for i := 0; i < window; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("VerifyBlockScripts: %v", err)
		}
	}

	for i := 0; i < window; i++ {
		isMain, isOrphan, err := chain.ProcessBlock(blocks[warm+1+i], BFFastAdd)
		if err != nil {
			t.Fatalf("ProcessBlock BFFastAdd[%d]: %v", i, err)
		}
		if !isMain || isOrphan {
			t.Fatalf("commit[%d]: isMain=%v isOrphan=%v", i, isMain, isOrphan)
		}
	}
	wantHeight := int32(warm + window)
	if tip := chain.BestSnapshot(); tip.Height != wantHeight {
		t.Fatalf("tip height=%d, want %d", tip.Height, wantHeight)
	}
}

// TestUtxoViewpointCloneIsolatesMutations ensures Clone does not share
// UtxoEntry pointers with the source view.
func TestUtxoViewpointCloneIsolatesMutations(t *testing.T) {
	view := NewUtxoViewpoint()
	op := wire.OutPoint{Hash: chainhash.Hash{0xab}, Index: 0}
	view.entries[op] = &UtxoEntry{
		amount:      42,
		pkScript:    []byte{0x51},
		blockHeight: 7,
		packedFlags: tfModified,
	}
	snap := view.Clone()
	live := view.LookupEntry(op)
	cloned := snap.LookupEntry(op)
	if live == nil || cloned == nil {
		t.Fatal("missing entries")
	}
	if live == cloned {
		t.Fatal("clone shared UtxoEntry pointer with live view")
	}
	live.Spend()
	if cloned.IsSpent() {
		t.Fatal("spending live entry mutated clone")
	}
	if live.Amount() != cloned.Amount() || string(live.PkScript()) != string(cloned.PkScript()) {
		t.Fatal("clone did not copy amount/script")
	}
}

// TestVerifySnapshotIsolatesFromLiveView locks in the pipelined-batch safety
// property: VerifyBlockScripts reads res.VerifyView, a minimal snapshot whose
// entries do not share UtxoEntry pointers with the shared soft-connect View.
// A later block's soft-connect may mutate/spend entries in View while the
// previous block's scripts verify concurrently; the snapshot must be immune.
func TestVerifySnapshotIsolatesFromLiveView(t *testing.T) {
	chain, tearDown, err := chainSetup("verifysnap", &chaincfg.MainNetParams)
	if err != nil {
		t.Fatalf("chainSetup: %v", err)
	}
	defer tearDown()

	blocks, err := loadBlocks("blk_0_to_14131.dat")
	if err != nil {
		t.Fatalf("loadBlocks: %v", err)
	}
	const warm = 50
	for i := 1; i <= warm; i++ {
		if _, _, err := chain.ProcessBlock(blocks[i], BFNone); err != nil {
			t.Fatalf("ProcessBlock[%d]: %v", i, err)
		}
	}

	next := blocks[warm+1]
	view := chain.NewTipUtxoView()
	res, err := chain.SoftConnectNext(next, view)
	if err != nil {
		t.Fatalf("SoftConnectNext: %v", err)
	}
	if res.VerifyView == nil {
		t.Fatal("SoftConnectResult.VerifyView is nil; pipeline snapshot not built")
	}
	if res.VerifyView == res.View {
		t.Fatal("VerifyView aliases View; not isolated")
	}

	// Every input the block verifies must resolve in the snapshot, and via a
	// distinct (cloned) entry pointer from the live view.
	for _, tx := range next.Transactions() {
		for _, txIn := range tx.MsgTx().TxIn {
			if txIn.PreviousOutPoint.Index == wire.MaxPrevOutIndex {
				continue
			}
			op := txIn.PreviousOutPoint
			live := view.LookupEntry(op)
			snap := res.VerifyView.LookupEntry(op)
			if live == nil {
				continue // not a tip-cached input (filled by soft-connect fetch)
			}
			if snap == nil {
				t.Fatalf("snapshot missing input %v that live view has", op)
			}
			if live == snap {
				t.Fatalf("snapshot shares UtxoEntry pointer with live view for %v", op)
			}
		}
	}

	// Simulate the next block's soft-connect spending/mutating live entries.
	for op, entry := range view.entries {
		if entry != nil {
			entry.Spend()
			_ = op
		}
	}
	// Snapshot entries must be unaffected.
	for op, snap := range res.VerifyView.entries {
		if snap != nil && snap.IsSpent() {
			t.Fatalf("snapshot entry %v mutated by live view Spend()", op)
		}
	}

	// VerifyBlockScripts must succeed against the isolated snapshot.
	if err := chain.VerifyBlockScripts(res); err != nil {
		t.Fatalf("VerifyBlockScripts via snapshot: %v", err)
	}
}

// syntheticChainedSpendBlock builds a block whose second non-coinbase tx spends
// the first — the pattern that makes findInputsToFetch call AddTxOuts.
func syntheticChainedSpendBlock() *btcutil.Block {
	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: *wire.NewOutPoint(&chainhash.Hash{}, wire.MaxPrevOutIndex),
		Sequence:         wire.MaxTxInSequenceNum,
		SignatureScript:  []byte{0x01, 0x00},
	})
	coinbase.AddTxOut(wire.NewTxOut(50*1e8, []byte{0x51}))

	txA := wire.NewMsgTx(1)
	txA.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{1}, Index: 0},
		Sequence:         wire.MaxTxInSequenceNum,
		SignatureScript:  []byte{0x51},
	})
	txA.AddTxOut(wire.NewTxOut(40*1e8, []byte{0x51}))

	txB := wire.NewMsgTx(1)
	txB.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: txA.TxHash(), Index: 0},
		Sequence:         wire.MaxTxInSequenceNum,
		SignatureScript:  []byte{0x51},
	})
	txB.AddTxOut(wire.NewTxOut(39*1e8, []byte{0x51}))

	msg := wire.NewMsgBlock(&wire.BlockHeader{Version: 1})
	msg.AddTransaction(coinbase)
	msg.AddTransaction(txA)
	msg.AddTransaction(txB)
	return btcutil.NewBlock(msg)
}

// TestFindInputsToFetchPoisonsUnsetHeight documents the Prefetch footgun:
// findInputsToFetch mutates the view with AddTxOuts at block.Height(), which is
// -1 for pending IBD blocks. Those entries then trip BIP30.
func TestFindInputsToFetchPoisonsUnsetHeight(t *testing.T) {
	block := syntheticChainedSpendBlock()
	if block.Height() != btcutil.BlockHeightUnknown {
		t.Fatalf("Height()=%d, want %d", block.Height(), btcutil.BlockHeightUnknown)
	}

	view := NewUtxoViewpoint()
	_ = view.findInputsToFetch(block)

	txAHash := block.Transactions()[1].Hash()
	op := wire.OutPoint{Hash: *txAHash, Index: 0}
	entry := view.LookupEntry(op)
	if entry == nil {
		t.Fatal("expected findInputsToFetch to AddTxOuts in-flight txA")
	}
	if entry.BlockHeight() != btcutil.BlockHeightUnknown {
		t.Fatalf("poison height=%d, want %d", entry.BlockHeight(), btcutil.BlockHeightUnknown)
	}
}

// TestCollectPrefetchOutpointsSkipsWindowCreated ensures Prefetch never asks
// for outs produced inside the soft-connect window (same-block or prior block
// in the batch) — the inputs that findInputsToFetch would AddTxOuts.
func TestCollectPrefetchOutpointsSkipsWindowCreated(t *testing.T) {
	block := syntheticChainedSpendBlock()
	txAHash := *block.Transactions()[1].Hash()
	external := wire.OutPoint{Hash: chainhash.Hash{1}, Index: 0}

	needed := collectPrefetchOutpoints(NewUtxoViewpoint(), []*btcutil.Block{block})
	for _, op := range needed {
		if op.Hash == txAHash {
			t.Fatalf("collected in-flight outpoint %v", op)
		}
	}
	foundExternal := false
	for _, op := range needed {
		if op == external {
			foundExternal = true
		}
	}
	if !foundExternal {
		t.Fatalf("expected external tip outpoint %v in needed=%v", external, needed)
	}
}

// TestPrefetchBlocksInputsNoBIP30HeightPoison reproduces the IBD Prefetch bug
// against mainnet block 546 (same-block chained spends in testdata):
//
//  1. findInputsToFetch on Height()-unset blocks inserts utxos at height -1
//  2. SoftConnect then fails BIP30 ("overwrite ... at block height -1")
//  3. PrefetchBlocksInputs must not poison, and SoftConnect must succeed
func TestPrefetchBlocksInputsNoBIP30HeightPoison(t *testing.T) {
	chain, tearDown, err := chainSetup("prefetchpoison", &chaincfg.MainNetParams)
	if err != nil {
		t.Fatalf("chainSetup: %v", err)
	}
	defer tearDown()

	blocks, err := loadBlocks("blk_0_to_14131.dat")
	if err != nil {
		t.Fatalf("loadBlocks: %v", err)
	}
	// Block index 546 in blk_0_to_14131.dat has same-block spends that trigger
	// findInputsToFetch → AddTxOuts(..., Height()).
	const chainedIdx = 546
	const warm = chainedIdx - 1
	const window = 4
	if len(blocks) <= warm+window {
		t.Fatalf("need >%d blocks", warm+window)
	}
	for i := 1; i <= warm; i++ {
		if _, _, err := chain.ProcessBlock(blocks[i], BFNone); err != nil {
			t.Fatalf("ProcessBlock[%d]: %v", i, err)
		}
	}

	chained := blocks[chainedIdx]
	if chained.Height() != btcutil.BlockHeightUnknown {
		t.Fatalf("chained Height()=%d, want unset (-1)", chained.Height())
	}

	// --- buggy path: Prefetch-via-findInputsToFetch poisons SoftConnect ---
	buggyView := chain.NewTipUtxoView()
	_ = buggyView.findInputsToFetch(chained)
	poisoned := false
	for op, e := range buggyView.entries {
		if e != nil && e.BlockHeight() == btcutil.BlockHeightUnknown {
			poisoned = true
			t.Logf("buggy path poisoned %v at height -1", op)
			break
		}
	}
	if !poisoned {
		t.Fatal("expected findInputsToFetch to poison view on block 546")
	}
	chained.SetHeight(btcutil.BlockHeightUnknown)
	_, err = chain.SoftConnectNext(chained, buggyView)
	if err == nil || !isOverwriteHeightUnknown(err) {
		t.Fatalf("buggy SoftConnect(546): want BIP30 height-(-1), got %v", err)
	}
	t.Logf("buggy SoftConnect(546) failed as expected: %v", err)

	// --- fixed path: PrefetchBlocksInputs must not poison ---
	batch := blocks[warm+1 : warm+1+window]
	for _, b := range batch {
		b.SetHeight(btcutil.BlockHeightUnknown)
	}
	view := chain.NewTipUtxoView()
	if err := chain.PrefetchBlocksInputs(view, batch); err != nil {
		t.Fatalf("PrefetchBlocksInputs: %v", err)
	}
	assertViewHasNoUnknownHeight(t, view)
	assertViewHasNoPendingTxOutputs(t, view, batch)

	var prev *SoftConnectResult
	for i, b := range batch {
		b.SetHeight(btcutil.BlockHeightUnknown)
		res, err := chain.SoftConnectNextAfter(b, view, prev)
		if err != nil {
			t.Fatalf("SoftConnectNextAfter[%d] after Prefetch: %v", i, err)
		}
		prev = res
	}
}

func isOverwriteHeightUnknown(err error) bool {
	re, ok := err.(RuleError)
	if !ok {
		return false
	}
	return re.ErrorCode == ErrOverwriteTx &&
		strings.Contains(re.Description, "height -1")
}

// TestPrefetchDoesNotChangeSoftConnectOutcome checks Prefetch is consensus-
// neutral: soft-connecting the same window with and without Prefetch both
// succeed and produce the same connected tip height on the view.
func TestPrefetchDoesNotChangeSoftConnectOutcome(t *testing.T) {
	chain, tearDown, err := chainSetup("prefetchneutral", &chaincfg.MainNetParams)
	if err != nil {
		t.Fatalf("chainSetup: %v", err)
	}
	defer tearDown()

	blocks, err := loadBlocks("blk_0_to_14131.dat")
	if err != nil {
		t.Fatalf("loadBlocks: %v", err)
	}
	const warm = 50
	const window = 6
	if len(blocks) <= warm+window {
		t.Fatalf("need >%d blocks", warm+window)
	}
	for i := 1; i <= warm; i++ {
		if _, _, err := chain.ProcessBlock(blocks[i], BFNone); err != nil {
			t.Fatalf("ProcessBlock[%d]: %v", i, err)
		}
	}
	batch := blocks[warm+1 : warm+1+window]

	softConnectWindow := func(prefetch bool) *chainhash.Hash {
		view := chain.NewTipUtxoView()
		if prefetch {
			if err := chain.PrefetchBlocksInputs(view, batch); err != nil {
				t.Fatalf("PrefetchBlocksInputs: %v", err)
			}
		}
		var prev *SoftConnectResult
		for i, b := range batch {
			// SoftConnectNextAfter SetHeight on the shared block objects;
			// reset so both runs start from HeightUnknown like IBD.
			b.SetHeight(btcutil.BlockHeightUnknown)
			res, err := chain.SoftConnectNextAfter(b, view, prev)
			if err != nil {
				t.Fatalf("prefetch=%v SoftConnect[%d]: %v", prefetch, i, err)
			}
			prev = res
		}
		h := view.BestHash()
		out := *h
		return &out
	}

	without := softConnectWindow(false)
	with := softConnectWindow(true)
	if !without.IsEqual(with) {
		t.Fatalf("view best hash diverged: without=%v with=%v", without, with)
	}
}

func assertViewHasNoUnknownHeight(t *testing.T, view *UtxoViewpoint) {
	t.Helper()
	for op, e := range view.entries {
		if e != nil && e.BlockHeight() == btcutil.BlockHeightUnknown {
			t.Fatalf("view has height-(-1) entry at %v (BIP30 poison)", op)
		}
	}
}

func assertViewHasNoPendingTxOutputs(t *testing.T, view *UtxoViewpoint, blocks []*btcutil.Block) {
	t.Helper()
	pending := make(map[chainhash.Hash]struct{})
	for _, b := range blocks {
		for _, tx := range b.Transactions() {
			pending[*tx.Hash()] = struct{}{}
		}
	}
	for op, e := range view.entries {
		if e == nil {
			continue
		}
		if _, ok := pending[op.Hash]; ok {
			t.Fatalf("Prefetch inserted output of pending-window tx %v "+
				"(findInputsToFetch AddTxOuts pattern)", op)
		}
	}
}

// runParallelPipelineAtChainLayer mirrors netsync.processParallelBatch at the
// blockchain layer: prefetch → soft-connect the window on a fresh tip view →
// verify scripts concurrently → commit each in height order with BFFastAdd.
func runParallelPipelineAtChainLayer(chain *BlockChain, window []*btcutil.Block) error {
	view := chain.NewTipUtxoView()
	if err := chain.PrefetchBlocksInputs(view, window); err != nil {
		return err
	}

	results := make([]*SoftConnectResult, len(window))
	var prev *SoftConnectResult
	for i, b := range window {
		res, err := chain.SoftConnectNextAfter(b, view, prev)
		if err != nil {
			return err
		}
		results[i] = res
		prev = res
	}

	errs := make([]error, len(window))
	var wg sync.WaitGroup
	for i, res := range results {
		wg.Add(1)
		go func(i int, res *SoftConnectResult) {
			defer wg.Done()
			errs[i] = chain.VerifyBlockScripts(res)
		}(i, res)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	for _, b := range window {
		if _, _, err := chain.ProcessBlock(b, BFFastAdd); err != nil {
			return err
		}
	}
	return nil
}

// TestParallelPipelineMatchesSerial is the consensus gate: the parallel
// validation pipeline (soft-connect + VerifyBlockScripts + commit BFFastAdd)
// must accept the same blocks and produce the same UTXO set as serial
// ProcessBlock(BFNone). Any divergence is a consensus bug, not a perf
// regression.
func TestParallelPipelineMatchesSerial(t *testing.T) {
	blocks, err := loadBlocks("blk_0_to_14131.dat")
	if err != nil {
		t.Fatalf("loadBlocks: %v", err)
	}
	const warm = 50
	const window = 8
	if len(blocks) <= warm+window {
		t.Fatalf("need >%d blocks", warm+window)
	}

	// chainSetup's teardown does os.RemoveAll(testDbRoot), which nukes the whole
	// shared testdbs dir. With two chains sharing that dir, the first teardown
	// (LIFO) would delete the other chain's leveldb files out from under it and
	// make the second db.Close() hang. So discard chainSetup's teardowns and
	// remove only each chain's own dbPath, nuking the shared root once both DBs
	// are closed.
	chainSerial, _, err := chainSetup("diffserial", &chaincfg.MainNetParams)
	if err != nil {
		t.Fatalf("chainSetup serial: %v", err)
	}
	defer os.RemoveAll(testDbRoot)
	defer func() {
		chainSerial.db.Close()
		os.RemoveAll(filepath.Join(testDbRoot, "diffserial"))
	}()
	chainParallel, _, err := chainSetup("diffparallel",
		&chaincfg.MainNetParams)
	if err != nil {
		t.Fatalf("chainSetup parallel: %v", err)
	}
	defer func() {
		chainParallel.db.Close()
		os.RemoveAll(filepath.Join(testDbRoot, "diffparallel"))
	}()

	// Identical serial warm-up on both chains.
	for i := 1; i <= warm; i++ {
		if _, _, err := chainSerial.ProcessBlock(blocks[i], BFNone); err != nil {
			t.Fatalf("serial warm[%d]: %v", i, err)
		}
		if _, _, err := chainParallel.ProcessBlock(blocks[i], BFNone); err != nil {
			t.Fatalf("parallel warm[%d]: %v", i, err)
		}
	}

	win := blocks[warm+1 : warm+1+window]

	// Serial reference path.
	for i, b := range win {
		if _, _, err := chainSerial.ProcessBlock(b, BFNone); err != nil {
			t.Fatalf("serial[%d]: %v", i, err)
		}
	}

	// Parallel pipeline path on the independent chain.
	if err := runParallelPipelineAtChainLayer(chainParallel, win); err != nil {
		t.Fatalf("parallel pipeline: %v", err)
	}

	// Same tip after the window.
	serialTip := chainSerial.BestSnapshot()
	parallelTip := chainParallel.BestSnapshot()
	if !serialTip.Hash.IsEqual(&parallelTip.Hash) {
		t.Fatalf("tip divergence: serial=%s (h=%d) parallel=%s (h=%d)",
			serialTip.Hash, serialTip.Height,
			parallelTip.Hash, parallelTip.Height)
	}

	// Same canonical UTXO set. Both chains committed via the same connectBlock
	// code, so identical tips imply identical UTXO sets; this is the
	// belt-and-suspenders proof. In-memory cache fingerprint is sufficient
	// here because the window is small enough to avoid cache eviction.
	serialFP := chainSerial.UtxoCacheFingerprint()
	parallelFP := chainParallel.UtxoCacheFingerprint()
	if !serialFP.IsEqual(&parallelFP) {
		t.Fatalf("UTXO set divergence: serial=%s parallel=%s",
			serialFP, parallelFP)
	}
}
