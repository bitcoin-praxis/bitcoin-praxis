// Copyright (c) 2025 The btcd developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// scriptCheckWorkers caps per-block script goroutines. Zero means the default
// of runtime.NumCPU()*3. Parallel IBD sets this to the full serial budget for
// each concurrent block so multi-block windows can use idle cores.
var scriptCheckWorkers atomic.Int32

// SetScriptCheckWorkers sets the maximum goroutines used by a single
// checkBlockScripts / ValidateTransactionScripts call. Values <= 0 restore the
// default (NumCPU()*3).
func SetScriptCheckWorkers(n int) {
	if n < 0 {
		n = 0
	}
	scriptCheckWorkers.Store(int32(n))
}

// ScriptCheckWorkers returns the configured per-block script worker cap, or
// the default when unset.
func ScriptCheckWorkers() int {
	if n := int(scriptCheckWorkers.Load()); n > 0 {
		return n
	}
	n := runtime.NumCPU() * 3
	if n <= 0 {
		return 1
	}
	return n
}

// SoftConnectResult is the outcome of soft-connecting one block onto a UTXO
// view without running scripts. The view is mutated in place to represent the
// chain tip after this block.
type SoftConnectResult struct {
	Block       *btcutil.Block
	View        *UtxoViewpoint
	ScriptFlags txscript.ScriptFlags
	NeedScripts bool
	Height      int32
	Hash        chainhash.Hash

	// VerifyView is a minimal, isolated snapshot of just this block's input
	// entries, built under chainLock during soft-connect. Script verification
	// reads it instead of the shared View so a pipelined batch can keep
	// soft-connecting the next block (mutating View) while the previous
	// block's scripts verify concurrently without a map data race.
	VerifyView *UtxoViewpoint

	// node is a throwaway block index node used only to chain further
	// SoftConnectNextAfter calls before the block is committed.
	node *blockNode
}

// NewTipUtxoView returns an empty UTXO view whose best hash is the current
// main-chain tip. Safe for concurrent access.
func (b *BlockChain) NewTipUtxoView() *UtxoViewpoint {
	b.chainLock.RLock()
	defer b.chainLock.RUnlock()

	view := NewUtxoViewpoint()
	tip := b.bestChain.Tip()
	view.SetBestHash(&tip.hash)
	return view
}

// SoftConnectNext extends view by connecting block's transactions and running
// all checkConnectBlock rules except scripts. view.BestHash must equal the
// block's previous-block hash, which must already be in the block index (the
// current tip for the first call in a window).
//
// This is intended for the serial UTXO-prep stage of parallel IBD validation.
// Scripts are verified afterward via VerifyBlockScripts, then the block is
// committed with ProcessBlock(..., BFNoScriptCheck).
//
// This function is safe for concurrent access with respect to the chain, but
// the caller must not mutate view from another goroutine during the call.
func (b *BlockChain) SoftConnectNext(block *btcutil.Block, view *UtxoViewpoint) (*SoftConnectResult, error) {
	return b.SoftConnectNextAfter(block, view, nil)
}

// SoftConnectNextAfter is like SoftConnectNext but accepts the previous
// SoftConnectResult so a multi-block window can be soft-connected before any
// of those blocks are committed to the index.
func (b *BlockChain) SoftConnectNextAfter(block *btcutil.Block, view *UtxoViewpoint, prev *SoftConnectResult) (*SoftConnectResult, error) {
	b.chainLock.Lock()
	defer b.chainLock.Unlock()

	var parent *blockNode
	if prev != nil {
		parent = prev.node
	}
	return b.softConnectNextLocked(block, view, parent)
}

func (b *BlockChain) softConnectNextLocked(block *btcutil.Block, view *UtxoViewpoint, parent *blockNode) (*SoftConnectResult, error) {
	prevHash := &block.MsgBlock().Header.PrevBlock
	if !view.BestHash().IsEqual(prevHash) {
		return nil, AssertError(fmt.Sprintf("soft-connect view best hash %v "+
			"does not match block prev %v", view.BestHash(), prevHash))
	}

	if err := checkBlockSanity(block, b.chainParams.PowLimit, b.timeSource, BFNone); err != nil {
		return nil, err
	}

	prevNode := parent
	if prevNode == nil {
		prevNode = b.index.LookupNode(prevHash)
	}
	if prevNode == nil {
		str := fmt.Sprintf("previous block %s is unknown", prevHash)
		return nil, ruleError(ErrPreviousBlockUnknown, str)
	}
	if !prevNode.hash.IsEqual(prevHash) {
		return nil, AssertError(fmt.Sprintf("soft-connect parent hash %v "+
			"does not match block prev %v", prevNode.hash, prevHash))
	}

	height := prevNode.height + 1
	block.SetHeight(height)

	if err := b.checkBlockContext(block, prevNode, BFNone); err != nil {
		return nil, err
	}

	newNode := newBlockNode(&block.MsgBlock().Header, prevNode)
	scriptFlags, needScripts, err := b.checkConnectBlockNoScripts(
		newNode, block, view, nil,
	)
	if err != nil {
		return nil, err
	}

	return &SoftConnectResult{
		Block:       block,
		View:        view,
		ScriptFlags: scriptFlags,
		NeedScripts: needScripts,
		Height:      height,
		Hash:        *block.Hash(),
		VerifyView:  buildVerifySnapshot(block, view),
		node:        newNode,
	}, nil
}

// buildVerifySnapshot returns a minimal, isolated UTXO view holding only the
// entries this block's script verification will read: the previous outputs
// for every non-coinbase input (including outputs created earlier in the same
// block, which soft-connect has already placed in view). Entries are cloned so
// a concurrent soft-connect of the next block cannot race verify's reads.
//
// Must be called while view is stable (soft-connect holds chainLock). After a
// successful soft-connect every input has a non-nil entry, so missing entries
// are skipped rather than carried as nil sentinels.
func buildVerifySnapshot(block *btcutil.Block, view *UtxoViewpoint) *UtxoViewpoint {
	snap := NewUtxoViewpoint()
	snap.SetBestHash(block.Hash())
	for _, tx := range block.Transactions() {
		for _, txIn := range tx.MsgTx().TxIn {
			if txIn.PreviousOutPoint.Index == wire.MaxPrevOutIndex {
				continue // coinbase
			}
			op := txIn.PreviousOutPoint
			if _, ok := snap.entries[op]; ok {
				continue
			}
			if entry := view.entries[op]; entry != nil {
				snap.entries[op] = entry.Clone()
			}
		}
	}
	return snap
}

// VerifyBlockScripts runs the expensive script checks for a SoftConnectResult.
// It does not hold chainLock; sigCache/hashCache are concurrent-safe. It reads
// the result's isolated VerifyView snapshot so it is safe to run concurrently
// with a later block's soft-connect mutating the shared View.
func (b *BlockChain) VerifyBlockScripts(res *SoftConnectResult) error {
	if res == nil || !res.NeedScripts {
		return nil
	}
	v := res.VerifyView
	if v == nil {
		v = res.View
	}
	return checkBlockScripts(res.Block, v, res.ScriptFlags,
		b.sigCache, b.hashCache)
}

// ReplaceScriptCaches swaps the signature and sighash caches used by
// VerifyBlockScripts. Intended for verification benches that need a cold
// cache per timed mode so a prior SERIAL run cannot inflate parallel results.
func (b *BlockChain) ReplaceScriptCaches(sig *txscript.SigCache, hash *txscript.HashCache) {
	b.chainLock.Lock()
	defer b.chainLock.Unlock()
	b.sigCache = sig
	b.hashCache = hash
}

// SoftConnectAndVerify is a convenience that soft-connects then verifies
// scripts for a single block extending the current tip. Useful for tests.
func (b *BlockChain) SoftConnectAndVerify(block *btcutil.Block) error {
	view := b.NewTipUtxoView()
	res, err := b.SoftConnectNext(block, view)
	if err != nil {
		return err
	}
	return b.VerifyBlockScripts(res)
}

// PrefetchBlocksInputs bulk-loads tip-known inputs for the given blocks into
// view from the live UTXO cache. This is a read-only optimization ahead of
// SoftConnectNext: it must not change connect/BIP30 outcomes versus skipping
// the prefetch entirely.
//
// Consensus-safety constraints (do not relax without a regression test):
//  1. Never call findInputsToFetch. That helper mutates the view via
//     AddTxOuts(..., block.Height()). Pending IBD blocks still have
//     Height() == -1, which inserts unspent entries at height -1 and makes
//     checkBIP0030 reject the block ("overwrite ... at block height -1").
//  2. Never invent outputs (no AddTxOuts). Only clone non-nil tip-cache
//     entries for previous outpoints.
//  3. Skip same-block and same-window created outpoints; those are not tip
//     UTXOs yet and are filled by SoftConnectNext with the correct height.
//
// Safe for concurrent access w.r.t. the chain; view must not be mutated by
// another goroutine during the call.
func (b *BlockChain) PrefetchBlocksInputs(view *UtxoViewpoint, blocks []*btcutil.Block) error {
	needed := collectPrefetchOutpoints(view, blocks)
	if len(needed) == 0 {
		return nil
	}

	b.chainLock.RLock()
	defer b.chainLock.RUnlock()
	entries, err := b.utxoCache.fetchEntries(needed)
	if err != nil {
		return err
	}
	for i, entry := range entries {
		// Skip nil: missing tip entries are filled (or confirmed missing)
		// during SoftConnectNext's fetchInputUtxos. Storing nil here would
		// also differ from inventing outputs, but skipping keeps the view
		// free of sentinel entries Prefetch does not own.
		if entry == nil {
			continue
		}
		view.entries[needed[i]] = entry.Clone()
	}
	return nil
}

// PrefetchCacheInputs warms the live UTXO cache with tip-known inputs for
// blocks without mutating any soft-connect view. Intended to run concurrent
// with VerifyBlockScripts of the previous block so LevelDB miss latency for
// block N+1 overlaps script CPU of block N.
//
// Same consensus constraints as PrefetchBlocksInputs (no findInputsToFetch,
// no inventing outputs). Takes chainLock for writes because fetchEntries
// mutates the live cache; script verify does not hold the lock, so this
// still overlaps sig CPU.
func (b *BlockChain) PrefetchCacheInputs(blocks []*btcutil.Block) error {
	needed := collectPrefetchOutpoints(nil, blocks)
	if len(needed) == 0 {
		return nil
	}
	b.chainLock.Lock()
	defer b.chainLock.Unlock()
	_, err := b.utxoCache.fetchEntries(needed)
	return err
}

// collectPrefetchOutpoints returns tip-likely previous outpoints referenced by
// blocks, excluding anything created inside the same soft-connect window or
// already present in view. Pure: does not mutate view.
func collectPrefetchOutpoints(view *UtxoViewpoint, blocks []*btcutil.Block) []wire.OutPoint {
	if len(blocks) == 0 {
		return nil
	}

	// Outputs produced by any tx in this window are not tip UTXOs yet.
	windowCreated := make(map[chainhash.Hash]struct{}, 64)
	for _, block := range blocks {
		for _, tx := range block.Transactions() {
			windowCreated[*tx.Hash()] = struct{}{}
		}
	}

	needed := make([]wire.OutPoint, 0, 1024)
	seen := make(map[wire.OutPoint]struct{}, 1024)
	for _, block := range blocks {
		txs := block.Transactions()
		if len(txs) < 2 {
			continue
		}
		for _, tx := range txs[1:] {
			for _, txIn := range tx.MsgTx().TxIn {
				op := txIn.PreviousOutPoint
				if _, ok := windowCreated[op.Hash]; ok {
					continue
				}
				if _, ok := seen[op]; ok {
					continue
				}
				if view != nil {
					if _, ok := view.entries[op]; ok {
						continue
					}
				}
				seen[op] = struct{}{}
				needed = append(needed, op)
			}
		}
	}
	return needed
}
