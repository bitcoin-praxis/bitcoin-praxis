// Copyright (c) 2025 The btcd developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"runtime"
	"sort"
	"sync"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/mempool"
	peerpkg "github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire/v2"
)

const (
	// maxParallelValidateWindow caps the soft-connect / parallel-script
	// batch size. Sized high enough that runtime.NumCPU() is the usual
	// limit on typical workstations (8–16–32 cores).
	maxParallelValidateWindow = 32

	// maxPendingValidateCaps how many full block bodies may sit in
	// pendingValidate waiting for the tip to catch up. Sized to the
	// parallel-fetch in-flight budget plus one validate window. Without
	// this bound, off-path / reorged blocks from peer churn accumulate
	// forever and can OOM the process (observed ~11GB RSS on testnet4).
	maxPendingValidate = maxParallelBlockPeers*maxInFlightPerPeer + maxParallelValidateWindow
)

// pendingIBDBlock is a peer-delivered block waiting for the ordered parallel
// validation pipeline during IBD.
type pendingIBDBlock struct {
	bmsg  *blockMsg
	flags blockchain.BehaviorFlags
}

// parallelValidatePoolSize returns the target validation window / in-flight
// depth based on available CPUs.
func parallelValidatePoolSize() int {
	n := runtime.NumCPU()
	if n < 2 {
		n = 2
	}
	if n > maxParallelValidateWindow {
		n = maxParallelValidateWindow
	}
	return n
}

// scriptWorkersPerBlock returns how many script goroutines each block in a
// parallel batch may use.
//
// Each concurrent block gets the full serial budget (numCPU*3). Sharing one
// NumCPU*3 pie across the window was measured to leave ~⅓ of cores idle on
// dense testnet4 IBD (same aggregate workers as serial, no multi-block win).
// GOMAXPROCS still bounds real parallelism; extra goroutines just keep the
// run queue fed. batchLen is retained for call-site clarity / tests.
func scriptWorkersPerBlock(numCPU, batchLen int) int {
	_ = batchLen
	total := numCPU * 3
	if total < 1 {
		return 1
	}
	return total
}

// parallelValidateMinSigs is the minimum estimated signature-verification count
// at which the parallel pipeline pays off. Below this, the fixed pipeline
// overhead (prefetch pass, soft-connect bookkeeping, goroutine spawn) exceeds
// the overlap savings, so the historical serial ProcessBlock path is faster.
// Tuned from cmd/fullvaltip: a dense mainnet block is ~150-250 sigs and the
// DEPTH1 overlap saves ~140us/sig, so ~20 sigs amortize the ~2-3ms overhead.
// Empty/sparse blocks (testnet, early mainnet) have 0-few sigs and go serial.
const parallelValidateMinSigs = 24

// shouldEnqueueIBD reports whether an IBD body should sit in pendingValidate.
// Every non-fast-add IBD block is enqueued so a light serial accept cannot
// strand a heavier block that arrived first. Checkpoints (BFFastAdd) skip
// scripts and stay on the historical serial path.
func shouldEnqueueIBD(ibdMode bool, flags blockchain.BehaviorFlags) bool {
	return ibdMode && flags&blockchain.BFFastAdd == 0
}

// shouldParallelValidate reports whether an enqueued IBD block should use the
// soft-connect + pipelined scripts path. Light blocks stay serial: the
// pipeline's fixed overhead exceeds its overlap savings on them.
func shouldParallelValidate(ibdMode bool, flags blockchain.BehaviorFlags,
	block *btcutil.Block) bool {
	if !shouldEnqueueIBD(ibdMode, flags) {
		return false
	}
	return estimateScriptSigs(block) >= parallelValidateMinSigs
}

// estimateScriptSigs returns a cheap estimate of the number of signature
// verifications a block will perform, without executing any script. Each
// non-coinbase transaction input typically drives one or more CHECKSIG
// operations, so the non-coinbase input count is a tight, allocation-free proxy
// for the dominant script-verify cost. The coinbase transaction is excluded.
func estimateScriptSigs(block *btcutil.Block) int {
	txns := block.Transactions()
	if len(txns) <= 1 {
		return 0
	}
	var sigs int
	for _, tx := range txns[1:] {
		sigs += len(tx.MsgTx().TxIn)
	}
	return sigs
}

// enqueueParallelBlock stores a block for ordered parallel validation.
func (sm *SyncManager) enqueueParallelBlock(bmsg *blockMsg, flags blockchain.BehaviorFlags) {
	if sm.pendingValidate == nil {
		sm.pendingValidate = make(map[chainhash.Hash]*pendingIBDBlock)
	}
	hash := *bmsg.block.Hash()
	sm.pendingValidate[hash] = &pendingIBDBlock{bmsg: bmsg, flags: flags}
	sm.prunePendingValidate()
}

// pendingKeepRange is the best-header-path height range whose bodies may sit
// in pendingValidate. Start is the first header after the best-chain /
// best-header fork, not tip+1: a reorg prefix lives at or below the current
// tip height and must be kept until ProcessBlock can walk the side chain.
func pendingKeepRange(forkHeight, headerHeight int32, capN int) (start, maxHeight int32) {
	start = forkHeight + 1
	if start < 1 {
		start = 1
	}
	maxHeight = start + int32(capN) - 1
	if maxHeight > headerHeight {
		maxHeight = headerHeight
	}
	return start, maxHeight
}

func (sm *SyncManager) pendingKeepBounds() (start, maxHeight int32) {
	_, headerHeight := sm.chain.BestHeader()
	return pendingKeepRange(
		sm.chain.BestChainHeaderForkHeight(),
		headerHeight,
		maxPendingValidate,
	)
}

// forgetPendingBody drops an uncommitted IBD body and lets a later getdata
// copy through QueueBlock. markDelivered runs before enqueue; if prune then
// discards the body, leaving recentDelivered set would drop the re-fetch
// forever (reorg prefix stall: tip=15, lasthdr=30, inflight=0).
func (sm *SyncManager) forgetPendingBody(hash chainhash.Hash) {
	delete(sm.pendingValidate, hash)
	sm.recentDelivered.Delete(hash)
}

func (sm *SyncManager) hasPendingValidate(hash chainhash.Hash) bool {
	if sm.pendingValidate == nil {
		return false
	}
	_, ok := sm.pendingValidate[hash]
	return ok
}

func (sm *SyncManager) headerHashAt(height int32) (chainhash.Hash, error) {
	hash, err := sm.chain.HeaderHashByHeight(height)
	if err != nil {
		return chainhash.Hash{}, err
	}
	return *hash, nil
}

// prunePendingValidate drops full block bodies that can never (or should not)
// be connected along the best header path: off-path, already behind the
// chain/header fork, or beyond the in-flight lookahead cap. Far-ahead on-path
// bodies that exceed maxPendingValidate are discarded and requeued for fetch
// so memory stays bounded under peer churn.
func (sm *SyncManager) prunePendingValidate() {
	if len(sm.pendingValidate) == 0 {
		return
	}

	start, maxHeight := sm.pendingKeepBounds()
	onPath := make(map[chainhash.Hash]int32, maxPendingValidate)
	if start <= maxHeight {
		for h := start; h <= maxHeight; h++ {
			hash, err := sm.chain.HeaderHashByHeight(h)
			if err != nil {
				break
			}
			onPath[*hash] = h
		}
	}

	var droppedOffPath int
	for hash := range sm.pendingValidate {
		if _, ok := onPath[hash]; ok {
			continue
		}
		sm.forgetPendingBody(hash)
		droppedOffPath++
	}

	var droppedFar int
	if len(sm.pendingValidate) > maxPendingValidate {
		type ranked struct {
			hash   chainhash.Hash
			height int32
		}
		list := make([]ranked, 0, len(sm.pendingValidate))
		for hash := range sm.pendingValidate {
			list = append(list, ranked{hash: hash, height: onPath[hash]})
		}
		sort.Slice(list, func(i, j int) bool {
			return list[i].height > list[j].height
		})
		if sm.priorityBlocks == nil {
			sm.priorityBlocks = make(map[chainhash.Hash]struct{})
		}
		for _, it := range list {
			if len(sm.pendingValidate) <= maxPendingValidate {
				break
			}
			sm.forgetPendingBody(it.hash)
			sm.priorityBlocks[it.hash] = struct{}{}
			droppedFar++
		}
	}

	if droppedOffPath > 0 || droppedFar > 0 {
		// Rate-limit: off-path singles are common under peer churn; only
		// emit when we drop a meaningful batch or breach the soft warn size.
		if droppedOffPath+droppedFar >= 8 || droppedFar > 0 ||
			len(sm.pendingValidate) > maxPendingValidate/2 {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			log.Infof("Pruned pendingValidate: off-path=%d far=%d remain=%d "+
				"(heap_alloc=%d MiB sys=%d MiB)",
				droppedOffPath, droppedFar, len(sm.pendingValidate),
				ms.HeapAlloc/1024/1024, ms.Sys/1024/1024)
		}
	}
}

// findPendingExtending looks up a pending block whose prev hash is parent.
func (sm *SyncManager) findPendingExtending(parent chainhash.Hash) *pendingIBDBlock {
	for _, p := range sm.pendingValidate {
		if p.bmsg.block.MsgBlock().Header.PrevBlock == parent {
			return p
		}
	}
	return nil
}

// flushParallelValidate commits consecutive pending bodies from tip. Light
// blocks use serial ProcessBlock; dense blocks use the pipelined verify
// window. Must run on the blockHandler goroutine.
func (sm *SyncManager) flushParallelValidate() {
	sm.prunePendingValidate()
	window := parallelValidatePoolSize()
	for {
		kind, batch := nextIBDFlush(sm.ibdMode, sm.collectConsecutivePending(window), window)
		switch kind {
		case ibdFlushNone:
			// Gap at the fork/tip: keep fetching the missing prefix
			// instead of sitting on later pending bodies with inflight=0.
			sm.fillAllBlockRequests()
			return
		case ibdFlushSerial:
			if !sm.flushSerialPending(batch[0]) {
				sm.fillAllBlockRequests()
				return
			}
		case ibdFlushPipeline:
			if !sm.processParallelBatch(batch) {
				sm.fillAllBlockRequests()
				return
			}
		}
	}
}

type ibdFlushKind int

const (
	ibdFlushNone ibdFlushKind = iota
	ibdFlushSerial
	ibdFlushPipeline
)

// nextIBDFlush decides how to commit the next consecutive pending bodies.
// A light tip-extender is serial; a dense run is pipelined up to window,
// stopping before the first light block so it is not stranded.
func nextIBDFlush(ibd bool, consecutive []*pendingIBDBlock, window int) (ibdFlushKind, []*pendingIBDBlock) {
	if len(consecutive) == 0 {
		return ibdFlushNone, nil
	}
	if !shouldParallelValidate(ibd, consecutive[0].flags, consecutive[0].bmsg.block) {
		return ibdFlushSerial, consecutive[:1]
	}
	n := 0
	for n < len(consecutive) && n < window {
		if !shouldParallelValidate(ibd, consecutive[n].flags, consecutive[n].bmsg.block) {
			break
		}
		n++
	}
	if n == 0 {
		return ibdFlushNone, nil
	}
	return ibdFlushPipeline, consecutive[:n]
}

func (sm *SyncManager) flushSerialPending(p *pendingIBDBlock) bool {
	hash := *p.bmsg.block.Hash()
	isCheckpoint, _ := sm.checkHeadersList(&hash)
	ok := sm.processBlockSerial(p.bmsg, p.flags, isCheckpoint)
	delete(sm.pendingValidate, hash)
	return ok
}

// collectConsecutivePending gathers up to window consecutive pending blocks
// along the best-header path from the chain/header fork. Side-chain connects
// during a reorg do not move the tip until the new chain has more work, so
// already-HaveBlock hashes are skipped rather than treated as a gap.
// Light and dense bodies are both included; nextIBDFlush splits the run.
func (sm *SyncManager) collectConsecutivePending(window int) []*pendingIBDBlock {
	if len(sm.pendingValidate) == 0 || window <= 0 {
		return nil
	}
	start, maxHeight := sm.pendingKeepBounds()
	if start > maxHeight {
		return nil
	}
	parent, err := sm.headerHashAt(start - 1)
	if err != nil {
		return nil
	}
	batch := make([]*pendingIBDBlock, 0, window)
	for h := start; h <= maxHeight && len(batch) < window; h++ {
		want, err := sm.headerHashAt(h)
		if err != nil {
			break
		}
		p := sm.pendingValidate[want]
		if p == nil {
			have, herr := sm.chain.HaveBlock(&want)
			if herr != nil || !have {
				break
			}
			parent = want
			continue
		}
		if p.bmsg.block.MsgBlock().Header.PrevBlock != parent {
			break
		}
		batch = append(batch, p)
		parent = want
	}
	return batch
}

// processParallelBatch soft-connects the window, verifies each block with
// the full serial script-worker budget (verifies stay DEPTH1 / serialized
// against each other), then commits in height order. Returns false if a
// block was rejected (pipeline stops for this flush).
func (sm *SyncManager) processParallelBatch(batch []*pendingIBDBlock) bool {
	view := sm.chain.NewTipUtxoView()
	blocks := make([]*btcutil.Block, len(batch))
	for i, p := range batch {
		blocks[i] = p.bmsg.block
	}

	// Warm cache for the first block before the pipeline starts. Further
	// blocks are prefetched while the previous block's scripts verify so
	// LevelDB miss latency overlaps sig CPU.
	if err := sm.chain.PrefetchCacheInputs(blocks[:1]); err != nil {
		log.Warnf("UTXO prefetch failed; continuing without prefetch: %v", err)
	}

	// Full serial script budget per concurrent block so a multi-block
	// window can actually use idle cores (see scriptWorkersPerBlock).
	perBlock := scriptWorkersPerBlock(runtime.NumCPU(), len(batch))
	blockchain.SetScriptCheckWorkers(perBlock)
	defer blockchain.SetScriptCheckWorkers(0)

	// Pipelined soft-connect + verify + UTXO prefetch. Soft-connect is serial
	// (chainLock); verify of block i overlaps PrefetchCacheInputs of i+1.
	// Verifies stay serialized against each other (DEPTH1) so we do not
	// oversubscribe the shared script-worker budget.
	errs := make([]error, len(batch))
	connected := 0
	var prev *blockchain.SoftConnectResult
	var waitPrev func() error
	var waitPrefetch func() error
	for i, p := range batch {
		if waitPrefetch != nil {
			if perr := waitPrefetch(); perr != nil {
				log.Warnf("UTXO prefetch failed; continuing without prefetch: %v", perr)
			}
			waitPrefetch = nil
		}

		res, err := sm.chain.SoftConnectNextAfter(p.bmsg.block, view, prev)
		if err != nil {
			if waitPrev != nil {
				waitPrev()
			}
			sm.rejectPendingBlock(p, err)
			sm.dropPendingFrom(batch[i:])
			break
		}
		if waitPrev != nil {
			if verr := waitPrev(); verr != nil {
				sm.rejectPendingBlock(batch[i-1], verr)
				sm.dropPendingFrom(batch[i:])
				break
			}
		}

		var vg sync.WaitGroup
		vg.Add(1)
		idx := i
		go func() {
			defer vg.Done()
			errs[idx] = sm.chain.VerifyBlockScripts(res)
		}()
		waitPrev = func() error { vg.Wait(); return errs[idx] }

		// Prefetch next block's tip inputs into the UTXO cache while this
		// block verifies. Soft-connect of i+1 waits on waitPrefetch first.
		if i+1 < len(batch) {
			var pg sync.WaitGroup
			pg.Add(1)
			nextBlocks := blocks[i+1 : i+2]
			var prefErr error
			go func() {
				defer pg.Done()
				prefErr = sm.chain.PrefetchCacheInputs(nextBlocks)
			}()
			waitPrefetch = func() error { pg.Wait(); return prefErr }
		}

		prev = res
		connected++
	}
	if waitPrev != nil {
		if verr := waitPrev(); verr != nil {
			sm.rejectPendingBlock(batch[connected-1], verr)
			sm.dropPendingFrom(batch[connected:])
		}
	}
	if waitPrefetch != nil {
		waitPrefetch() // drain; next soft-connect will not run
	}

	// Commit in height order. Commit must stay on the blockHandler goroutine
	// (ProcessBlock + finishBlockAccept touch peer state), so it is serial
	// after the verifies rather than overlapped.
	for i := 0; i < connected; i++ {
		if errs[i] != nil {
			sm.rejectPendingBlock(batch[i], errs[i])
			sm.dropPendingFrom(batch[i:])
			return false
		}
		// Soft-connect + scripts already ran; BFNoScriptCheck skips only
		// scripts on commit and still runs the rest of checkConnectBlock.
		flags := batch[i].flags | blockchain.BFNoScriptCheck
		if !sm.commitParallelBlock(batch[i], flags) {
			sm.dropPendingFrom(batch[i:])
			return false
		}
		delete(sm.pendingValidate, *batch[i].bmsg.block.Hash())
	}
	return connected == len(batch)
}

func (sm *SyncManager) dropPendingFrom(batch []*pendingIBDBlock) {
	for _, p := range batch {
		delete(sm.pendingValidate, *p.bmsg.block.Hash())
	}
}

func (sm *SyncManager) rejectPendingBlock(p *pendingIBDBlock, err error) {
	blockHash := p.bmsg.block.Hash()
	peer := p.bmsg.peer
	if _, ok := err.(blockchain.RuleError); ok {
		log.Infof("Rejected block %v from %s: %v", blockHash, peer, err)
	} else {
		log.Errorf("Failed to process block %v: %v", blockHash, err)
	}
	if dbErr, ok := err.(database.Error); ok && dbErr.ErrorCode ==
		database.ErrCorruption {
		panic(dbErr)
	}
	code, reason := mempool.ErrToRejectErr(err)
	peer.PushRejectMsg(wire.CmdBlock, code, reason, blockHash, false)
}

// commitParallelBlock runs ProcessBlock after scripts were verified and
// performs the same post-accept bookkeeping as handleBlockMsg.
func (sm *SyncManager) commitParallelBlock(p *pendingIBDBlock, flags blockchain.BehaviorFlags) bool {
	bmsg := p.bmsg
	peer := bmsg.peer
	blockHash := bmsg.block.Hash()

	_, isOrphan, err := sm.chain.ProcessBlock(bmsg.block, flags)
	if err != nil {
		sm.rejectPendingBlock(p, err)
		return false
	}

	isCheckpointBlock, _ := sm.checkHeadersList(blockHash)
	sm.finishBlockAccept(bmsg, peer, blockHash, isOrphan, isCheckpointBlock)
	return true
}

// finishBlockAccept shares post-ProcessBlock IBD bookkeeping between the
// serial and parallel validation paths.
func (sm *SyncManager) finishBlockAccept(bmsg *blockMsg, peer *peerpkg.Peer,
	blockHash *chainhash.Hash, isOrphan, isCheckpointBlock bool) {

	var heightUpdate int32
	var blkHashUpdate *chainhash.Hash

	if isOrphan {
		header := &bmsg.block.MsgBlock().Header
		if blockchain.ShouldHaveSerializedBlockHeight(header) {
			coinbaseTx := bmsg.block.Transactions()[0]
			cbHeight, err := blockchain.ExtractCoinbaseHeight(coinbaseTx)
			if err != nil {
				log.Warnf("Unable to extract height from "+
					"coinbase tx: %v", err)
			} else {
				log.Debugf("Extracted height of %v from "+
					"orphan block", cbHeight)
				heightUpdate = cbHeight
				blkHashUpdate = blockHash
			}
		}

		orphanRoot := sm.chain.GetOrphanRoot(blockHash)
		locator, err := sm.chain.LatestBlockLocator()
		if err != nil {
			log.Warnf("Failed to get block locator for the "+
				"latest block: %v", err)
		} else {
			peer.PushGetBlocksMsg(locator, orphanRoot)
		}
	} else {
		if sm.isBlockDownloadPeer(peer) || peer == sm.syncPeer {
			sm.noteBlockPeerProgress(peer)
		}

		sm.progressLogger.LogBlockHeight(bmsg.block, sm.chain)

		best := sm.chain.BestSnapshot()
		heightUpdate = best.Height
		blkHashUpdate = &best.Hash

		sm.rejectedTxns = make(map[chainhash.Hash]struct{})
	}

	if blkHashUpdate != nil && heightUpdate != 0 {
		peer.UpdateLastBlockHeight(heightUpdate)
		if isOrphan || sm.current() {
			go sm.peerNotifier.UpdatePeerHeights(blkHashUpdate, heightUpdate,
				peer)
		}
	}

	if !sm.ibdMode {
		if err := sm.chain.FlushUtxoCache(blockchain.FlushPeriodic); err != nil {
			log.Errorf("Error while flushing the blockchain cache: %v", err)
		}
		return
	}

	if isCheckpointBlock {
		log.Infof("Continuing IBD, on checkpoint block %v(%v)",
			bmsg.block.Hash(), bmsg.block.Height())
		nextCheckpoint := sm.findNextHeaderCheckpoint(bmsg.block.Height())
		if nextCheckpoint == nil {
			log.Infof("Reached the final checkpoint -- " +
				"switching to normal mode")
		}
	}

	_, lastHeight := sm.chain.BestHeader()
	if bmsg.block.Height() < lastHeight {
		sm.fillAllBlockRequests()
		return
	}

	if bmsg.block.Height() >= lastHeight {
		log.Infof("Finished the initial block download and "+
			"caught up to block %v(%v) -- now listening to blocks.",
			bmsg.block.Hash(), bmsg.block.Height())
		sm.ibdMode = false
		sm.blockPeers = make(map[*peerpkg.Peer]struct{})
		sm.pendingValidate = nil
	}
}

// processBlockSerial is the historical single-block ProcessBlock path.
func (sm *SyncManager) processBlockSerial(bmsg *blockMsg, behaviorFlags blockchain.BehaviorFlags,
	isCheckpointBlock bool) bool {

	peer := bmsg.peer
	blockHash := bmsg.block.Hash()

	_, isOrphan, err := sm.chain.ProcessBlock(bmsg.block, behaviorFlags)
	if err != nil {
		sm.rejectPendingBlock(&pendingIBDBlock{bmsg: bmsg, flags: behaviorFlags}, err)
		return false
	}
	sm.finishBlockAccept(bmsg, peer, blockHash, isOrphan, isCheckpointBlock)
	return true
}
