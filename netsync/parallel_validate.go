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

// shouldParallelValidate reports whether this IBD block should go through the
// soft-connect + parallel scripts path. BFFastAdd already skips scripts via
// checkpoints, so the serial ProcessBlock path is fine there. Light blocks
// (few signature verifications) also go serial: the pipeline's fixed overhead
// exceeds its overlap savings on them, which is the source of the light-range
// regression on testnet / early-mainnet IBD.
func shouldParallelValidate(ibdMode bool, flags blockchain.BehaviorFlags,
	block *btcutil.Block) bool {
	if !ibdMode {
		return false
	}
	if flags&blockchain.BFFastAdd != 0 {
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

// prunePendingValidate drops full block bodies that can never (or should not)
// be soft-connected from the current tip: off best-header-path, already behind
// tip, or beyond the in-flight lookahead cap. Far-ahead on-path bodies that
// exceed maxPendingValidate are discarded and requeued for fetch so memory
// stays bounded under peer churn.
func (sm *SyncManager) prunePendingValidate() {
	if len(sm.pendingValidate) == 0 {
		return
	}

	tip := sm.chain.BestSnapshot()
	_, headerHeight := sm.chain.BestHeader()
	maxHeight := tip.Height + int32(maxPendingValidate)
	if maxHeight > headerHeight {
		maxHeight = headerHeight
	}

	onPath := make(map[chainhash.Hash]int32, maxPendingValidate)
	for h := tip.Height + 1; h <= maxHeight; h++ {
		hash, err := sm.chain.HeaderHashByHeight(h)
		if err != nil {
			break
		}
		onPath[*hash] = h
	}

	var droppedOffPath int
	for hash := range sm.pendingValidate {
		if _, ok := onPath[hash]; ok {
			continue
		}
		delete(sm.pendingValidate, hash)
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
			delete(sm.pendingValidate, it.hash)
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

// flushParallelValidate soft-connects a consecutive window from tip, verifies
// scripts across the window in parallel, then commits each block in height
// order. Must run on the blockHandler goroutine.
func (sm *SyncManager) flushParallelValidate() {
	sm.prunePendingValidate()
	window := parallelValidatePoolSize()
	for {
		batch := sm.collectParallelBatch(window)
		if len(batch) == 0 {
			return
		}
		if !sm.processParallelBatch(batch) {
			// Soft-connect/script failure drops requested hashes; refill so
			// IBD does not stall waiting for a block peers already sent.
			sm.fillAllBlockRequests()
			return
		}
	}
}

// collectParallelBatch gathers up to window consecutive pending blocks
// extending the current tip along the best-header path (not an arbitrary fork).
func (sm *SyncManager) collectParallelBatch(window int) []*pendingIBDBlock {
	if len(sm.pendingValidate) == 0 {
		return nil
	}
	tip := sm.chain.BestSnapshot()
	parent := tip.Hash
	batch := make([]*pendingIBDBlock, 0, window)
	for len(batch) < window {
		nextHeight := tip.Height + int32(len(batch)) + 1
		wantHash, err := sm.chain.HeaderHashByHeight(nextHeight)
		if err != nil {
			break
		}
		p := sm.pendingValidate[*wantHash]
		if p == nil {
			break
		}
		if p.bmsg.block.MsgBlock().Header.PrevBlock != parent {
			break
		}
		batch = append(batch, p)
		parent = *wantHash
	}
	return batch
}

// processParallelBatch soft-connects the window, verifies scripts for all
// blocks concurrently (shared NumCPU*3 worker budget), then commits in order.
// Returns false if a block was rejected (pipeline stops for this flush).
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
		sm.rejectPendingBlock(batch[0], err)
		sm.dropPendingFrom(batch)
		return false
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
				if waitPrev != nil {
					waitPrev()
				}
				sm.rejectPendingBlock(p, perr)
				sm.dropPendingFrom(batch[i:])
				break
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
		// Soft-connect + scripts already enforced full connect rules; BFFastAdd
		// skips re-running checkConnectBlock on the serial commit path.
		flags := batch[i].flags | blockchain.BFFastAdd
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
	isCheckpointBlock bool) {

	peer := bmsg.peer
	blockHash := bmsg.block.Hash()

	_, isOrphan, err := sm.chain.ProcessBlock(bmsg.block, behaviorFlags)
	if err != nil {
		sm.rejectPendingBlock(&pendingIBDBlock{bmsg: bmsg, flags: behaviorFlags}, err)
		return
	}
	sm.finishBlockAccept(bmsg, peer, blockHash, isOrphan, isCheckpointBlock)
}
