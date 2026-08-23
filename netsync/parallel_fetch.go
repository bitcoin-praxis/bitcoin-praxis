// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/btcsuite/btcd/chainhash/v2"
	peerpkg "github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire/v2"
)

const (
	// maxParallelBlockPeers is the number of peers used concurrently for
	// IBD block download once headers are caught up.
	maxParallelBlockPeers = 3

	// maxInFlightPerPeer caps outstanding getdata block hashes per peer so
	// work stays partitioned across the parallel pool.
	maxInFlightPerPeer = 64
)

// startParallelBlockFetch selects up to maxParallelBlockPeers download peers
// and begins partitioned getdata against the header chain.
func (sm *SyncManager) startParallelBlockFetch() {
	best := sm.chain.BestSnapshot()
	candidates := sm.fetchHigherPeers(best.Height)
	if len(candidates) == 0 {
		log.Warnf("No sync peer candidates available for block fetch")
		return
	}

	sm.ibdMode = true
	sm.lastProgressTime = time.Now()

	// Start with the header sync peer only. Unproven peers often claim
	// NODE_NETWORK but cannot serve history; assigning tip+1/+2 to them
	// stalls IBD until the long stall timer fires. Grow the pool only
	// after peers prove they can deliver (see maybeGrowBlockPeerPool).
	if sm.blockPeers == nil {
		sm.blockPeers = make(map[*peerpkg.Peer]struct{})
	}
	if sm.syncPeer != nil {
		if _, ok := sm.blockPeers[sm.syncPeer]; !ok {
			sm.addBlockPeer(sm.syncPeer)
		}
	} else {
		sm.ensureBlockPeers()
	}

	if len(sm.blockPeers) == 0 {
		log.Warnf("Failed to populate IBD block peer pool")
		return
	}

	sm.pickSyncPeerFromBlockPool()
	log.Infof("Syncing blocks to height %d from %d peers (parallel IBD)",
		sm.maxBlockPeerHeight(), len(sm.blockPeers))
	sm.fillAllBlockRequests()
}

// ensureBlockPeers grows the download pool up to maxParallelBlockPeers from
// sync candidates that advertise blocks we still need. Newly added peers are
// probed immediately so they can earn blocksDelivered (fill previously
// skipped undelivered peers entirely, leaving them idle).
func (sm *SyncManager) ensureBlockPeers() {
	if sm.blockPeers == nil {
		sm.blockPeers = make(map[*peerpkg.Peer]struct{})
	}

	bestHeight := sm.chain.BestSnapshot().Height
	var added []*peerpkg.Peer

	// Prefer the current sync peer first (often the peer that just finished
	// headers quickly) so it is not left out of a randomly ordered map walk.
	if sm.syncPeer != nil {
		if state := sm.peerStates[sm.syncPeer]; state != nil &&
			state.syncCandidate &&
			sm.syncPeer.LastBlock() > bestHeight {
			if _, exists := sm.blockPeers[sm.syncPeer]; !exists &&
				len(sm.blockPeers) < maxParallelBlockPeers {
				sm.addBlockPeer(sm.syncPeer)
				added = append(added, sm.syncPeer)
			}
		}
	}

	for peer, state := range sm.peerStates {
		if len(sm.blockPeers) >= maxParallelBlockPeers {
			break
		}
		if !state.syncCandidate {
			continue
		}
		if _, exists := sm.blockPeers[peer]; exists {
			continue
		}
		if peer.LastBlock() <= bestHeight {
			continue
		}

		sm.addBlockPeer(peer)
		added = append(added, peer)
	}

	for _, peer := range added {
		sm.probeNewBlockPeer(peer)
	}
}

// addBlockPeer registers peer for parallel IBD getdata and marks it as having
// just progressed so a fresh join is not immediately treated as stalled.
func (sm *SyncManager) addBlockPeer(peer *peerpkg.Peer) {
	state, exists := sm.peerStates[peer]
	if !exists {
		return
	}

	sm.blockPeers[peer] = struct{}{}
	state.lastBlockProgress = time.Now()
	log.Infof("IBD block peer +%s (%d/%d)",
		peer.Addr(), len(sm.blockPeers), maxParallelBlockPeers)
}

// pickSyncPeerFromBlockPool keeps syncPeer non-nil for header/inv compatibility
// by selecting any active block peer (preferring the least loaded).
func (sm *SyncManager) pickSyncPeerFromBlockPool() {
	peers := sm.blockPeersByLoad()
	if len(peers) == 0 {
		sm.syncPeer = nil
		return
	}
	sm.syncPeer = peers[0]
}

// maxBlockPeerHeight returns the highest LastBlock among block peers.
func (sm *SyncManager) maxBlockPeerHeight() int32 {
	var maxHeight int32
	for peer := range sm.blockPeers {
		if h := peer.LastBlock(); h > maxHeight {
			maxHeight = h
		}
	}
	return maxHeight
}

// blockPeersByLoad returns block peers sorted for assignment: peers that have
// already delivered IBD blocks are preferred, then fewest in-flight.
func (sm *SyncManager) blockPeersByLoad() []*peerpkg.Peer {
	peers := make([]*peerpkg.Peer, 0, len(sm.blockPeers))
	for peer := range sm.blockPeers {
		peers = append(peers, peer)
	}

	haveWorking := false
	for _, peer := range peers {
		if state := sm.peerStates[peer]; state != nil && state.blocksDelivered > 0 {
			haveWorking = true
			break
		}
	}

	sort.Slice(peers, func(i, j int) bool {
		si := sm.peerStates[peers[i]]
		sj := sm.peerStates[peers[j]]
		li, lj := 0, 0
		di, dj := uint64(0), uint64(0)
		if si != nil {
			li = len(si.requestedBlocks)
			di = si.blocksDelivered
		}
		if sj != nil {
			lj = len(sj.requestedBlocks)
			dj = sj.blocksDelivered
		}
		if haveWorking {
			wi, wj := di > 0, dj > 0
			if wi != wj {
				return wi
			}
		}
		if li != lj {
			return li < lj
		}
		return peers[i].Addr() < peers[j].Addr()
	})
	return peers
}

// fillAllBlockRequests assigns more getdata work to under-filled block peers.
//
// Under full race-to-first every pool peer requests the same tip-ahead
// heights (first delivery wins, late copies dropped), so each peer is filled
// in bulk: one getdata of up to maxInFlightPerPeer hashes per peer. Chunked
// interleaving was for the removed exclusive-partition scheme; under racing it
// only fragmented the request into 64 single-hash getdata messages per peer,
// and on the light range (tiny blocks) that getdata overhead dominated and ran
// ~2× slower than serial. Unproven peers participate immediately (no
// workingOnly skip): a non-serving newcomer is removed by the hard-stall kick,
// not by starving it of work.
func (sm *SyncManager) fillAllBlockRequests() {
	if len(sm.blockPeers) == 0 {
		return
	}

	_, bestHeaderHeight := sm.chain.BestHeader()
	bestHeight := sm.chain.BestSnapshot().Height
	if bestHeight >= bestHeaderHeight {
		return
	}

	for {
		assignedAny := false
		for _, peer := range sm.blockPeersByLoad() {
			state := sm.peerStates[peer]
			if state == nil {
				continue
			}
			room := maxInFlightPerPeer - len(state.requestedBlocks)
			if room <= 0 {
				continue
			}
			if sm.fetchHeaderBlocksLimited(peer, room) > 0 {
				assignedAny = true
			}
		}
		if !assignedAny {
			return
		}
	}
}

// fetchHeaderBlocksLimited requests up to maxNew header-chain blocks from peer.
func (sm *SyncManager) fetchHeaderBlocksLimited(peer *peerpkg.Peer, maxNew int) int {
	if peer == nil || maxNew <= 0 {
		return 0
	}
	gdmsg := sm.buildBlockRequest(peer, maxNew)
	if len(gdmsg.InvList) == 0 {
		return 0
	}
	peer.QueueMessage(gdmsg, nil)
	return len(gdmsg.InvList)
}

// headersCaughtUp reports whether we have no peers advertising headers beyond
// our best header tip (block download may proceed).
func (sm *SyncManager) headersCaughtUp() bool {
	_, bestHeaderHeight := sm.chain.BestHeader()
	return len(sm.fetchHigherPeers(bestHeaderHeight)) == 0
}

// isBlockDownloadPeer reports whether peer is in the parallel IBD pool.
func (sm *SyncManager) isBlockDownloadPeer(peer *peerpkg.Peer) bool {
	_, ok := sm.blockPeers[peer]
	return ok
}

// noteBlockPeerProgress records that peer delivered a connected IBD block.
func (sm *SyncManager) noteBlockPeerProgress(peer *peerpkg.Peer) {
	state, exists := sm.peerStates[peer]
	if !exists {
		return
	}

	state.blocksDelivered++
	state.lastBlockProgress = time.Now()
	sm.lastProgressTime = time.Now()

	// After a peer proves it can serve blocks, try growing the pool.
	sm.maybeGrowBlockPeerPool()
}

// maybeGrowBlockPeerPool adds one unproven candidate when we have a working
// peer and spare pool capacity. Newcomers race tip-ahead heights immediately
// (fillAll does not skip undelivered peers); this just opens a pool slot so a
// candidate can start racing without waiting for the next ensureBlockPeers.
func (sm *SyncManager) maybeGrowBlockPeerPool() {
	if len(sm.blockPeers) >= maxParallelBlockPeers {
		return
	}
	haveWorking := false
	for peer := range sm.blockPeers {
		if state := sm.peerStates[peer]; state != nil && state.blocksDelivered > 0 {
			haveWorking = true
			break
		}
	}
	if !haveWorking {
		return
	}

	bestHeight := sm.chain.BestSnapshot().Height
	for peer, state := range sm.peerStates {
		if len(sm.blockPeers) >= maxParallelBlockPeers {
			return
		}
		if !state.syncCandidate {
			continue
		}
		if _, exists := sm.blockPeers[peer]; exists {
			continue
		}
		if peer.LastBlock() <= bestHeight {
			continue
		}
		sm.addBlockPeer(peer)
		// Give the newcomer a tip-adjacent priority probe so it can prove
		// itself; if it notfounds/stalls it will be kicked quickly.
		sm.probeNewBlockPeer(peer)
		return
	}
}

// probeNewBlockPeer assigns a single missing tip-adjacent block to a newly
// added peer so it can earn blocksDelivered (or get kicked on notfound/stall).
func (sm *SyncManager) probeNewBlockPeer(peer *peerpkg.Peer) {
	state := sm.peerStates[peer]
	if state == nil {
		return
	}
	// Send one getdata immediately so a freshly added peer starts racing
	// tip-ahead heights without waiting for the next fillAll cycle.
	if sm.fetchHeaderBlocksLimited(peer, 1) > 0 {
		log.Infof("IBD probe getdata → %s", peer.Addr())
	}
}

// raceEligibleHeight reports whether height may be requested from more than
// one IBD peer. Racing is the default fetch strategy: every tip-ahead height
// is getdata'd from all willing pool peers and the first delivery wins; late
// copies are dropped via isLateRaceBlock without disconnecting the loser.
// Racing trades redundant WAN bandwidth for min-latency per block and makes
// IBD robust to a single peer stalling — the product priority, not peer
// fairness. Exclusive partitioning was removed: it multiplied aggregate
// throughput only when no peer stalled, but a single trickling peer stranded
// its exclusive range until a 3-minute hard-stall kick.
func (sm *SyncManager) raceEligibleHeight(height int32) bool {
	if !sm.ibdMode || height < 1 {
		return false
	}
	tip := sm.chain.BestSnapshot().Height
	return height > tip
}

// peersRequestingBlock counts peers that currently have hash in-flight.
func (sm *SyncManager) peersRequestingBlock(hash chainhash.Hash) int {
	n := 0
	for _, state := range sm.peerStates {
		if state == nil {
			continue
		}
		if _, ok := state.requestedBlocks[hash]; ok {
			n++
		}
	}
	return n
}

// markBlockRequested records an outstanding getdata for hash from peer.
func (sm *SyncManager) markBlockRequested(peer *peerpkg.Peer, hash chainhash.Hash) {
	state := sm.peerStates[peer]
	if state == nil {
		return
	}
	state.requestedBlocks[hash] = struct{}{}
	sm.requestedBlocks[hash] = struct{}{}
	delete(sm.priorityBlocks, hash)
}

// clearAllBlockRequests drops hash from every peer's in-flight map and the
// global set. Used when the race winner's block arrives so losers' late
// copies are no longer "requested" by those peers.
func (sm *SyncManager) clearAllBlockRequests(hash chainhash.Hash) {
	for _, state := range sm.peerStates {
		if state == nil {
			continue
		}
		delete(state.requestedBlocks, hash)
	}
	delete(sm.requestedBlocks, hash)
	delete(sm.priorityBlocks, hash)
}

// recentDeliveredTTL is how long a race-winner hash stays in recentDelivered.
// Late copies arrive within ~1 WAN RTT of the winner; 60s covers even slow
// peers with plenty of margin while bounding the map to a few thousand entries.
const recentDeliveredTTL = 60 * time.Second

// markDelivered records that an IBD block hash was accepted as the race winner,
// so late copies from losing peers can be dropped cheaply in QueueBlock. Only
// meaningful during IBD; called from the blockHandler thread.
func (sm *SyncManager) markDelivered(hash chainhash.Hash) {
	if !sm.ibdMode {
		return
	}
	sm.recentDelivered.Store(hash, time.Now().Unix())
}

// sweepRecentDelivered evicts recentDelivered entries older than the TTL so
// the map stays bounded across a long IBD. Called from the blockHandler tick.
func (sm *SyncManager) sweepRecentDelivered() {
	cutoff := time.Now().Add(-recentDeliveredTTL).Unix()
	sm.recentDelivered.Range(func(k, v interface{}) bool {
		if ts, ok := v.(int64); ok && ts < cutoff {
			sm.recentDelivered.Delete(k)
		}
		return true
	})
}

// unmarkPeerBlockRequest clears hash from one peer. If no other peer still
// holds it, the global in-flight entry is removed too.
func (sm *SyncManager) unmarkPeerBlockRequest(state *peerSyncState, hash chainhash.Hash) {
	if state == nil {
		return
	}
	delete(state.requestedBlocks, hash)
	if sm.peersRequestingBlock(hash) == 0 {
		delete(sm.requestedBlocks, hash)
	}
}

// mayRequestBlock reports whether peer may send getdata for hash at height.
//
// A hash nobody currently holds in flight is always safe to request. A hash
// already in flight on another peer is re-requested only when racing
// tip-ahead (full race-to-first): the first delivery wins and late copies are
// dropped via isLateRaceBlock without disconnecting the loser. Outside IBD
// (serial sync) or for non-tip-ahead hashes (e.g. reorg blocks at or below the
// current tip), duplicate getdata is suppressed so a single holder stays
// responsible for the block.
func (sm *SyncManager) mayRequestBlock(peer *peerpkg.Peer, hash chainhash.Hash, height int32) bool {
	state := sm.peerStates[peer]
	if state == nil {
		return false
	}
	if _, already := state.requestedBlocks[hash]; already {
		return false
	}
	if _, inflight := sm.requestedBlocks[hash]; !inflight {
		return true
	}
	return sm.raceEligibleHeight(height)
}

// isLateRaceBlock reports that an unsolicited-looking block is a race loser
// we should drop without disconnecting: we already hold it pending validate,
// or the chain already knows it.
func (sm *SyncManager) isLateRaceBlock(hash *chainhash.Hash) bool {
	if hash == nil {
		return false
	}
	if sm.hasPendingValidate(*hash) {
		return true
	}
	have, err := sm.haveInventory(wire.NewInvVect(wire.InvTypeBlock, hash))
	return err == nil && have
}

// requeuePeerBlockRequests returns a peer's outstanding IBD block hashes to
// the front of the assign line when no other peer is still racing them.
// Hashes still in-flight on another racer are left alone (they will deliver
// or notfound independently).
func (sm *SyncManager) requeuePeerBlockRequests(state *peerSyncState) int {
	if state == nil {
		return 0
	}
	if sm.priorityBlocks == nil {
		sm.priorityBlocks = make(map[chainhash.Hash]struct{})
	}

	n := 0
	for blockHash := range state.requestedBlocks {
		delete(state.requestedBlocks, blockHash)
		if sm.peersRequestingBlock(blockHash) > 0 {
			continue // other racers still hold it
		}
		delete(sm.requestedBlocks, blockHash)
		sm.priorityBlocks[blockHash] = struct{}{}
		n++
	}
	state.requestedBlocks = make(map[chainhash.Hash]struct{})
	return n
}

// priorityHashesByHeight returns priority block hashes ordered by ascending
// header height so tip-adjacent gaps are filled first.
func (sm *SyncManager) priorityHashesByHeight() []chainhash.Hash {
	type item struct {
		hash   chainhash.Hash
		height int32
	}
	items := make([]item, 0, len(sm.priorityBlocks))
	for hash := range sm.priorityBlocks {
		height, err := sm.chain.HeaderHeightByHash(hash)
		if err != nil {
			// Unknown to header index; still try soon with height 0.
			items = append(items, item{hash: hash, height: 0})
			continue
		}
		items = append(items, item{hash: hash, height: height})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].height != items[j].height {
			return items[i].height < items[j].height
		}
		return items[i].hash.String() < items[j].hash.String()
	})
	out := make([]chainhash.Hash, len(items))
	for i := range items {
		out[i] = items[i].hash
	}
	return out
}

// freeAheadSlotsForPriority cancels the farthest in-flight IBD requests so
// peers have room to pull priority (requeued) hashes next to tip.
func (sm *SyncManager) freeAheadSlotsForPriority() {
	need := len(sm.priorityBlocks)
	if need == 0 || len(sm.blockPeers) == 0 {
		return
	}

	free := 0
	for peer := range sm.blockPeers {
		state := sm.peerStates[peer]
		if state == nil {
			continue
		}
		if n := maxInFlightPerPeer - len(state.requestedBlocks); n > 0 {
			free += n
		}
	}
	if free >= need {
		return
	}
	toFree := need - free

	type held struct {
		peer   *peerpkg.Peer
		hash   chainhash.Hash
		height int32
	}
	var far []held
	for peer := range sm.blockPeers {
		state := sm.peerStates[peer]
		if state == nil {
			continue
		}
		for hash := range state.requestedBlocks {
			if _, priority := sm.priorityBlocks[hash]; priority {
				continue
			}
			height, err := sm.chain.HeaderHeightByHash(hash)
			if err != nil {
				continue
			}
			far = append(far, held{peer: peer, hash: hash, height: height})
		}
	}
	sort.Slice(far, func(i, j int) bool {
		return far[i].height > far[j].height
	})

	for _, item := range far {
		if toFree <= 0 {
			break
		}
		state := sm.peerStates[item.peer]
		if state == nil {
			continue
		}
		if _, ok := state.requestedBlocks[item.hash]; !ok {
			continue
		}
		sm.unmarkPeerBlockRequest(state, item.hash)
		// Cancelled ahead-of-tip work becomes normal scan fodder later;
		// do not mark priority — tip gaps come first.
		toFree--
	}
}

// kickBlockPeer removes peer from the parallel pool, requeues its in-flight
// block hashes so they are not stranded, optionally disconnects, and refills
// the pool from remaining candidates.
func (sm *SyncManager) kickBlockPeer(peer *peerpkg.Peer, disconnect bool, reason string) {
	state, exists := sm.peerStates[peer]
	if !exists {
		return
	}
	if !sm.isBlockDownloadPeer(peer) {
		return
	}

	requeued := sm.requeuePeerBlockRequests(state)
	delete(sm.blockPeers, peer)
	// Prevent ensureBlockPeers from immediately re-adding this peer before
	// Disconnect/handleDonePeerMsg finishes removing it.
	state.syncCandidate = false

	log.Infof("IBD block peer -%s (%s); requeued %d in-flight block(s) to front of line; pool %d/%d",
		peer.Addr(), reason, requeued, len(sm.blockPeers), maxParallelBlockPeers)

	if peer == sm.syncPeer {
		sm.syncPeer = nil
		sm.pickSyncPeerFromBlockPool()
	}

	if disconnect {
		peer.Disconnect()
	}

	if len(sm.blockPeers) == 0 {
		// Emergency refill from any candidates when the pool is empty.
		sm.ensureBlockPeers()
		if len(sm.blockPeers) == 0 {
			sm.syncPeer = nil
			sm.startSync()
			return
		}
	}

	sm.freeAheadSlotsForPriority()
	sm.pickSyncPeerFromBlockPool()
	sm.fillAllBlockRequests()
	sm.maybeGrowBlockPeerPool()
}

// handleParallelStallSample ejects genuinely useless block peers: ones holding
// in-flight work but making no progress. It does NOT kick peers merely for
// being slower than the median — under race-to-first a slow peer loses races
// harmlessly and never drags tip, so a comparative "slowest peer" kick only
// churned the pool (disconnect + requeue + reconnect). A multi-peer soak
// measured 36 such kicks inflating the stall fraction to 37% (vs 21% serial)
// and pushing the 100k wall to 47.6 min (vs 29.9 serial). Header download
// still uses the single-syncPeer stall path.
func (sm *SyncManager) handleParallelStallSample() {
	if len(sm.blockPeers) == 0 {
		return
	}

	now := time.Now()

	// Hard-stall kicks: a peer holding in-flight work but making no block
	// progress. Unproven peers (no blocks delivered) fail fast at 30s so a
	// non-serving newcomer cannot pin tip-adjacent hashes for the full
	// stall duration; proven peers get the full maxStallDuration.
	for _, peer := range sm.blockPeerSnapshot() {
		state := sm.peerStates[peer]
		if state == nil {
			continue
		}
		if len(state.requestedBlocks) == 0 {
			continue
		}
		limit := maxStallDuration
		if state.blocksDelivered == 0 {
			limit = 30 * time.Second
		}
		if now.Sub(state.lastBlockProgress) <= limit {
			continue
		}
		sm.kickBlockPeer(peer, true, "stalled")
	}

	if len(sm.blockPeers) == 0 {
		// Pool emptied; resume normal startSync selection.
		sm.syncPeer = nil
		sm.startSync()
		return
	}
}

// logFetchDiagnostic emits a one-line snapshot of the fetch/validate pipeline
// state every 5s during IBD. Used to localize stalls: if requestedBlocks is
// empty during a stall the bug is on our side (we stopped asking); if it is
// full and height is frozen the peer is not serving.
func (sm *SyncManager) logFetchDiagnostic() {
	if !sm.ibdMode || len(sm.blockPeers) == 0 {
		return
	}
	tip := sm.chain.BestSnapshot()
	_, lastHdr := sm.chain.BestHeader()
	var perPeer []string
	for peer := range sm.blockPeers {
		state := sm.peerStates[peer]
		if state == nil {
			continue
		}
		perPeer = append(perPeer, fmt.Sprintf("%s inflight=%d deliv=%d",
			peer, len(state.requestedBlocks), state.blocksDelivered))
	}
	log.Infof("fetchdiag: tip=%d lasthdr=%d pendingValidate=%d "+
		"globalRequested=%d peers[%d] %s",
		tip.Height, lastHdr, len(sm.pendingValidate),
		len(sm.requestedBlocks), len(sm.blockPeers),
		strings.Join(perPeer, " "))
}

// blockPeerSnapshot copies block peer pointers for safe mutation while ranging.
func (sm *SyncManager) blockPeerSnapshot() []*peerpkg.Peer {
	peers := make([]*peerpkg.Peer, 0, len(sm.blockPeers))
	for peer := range sm.blockPeers {
		peers = append(peers, peer)
	}
	return peers
}
