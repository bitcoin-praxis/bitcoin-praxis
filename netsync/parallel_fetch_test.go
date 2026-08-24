// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/peer"
	"github.com/stretchr/testify/require"
)

// TestParallelBlockFetchRacesWork verifies that under full race-to-first every
// pool peer requests the same tip-ahead heights (first delivery wins, late
// copies dropped), and that kicking a peer leaves its hashes on the remaining
// racers rather than stranding them.
func TestParallelBlockFetchRacesWork(t *testing.T) {
	t.Parallel()

	params := chaincfg.RegressionNetParams
	params.Checkpoints = nil

	sm, tearDown := makeMockSyncManager(t, &params)
	defer tearDown()

	const totalBlocks = 48
	blocks := generateTestBlocks(t, &params, totalBlocks)

	peer1 := newSyncCandidate(t, sm, int32(totalBlocks))
	peer2 := newSyncCandidate(t, sm, int32(totalBlocks))
	peer3 := newSyncCandidate(t, sm, int32(totalBlocks))

	// Install headers so block fetch can proceed.
	for _, block := range blocks {
		header := &block.MsgBlock().Header
		_, err := sm.chain.ProcessBlockHeader(
			header, blockchain.BFNone, false)
		require.NoError(t, err)
	}

	sm.startParallelBlockFetch()
	require.Len(t, sm.blockPeers, 3)
	require.NotNil(t, sm.syncPeer)
	require.True(t, sm.ibdMode)

	assigned := make(map[chainhash.Hash][]*peer.Peer)
	for _, p := range []*peer.Peer{peer1, peer2, peer3} {
		state := sm.peerStates[p]
		require.NotEmpty(t, state.requestedBlocks)
		for hash := range state.requestedBlocks {
			assigned[hash] = append(assigned[hash], p)
			_, inGlobal := sm.requestedBlocks[hash]
			require.True(t, inGlobal)
		}
	}

	// Full racing: every assigned height is tip-ahead (race-eligible) and
	// the same heights are requested from multiple peers.
	tip := sm.chain.BestSnapshot().Height
	raced := 0
	for hash, peers := range assigned {
		height, err := sm.chain.HeaderHeightByHash(hash)
		require.NoError(t, err)
		require.True(t, sm.raceEligibleHeight(height),
			"assigned height %d not race-eligible (tip=%d)", height, tip)
		if len(peers) > 1 {
			raced++
		}
	}
	require.Greater(t, raced, 0,
		"expected tip-ahead heights to race across multiple peers")

	// Kick peer1 and ensure its hashes stay on the remaining racers, not
	// stranded. With full racing peer2/peer3 already hold every kicked
	// hash, so nothing is re-prioritized.
	state1 := sm.peerStates[peer1]
	kickedHashes := make([]chainhash.Hash, 0, len(state1.requestedBlocks))
	for hash := range state1.requestedBlocks {
		kickedHashes = append(kickedHashes, hash)
	}
	require.NotEmpty(t, kickedHashes)

	sm.kickBlockPeer(peer1, false, "test kick")
	require.False(t, sm.isBlockDownloadPeer(peer1))
	require.Empty(t, state1.requestedBlocks)

	for _, hash := range kickedHashes {
		_, onKicked := state1.requestedBlocks[hash]
		require.False(t, onKicked)

		found := false
		for p := range sm.blockPeers {
			if _, ok := sm.peerStates[p].requestedBlocks[hash]; ok {
				found = true
				require.NotEqual(t, peer1, p)
				_, inGlobal := sm.requestedBlocks[hash]
				require.True(t, inGlobal)
				break
			}
		}
		require.True(t, found,
			"raced hash %v not held by any remaining peer", hash)
		_, stillPriority := sm.priorityBlocks[hash]
		require.False(t, stillPriority,
			"hash %v still raced by another peer must not be re-prioritized", hash)
	}
}

// TestStallSampleDoesNotKickSlowPeer ensures handleParallelStallSample only
// ejects genuinely useless peers — one holding in-flight work but making no
// progress — and never a peer merely for being slower than the pool median.
// Under race-to-first a slow peer loses races harmlessly and never drags tip,
// so kicking it would only churn the pool.
func TestStallSampleDoesNotKickSlowPeer(t *testing.T) {
	t.Parallel()

	params := chaincfg.RegressionNetParams
	params.Checkpoints = nil
	sm, tearDown := makeMockSyncManager(t, &params)
	defer tearDown()

	fast := newSyncCandidate(t, sm, 100)
	slow := newSyncCandidate(t, sm, 100)
	sm.blockPeers[fast] = struct{}{}
	sm.blockPeers[slow] = struct{}{}
	// Both peers are progressing (recent lastBlockProgress) and hold work,
	// but slow has delivered far fewer blocks than fast.
	sm.peerStates[fast].blocksDelivered = 1000
	sm.peerStates[slow].blocksDelivered = 1
	sm.peerStates[fast].lastBlockProgress = time.Now()
	sm.peerStates[slow].lastBlockProgress = time.Now()
	var h chainhash.Hash
	h[0] = 7
	sm.peerStates[fast].requestedBlocks[h] = struct{}{}
	sm.peerStates[slow].requestedBlocks[h] = struct{}{}

	sm.handleParallelStallSample()

	require.True(t, sm.isBlockDownloadPeer(slow),
		"slow-but-progressing peer must not be kicked")
	require.True(t, sm.isBlockDownloadPeer(fast))
}

// TestRequeuePeerBlockRequestsClearsPeerAndGlobal maps so IBD cannot stall
// with hashes stuck on a dead peer.
func TestRequeuePeerBlockRequestsClearsPeerAndGlobal(t *testing.T) {
	t.Parallel()

	params := chaincfg.RegressionNetParams
	sm, tearDown := makeMockSyncManager(t, &params)
	defer tearDown()

	p := newSyncCandidate(t, sm, 10)
	var h1, h2 chainhash.Hash
	h1[0], h2[0] = 1, 2
	sm.requestedBlocks[h1] = struct{}{}
	sm.requestedBlocks[h2] = struct{}{}
	sm.peerStates[p].requestedBlocks[h1] = struct{}{}
	sm.peerStates[p].requestedBlocks[h2] = struct{}{}

	n := sm.requeuePeerBlockRequests(sm.peerStates[p])
	require.Equal(t, 2, n)
	require.Empty(t, sm.peerStates[p].requestedBlocks)
	_, ok1 := sm.requestedBlocks[h1]
	_, ok2 := sm.requestedBlocks[h2]
	require.False(t, ok1)
	require.False(t, ok2)
	_, p1 := sm.priorityBlocks[h1]
	_, p2 := sm.priorityBlocks[h2]
	require.True(t, p1, "requeued hash should be at front of line")
	require.True(t, p2, "requeued hash should be at front of line")
}

// TestRequeueLeavesRacingPeerIntact ensures kicking one racer does not
// strand or re-priority a hash still in-flight on another peer.
func TestRequeueLeavesRacingPeerIntact(t *testing.T) {
	t.Parallel()

	params := chaincfg.RegressionNetParams
	sm, tearDown := makeMockSyncManager(t, &params)
	defer tearDown()

	a := newSyncCandidate(t, sm, 10)
	b := newSyncCandidate(t, sm, 10)
	var h chainhash.Hash
	h[0] = 9
	sm.ibdMode = true
	sm.markBlockRequested(a, h)
	sm.markBlockRequested(b, h)
	require.Equal(t, 2, sm.peersRequestingBlock(h))

	n := sm.requeuePeerBlockRequests(sm.peerStates[a])
	require.Equal(t, 0, n, "hash still raced by peer b must not go to priority")
	require.Empty(t, sm.peerStates[a].requestedBlocks)
	_, stillB := sm.peerStates[b].requestedBlocks[h]
	require.True(t, stillB)
	_, inGlobal := sm.requestedBlocks[h]
	require.True(t, inGlobal)
	_, pri := sm.priorityBlocks[h]
	require.False(t, pri)
}

// TestRaceTipAdjacentGetData verifies full race-to-first: every tip-ahead
// height is requested from both pool peers, not just a small tip window.
func TestRaceTipAdjacentGetData(t *testing.T) {
	t.Parallel()

	params := chaincfg.RegressionNetParams
	params.Checkpoints = nil
	sm, tearDown := makeMockSyncManager(t, &params)
	defer tearDown()

	const totalBlocks = 80
	blocks := generateTestBlocks(t, &params, totalBlocks)
	for _, block := range blocks {
		_, err := sm.chain.ProcessBlockHeader(
			&block.MsgBlock().Header, blockchain.BFNone, false)
		require.NoError(t, err)
	}

	peer1 := newSyncCandidate(t, sm, int32(totalBlocks))
	peer2 := newSyncCandidate(t, sm, int32(totalBlocks))
	sm.startParallelBlockFetch()
	// Force both peers into the download pool (start only seeds syncPeer).
	sm.addBlockPeer(peer1)
	sm.addBlockPeer(peer2)
	sm.fillAllBlockRequests()
	require.True(t, sm.ibdMode)

	tip := sm.chain.BestSnapshot().Height
	raced := 0
	seen := make(map[chainhash.Hash]int)
	for _, p := range []*peer.Peer{peer1, peer2} {
		for hash := range sm.peerStates[p].requestedBlocks {
			seen[hash]++
		}
	}
	for hash, n := range seen {
		height, err := sm.chain.HeaderHeightByHash(hash)
		require.NoError(t, err)
		// Every requested height must be tip-ahead and race-eligible.
		require.True(t, sm.raceEligibleHeight(height),
			"assigned height %d not race-eligible (tip=%d)", height, tip)
		if n > 1 {
			raced++
		}
	}
	// With full racing and lookahead < per-peer in-flight cap, both peers
	// hold the same heights — racing reaches beyond any fixed tip window.
	require.Greater(t, raced, 0, "expected tip-ahead hashes to race across peers")
}

// TestLateRaceArrivalDoesNotDisconnect drops the loser copy after the winner
// is accepted, without treating it as misbehavior.
func TestLateRaceArrivalDoesNotDisconnect(t *testing.T) {
	t.Parallel()

	params := chaincfg.RegressionNetParams
	params.Checkpoints = nil
	sm, tearDown := makeMockSyncManager(t, &params)
	defer tearDown()

	const totalBlocks = 8
	blocks := generateTestBlocks(t, &params, totalBlocks)
	for _, block := range blocks {
		_, err := sm.chain.ProcessBlockHeader(
			&block.MsgBlock().Header, blockchain.BFNone, false)
		require.NoError(t, err)
	}

	peer1 := newSyncCandidate(t, sm, int32(totalBlocks))
	peer2 := newSyncCandidate(t, sm, int32(totalBlocks))
	sm.ibdMode = true
	sm.blockPeers[peer1] = struct{}{}
	sm.blockPeers[peer2] = struct{}{}

	first := blocks[0]
	hash := *first.Hash()
	sm.markBlockRequested(peer1, hash)
	sm.markBlockRequested(peer2, hash)
	require.Equal(t, 2, sm.peersRequestingBlock(hash))

	sm.handleBlockMsg(&blockMsg{
		block: first,
		peer:  peer1,
		reply: make(chan struct{}, 1),
	})
	require.Equal(t, 0, sm.peersRequestingBlock(hash))
	require.True(t, sm.isLateRaceBlock(&hash),
		"winner should leave hash pending or known for late-race drop")

	// Switch off regression-net unrequested exception so a failed late-race
	// check would disconnect peer2.
	sm.chainParams = &chaincfg.MainNetParams
	sm.handleBlockMsg(&blockMsg{
		block: first,
		peer:  peer2,
		reply: make(chan struct{}, 1),
	})

	// If late-race handling failed, Disconnect would have been called.
	disconnected := make(chan struct{})
	go func() {
		peer2.WaitForDisconnect()
		close(disconnected)
	}()
	select {
	case <-disconnected:
		t.Fatal("late race loser disconnected peer2")
	case <-time.After(50 * time.Millisecond):
		// expected: still connected
	}
	require.Equal(t, 0, sm.peersRequestingBlock(hash))
}

// TestUnprovenPeerGetsRacedWork ensures newcomers are not starved once a
// working peer exists: under full race-to-first an unproven peer receives
// the same tip-ahead getdata as proven peers so it can prove itself and help
// cover a trickling peer.
func TestUnprovenPeerGetsRacedWork(t *testing.T) {
	t.Parallel()

	params := chaincfg.RegressionNetParams
	params.Checkpoints = nil
	sm, tearDown := makeMockSyncManager(t, &params)
	defer tearDown()

	const totalBlocks = 80
	blocks := generateTestBlocks(t, &params, totalBlocks)
	for _, block := range blocks {
		_, err := sm.chain.ProcessBlockHeader(
			&block.MsgBlock().Header, blockchain.BFNone, false)
		require.NoError(t, err)
	}

	working := newSyncCandidate(t, sm, int32(totalBlocks))
	newbie := newSyncCandidate(t, sm, int32(totalBlocks))
	sm.ibdMode = true
	sm.blockPeers[working] = struct{}{}
	sm.blockPeers[newbie] = struct{}{}
	sm.peerStates[working].blocksDelivered = 10 // working is proven

	// Seed tip+1 on the working peer so newbie's request races it.
	tip := sm.chain.BestSnapshot().Height
	h1, err := sm.chain.HeaderHashByHeight(tip + 1)
	require.NoError(t, err)
	sm.markBlockRequested(working, *h1)

	n := sm.fetchHeaderBlocksLimited(newbie, 8)
	require.Greater(t, n, 0, "unproven peer must get raced tip-ahead getdata")
	for hash := range sm.peerStates[newbie].requestedBlocks {
		height, err := sm.chain.HeaderHeightByHash(hash)
		require.NoError(t, err)
		require.True(t, sm.raceEligibleHeight(height),
			"unproven peer got non-race-eligible height %d", height)
	}
}

// TestQueueBlockDropsLateRaceCopy confirms a late race-to-first copy (hash
// already in recentDelivered) is dropped by QueueBlock without queueing to the
// single-threaded blockHandler, and that a fresh hash is not in recentDelivered
// so it would pass through to the queue.
func TestQueueBlockDropsLateRaceCopy(t *testing.T) {
	t.Parallel()

	params := chaincfg.RegressionNetParams
	params.Checkpoints = nil
	sm, tearDown := makeMockSyncManager(t, &params)
	defer tearDown()

	blocks := generateTestBlocks(t, &params, 4)
	winner := blocks[1]
	fresh := blocks[3]

	// Outside IBD the early-drop is inert even if a hash were recorded.
	sm.ibdMode = false
	sm.markDelivered(*winner.Hash())
	_, marked := sm.recentDelivered.Load(*winner.Hash())
	require.False(t, marked, "markDelivered must be a no-op outside IBD")

	// In IBD the winner's hash is recorded.
	sm.ibdMode = true
	sm.markDelivered(*winner.Hash())
	_, marked = sm.recentDelivered.Load(*winner.Hash())
	require.True(t, marked, "markDelivered must record the winner during IBD")

	// A late copy of the winner: dropped in QueueBlock (done signaled, nothing
	// queued to the blockHandler).
	doneLate := make(chan struct{}, 1)
	sm.QueueBlock(winner, nil, doneLate)
	select {
	case <-doneLate:
	default:
		t.Fatal("late race copy was not dropped (done not signaled)")
	}
	select {
	case m := <-sm.msgChan:
		t.Fatalf("late race copy was queued to blockHandler: %T", m)
	default:
	}

	// A fresh hash is not in recentDelivered, so it would pass the early-drop
	// gate and be queued (do not actually queue it here — the live
	// blockHandler would process a nil peer).
	_, freshMarked := sm.recentDelivered.Load(*fresh.Hash())
	require.False(t, freshMarked, "fresh hash must not be in recentDelivered")
}
