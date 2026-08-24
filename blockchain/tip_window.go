// Copyright (c) 2026 The btcd developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"fmt"

	"github.com/btcsuite/btcd/btcutil/v2"
)

// TipWindowForVerify builds an in-memory UTXO view at tip-n by disconnecting
// the last n main-chain blocks into a fresh view (no DB writes, no chain-state
// mutation), and returns those n blocks in height order for soft-connect +
// script-verify benchmarking.
//
// SoftConnectNext / VerifyBlockScripts can then validate the returned blocks
// against the view without committing. The caller should Clone the view before
// each timed run so SERIAL and parallel paths start from the same UTXO state.
//
// This is intended for CPU-bound verification benches against a read-only
// blocks_ffldb (e.g. a mainnet tip window streamed over NFS).
func (b *BlockChain) TipWindowForVerify(n int) (*UtxoViewpoint, []*btcutil.Block, error) {
	if n < 1 {
		return nil, nil, fmt.Errorf("TipWindowForVerify: n must be >= 1")
	}

	tipHeight := b.BestSnapshot().Height
	if tipHeight < int32(n) {
		return nil, nil, fmt.Errorf("TipWindowForVerify: tip %d shorter than window %d",
			tipHeight, n)
	}

	view := b.NewTipUtxoView()
	blocks := make([]*btcutil.Block, n)

	// Disconnect tip, tip-1, …, tip-n+1 so view.BestHash becomes tip-n.
	// Store blocks in forward height order for the subsequent soft-connect.
	for i := 0; i < n; i++ {
		h := tipHeight - int32(i)
		blk, err := b.BlockByHeight(h)
		if err != nil {
			return nil, nil, fmt.Errorf("TipWindowForVerify: BlockByHeight(%d): %w", h, err)
		}
		stxos, err := b.FetchSpendJournal(blk)
		if err != nil {
			return nil, nil, fmt.Errorf("TipWindowForVerify: FetchSpendJournal(%d): %w", h, err)
		}
		if err := view.disconnectTransactions(b.db, blk, stxos); err != nil {
			return nil, nil, fmt.Errorf("TipWindowForVerify: disconnect %d: %w", h, err)
		}
		blocks[n-1-i] = blk
	}

	return view, blocks, nil
}
