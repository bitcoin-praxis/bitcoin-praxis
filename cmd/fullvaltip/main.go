// Copyright (c) 2026 The btcd developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Command fullvaltip micro-benchmarks full script validation on an in-memory
// tip window streamed from a read-only blocks_ffldb (no network, no local
// FastAdd setup, no DB writes).
//
// It rewinds the last -window main-chain blocks into a UTXO view via
// TipWindowForVerify, warms the UTXO cache once, then compares timed modes
// each with a fresh sig/hash cache:
//
//	SERIAL — soft-connect + verify one block at a time, NumCPU*3 workers
//	DEPTH1 — soft-connect N+1 while verifying N on an input snapshot,
//	         full NumCPU*3 workers (current netsync pipeline shape)
//	BATCH  — soft-connect window with concurrent verifies (netsync batch)
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/txscript/v2"
)

func main() {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	defaultData := filepath.Join(home, ".btcd", "data", "mainnet")
	datadir := flag.String("datadir", defaultData,
		"datadir containing blocks_ffldb")
	window := flag.Int("window", 256, "tip blocks to rewind and verify")
	testnet4 := flag.Bool("testnet4", false, "use TestNet4Params instead of mainnet")
	flag.Parse()

	params := chaincfg.MainNetParams
	if *testnet4 {
		params = chaincfg.TestNet4Params
	}
	srcPath := filepath.Join(*datadir, "blocks_ffldb")
	fmt.Printf("source: %s\n", srcPath)

	db, err := database.Open("ffldb", srcPath, params.Net)
	if err != nil {
		fatalf("open: %v", err)
	}
	defer db.Close()

	chain, err := newChain(db, &params)
	if err != nil {
		fatalf("chain: %v", err)
	}

	tip := chain.BestSnapshot()
	fmt.Printf("tip=%d hash=%s\n", tip.Height, tip.Hash)
	fmt.Printf("rewind+verify last %d blocks (in-memory, no DB writes)\n", *window)
	fmt.Printf("CPUs=%d\n", runtime.NumCPU())

	tRewind := time.Now()
	baseView, blocks, err := chain.TipWindowForVerify(*window)
	if err != nil {
		fatalf("TipWindowForVerify: %v", err)
	}
	fmt.Printf("rewind done in %s (view at height %d)\n",
		time.Since(tRewind).Round(time.Millisecond), tip.Height-int32(*window))

	var nTx, nIn int
	for _, blk := range blocks {
		txs := blk.Transactions()
		nTx += len(txs)
		for _, tx := range txs[1:] {
			nIn += len(tx.MsgTx().TxIn)
		}
	}
	fmt.Printf("window: %d blocks, %d txs, %d inputs (%.1f tx/blk, %.1f in/blk)\n",
		len(blocks), nTx, nIn,
		float64(nTx)/float64(len(blocks)),
		float64(nIn)/float64(len(blocks)))

	report := func(label string, total, softD, scriptD time.Duration) {
		sec := total.Seconds()
		fmt.Printf("\n== %s ==\n", label)
		fmt.Printf("wall:         %s\n", total.Round(time.Millisecond))
		fmt.Printf("soft-connect: %s (%.1f%%)\n", softD.Round(time.Millisecond), 100*softD.Seconds()/sec)
		fmt.Printf("scripts:      %s (%.1f%%)\n", scriptD.Round(time.Millisecond), 100*scriptD.Seconds()/sec)
		fmt.Printf("blk/s:        %.2f\n", float64(len(blocks))/sec)
		fmt.Printf("tx/s:         %.2f\n", float64(nTx)/sec)
		fmt.Printf("input/s:      %.2f\n", float64(nIn)/sec)
	}

	resetHeights := func() {
		for _, b := range blocks {
			b.SetHeight(btcutil.BlockHeightUnknown)
		}
	}

	coldCaches := func() {
		chain.ReplaceScriptCaches(
			txscript.NewSigCache(1000000),
			txscript.NewHashCache(1000000),
		)
	}

	// One discarded SERIAL pass warms the UTXO cache so all timed modes share
	// the same NFS-hit profile; each timed mode still gets a cold sig/hash cache.
	fmt.Printf("warming UTXO cache (discarded SERIAL)...\n")
	coldCaches()
	_, _ = runSerial(chain, baseView.Clone(), blocks, resetHeights, report, true)

	coldCaches()
	serial, serialFP := runSerial(chain, baseView.Clone(), blocks, resetHeights, report, false)
	coldCaches()
	depth1, depth1FP := runDepth1(chain, baseView.Clone(), blocks, resetHeights, report)
	coldCaches()
	batch, batchFP := runBatch(chain, baseView.Clone(), blocks, resetHeights, report)

	// Consensus gate: every mode soft-connects + verifies the same window, so
	// the final UTXO set must be identical across SERIAL / DEPTH1 / BATCH. A
	// divergence here means the parallel pipeline mutates the UTXO set
	// differently than serial — a consensus bug, not a perf regression.
	if !bytes.Equal(serialFP[:], depth1FP[:]) || !bytes.Equal(serialFP[:], batchFP[:]) {
		fatalf("CONSENSUS DIVERGENCE: serial=%s depth1=%s batch=%s",
			serialFP, depth1FP, batchFP)
	}
	fmt.Printf("consensus OK: SERIAL/DEPTH1/BATCH agree on UTXO fingerprint %s\n",
		serialFP)

	fmt.Printf("\nvs SERIAL:  DEPTH1 %.2fx  BATCH %.2fx\n",
		serial.Seconds()/depth1.Seconds(),
		serial.Seconds()/batch.Seconds())
}

func runSerial(chain *blockchain.BlockChain, view *blockchain.UtxoViewpoint,
	blocks []*btcutil.Block, resetHeights func(),
	report func(string, time.Duration, time.Duration, time.Duration),
	quiet bool) (time.Duration, chainhash.Hash) {

	resetHeights()
	workers := runtime.NumCPU() * 3
	blockchain.SetScriptCheckWorkers(workers)
	defer blockchain.SetScriptCheckWorkers(0)

	var softD, scriptD time.Duration
	var prev *blockchain.SoftConnectResult
	t0 := time.Now()
	for i, b := range blocks {
		s0 := time.Now()
		res, err := chain.SoftConnectNextAfter(b, view, prev)
		if err != nil {
			fatalf("SERIAL SoftConnect[%d]: %v", i, err)
		}
		softD += time.Since(s0)

		s1 := time.Now()
		if err := chain.VerifyBlockScripts(res); err != nil {
			fatalf("SERIAL Verify[%d]: %v", i, err)
		}
		scriptD += time.Since(s1)
		prev = res
		view = res.View
	}
	total := time.Since(t0)
	if !quiet {
		report(fmt.Sprintf("SERIAL (%d workers/block)", workers), total, softD, scriptD)
	} else {
		fmt.Printf("  warm done in %s\n", total.Round(time.Millisecond))
	}
	return total, view.Fingerprint()
}

func runDepth1(chain *blockchain.BlockChain, view *blockchain.UtxoViewpoint,
	blocks []*btcutil.Block, resetHeights func(),
	report func(string, time.Duration, time.Duration, time.Duration)) (time.Duration, chainhash.Hash) {

	resetHeights()
	workers := runtime.NumCPU() * 3
	blockchain.SetScriptCheckWorkers(workers)
	defer blockchain.SetScriptCheckWorkers(0)

	var softD, scriptD time.Duration
	var prev *blockchain.SoftConnectResult
	var ready *blockchain.SoftConnectResult
	var readyErr error
	t0 := time.Now()
	for i := 0; i < len(blocks); i++ {
		var res *blockchain.SoftConnectResult
		var err error
		s0 := time.Now()
		if ready != nil || readyErr != nil {
			res, err = ready, readyErr
			ready, readyErr = nil, nil
		} else {
			res, err = chain.SoftConnectNextAfter(blocks[i], view, prev)
		}
		softD += time.Since(s0)
		if err != nil {
			fatalf("DEPTH1 SoftConnect[%d]: %v", i, err)
		}
		prev = res

		errCh := make(chan error, 1)
		s1 := time.Now()
		go func(res *blockchain.SoftConnectResult) {
			errCh <- chain.VerifyBlockScripts(res)
		}(res)

		if i+1 < len(blocks) {
			s2 := time.Now()
			ready, readyErr = chain.SoftConnectNextAfter(blocks[i+1], view, prev)
			softD += time.Since(s2)
		}

		if vErr := <-errCh; vErr != nil {
			fatalf("DEPTH1 Verify[%d]: %v", i, vErr)
		}
		scriptD += time.Since(s1)
		if readyErr != nil {
			fatalf("DEPTH1 SoftConnect[%d+1]: %v", i, readyErr)
		}
	}
	total := time.Since(t0)
	report(fmt.Sprintf("DEPTH1 (full %d workers, soft-connect overlap)", workers), total, softD, scriptD)
	return total, view.Fingerprint()
}

func runBatch(chain *blockchain.BlockChain, view *blockchain.UtxoViewpoint,
	blocks []*btcutil.Block, resetHeights func(),
	report func(string, time.Duration, time.Duration, time.Duration)) (time.Duration, chainhash.Hash) {

	resetHeights()
	batchSize := runtime.NumCPU()
	if batchSize > len(blocks) {
		batchSize = len(blocks)
	}
	workersTotal := runtime.NumCPU() * 3

	var softD, scriptD time.Duration
	var prev *blockchain.SoftConnectResult
	t0 := time.Now()
	for off := 0; off < len(blocks); {
		n := batchSize
		if off+n > len(blocks) {
			n = len(blocks) - off
		}
		batch := blocks[off : off+n]

		s0 := time.Now()
		if err := chain.PrefetchBlocksInputs(view, batch); err != nil {
			fatalf("BATCH Prefetch[%d]: %v", off, err)
		}
		softD += time.Since(s0)

		// Match netsync scriptWorkersPerBlock: full NumCPU*3 per concurrent
		// block (not split across the batch). Oversubscribe is intentional —
		// dense blocks already saturate with intra-block parallelism, but
		// multi-block verify still hides soft-connect and load imbalance.
		blockchain.SetScriptCheckWorkers(workersTotal)

		errs := make([]error, n)
		results := make([]*blockchain.SoftConnectResult, n)
		var verifyWG sync.WaitGroup
		sSoft := time.Now()
		for i, b := range batch {
			res, err := chain.SoftConnectNextAfter(b, view, prev)
			if err != nil {
				fatalf("BATCH SoftConnect[%d]: %v", off+i, err)
			}
			results[i] = res
			prev = res
			verifyWG.Add(1)
			go func(idx int, res *blockchain.SoftConnectResult) {
				defer verifyWG.Done()
				errs[idx] = chain.VerifyBlockScripts(res)
			}(i, res)
		}
		softD += time.Since(sSoft)

		s1 := time.Now()
		verifyWG.Wait()
		scriptD += time.Since(s1)
		blockchain.SetScriptCheckWorkers(0)

		for i, err := range errs {
			if err != nil {
				fatalf("BATCH Verify[%d]: %v", off+i, err)
			}
		}
		view = results[n-1].View
		off += n
	}
	total := time.Since(t0)
	report(fmt.Sprintf("BATCH (batch=%d, full %d workers/block)", batchSize, workersTotal), total, softD, scriptD)
	return total, view.Fingerprint()
}

func newChain(db database.DB, params *chaincfg.Params) (*blockchain.BlockChain, error) {
	p := *params
	return blockchain.New(&blockchain.Config{
		DB:               db,
		ChainParams:      &p,
		Checkpoints:      nil,
		TimeSource:       blockchain.NewMedianTime(),
		SigCache:         txscript.NewSigCache(1000000),
		HashCache:        txscript.NewHashCache(1000000),
		UtxoCacheMaxSize: 512 << 20,
	})
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
