# M2 Test Plan: Faster Full-Validation IBD

## Overview

M2 speeds up nocheckpoints IBD without changing consensus. Three levers:

1. **Ordered parallel validation** — scripts run off the tip lock; UTXO connect stays serial.
2. **libsecp256k1 verify** (cgo default on non-Windows; pure-Go `btcec` fallback).
3. **UTXO miss-path** — prefetch N+1 during verify N, parallel LevelDB Gets, keep-hot cache.

Wiring: root `go.mod` must `replace github.com/btcsuite/btcd/txscript/v2 => ./txscript` or the daemon links the published pure-Go module and ships no libsecp speedup.

## Design under test

1. Serial soft-connect (`SoftConnectNext` / `SoftConnectNextAfter`) builds a UTXO view without running scripts.
2. Parallel `VerifyBlockScripts` on an isolated snapshot (nested workers via `SetScriptCheckWorkers`).
3. Serial `ProcessBlock(..., BFNoScriptCheck)` commits height order.
4. Every non-`BFFastAdd` IBD body is enqueued (`shouldEnqueueIBD`). The sig
   heuristic (`shouldParallelValidate`) only chooses serial vs pipelined verify.

## Unit coverage

| Test | Location | Verifies |
|---|---|---|
| `TestSoftConnectAndVerifyExtendsTip` | `blockchain/parallel_validate_test.go` | Soft-connect + scripts + `BFNoScriptCheck` commit |
| `TestSoftConnectWindowParallelScripts` | `blockchain/parallel_validate_test.go` | Multi-height window + `BFNoScriptCheck` commit |
| `TestScriptCheckWorkersBudget` | `blockchain/parallel_validate_test.go` | Nested worker budget |
| `TestParallelPipelineMatchesSerial` | `blockchain/parallel_validate_test.go` | Same UTXO fingerprint as serial |
| `TestShouldEnqueueIBD` | `netsync/parallel_validate_test.go` | All non-fast-add IBD bodies enqueue |
| `TestShouldParallelValidate` | `netsync/parallel_validate_test.go` | Sig heuristic vs `BFFastAdd` |
| `TestLightThenHeavyFlushOrder` | `netsync/parallel_validate_test.go` | Light H+1 does not strand heavy H+2 |
| `TestFindPendingExtendingOOO` | `netsync/parallel_validate_test.go` | Out-of-order pending lookup |
| `TestVerify*MatchesBtcec` | `txscript/secp256k1verify` | libsecp vs `btcec` on valid + tampered inputs |
| `TestUtxoCacheKeepHotEvictsWhenFull` | `blockchain/utxocache_test.go` | Size flush keeps a budgeted working set |
| `TestMapSliceCompactUsesBudgetedCap` | `blockchain/utxocache_test.go` | Compact uses startup map cap, not 2× remaining |
| `go test -race ./blockchain/ ./netsync/` | CI | Race-clean |
| `TestFullBlocks` | `blockchain/fullblocks_test.go` | Consensus suite unchanged |

## Bench methodology

`fullvaltip` clears the sig/hash cache per mode. Warm-cache comparisons mix a
filled cache into the second run and overstate parallel-script speedup;
cold-cache dense mainnet is **~1.1–1.2×**. IBD speedup comes from libsecp and
the UTXO miss-path, not from parallel scripts alone.

Reproduce the validation-wall bench (needs a local mainnet `blocks_ffldb`):

```bash
go build -o /tmp/fullvaltip ./cmd/fullvaltip
/tmp/fullvaltip -datadir <datadir>/mainnet -window 256
```

| | Number |
|---|---|
| Parallel validation (cold cache, dense tip) | **~1.1–1.2×** |
| libsecp per-sig vs `btcec` | **~4× ECDSA / ~4.7× Schnorr** |
| libsecp fullvaltip SERIAL / DEPTH1 | **~2.4× / ~2.8×** (UTXO fingerprint identical) |
| Testnet4 tip-to-tip vs stock (sole LAN peer, local disk) | ≈ serial (~36–38 min / ~145k); fetch-bound |

## Mainnet nocheckpoints IBD A/B

Full-validation A/B vs stock btcd **v0.26.0**. Checkpoints skip scripts below the latest checkpoint, so both sides run `--nocheckpoints`. Same machine, same sole LAN Core peer, same datadir family.

| | Stock | Praxis |
|---|---|---|
| Binary | btcd v0.26.0 | `praxisd` with libsecp + parallel validate + UTXO opts |
| Flags | `--nocheckpoints`, sole `connect=` to a local Core | same; compression uses default `witness-buffer=2016` |
| Stock wall | **11d 17h 51m ≈ 281.8h** to height **960998** | — |

**Uncompressed** (keep-hot + 2 GiB UTXO cache): **~4×** (~3.8× at height 570–578k).

**Default compression** (`witness-buffer=2016`): same speed opts. Age-out skips already-cold bodies; stale-side-chain scans run once per retarget; keep-hot compact uses the budgeted map cap **after** the UTXO flush commits.


| Height | vs stock |
|---|---|
| ~410k | **2.0×** |
| ~581k | **2.4×** |
| ~663k | **2.7×** |
| ~732k | **2.8×** |

Dense tail still dominates stock's remaining wall. **~3×** with default compression at tip.

### UTXO opts

Live IBD was LevelDB UTXO-miss bound (`pendingValidate=0`), not sig-bound:

1. Parallel LevelDB Gets on ≥32 misses (`fetchMissingFromDB`, up to 8 workers).
2. Prefetch inputs for N+1 during Verify(N). Must **not** use `findInputsToFetch` (BIP30 height−1 poison).
3. Keep-hot flush: persist dirty, keep unspent, evict to 50% entry budget **and** a single budgeted map when at cap; compact after the DB transaction commits.

Consensus: `TestParallelPipelineMatchesSerial`, `TestVerify*MatchesBtcec`. LND / Neutrino unaffected.

## Checklist

- [x] Parallel validation pipeline + adaptive serial-for-small blocks
- [x] libsecp256k1 cgo backend + pure-Go fallback + CI (`CGO_ENABLED=0` and cgo)
- [x] UTXO prefetch + parallel Gets + keep-hot (budgeted compact after commit)
- [x] Cold-cache `fullvaltip`
- [x] Testnet4 sole-LAN ≈ serial (fetch-bound)
- [ ] Compression-B mainnet nocheckpoints tip wall vs stock 281.8h
- [x] POSIX `posix_fadvise(SEQUENTIAL)` on hot/cold open (Windows no-op)
