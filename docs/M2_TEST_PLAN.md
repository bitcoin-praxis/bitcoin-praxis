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
3. Serial `ProcessBlock(..., BFFastAdd)` commits height order.
4. Light blocks stay on the serial `ProcessBlock` path (`shouldParallelValidate`).

## Unit coverage

| Test | Location | Verifies |
|---|---|---|
| `TestSoftConnectAndVerifyExtendsTip` | `blockchain/parallel_validate_test.go` | Soft-connect + scripts + `BFNoScriptCheck` commit |
| `TestSoftConnectWindowParallelScripts` | `blockchain/parallel_validate_test.go` | Multi-height window + `BFFastAdd` commit |
| `TestScriptCheckWorkersBudget` | `blockchain/parallel_validate_test.go` | Nested worker budget |
| `TestParallelPipelineMatchesSerial` | `blockchain/parallel_validate_test.go` | Same UTXO fingerprint as serial |
| `TestShouldParallelValidate` | `netsync/parallel_validate_test.go` | IBD gating vs `BFFastAdd` |
| `TestFindPendingExtendingOOO` | `netsync/parallel_validate_test.go` | Out-of-order pending lookup |
| `TestVerify*MatchesBtcec` | `txscript/secp256k1verify` | libsecp vs `btcec` on valid + tampered inputs |
| `TestUtxoCacheKeepHotEvictsWhenFull` | `blockchain/utxocache_test.go` | Size flush keeps a budgeted working set |
| `TestMapSliceCompactUsesBudgetedCap` | `blockchain/utxocache_test.go` | Compact uses startup map cap, not 2× remaining |
| `go test -race ./blockchain/ ./netsync/` | CI | Race-clean |
| `TestFullBlocks` | `blockchain/fullblocks_test.go` | Consensus suite unchanged |

## What the numbers mean

**Do not cite warm-cache ~4× parallel validation.** That was SERIAL filling the sig/hash cache and DEPTH1 replaying it. Fair cold-cache parallel validation on dense mainnet is **~1.1–1.2×**. The 2–3× IBD target is met by libsecp + UTXO cache work, not by parallel scripts alone.

Reproduce the validation-wall bench (needs a local mainnet `blocks_ffldb`):

```bash
go build -o /tmp/fullvaltip ./cmd/fullvaltip
/tmp/fullvaltip -datadir <datadir>/mainnet -window 256
```

| | Number |
|---|---|
| Parallel validation alone (cold cache, dense tip) | **~1.1–1.2×** |
| libsecp per-sig vs `btcec` | **~4× ECDSA / ~4.7× Schnorr** |
| libsecp fullvaltip SERIAL / DEPTH1 | **~2.4× / ~2.8×** (UTXO fingerprint identical) |
| Testnet4 tip-to-tip vs stock (sole LAN peer, local disk) | ≈ serial (~36–38 min / ~145k); fetch-bound, not the speedup bench |

## Mainnet nocheckpoints IBD A/B

Fair full-validation A/B vs stock btcd **v0.26.0**. Checkpoints skip scripts below the latest checkpoint, so both sides run `--nocheckpoints`. Same machine, same sole LAN Core peer, same datadir family.

| | Stock | Praxis |
|---|---|---|
| Binary | btcd v0.26.0 | `praxisd` with libsecp + parallel validate + UTXO opts |
| Flags | `--nocheckpoints`, sole `connect=` to a local Core | same; compression B uses default `witness-buffer=2016` |
| Stock wall | **11d 17h 51m ≈ 281.8h** to height **960998** | — |

**Nocompress (in-band, not a finished tip wall):** keep-hot + 2 GiB UTXO cache. Dense ~570–578k **~3.8×** stock → public line **nearly 4× IBD speed increase**. Do not claim 5×.

**Compression B (default witness buffer, running):** same speed opts, `witness-buffer=2016`. Age-out skips already-cold bodies; stale-side-chain scans run once per retarget; keep-hot compact uses the budgeted map cap **after** the UTXO flush commits.

Gap-adjusted time-to-height vs stock 281.8h (OOM restart gap removed):

| Height | vs stock |
|---|---|
| ~410k | **2.0×** |
| ~581k | **2.4×** |
| ~663k | **2.7×** |
| ~732k | **2.8×** |

Stock still has most of its wall clock in the remaining dense tail. **~3× net with compression** is the expected tip landing; lock the public figure when this run tips. Do not claim 5×.

### UTXO opts (why live IBD was not 2.4× from libsecp alone)

Live IBD was **LevelDB UTXO-miss bound** (`pendingValidate=0`), not sig-bound. Shipped:

1. Parallel LevelDB Gets on ≥32 misses (`fetchMissingFromDB`, up to 8 workers).
2. Prefetch inputs for N+1 during Verify(N). Must **not** use `findInputsToFetch` (BIP30 height−1 poison).
3. Keep-hot flush: persist dirty, keep unspent, evict to 50% entry budget **and** a single budgeted map when at cap; compact after the DB transaction commits.

### Headline rules

- **Nocompress:** *nearly 4× IBD speed increase*.
- **Compression:** *heading to ~3×* until the B run tips vs 281.8h.
- **Libsecp / fullvaltip:** ~2.4× validation wall; ~4× per-sig.
- Parallel validation alone is **not** the IBD headline.
- LND / Neutrino unaffected. Consensus: `TestParallelPipelineMatchesSerial`, `TestVerify*MatchesBtcec`.

## Checklist

- [x] Parallel validation pipeline + adaptive serial-for-small blocks
- [x] libsecp256k1 cgo backend + pure-Go fallback + CI (`CGO_ENABLED=0` and cgo)
- [x] UTXO prefetch + parallel Gets + keep-hot (budgeted compact after commit)
- [x] Cold-cache `fullvaltip` (do not cite warm-cache 4×)
- [x] Testnet4 sole-LAN ≈ serial (fetch-bound)
- [ ] Compression-B mainnet nocheckpoints tip wall vs stock 281.8h (~2.8× at 732k; ~3× expected)
- [x] POSIX `posix_fadvise(SEQUENTIAL)` on hot/cold open (Windows no-op)
