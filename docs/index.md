# Bitcoin-Praxis documentation

Bitcoin-Praxis is a Bitcoin full node forked from btcd. The daemon is
**`praxisd`**. Consensus rules match Bitcoin Core; storage, IBD, wallet, and
mining work live below or beside the consensus layer.

## Fork docs

* [Roadmap](ROADMAP.md) — milestones M1–M5
* [M1 test plan](M1_TEST_PLAN.md) — witness-separated storage matrix and tests we ran
* [M2 test plan](M2_TEST_PLAN.md) — parallel validation, libsecp, UTXO cache, mainnet IBD

libsecp256k1 (cgo) is the default verify backend on non-Windows (~4× per-sig,
~2.4× validation wall). Nocheckpoints IBD is **~4×** uncompressed and **~3×**
with the default witness-buffer. See the M2 test plan.

## Contents

* [Installation](installation.md)
* [Update](update.md)
* [Configuration](configuration.md)
* [Configuring TOR](configuring_tor.md)
* [Docker](using_docker.md)
* [Controlling](controlling.md)
* [Mining](mining.md)
* [Wallet](wallet.md)
* [Developer resources](developer_resources.md)
* [JSON RPC API](json_rpc_api.md)
* [Code contribution guidelines](code_contribution_guidelines.md)
* [Code formatting rules](code_formatting_rules.md)
* [Contact](contact.md)
