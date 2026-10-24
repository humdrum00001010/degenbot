# degenbot-pool-updater

Pure-Rust pool catalog discovery from `PoolCreated` and `Initialize` events.

Each chunk commits pool metadata and exchange discovery checkpoints in one transaction. Complete tick maps come from block-pinned `erigon_getTickMaps` reads during state initialization; the updater does not reconstruct liquidity from historical Mint/Burn events.

## Usage

```toml
degenbot-pool-updater = "0.6.0-alpha.5"
```

Or: `cargo add degenbot-pool-updater` (the pre-release version must be pinned explicitly, e.g. "0.6.0-alpha.5").

Part of [degenbot](https://github.com/BowTiedDevil/degenbot) — a Rust-first MEV bot for EVM chains. The in-repo root README and `docs/` cover the full architecture; this crate is published standalone so you can depend on exactly the pieces you need.
