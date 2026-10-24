//! Per-domain binding-crate modules.
//!
//! `engine/` is the first inhabitant of the split binding layer; the
//! other `bot` / `bot::pool` wrappers live alongside it.

pub mod build_flights;
pub mod deployments;
pub mod dex_identity;
pub mod engine;
mod errmap;
pub mod intake;
pub mod pool;
pub mod pump;
pub mod py_bot_io;
pub mod session_registry;
#[cfg(feature = "auto-initialize")]
pub mod test_gil;
pub mod token;

// === PyBot (moved from the former root `py_bot.rs`) ===
//
// `PyO3` wrappers for `BotState` — thin Python handle over Rust-owned state.
// Implements the Polars-inspired three-layer topology (ADR-005): Rust [`BotState`]
// core → `PyBot` `#[pyclass]` wrapper holding `Arc<parking_lot::RwLock<BotState>>`
// → Python `BotState` session. `PyLiquidityPool`/`PyErc20Token` clone the same
// `Arc` so many Python handles reference one Rust-owned `BotState`.
// See: `docs/adr/ADR-005-polars-inspired-three-layer-architecture.md`.

use crate::prelude::*;
use std::sync::Arc;

use alloy::primitives::Address;

use crate::bot::engine::{hex_string_to_pool_id, SpecViolationError};
use crate::bot::errmap::{
    journal_err_to_py, map_aerodrome_build_err, map_aerodrome_registration_err, map_build_err,
    map_calc_tokens_in_err, map_calc_tokens_out_err, map_driver_err, map_no_construction_io,
    map_register_v2_err, map_register_v4_err, map_resolve_v4_identity_err, map_v2_build_err,
    map_v2_registration_err, map_v3_build_err, map_v3_registration_err, map_v4_build_err,
};

/// Narrow a Python-supplied `U256` reserve to `U112` (the on-chain `uint112`
/// width), raising `SpecViolationError` if bits ≥ 112 are set.
fn narrow_reserve(
    value: alloy::primitives::U256,
    field: &'static str,
) -> PyResult<alloy::primitives::aliases::U112> {
    degenbot_pools::spec_bounds::narrow_v2_reserve(value, field)
        .map_err(|sv| SpecViolationError::new_err(format!("{sv}")))
}
pub use crate::bot::pool::PyLiquidityPool;
use crate::bot::token::PyErc20Token;
use degenbot_bot::bot_core::PoolTickCoverage;
use degenbot_bot::bot_core::RegisteredPoolFamily;
use degenbot_bot::bot_core::{
    Bot, RegisterBalancerStablePoolParams, RegisterBalancerWeightedPoolParams,
    RegisterCurvePoolParams, RegisterV2PoolParams, RegisterV4PoolParams, V4PoolKey,
};
use degenbot_uniswap::dex_identity::DexVariant;
use pyo3::types::{PyDict, PyList};
use pyo3::Bound;

// ---------------------------------------------------------------------------
// PyBot — owns the Bot orchestrator; hands out shared BotState Arcs
// ---------------------------------------------------------------------------

/// Python handle to the per-chain `Bot` orchestrator (ADR-006 D4).
///
/// Python constructs a `PyBot` (or receives a shared handle), registers
/// pools/tokens, then reads results. `PyBot` owns a [`Bot`] via `Arc` and hands
/// out clones of its shared `Arc<RwLock<BotState>>` (`state_arc`) so
/// `PyLiquidityPool` / `PyErc20Token` / the engine wrapper all reach ONE
/// Rust-owned `BotState`; `BlockPump` clones the same `Arc<Bot>` so its
/// `dispatch_log` writes flow through to the engine's reads (N handles → one
/// state — the Polars three-layer invariant, preserved + generalized by D4).
#[pyclass(name = "Bot", skip_from_py_object, module = "degenbot._ffi")]
pub struct PyBot {
    bot: Arc<Bot>,
    /// ADR-006 D4 (T3): the pump lifecycle state, shared with the
    /// `PyArbEngine` this bot owns. `None` until an engine is
    /// constructed against this bot (the engine attaches its `Arc<PumpState>`
    /// back here during `new()`). Once attached, the three pump methods —
    /// `subscribe`, `backfill_from_snapshot`, `resume` — are drivable from
    /// `PyBot` (the D4 owner) and read/write the SAME `EngineDriver` the
    /// engine's snapshot/solve slices read.
    pump: parking_lot::Mutex<Option<Arc<degenbot_bot::arb_engine::EngineDriver>>>,
    /// PRG-1: the engine-internal single-flight build table —
    /// claims collapsed into the Rust build path (was the `PoolBuildClaims`
    /// FFI peer + the Python `(family, key)` claim driver).
    flights: build_flights::BuildFlights,
}

/// Crate-internal Rust surface on `PyBot` (not Python-visible).
impl PyBot {
    /// Hand out a clone of the shared `Arc<Bot>` so `BlockPump` (and other
    /// sibling consumers) drive the SAME `Bot`'s `dispatch_log` + state that
    /// `PyBot` owns (ADR-006 D4: pump lifecycle relocation onto `Bot`).
    #[must_use]
    pub(crate) fn bot_arc(&self) -> Arc<Bot> {
        Arc::clone(&self.bot)
    }

    // ===================== PRG-1 build-flight seam ==================

    /// Re-raise a single-flight peer's published failure: the exception value
    /// the leader raised, converted back into a `PyErr` under the caller's
    /// GIL token.
    fn py_err_from_exc(py: Python<'_>, exc: Py<pyo3::exceptions::PyBaseException>) -> pyo3::PyErr {
        pyo3::PyErr::from_value(exc.into_bound(py).into_any())
    }

    /// Run one pool build under the engine-internal single flight (PRG-1):
    /// `precheck` answers from the `BotState` registry of record, `lead` runs
    /// the full build choreography (fetch → register) exactly once under the
    /// `(family, chain_id, key)` claim, and concurrent peers park GIL-free on
    /// the claim and wake with the published result (id or failure).
    ///
    /// Publication is strictly after the leader's `BotState` insertion, so a
    /// `Built` wake never precedes the registry. A lead whose claim window
    /// closed before it subscribed loops back through `precheck`; past a
    /// defensive retry bound the call falls through to an unclaimed build
    /// (the terminal registration refuses duplicates exactly as the legacy
    /// path did).
    fn run_flight<T>(
        &self,
        py: Python<'_>,
        key: &build_flights::FlightKey,
        mut precheck: impl FnMut() -> PyResult<Option<T>>,
        lead: impl FnOnce() -> PyResult<(u64, T)>,
    ) -> PyResult<T> {
        let mut iterations: u32 = 0;
        loop {
            if let Some(payload) = precheck()? {
                return Ok(payload);
            }
            match self.flights.enter(key) {
                build_flights::FlightEnter::Lead(guard) => {
                    let outcome = lead();
                    match outcome {
                        Ok((pool_id, payload)) => {
                            guard.publish_built(pool_id);
                            return Ok(payload);
                        }
                        Err(err) => {
                            guard.publish_failed(py, &err);
                            return Err(err);
                        }
                    }
                }
                build_flights::FlightEnter::Peer(slot) => {
                    // Park GIL-free; `Built` re-answers through `precheck`, a
                    // failure re-raises the leader's exact exception.
                    if let build_flights::Parked::Failed(exc) = slot.wait(py) {
                        return Err(Self::py_err_from_exc(py, exc));
                    }
                }
            }
            iterations += 1;
            if iterations > 8 {
                return lead().map(|(_, payload)| payload);
            }
        }
    }

    /// The address-keyed registry-of-record payload for a V2 build: identity
    /// straight off the registered entry (parity with the builder's return
    /// surface — token0/token1/address/variant — read from the SAME source).
    /// The shaping is core-owned ([`Bot::registered_v2_payload`]); this
    /// wrapper only releases the GIL across the read.
    fn registered_v2_payload(
        &self,
        py: Python<'_>,
        addr: &Address,
    ) -> Option<(u64, String, String, String, String)> {
        // GIL hygiene: the read guard is acquired inside the core method,
        // which runs under this py.detach (inversion class).
        py.detach(|| self.bot.registered_v2_payload(addr))
    }

    /// The V3 twin of [`Self::registered_v2_payload`] — family string
    /// resolved from the registered `factory` exactly as the builder's
    /// return surface does.
    fn registered_v3_payload(
        &self,
        py: Python<'_>,
        addr: &Address,
        chain_id: u64,
    ) -> Option<(u64, String, String, String, String)> {
        // GIL hygiene: the read guard is acquired inside the core method,
        // which runs under this py.detach (inversion class).
        py.detach(|| self.bot.registered_v3_payload(addr, chain_id))
    }

    /// The id-only registry-of-record payload for the Aerodrome / Balancer
    /// builds (their build adapters return just the pool id).
    fn registered_family_pool_id(
        &self,
        py: Python<'_>,
        addr: &Address,
        want: RegisteredPoolFamily,
    ) -> Option<u64> {
        // GIL hygiene: the read guard is acquired inside the core method,
        // which runs under this py.detach (inversion class).
        py.detach(|| self.bot.registered_family_pool_id(addr, want))
    }

    /// The V4 registry-of-record payload (`pool_manager`, `pool_id`)-keyed — the
    /// already-registered fast path, EXTRACTED so the single-flight peer-wait
    /// twin uses the same identity/fee/coverage surface as the original
    /// inline block: identity from the core's immutable pool key,
    /// protocol fee from the core's state machine, coverage as recorded.
    fn registered_v4_payload(
        &self,
        py: Python<'_>,
        pm: Address,
        pid: &[u8; 32],
    ) -> PyResult<Option<degenbot_bot::bot_core::registration::V4RegisteredPayload>> {
        // PRG-2: the keyed registration gate refuses an immutable-admission
        // pool (dynamic fee / fee-exceeds-encoder-limit) BEFORE any RPC work
        // on the registration path, with the exact exception class the live
        // registration refusal would have raised (map_register_v4_err). The
        // verdict consult + payload shaping are core-owned
        // ([`Bot::registered_v4_payload`]); this wrapper only releases the
        // GIL across the read and maps the typed refusal.
        py.detach(|| self.bot.registered_v4_payload(pm, pid))
            .map_err(map_register_v4_err)
    }

    /// ADR-006 D4 (T3): attach the pump lifecycle state owned by a
    /// `PyArbEngine` constructed against this bot. Called from
    /// `PyArbEngine::new` when `py_bot` is supplied. After this, the
    /// pump methods on `PyBot` drive the same `PumpState` the engine reads.
    pub(crate) fn attach_pump_state(&self, driver: Arc<degenbot_bot::arb_engine::EngineDriver>) {
        *self.pump.lock() = Some(driver);
    }

    /// Borrow the attached `EngineDriver`,
    /// error if no engine was constructed against this bot.
    fn pump_state(&self) -> PyResult<Arc<degenbot_bot::arb_engine::EngineDriver>> {
        self.pump.lock().clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "No engine attached to this Bot. Construct an engine against it (py_bot=...) \
                 before calling pump lifecycle methods on Bot.",
            )
        })
    }
}

#[pymethods]
impl PyBot {
    #[new]
    #[pyo3(signature = (chain_id = 0))]
    #[must_use]
    pub fn new(chain_id: u64) -> Self {
        // ADR-006 slice 8b: the Python ``Bot`` facade is now single-chain,
        // so it passes its ``config.default_chain_id`` here. The ``chain_id = 0``
        // default keeps the bare ``PyBot()`` lower-level test fixtures (which
        // only exercise the Rust core) working without a chain invariant.
        // `Arc<Bot>` so `BlockPump` clones the same orchestrator (ADR-006 D4).
        Self {
            bot: Arc::new(Bot::new(chain_id)),
            pump: parking_lot::Mutex::new(None),
            flights: build_flights::BuildFlights::default(),
        }
    }

    /// PRG-3: whether the fleet hosts the registration intake (the
    /// `fleet.stance=fleet` migration stance installed this process's
    /// intake boot descriptor at engine construction). The Python driver
    /// reads this ONCE at pipeline construction (stance like the executor
    /// field — never re-read on the hot path).
    pub fn registration_fleet_hosted(&self) -> bool {
        degenbot_bot::fleet_intake::registration_boot_installed()
    }

    /// PRG-3: submit one intake unit (a pool-build callable) to the fleet
    /// intake executor. Returns a receipt whose `.wait()` joins the unit.
    /// Refuses loudly under the legacy stance — the driver must keep the
    /// incumbent worker pool there.
    ///
    /// FF-T1: a refused fleet boot raises the TYPED `BootRefused`
    /// (detected budget + floor + one operator hint) at this submit and at
    /// every submit after it (the sticky materializer); the host process
    /// survives — the library never aborts on the boot-refusal arm.
    #[expect(
        clippy::unused_self,
        reason = "pymethod keeps its instance surface; the stance gate is process-global"
    )]
    fn submit_registration_unit(&self, fn_work: Py<PyAny>) -> PyResult<intake::PyIntakeReceipt> {
        // The PRG-3 stance gate is core-owned (the typed refusal carries the
        // historical message); the receipt itself stays pyo3-bound here.
        degenbot_bot::bot_core::registration::ensure_registration_fleet_hosted()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        intake::submit(fn_work)
    }

    /// Return a fresh async iterator over `newHeads` block notifications —
    /// the settlement bot's authoritative block clock .
    ///
    /// ADR-027 completion : the block-clock pipe is
    /// coordinator-owned, so this method lives on `PyBot` (the pump-lifecycle
    /// handle) — NOT on the engine wrapper, which is out of the block path
    /// entirely. The receiver is born on the shared `PumpState` beside the
    /// pipe's sender and is handed out exactly once.
    ///
    /// Each yielded item is a dict: `number`, `timestamp`,
    /// `base_fee_per_gas` (int | None), `gas_used`, `gas_limit`.
    ///
    /// Can be called at most once (the receiver is moved into the iterator).
    /// Subsequent calls raise `RuntimeError`. Raises `RuntimeError` too if
    /// no engine was constructed against this bot (no pump state).
    ///
    /// # Errors
    /// `RuntimeError` if no pump state is attached (no engine constructed
    /// against this bot) or the receiver was already handed out.
    pub fn block_stream(&self) -> PyResult<crate::bot::engine::BlockStream> {
        use degenbot_bot::bot_core::bot::BlockStreamError;
        // D4 state: the pump `Mutex<Option<Arc<EngineDriver>>>` is read here;
        // the once-only receiver hand-off is the core `Bot::block_stream`.
        let pump = self.pump.lock().clone();
        let block_rx = Bot::block_stream(pump.as_deref()).map_err(|e| match e {
            BlockStreamError::NoPumpState => pyo3::exceptions::PyRuntimeError::new_err(
                "no pump state: construct an engine against this Bot first",
            ),
            BlockStreamError::AlreadyTaken => {
                pyo3::exceptions::PyRuntimeError::new_err("block_stream() can only be called once")
            }
        })?;
        Ok(crate::bot::engine::BlockStream::new(block_rx))
    }

    /// Attach a construction-I/O handle built from a Python provider + an
    /// optional `database_path` (architecture review 2025-07-18 / candidate 1).
    /// The Python `Bot.__init__` calls this after constructing `PyBot` so the
    /// 7 generic RPC + 12 DB atomic methods on `PyBotIo` can delegate through
    /// the core trait objects. Alloy-only — a non-alloy provider raises
    /// `RuntimeError` (Q6-narrow; the 27 choreography wrappers keep their
    /// Python-delegation fallback this slice, deleted with the builder-
    /// choreography port).
    ///
    /// `database_path = None` → the DB half is `NoDb` (every read returns
    /// `None`/empty, the no-DB path). `Some(path)` → a held `DegenbotDb` is
    /// opened (WAL permits the concurrent connection) + wrapped in
    /// `DegenbotDbConstruction`; construction DB reads route through this held
    /// connection (no per-call `DegenbotDb::open`).
    #[pyo3(signature = (provider, database_path=None))]
    #[expect(clippy::unnecessary_wraps, clippy::needless_pass_by_value)]
    fn attach_construction_io(
        &self,
        py: Python<'_>,
        provider: Bound<'_, PyAny>,
        database_path: Option<&str>,
    ) -> PyResult<()> {
        use crate::bot::py_bot_io::extract_native_alloy;
        use degenbot_bot::bot_core::construction_io::{
            AlloyRpcConstruction, ConstructionIo, DegenbotDbConstruction, NoDb,
        };

        // Q6-narrow: the trait is alloy-only. A non-alloy provider (legacy
        // test doubles that exercise the choreography fallback) yields `None`
        // here — we skip attaching, leaving `construction_io = None`. The 7
        // RPC + 12 DB methods on `PyBotIo` then fall back to the `self.alloy` /
        // Python path (the `None` branch in each). This preserves the
        // choreography-test path; a production driver MUST supply an alloy
        // provider to get the trait delegation (the migration note documents
        // this; the fallback is deleted with the builder-choreography port).
        let Some(alloy) = extract_native_alloy(&provider) else {
            return Ok(());
        };
        let rpc = std::sync::Arc::new(AlloyRpcConstruction::new((*alloy).clone()));

        // The DB half — open (write-capable; the construction executor does
        // reads AND the `update_erc20_token_metadata` write-back), with the
        // missing-file cold-start fallback to `NoDb` so the DB methods
        // return the no-DB shape (matching the original `database_path`
        // cold-start skip; a `Bot` restart after the file exists picks it
        // up) — is core-owned ([`Bot::open_construction_db`], run under this
        // py.detach).
        let db: std::sync::Arc<
            dyn degenbot_bot::bot_core::construction_io::DbConstruction + Send + Sync,
        > = match py
            .detach(|| degenbot_bot::bot_core::bot::Bot::open_construction_db(database_path))
        {
            Some(db) => {
                // Bind the session's position reader HERE, and only
                // here: this is the one point in a bot boot where the
                // session and the database handle its reads are sourced
                // from are both in hand. The reader is a lending
                // integration's, the session registry is the engine's,
                // and neither may take the other's edge, so the boot
                // that already owns both composes them. Nothing about
                // the reading is decided at this site — which rows
                // answer, how fresh they are, and which faults refuse
                // are the observer's own (`degenbot_aave::positions`).
                #[cfg(feature = "aave-updater")]
                {
                    let observer: std::sync::Arc<
                        dyn degenbot_substrate::session_registry::PositionObserver,
                    > = std::sync::Arc::new(degenbot_aave::AavePositionObserver::new(
                        std::sync::Arc::clone(&db),
                    ));
                    // A refused second install is the registry's to
                    // report: it already logs at ERROR and hands the
                    // observer back, precisely because it cannot be the
                    // site that installs one.
                    let _ = self
                        .bot
                        .session_registry()
                        .install_position_observer(observer);
                }
                std::sync::Arc::new(DegenbotDbConstruction::new(db))
            }
            None => std::sync::Arc::new(NoDb),
        };

        let io = ConstructionIo::new(db, rpc);
        // Attach to the shared `Bot` (interior-mutable `construction_io` field);
        // `Bot` is held as `Arc` by `PyBot`, so we mutate through the `RwLock`
        // rather than adopting the `Arc` out.
        self.bot.set_construction_io(io);
        Ok(())
    }

    /// PRG-2: the Python driver records registration skips into
    /// the Rust `degenbot.registration.skips` meter family; the former
    /// Python `SkipGate` memo is retired — immutable V4 admission verdicts
    /// are refused pre-RPC by the core registration gate. The label
    /// collapse + meter write are core-owned; observation only.
    #[pyo3(signature = (reason))]
    fn record_registration_skip(&self, py: Python<'_>, reason: &str) {
        // GIL hygiene: the skip-kind collapse + meter write run detached —
        // both are core-owned ([`Bot::record_registration_skip`]); no state
        // guard is involved.
        py.detach(|| self.bot.record_registration_skip(reason));
    }

    /// The earliest complete pool snapshot horizon available for pump replay.
    #[getter]
    fn snapshot_seed_block(&self, py: Python<'_>) -> Option<u64> {
        // GIL hygiene: the read guard is acquired inside the core method,
        // which runs under this py.detach (inversion class).
        py.detach(|| self.bot.snapshot_seed_block())
    }

    /// Subscribe to the WS `newHeads` + logs streams (ADR-006 D4 T3).
    ///
    /// The Bot-owned pump entry point — delegates to the shared `PumpState`
    /// attached when an engine wrapper was constructed against this bot.
    /// Blocks (sync, via the shared tokio runtime) until the first block is
    /// observed, then returns the first WS block number (the backfill target).
    ///
    /// # Errors
    /// `PyRuntimeError` if no engine is attached, the pump is already
    /// started/subscribed, or the WS subscribe fails.
    #[pyo3(signature = (rpc_url))]
    fn subscribe(&self, py: Python<'_>, rpc_url: &str) -> PyResult<u64> {
        let driver = self.pump_state()?;
        // GIL-release across the WS handshake block_on (the block_on lives in
        // the core `Bot::subscribe`): the handshake future (WS subscribe +
        // header polling) does NOT need the GIL to complete.
        py.detach(|| Bot::subscribe(&driver, rpc_url))
            .map_err(map_driver_err)
    }

    /// Resume the pump — begin normal WS processing (ADR-006 D4 T3).
    ///
    /// The snapshot→WS gap is closed automatically inside the core
    /// `BlockPump::resume_from_subscribe`; the pyo3
    /// `backfill_from_snapshot` method is retired. Delegates to the
    /// shared `PumpState`.
    fn resume(&self, py: Python<'_>) -> PyResult<()> {
        let driver = self.pump_state()?;
        // GIL-release across the backfill block_on (the block_on lives in the
        // core `Bot::resume`): the backfill (`eth_getLogs` + `BotState`
        // mutation) is pure Rust async and does not need the GIL.
        py.detach(|| Bot::resume(&driver)).map_err(map_driver_err)
    }

    /// Stop the pump and signal the Rust core to clean up (ADR-006 D4).
    ///
    /// The symmetric teardown half of the `subscribe` → `backfill_from_snapshot`
    /// → `resume` lifecycle. Sets the shutdown flag and aborts the spawned
    /// pump task so a Ctrl-C exits promptly (the pump loop otherwise blocks
    /// up to 60s on a silent WS stream). Idempotent — safe to call from both
    /// the `__aexit__` path and a signal handler. Delegates to the shared
    /// `PumpState`.
    fn stop(&self, _py: Python<'_>) -> PyResult<()> {
        let driver = self.pump_state()?;
        Bot::stop(&driver).map_err(map_driver_err)
    }

    /// Set the HTTP RPC URL used for verification (ADR-006 D4 T4).
    /// Delegates to the shared `PumpState`.
    #[pyo3(signature = (rpc_url))]
    fn set_verify_rpc_url(&self, rpc_url: &str) {
        // No engine attached → no-op (the swallowed pump_state error, now the
        // core `None` arm).
        let pump = self.pump_state().ok();
        Bot::set_verify_rpc_url(pump.as_deref(), rpc_url);
    }

    /// Set the `StateView` contract address for V4 verification (ADR-006 D4 T4).
    #[pyo3(signature = (state_view_address))]
    fn set_verify_state_view(&self, state_view_address: &str) {
        // No engine attached → no-op (the swallowed pump_state error, now the
        // core `None` arm).
        let pump = self.pump_state().ok();
        Bot::set_verify_state_view(pump.as_deref(), state_view_address);
    }

    /// Run a single V3 pool's registration verify-lifecycle end-to-end
    /// (ADR-022 D1): the core-owned `quarantine → seed-verify → drain+pin →
    /// post-drain-verify → set_live` choreography that replaces the Python
    /// driver's separate step calls. **Sparse** → immediate no-op (`Live`, no
    /// RPC); **Tracked** → verified with the mismatch tripwire before `Live`.
    /// Uses the bot's single verify provider + stored verify config (D-B/D-C).
    #[pyo3(signature = (address))]
    fn run_v3_registration_lifecycle<'py>(
        &self,
        py: Python<'py>,
        address: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::bot::pump::run_v3_registration_lifecycle(py, &self.pump_state()?, address)
    }

    /// V4 twin of `run_v3_registration_lifecycle`, keyed by
    /// (`pool_manager_address`, `pool_id_hex`). A tracked V4 pool with no
    /// `verify_state_view` configured fails fast (D-C no-config posture).
    #[pyo3(signature = (pool_manager_address, pool_id_hex))]
    fn run_v4_registration_lifecycle<'py>(
        &self,
        py: Python<'py>,
        pool_manager_address: &str,
        pool_id_hex: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::bot::pump::run_v4_registration_lifecycle(
            py,
            &self.pump_state()?,
            pool_manager_address,
            pool_id_hex,
        )
    }

    /// Register a V2 pool by contract address.
    ///
    /// Returns the auto-assigned pool ID.
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (address, token0, token1, reserve0, reserve1, gamma_numer0, fee_denom0, gamma_numer1, fee_denom1, factory, update_block=0, variant="uniswap-v2", stable_swap=false, fee_denominator=None))]
    fn register_v2_pool(
        &self,
        py: Python<'_>,
        address: &str,
        token0: &str,
        token1: &str,
        reserve0: &Bound<'_, PyAny>,
        reserve1: &Bound<'_, PyAny>,
        gamma_numer0: u64,
        fee_denom0: u64,
        gamma_numer1: u64,
        fee_denom1: u64,
        factory: &str,
        update_block: u64,
        variant: &str,
        stable_swap: bool,
        fee_denominator: Option<u64>,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let t0 = parse_address(token0)?;
        let t1 = parse_address(token1)?;
        let fac = parse_address(factory)?;
        let r0 = narrow_reserve(
            crate::conversion::alloy::extract_python_u256(reserve0)?,
            "reserve0",
        )?;
        let r1 = narrow_reserve(
            crate::conversion::alloy::extract_python_u256(reserve1)?,
            "reserve1",
        )?;
        let variant_enum = DexVariant::try_from(variant)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;

        // The write guard is acquired inside the core method
        // ([`Bot::register_v2_pool`]); the GIL is released across this
        // py.detach — never hold the GIL while parked on the BotState write.
        let pool_id = py.detach(|| {
            self.bot
                .register_v2_pool(
                    addr,
                    t0,
                    t1,
                    r0,
                    r1,
                    (gamma_numer0, fee_denom0),
                    (gamma_numer1, fee_denom1),
                    fac,
                    update_block,
                    variant_enum,
                    stable_swap,
                    fee_denominator,
                )
                .map_err(map_v2_registration_err)
        })?;
        // Telemetry: see build_v2_pool — one Jaeger node per V2 registration.
        let _reg = tracing::info_span!(
            "degenbot.pool.register",
            pool.version = "v2",
            pool.address = %addr,
            token0 = %t0,
            token1 = %t1,
            pool.id = pool_id,
        )
        .entered();
        Ok(pool_id)
    }

    /// Build + register a V2 pool through the Rust `PoolBuilder` (
    /// delegation adapter). Runs the core async builder — ALL the io
    /// choreography in Rust: immutable data, reserves, DEX-resolution incl.
    /// the Camelot branch, CREATE2 verify, deployer/init-hash, narrow reserves
    /// — on the attached `ConstructionIo`, registers into `BotState`, and
    /// returns the auto-assigned pool id for the Python caller to wrap.
    ///
    /// The `PoolBuilder` is alloy-only (Q6-narrow `ConstructionIo`): this
    /// requires an attached construction-I/O (D-B single `AlloyProvider`).
    ///
    /// # Errors
    ///
    /// `RuntimeError` if no `ConstructionIo` is attached, on a builder RPC /
    /// CREATE2 / spec / DB failure, or on a registration (already-registered /
    /// spec) failure.
    #[pyo3(signature = (address, block=None))]
    fn build_v2_pool(
        &self,
        py: Python<'_>,
        address: &str,
        block: Option<u64>,
    ) -> PyResult<(u64, String, String, String, String)> {
        let addr = parse_address(address)?;
        let chain_id = self.bot.chain_id();
        // PRG-1 registry unification: the build (pre-check → build →
        // publish) runs under the engine-internal single flight keyed by
        // (V2, chain, address). The registry-of-record pre-check answers a
        // registered address with its identity — no duplicate builder replay
        // racing into an AlreadyRegistered refusal (a race class).
        let key = build_flights::FlightKey(
            build_flights::flight_family::V2,
            chain_id,
            addr.to_checksum(None).to_lowercase(),
        );
        self.run_flight(
            py,
            &key,
            || Ok(self.registered_v2_payload(py, &addr)),
            || {
                // The fetch/build/register choreography is core-owned
                // ([`Bot::build_and_register_v2`]); the GIL is released
                // across the whole scope — never hold the GIL while parked
                // on the runtime or the BotState write.
                let (pool_id, identity) = py
                    .detach(|| self.bot.build_and_register_v2(addr, block))
                    .map_err(map_v2_build_err)?;
                Ok((
                    pool_id,
                    (pool_id, identity.0, identity.1, identity.2, identity.3),
                ))
            },
        )
    }

    /// Build + register an Aerodrome V2 pool through the Rust `PoolBuilder`
    /// (the delegation adapter) — the Aerodrome twin of
    /// [`Self::build_v2_pool`]. The core `builder::build_aerodrome_v2` runs the
    /// full `stable()`+`getFee()` + reserves + CREATE2 choreography and returns
    /// a [`RegisterAerodromeV2PoolParams`]; this adapter registers it into
    /// [`PoolEntry::AerodromeV2`]. Returns the `pool_id`.
    fn build_aerodrome_v2_pool(
        &self,
        py: Python<'_>,
        address: &str,
        block: Option<u64>,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let chain_id = self.bot.chain_id();
        // PRG-1 registry unification: single-flight + registry-of-record
        // pre-check (see `build_v2_pool`).
        let key = build_flights::FlightKey(
            build_flights::flight_family::AERODROME,
            chain_id,
            addr.to_checksum(None).to_lowercase(),
        );
        self.run_flight(
            py,
            &key,
            || Ok(self.registered_family_pool_id(py, &addr, RegisteredPoolFamily::AerodromeV2)),
            || {
                // Core-owned ([`Bot::build_and_register_aerodrome_v2`] — io
                // fetch, builder run, registration write; see `build_v2_pool`
                // for the GIL note).
                let pool_id = py
                    .detach(|| self.bot.build_and_register_aerodrome_v2(addr, block))
                    .map_err(map_aerodrome_build_err)?;
                Ok((pool_id, pool_id))
            },
        )
    }

    /// Build + register a Balancer V2 **weighted** pool through the Rust
    /// `PoolBuilder` (the delegation adapter). The core
    /// `builder::build_balancer_weighted` runs the full `getPoolId` + Vault
    /// `getPoolTokens` + `getSwapFeePercentage` + `getNormalizedWeights` +
    /// bytecode `PowVersion` detect + `decimals()` scaling-factor choreography
    /// and returns a [`RegisterBalancerWeightedPoolParams`]; this adapter
    /// registers it into [`PoolEntry::BalancerWeighted`]. Returns the `pool_id`.
    fn build_balancer_weighted_pool(
        &self,
        py: Python<'_>,
        address: &str,
        vault: &str,
        block: Option<u64>,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let vault_addr = parse_address(vault)?;
        // PRG-1 registry unification: single-flight + registry-of-record
        // pre-check (see `build_v2_pool`).
        let key = build_flights::FlightKey(
            build_flights::flight_family::BALANCER_WEIGHTED,
            self.bot.chain_id(),
            addr.to_checksum(None).to_lowercase(),
        );
        self.run_flight(
            py,
            &key,
            || {
                Ok(self.registered_family_pool_id(
                    py,
                    &addr,
                    RegisteredPoolFamily::BalancerWeighted,
                ))
            },
            || {
                // Core-owned ([`Bot::build_and_register_balancer_weighted`]
                // — see `build_v2_pool`).
                let pool_id = py
                    .detach(|| {
                        self.bot
                            .build_and_register_balancer_weighted(vault_addr, addr, block)
                    })
                    .map_err(|e| map_build_err("build_balancer_weighted_pool", e))?;
                Ok((pool_id, pool_id))
            },
        )
    }

    /// Build + register a Balancer V2 **stable** pool through the Rust
    /// `PoolBuilder` (the delegation adapter). The core
    /// `builder::build_balancer_stable` runs the `getPoolId` + Vault
    /// `getPoolTokens` + `getSwapFeePercentage` + `getAmplificationParameter`
    /// + BPT-detect + rate-provider/rate + scaling-factor + `invariant_version`
    /// resolution choreography and returns a [`RegisterBalancerStablePoolParams`];
    /// this adapter registers it into [`PoolEntry::BalancerStable`].
    /// `invariant_version` overrides the Vault-specialization heuristic.
    /// Returns the `pool_id`.
    #[pyo3(signature = (address, vault, block=None, invariant_version=None))]
    fn build_balancer_stable_pool(
        &self,
        py: Python<'_>,
        address: &str,
        vault: &str,
        block: Option<u64>,
        invariant_version: Option<u8>,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let vault_addr = parse_address(vault)?;
        // PRG-1 registry unification: single-flight + registry-of-record
        // pre-check (see `build_v2_pool`).
        let key = build_flights::FlightKey(
            build_flights::flight_family::BALANCER_STABLE,
            self.bot.chain_id(),
            addr.to_checksum(None).to_lowercase(),
        );
        self.run_flight(
            py,
            &key,
            || Ok(self.registered_family_pool_id(py, &addr, RegisteredPoolFamily::BalancerStable)),
            || {
                // Core-owned ([`Bot::build_and_register_balancer_stable`] —
                // see `build_v2_pool`).
                let pool_id = py
                    .detach(|| {
                        self.bot.build_and_register_balancer_stable(
                            vault_addr,
                            addr,
                            block,
                            invariant_version,
                        )
                    })
                    .map_err(|e| map_build_err("build_balancer_stable_pool", e))?;
                Ok((pool_id, pool_id))
            },
        )
    }

    /// Build + register a V3 pool through the Rust `PoolBuilder` (
    /// delegation adapter) — the V3 twin of [`Self::build_v2_pool`]. The tick
    /// map is a complete block-pinned Erigon snapshot. `block` defaults to the chain head.
    ///
    /// # Errors
    ///
    /// As [`Self::build_v2_pool`].
    #[pyo3(signature = (address, block=None, slot_layout=None, factories=None))]
    fn build_v3_pool(
        &self,
        py: Python<'_>,
        address: &str,
        block: Option<u64>,
        slot_layout: Option<&str>,
        factories: Option<Vec<String>>,
    ) -> PyResult<(u64, String, String, String, String)> {
        let addr = parse_address(address)?;
        let chain_id = self.bot.chain_id();
        // PRG-1 registry unification: single-flight + registry-of-record
        // pre-check (see `build_v2_pool`) — a registered V3 address answers
        // with its identity instead of replaying the RPC tick-map assembly
        // into an AlreadyRegistered refusal.
        let key = build_flights::FlightKey(
            build_flights::flight_family::V3,
            chain_id,
            addr.to_checksum(None).to_lowercase(),
        );
        self.run_flight(
            py,
            &key,
            || Ok(self.registered_v3_payload(py, &addr, chain_id)),
            || {
                use degenbot_bot::bot_core::pool_builder::route;
                // The io fetch is core-owned ([`Bot::require_construction_io`]);
                // the failure maps to the method-prefixed legacy `RuntimeError`.
                let io = py
                    .detach(|| self.bot.require_construction_io())
                    .map_err(|_| map_no_construction_io("build_v3_pool"))?;
                // The construction route (pool-construction card): ONE core
                // entry — get-or-register + route order + build + register,
                // classified on the registration taxonomy. The cockpit's
                // resolved route (the ordered factory rungs) rides the call;
                // `None` is the generic-only route (the behavior-preserving
                // default), and the LOUD unsupported-family refusal surfaces
                // as `UnsupportedPoolFamilyError` — never swallowed into
                // another rung.
                let route_plan = route::ConstructionRoute {
                    factories: factories
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|f| parse_address(f).ok())
                        .collect(),
                    generic: true,
                };
                // CL slot layout : explicit override (the Python
                // driver knows the pool class for non-JSON deployments) wins over the
                // builder's deployment-table resolution.
                let slot_override = match slot_layout {
                    None => None,
                    Some("pancakeswap") => Some(degenbot_pools::v3_state::ClSlotLayout::PancakeV3),
                    Some("uniswap") => Some(degenbot_pools::v3_state::ClSlotLayout::UniswapV3),
                    Some(other) => {
                        return Err(pyo3::exceptions::PyValueError::new_err(format!(
                            "build_v3_pool: slot_layout must be 'uniswap' or 'pancakeswap', got {other:?}"
                        )));
                    }
                };
                // The get-or-register + route-order + build + register
                // choreography is core-owned ([`Bot::build_and_register_v3`]);
                // the GIL is released across the whole scope.
                let (pool_id, identity) = py
                    .detach(|| {
                        self.bot.build_and_register_v3(
                            &io,
                            &route_plan,
                            slot_override,
                            addr,
                            block,
                        )
                    })
                    .map_err(map_v3_build_err)?;
                Ok((pool_id, (pool_id, identity.0, identity.1, identity.2, identity.3)))
            },
        )
    }

    /// Build a V4 pool via the Rust `PoolBuilder` ), registered
    /// into `BotState` via `register_v4_pool`.
    ///
    /// V4 identity is **caller-supplied** (mirrors `register_v4_pool`: the
    /// core never reads `getToken0`/`getHooks` on-chain). The builder fetches
    /// the complete tick map and live scalars from one block-pinned Erigon snapshot.
    ///
    /// The caller must register the V4 `state_view` mapping for
    /// `pool_manager` (done here, idempotent) before this returns, so the
    /// solver-state accuracy gate can verify V4 hops on-chain.
    ///
    /// # Errors
    ///
    /// As [`Self::register_v4_pool`] (builder RPC/decode errors map via
    /// [`map_builder_err`]; admission/filtering errors via
    /// [`map_register_v4_err`]).
    #[pyo3(signature = (chain_id, pool_manager, pool_id_hex, currency0=None, currency1=None, fee=None, tick_spacing=None, hook_address=None, state_view_address=None))]
    #[expect(clippy::too_many_arguments)]
    fn resolve_v4_identity(
        &self,
        py: Python<'_>,
        chain_id: u64,
        pool_manager: &str,
        pool_id_hex: &str,
        currency0: Option<&str>,
        currency1: Option<&str>,
        fee: Option<u32>,
        tick_spacing: Option<i32>,
        hook_address: Option<&str>,
        state_view_address: Option<&str>,
    ) -> PyResult<(String, String, u32, i32, u16, String, String)> {
        use degenbot_bot::bot_core::pool_builder::builder;
        let pm = parse_address(pool_manager).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "resolve_v4_identity: malformed pool_manager {pool_manager:?}: {e}"
            ))
        })?;
        let pool_id = hex_string_to_pool_id(pool_id_hex)?;
        let parse_opt = |s: Option<&str>, name: &str| -> PyResult<Option<Address>> {
            s.map(|s| {
                parse_address(s).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "resolve_v4_identity: malformed {name} {s:?}: {e}"
                    ))
                })
            })
            .transpose()
        };
        // the V4 identity (currency0/1, fee, tick_spacing, hook,
        // state_view) is resolved CORE-side — the DB two-step (manager → V4 row
        // → per-FK tokens) first, else these raw caller overrides. The override
        // fields are all optional; the resolver raises a typed `MissingIdentity`
        // (mapped to PyValueError) when neither source is complete.
        let overrides = builder::V4PoolBuildOverrides {
            currency0: parse_opt(currency0, "currency0")?,
            currency1: parse_opt(currency1, "currency1")?,
            fee,
            tick_spacing,
            hook_address: parse_opt(hook_address, "hook_address")?,
            state_view: parse_opt(state_view_address, "state_view_address")?,
        };
        // The ConstructionIo requirement + the DB-two-step/override
        // resolution live inside the core method
        // ([`Bot::resolve_v4_identity`]); this wrapper maps the typed
        // refusal to the historical method-prefixed `RuntimeError`.
        let id = py
            .detach(|| {
                self.bot
                    .resolve_v4_identity(chain_id, pm, pool_id, &overrides)
            })
            .map_err(map_resolve_v4_identity_err)?;
        Ok((
            id.currency0.to_checksum(None),
            id.currency1.to_checksum(None),
            id.fee,
            id.tick_spacing,
            // Derived mask — kept for the driver's existing flag-parity check.
            degenbot_bot::bot_core::pool_builder::builder::derive_hook_flags(id.hook_address),
            // The REAL hook address (pool-ID mismatch regression): the
            // driver must thread this through to build_v4_pool so the
            // registered pool key round-trips keccak(abi.encode(pool_key)).
            id.hook_address.to_checksum(None),
            id.state_view.to_checksum(None),
        ))
    }

    #[pyo3(signature = (pool_manager, pool_id_hex, currency0, currency1, fee, tick_spacing, hook_address, state_view_address, block=None))]
    #[expect(clippy::too_many_arguments)]
    #[expect(clippy::type_complexity)]
    fn build_v4_pool(
        &self,
        py: Python<'_>,
        pool_manager: &str,
        pool_id_hex: &str,
        currency0: &str,
        currency1: &str,
        fee: u32,
        tick_spacing: i32,
        // The REAL hook contract address (from resolve_v4_identity — DB
        // two-step or caller override; None/empty = no hook). The flag mask
        // is DERIVED here: the address is the single identity source so the
        // registered pool key round-trips keccak(abi.encode(pool_key)).
        hook_address: Option<&str>,
        state_view_address: &str,
        block: Option<u64>,
    ) -> PyResult<(
        u64,
        String,
        String,
        String,
        String,
        u32,
        i32,
        u16,
        String,
        u32,
        u32,
    )> {
        let pm = parse_address(pool_manager).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "build_v4_pool: malformed pool_manager {pool_manager:?}: {e}"
            ))
        })?;
        let pool_id = hex_string_to_pool_id(pool_id_hex)?;
        let state_view = parse_address(state_view_address).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "build_v4_pool: malformed state_view_address {state_view_address:?}: {e}"
            ))
        })?;
        // Register the Rust-owned V4 StateView registry first (ADR-005 /
        // Option 2) — the solver-state gate reads it to verify V4 hops.
        // Core-owned ([`Bot::register_v4_state_view`], LockSite::Core) under
        // this py.detach (inversion class).
        py.detach(|| self.bot.register_v4_state_view(pm, state_view));

        // PRG-1 registry unification: the build (pre-check → build → publish)
        // runs under the engine-internal single flight keyed by
        // (V4, chain, pool_manager:pool_id). The registry-of-record pre-check
        // returns the existing registration — identity from the core's
        // immutable key, protocol_fee from the core's state machine, coverage
        // as recorded — under ONE read guard and with NO RPC (the
        // missed-WS-pong 2026-08-28 fast path, now shared by single-flight
        // peers too). `block` is a no-op on
        // this path: the core-tracked state is the freshest source, and the
        // driver still runs the registration lifecycle off the returned
        // handle as usual. hook_address is NOT re-resolved here: the Python
        // driver resolves the identity (DB two-step, no RPC) before this call
        // and passes the same immutable hook address the core registered
        // with.
        let key = build_flights::FlightKey(
            build_flights::flight_family::V4,
            self.bot.chain_id(),
            format!(
                "{}:{}",
                pm.to_checksum(None).to_lowercase(),
                pool_id_hex.to_lowercase()
            ),
        );
        self.run_flight(
            py,
            &key,
            || self.registered_v4_payload(py, pm, &pool_id),
            // registered_v4_payload is now PyResult-returning (PRG-2 gate
            // consult); no further wrapping needed.
            || {
                use degenbot_bot::bot_core::pool_builder::builder;
                let hook_addr = hook_address
                    .filter(|s| !s.is_empty())
                    .map(|s| {
                        parse_address(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "build_v4_pool: malformed hook_address {s:?}: {e}"
                            ))
                        })
                    })
                    .transpose()?
                    .unwrap_or_default();
                let currency0_addr = parse_address(currency0).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "build_v4_pool: malformed currency0 {currency0:?}: {e}"
                    ))
                })?;
                let currency1_addr = parse_address(currency1).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "build_v4_pool: malformed currency1 {currency1:?}: {e}"
                    ))
                })?;
                // (builder return surface): the normalized identity echo is
                // the parsed caller-supplied identity's checksums + the
                // derived hook mask.
                let identity_ret = (
                    currency0_addr.to_checksum(None),
                    currency1_addr.to_checksum(None),
                    pm.to_checksum(None),
                    fee,
                    tick_spacing,
                    builder::derive_hook_flags(hook_addr),
                    format!("0x{}", alloy::hex::encode(pool_id)),
                );
                // The fetch/build/register choreography is core-owned
                // ([`Bot::build_and_register_v4`]); the GIL is released
                // across the whole scope.
                let (registered, coverage, protocol_fee, lp_fee) = py
                    .detach(|| {
                        self.bot.build_and_register_v4(
                            builder::V4PoolBuildIdentity {
                                pool_manager: pm,
                                state_view,
                                pool_id,
                                currency0: currency0_addr,
                                currency1: currency1_addr,
                                fee,
                                tick_spacing,
                                hook_address: hook_addr,
                            },
                            block,
                        )
                    })
                    .map_err(map_v4_build_err)?;
                // Return `(pool_id, coverage, identity..., protocol_fee, lp_fee)` so the
                // Python driver can set the companion's `_sparse_liquidity_map` (from
                // coverage), the normalized identity, and the fee overrides
                // (protocol_fee/lp_fee from the builder's own slot0 read) in one return
                // surface.
                Ok((
                    registered,
                    (
                        registered,
                        coverage,
                        identity_ret.0,
                        identity_ret.1,
                        identity_ret.2,
                        identity_ret.3,
                        identity_ret.4,
                        identity_ret.5,
                        identity_ret.6,
                        protocol_fee,
                        lp_fee,
                    ),
                ))
            },
        )
    }

    /// Prototype test-only V2 registration that bypasses CREATE2 verification.
    /// Wire to a new method on Python so tests can build a synthetic V2 pool.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn register_v2_pool_test_only(
        &self,
        address: &str,
        token0: &str,
        token1: &str,
        reserve0: &Bound<'_, PyAny>,
        reserve1: &Bound<'_, PyAny>,
        gamma_numer0: u64,
        fee_denom0: u64,
        gamma_numer1: u64,
        fee_denom1: u64,
        factory: &str,
        update_block: u64,
        variant: &str,
        stable_swap: bool,
        fee_denominator: Option<u64>,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let t0 = parse_address(token0)?;
        let t1 = parse_address(token1)?;
        let fac = parse_address(factory)?;
        let r0 = narrow_reserve(
            crate::conversion::alloy::extract_python_u256(reserve0)?,
            "reserve0",
        )?;
        let r1 = narrow_reserve(
            crate::conversion::alloy::extract_python_u256(reserve1)?,
            "reserve1",
        )?;
        let variant_enum = DexVariant::try_from(variant)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
        // T1-scan-exempt: test-only seam (pure-Rust test callers, no GIL held).
        self.bot
            .state_arc()
            .write_at(degenbot_substrate::state_lock::LockSite::Python)
            .register_v2_pool(&RegisterV2PoolParams {
                address: addr,
                token0: t0,
                token1: t1,
                reserve0: r0,
                reserve1: r1,
                fee_token0: (gamma_numer0, fee_denom0),
                fee_token1: (gamma_numer1, fee_denom1),
                factory: fac,
                deployer: fac,
                init_hash: alloy::primitives::B256::default(),
                update_block,
                variant: variant_enum,
                stable_swap,
                fee_denominator,
            })
            .map_err(map_register_v2_err)
    }

    /// Update a V2 pool's reserves from a Sync event.
    #[pyo3(signature = (address, reserve0, reserve1, block_number))]
    fn update_v2_pool(
        &self,
        py: Python<'_>,
        address: &str,
        reserve0: &Bound<'_, PyAny>,
        reserve1: &Bound<'_, PyAny>,
        block_number: u64,
    ) -> PyResult<()> {
        let addr = parse_address(address)?;
        let r0 = narrow_reserve(
            crate::conversion::alloy::extract_python_u256(reserve0)?,
            "reserve0",
        )?;
        let r1 = narrow_reserve(
            crate::conversion::alloy::extract_python_u256(reserve1)?,
            "reserve1",
        )?;

        // The write guard is core-owned ([`Bot::update_v2_pool`],
        // LockSite::Core) under this py.detach (inversion class).
        py.detach(|| self.bot.update_v2_pool(addr, r0, r1, block_number));
        Ok(())
    }

    /// Calculate the output token amount for a given input amount.
    ///
    /// Raises `ValueError` when the swap math overflows a `uint256` intermediate
    /// (mirrors on-chain `getAmountOut` `SafeMath` revert — cdbc03bb). A V3/V4
    /// sparse-map miss is still mapped to 0; callers needing the miss surfaced
    /// use `calculate_tokens_out_with_fetch` on the pool handle.
    #[pyo3(signature = (pool_id, zero_for_one, amount_in))]
    fn calculate_tokens_out(
        &self,
        py: Python<'_>,
        pool_id: u64,
        zero_for_one: bool,
        amount_in: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let amount = crate::conversion::alloy::extract_python_u256(amount_in)?;
        // The request assembly (sign convention), the write guard (the
        // sparse-miss recovery fills words), the SwapRead classification, and
        // the legacy 0-on-miss contract are core-owned
        // ([`Bot::calculate_tokens_out`]); the GIL is released across it
        // (inversion class).
        let out = py
            .detach(|| self.bot.calculate_tokens_out(pool_id, zero_for_one, amount))
            .map_err(|e| map_calc_tokens_out_err(&e))?;
        let bound = crate::conversion::alloy::u256_to_py(py, &out)?;
        Ok(bound.unbind())
    }

    /// Calculate the raw stableswap `get_dy` for a Curve pool (Rust-owned).
    /// Backs the companion `CurveStableswapPool
    /// .get_dy` so no Python provider / cache / calculator is on the path.
    ///
    /// Args:
    ///     `pool_id`: The pool ID returned by `register_curve_pool`.
    ///     `i`/`j`: Coin indices.
    ///     `dx`: Input amount (Python int).
    ///     `block_number`: Block to resolve against.
    ///     `override_balances`: Optional `list[int]` to use as the balance
    ///         source instead of the pool's current balances.
    ///
    /// Returns:
    ///     The raw `dy` output amount as a Python int.
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (pool_id, i, j, dx, block_number, override_balances=None))]
    fn curve_get_dy(
        &self,
        py: Python<'_>,
        pool_id: u64,
        i: usize,
        j: usize,
        dx: &Bound<'_, PyAny>,
        block_number: u64,
        override_balances: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let amount = crate::conversion::alloy::extract_python_u256(dx)?;
        let overrides = match override_balances {
            Some(list) => {
                let cast = list.cast::<PyList>().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err("override_balances must be a list[int]")
                })?;
                Some(extract_u256_list(cast)?)
            }
            None => None,
        };
        // The read guard is core-owned ([`Bot::curve_get_dy`],
        // LockSite::Core); the typed error passes through so the shell's
        // `Debug`-format `ValueError` surface is unchanged.
        let out = py
            .detach(|| {
                self.bot
                    .curve_get_dy(pool_id, i, j, amount, block_number, overrides.as_deref())
            })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{e:?}")))?;
        let bound = crate::conversion::alloy::u256_to_py(py, &out)?;
        Ok(bound.unbind())
    }

    /// Calculate the required input token amount for a given output amount.
    ///
    /// Args:
    ///     `pool_id`: The pool ID returned by `register_v2_pool`.
    ///     `zero_for_one`: True for token0→token1, False for token1→token0.
    ///     `amount_out`: Desired output token amount (Python int).
    ///
    /// Returns:
    ///     The required input token amount as a Python int.
    #[pyo3(signature = (pool_id, zero_for_one, amount_out))]
    fn calculate_tokens_in(
        &self,
        py: Python<'_>,
        pool_id: u64,
        zero_for_one: bool,
        amount_out: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let amount = crate::conversion::alloy::extract_python_u256(amount_out)?;
        // ADR-037: the exact-output request assembly (positive
        // user-perspective; the required input rides back as the consumed
        // magnitude), the write guard, and the SwapRead→result classification
        // (legacy 0-on-miss, typed family gap) are core-owned
        // ([`Bot::calculate_tokens_in`]); the GIL is released across it
        // (inversion class).
        let result = py
            .detach(|| self.bot.calculate_tokens_in(pool_id, zero_for_one, amount))
            .map_err(|e| map_calc_tokens_in_err(&e))?;
        let bound = crate::conversion::alloy::u256_to_py(py, &result)?;
        Ok(bound.unbind())
    }

    /// Number of registered pools.
    fn pool_count(&self, py: Python<'_>) -> usize {
        // GIL hygiene: the read guard is acquired inside the core method,
        // which runs under this py.detach (inversion class).
        py.detach(|| self.bot.pool_count())
    }

    /// The chain this `PyBot` orchestrates (ADR-006 D4). Wired from the
    /// single-chain Python `Bot` facade's ``config.default_chain_id``; `0` on
    /// the bare-test-fixture path (see `PyBot::new`).
    #[getter]
    fn chain_id(&self) -> u64 {
        self.bot.chain_id()
    }

    /// Drive a raw WS log through the event bus (ADR-006 D4).
    ///
    /// This is the Python-facing mirror of the `BlockPump`'s per-log call to
    /// `Bot::dispatch_log`: decode via the registered `LogDecoder`s, apply the
    /// decoded event to the shared `BotState` under a write guard, release it,
    /// then record the touched pool into the `EpochDelta` ledger so
    /// the next solve reads fresh keys.
    ///
    /// Reconstructs an `alloy::rpc::types::Log` from the WS-log shape Python
    /// passes — `(address, topics, data, block_number)` — so an offline test
    /// can drive the full pump→dispatch→solve loop without a live node.
    /// No-op if no decoder recognizes the log or the pool isn't registered.
    ///
    /// Args:
    ///     `address`: Emitter contract address (hex string, `0x` optional).
    ///     `topics`: Log topics (hex strings, `0x` optional). `topics[0]` is
    ///         the event signature hash the decoders match against.
    ///     `data`: ABI-encoded log data (hex string, `0x` optional).
    ///     `block_number`: Block number the synthetic log belongs to.
    #[pyo3(signature = (address, topics, data, block_number=0))]
    fn dispatch_log(
        &self,
        address: &str,
        topics: Vec<String>,
        data: &str,
        block_number: u64,
    ) -> PyResult<()> {
        let log = degenbot_bot::bot_core::build_rpc_log(address, topics, data, block_number)
            .map_err(pyo3::exceptions::PyValueError::new_err)?;
        self.bot.dispatch_log(&log);
        Ok(())
    }

    /// Get a thin `PyLiquidityPool` handle for the given pool ID.
    ///
    /// Args:
    ///     `pool_id`: The pool ID returned by `register_v2_pool`.
    ///
    /// Returns:
    ///     A `PyLiquidityPool` handle, or `None` if the pool ID is not registered.
    fn get_pool(&self, py: Python<'_>, pool_id: u64) -> Option<PyLiquidityPool> {
        // GIL hygiene: the has_pool read runs under this py.detach via the core.
        if py.detach(|| self.bot.has_pool(pool_id)) {
            Some(PyLiquidityPool::new(
                self.bot.state_arc(),
                pool_id,
                self.bot.chain_id(),
            ))
        } else {
            None
        }
    }

    /// Unregister a V2/V3 pool by its contract address.
    ///
    /// ADR-007 U3. Drops the `PoolEntry`, its reorg journal, its index entry,
    /// and any buffered V3 liquidity events for the address so a re-register
    /// does not replay stale Mint/Burn onto the fresh pool. `next_pool_id` is
    /// not reused — removed ids are retired to prevent stale `PyLiquidityPool`
    /// handles aliasing to a different pool on recreate.
    ///
    /// V2/V3 path only — `PyBot` exposes `register_v2/v3_pool` (no V4;
    /// V4 registration lives on the engine wrapper, and its symmetric
    /// unregister belongs there too — see ADR-007 Consequences).
    ///
    /// Returns `True` if a pool was found and removed; `False` if the address
    /// was never registered (silent no-op, mirroring Python `PoolRegistry.remove`).
    /// Register stays refusal-on-panic for duplicates (ADR-007 U2) — the
    /// asymmetry reflects the asymmetry in the operations' invariants.
    ///
    /// Args:
    ///     `address`: The V2/V3 pool contract address (checksum or 0x-hex).
    ///     `pool_id`: Reserved — must be `None` on `PyBot`. (V4 tuple-key
    ///         unregister is engine-side; kept in the signature for parity
    ///         with `BotState::unregister_pool`.)
    ///
    /// Raises:
    ///     `ValueError`: If `address` cannot be parsed.
    #[pyo3(signature = (address, pool_id=None))]
    #[expect(clippy::needless_pass_by_value)] // PyO3 binding idiom for optional bytes
    fn unregister_pool(
        &self,
        py: Python<'_>,
        address: &str,
        pool_id: Option<Vec<u8>>,
    ) -> PyResult<bool> {
        let addr = parse_address(address)?;
        // V4 on PyBot is intentionally not exposed: registration for V4 lives
        // on the engine wrapper (bot::engine), so the symmetric V4 unregister
        // belongs there too (ADR-007 Consequences / Deferred). A `Some` here
        // would be a caller bug — surface it rather than silently no-op.
        if pool_id.is_some() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Bot.unregister_pool does not handle V4 (pool_id set); \
                 V4 unregister is engine-side — use the engine’s unregister path.",
            ));
        }
        // The write guard is acquired inside the core method under this
        // py.detach — the GIL is released while parked on the BotState
        // write.
        Ok(py.detach(|| self.bot.unregister_pool(addr)))
    }

    /// Register a V3 pool by contract address.
    ///
    /// Returns the auto-assigned pool ID.
    #[expect(clippy::too_many_arguments)]
    /// Register a V3 pool, optionally seeding `tick_data` inline.
    ///
    /// ADR-006 rolling-start race closure: the builder previously called
    /// ``register_v3_pool`` (empty `tick_data`) THEN ``update_tick_data`` to seed
    /// the snapshot. Because ``resume()`` runs before ``build_paths``, a pump
    /// Mint/Burn landing between register and seed was applied to the empty
    /// `tick_data` then CLOBBERED by the seed overwrite — a lost update the
    /// on-chain ``verify_liquidity_maps`` reproduces. Folding the seed into
    /// ``register_v3_pool`` (one ``BotState`` write lock) closes the window:
    /// the pool is never visible to the pump in an unseeded state, so pump
    /// events always land on top of the seed and are never overwritten.
    ///
    /// `tick_data` is ``{tick: (liquidity_gross, liquidity_net, block)}``
    /// (symmetric with ``PyLiquidityPool.tick_data_snapshot``).
    /// `coverage` declares whether the injected map is complete (``"tracked"``)
    /// or partial (``"sparse"``).
    #[pyo3(signature = (address, token0, token1, fee, tick_spacing, factory, sqrt_price_x96, liquidity, tick, tick_data=None, update_block=0, coverage="sparse", tick_data_fetcher=None, tick_data_block=None, slot_layout=None))]
    fn register_v3_pool(
        &self,
        py: Python<'_>,
        address: &str,
        token0: &str,
        token1: &str,
        fee: u32,
        tick_spacing: i32,
        factory: &str,
        sqrt_price_x96: &Bound<'_, PyAny>,
        liquidity: &Bound<'_, PyAny>,
        tick: i32,
        tick_data: Option<Bound<'_, PyDict>>,
        update_block: u64,
        coverage: &str,
        tick_data_fetcher: Option<Bound<'_, PyAny>>,
        tick_data_block: Option<u64>,
        slot_layout: Option<&str>,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let t0 = parse_address(token0)?;
        let t1 = parse_address(token1)?;
        let fac = parse_address(factory)?;
        let spx = crate::conversion::alloy::extract_python_u256(sqrt_price_x96)?;
        // liquidity is uint128 — extracted as U256 then narrowed.
        let liq = crate::conversion::alloy::extract_python_u256(liquidity)?.to::<u128>();

        // Convert `{tick: (gross, net, block)}` → `HashMap<i32, TickInfo>`
        // (mirrors `PyLiquidityPool.update_tick_data`'s boundary conversion).
        let rust_tick_data: hashbrown::HashMap<i32, degenbot_bot::bot_core::TickInfo> =
            match tick_data {
                Some(dict) => {
                    let parsed: std::collections::HashMap<i32, (u128, i128, u64)> =
                        dict.extract().map_err(|_| {
                            pyo3::exceptions::PyTypeError::new_err(
                                "register_v3_pool: tick_data must be {tick: (gross, net, block)}",
                            )
                        })?;
                    parsed
                        .into_iter()
                        .map(|(tick, (gross, net, blk))| {
                            (
                                tick,
                                degenbot_bot::bot_core::TickInfo {
                                    liquidity_gross: alloy::primitives::U128::from(gross),
                                    liquidity_net: net,
                                    block: blk,
                                },
                            )
                        })
                        .collect()
                }
                None => hashbrown::HashMap::new(),
            };
        let cov = match coverage {
            "tracked" => PoolTickCoverage::Tracked,
            "sparse" => PoolTickCoverage::Sparse,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "register_v3_pool: coverage must be 'tracked' or 'sparse', got {other:?}",
                )));
            }
        };

        // CL slot layout : an explicit "pancakeswap" override forces the
        // PancakeV3 layout (the Python driver knows the pool class for
        // non-JSON deployments); "uniswap"/None defer to the deployment
        // table, resolved inside the core method. Validated BEFORE any
        // RPC-adjacent work (cheap string check first — no malformed
        // argument behind a CREATE2 verify).
        let slot_override = match slot_layout {
            Some("pancakeswap") => Some(degenbot_pools::v3_state::ClSlotLayout::PancakeV3),
            Some("uniswap") | None => None,
            Some(other) => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "register_v3_pool: slot_layout must be 'uniswap' or 'pancakeswap', got {other:?}"
                )));
            }
        };

        // The CREATE2 verify (Fork A), deployer/init-hash resolution, params
        // assembly, and the write-lock acquisition live inside the core
        // method ([`Bot::register_v3_pool`]). The seed fetcher is wrapped
        // here (GIL held) — a Python callback cannot cross into the core
        // closure — and the remaining params are all `Send`; the error is
        // mapped to a `PyErr` OUTSIDE the detach (GIL-held) since `PyErr`
        // construction needs the interpreter.
        let fetcher = tick_data_fetcher
            .filter(|f| !f.is_none())
            .map(|f| crate::bot::pool::make_tick_fetcher(f.clone().unbind()));
        py.detach(|| {
            self.bot
                .register_v3_pool(
                    addr,
                    t0,
                    t1,
                    fee,
                    tick_spacing,
                    fac,
                    spx,
                    liq,
                    tick,
                    rust_tick_data,
                    update_block,
                    cov,
                    fetcher,
                    tick_data_block,
                    slot_override,
                )
                .map_err(map_v3_registration_err)
        })
    }

    /// Register a V4 pool by `(pool_manager, pool_id)`.
    ///
    /// Returns the auto-assigned pool ID. The dynamic-fee admission floor
    /// lives in `BotState::register_v4_pool` (ADR-005 slice 9a): pools with
    /// dynamic fees (`fee == 0x100000`) or static fees exceeding the
    /// executor's 2-byte encoding limit (`fee > 65535`) are rejected here,
    /// surfacing as typed Python exceptions
    /// (`DynamicFeePoolRejectedError` / `HighFeePoolRejectedError`) so Python
    /// classifies by type, not string matching. Since ADR-037
    /// amount-modifying hooked pools are ADMITTED (the ADR-037 guards use the
    /// registered hook address at hop projection / simulation-caveat time),
    /// and the `HookedPoolRejectedError` raise site is reserved (no longer
    /// fires). The FULL `hook_address` rides into the registered pool key so
    /// the identity round-trips `keccak(abi.encode(pool_key))`.
    ///
    /// ADR-006 rolling-start race closure: the snapshot `tick_data` is seeded
    /// INLINE in `register_v4_pool` (one `BotState` write lock) so the pool is
    /// never visible to the live pump (resumed before `build_paths`) in an
    /// unseeded state. Previously the builder registered with empty `tick_data`
    /// then called `update_tick_data` to seed — a pump `ModifyLiquidity` landing
    /// between register and seed was applied then CLOBBERED by the seed
    /// overwrite (lost update → V4 tick-map desync → `verify_liquidity_maps`
    /// mismatch). Mirrors the V3 closure.
    ///
    /// Raises:
    ///     `DynamicFeePoolRejectedError`: If `fee == 0x100000`.
    ///     `HighFeePoolRejectedError`: If `fee > 65535` (executor can't encode).
    ///     `ValueError`: If `addresses/pool_id` are malformed or already
    ///         registered.
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (pool_manager, pool_id_hex, currency0, currency1, fee, tick_spacing, hook_address, sqrt_price_x96, liquidity, tick, block, tick_data=None, coverage="sparse", tick_data_fetcher=None, protocol_fee=0, tick_data_block=None))]
    fn register_v4_pool(
        &self,
        py: Python<'_>,
        pool_manager: &str,
        pool_id_hex: &str,
        currency0: &str,
        currency1: &str,
        fee: u32,
        tick_spacing: i32,
        // The REAL hook contract address (None/empty = no hook). Registered
        // in the pool key in full so the identity round-trips
        // keccak(abi.encode(pool_key)) (pool-ID mismatch regression);
        // the derived 16-bit mask rides along in params.hook_flags.
        hook_address: Option<&str>,
        sqrt_price_x96: &Bound<'_, PyAny>,
        liquidity: u128,
        tick: i32,
        block: u64,
        tick_data: Option<Bound<'_, pyo3::types::PyDict>>,
        coverage: &str,
        tick_data_fetcher: Option<Bound<'_, PyAny>>,
        protocol_fee: u32,
        tick_data_block: Option<u64>,
    ) -> PyResult<u64> {
        let pm = pool_manager.parse::<Address>().map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Invalid pool_manager address: {e}"))
        })?;
        let pool_id = hex_string_to_pool_id(pool_id_hex)?;
        let c0 = currency0.parse::<Address>().map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Invalid currency0 address: {e}"))
        })?;
        let c1 = currency1.parse::<Address>().map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Invalid currency1 address: {e}"))
        })?;
        let sp = crate::conversion::alloy::extract_python_u256(sqrt_price_x96)?;
        // Convert `{tick: (gross, net, block)}` → `HashMap<i32, TickInfo>` —
        // mirrors `register_v3_pool` (ADR-006 rolling-start race closure for
        // V4: seed inline so the pool is never visible to the live pump
        // unseeded).
        let rust_tick_data: hashbrown::HashMap<i32, degenbot_bot::bot_core::TickInfo> =
            match tick_data {
                Some(dict) => {
                    let parsed: std::collections::HashMap<i32, (u128, i128, u64)> =
                        dict.extract().map_err(|_| {
                            pyo3::exceptions::PyTypeError::new_err(
                                "register_v4_pool: tick_data must be {tick: (gross, net, block)}",
                            )
                        })?;
                    parsed
                        .into_iter()
                        .map(|(tick, (gross, net, blk))| {
                            (
                                tick,
                                degenbot_bot::bot_core::TickInfo {
                                    liquidity_gross: alloy::primitives::U128::from(gross),
                                    liquidity_net: net,
                                    block: blk,
                                },
                            )
                        })
                        .collect()
                }
                None => hashbrown::HashMap::new(),
            };
        let cov = match coverage {
            "tracked" => PoolTickCoverage::Tracked,
            "sparse" => PoolTickCoverage::Sparse,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "register_v4_pool: coverage must be 'tracked' or 'sparse', got {other:?}",
                )));
            }
        };
        let hook_addr = hook_address
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<Address>().map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("Invalid hook_address: {e}"))
                })
            })
            .transpose()?
            .unwrap_or_default();
        let params = RegisterV4PoolParams {
            pool_manager: pm,
            pool_id,
            pool_key: V4PoolKey {
                currency0: c0,
                currency1: c1,
                fee,
                tick_spacing,
                // The REAL hook address — the identity must round-trip
                // keccak(abi.encode(pool_key)) (pool-ID mismatch).
                hooks: hook_addr,
            },
            hook_flags: degenbot_bot::bot_core::pool_builder::builder::derive_hook_flags(hook_addr),
            protocol_fee,
            sqrt_price_x96: sp,
            liquidity,
            tick,
            tick_data: rust_tick_data,
            update_block: block,
            tick_data_block,
            coverage: cov,
            fetcher: tick_data_fetcher
                .filter(|f| !f.is_none())
                .map(|f| crate::bot::pool::make_tick_fetcher(f.clone().unbind())),
        };
        // The write-lock acquisition + the admission floor (which records
        // the PRG-2 keyed verdicts) live inside the core method
        // ([`Bot::register_v4_pool`]); the GIL is released across it (see
        // `register_token`). `RegisterV4PoolParams` is `Send`; the error is
        // mapped to a `PyErr` OUTSIDE the closure.
        let result = py.detach(|| self.bot.register_v4_pool(&params));
        result.map_err(map_register_v4_err)
    }

    /// Seed the Rust-owned V4 `StateView` registry (ADR-005 / Option 2):
    /// record the canonical `StateView` contract whose `getSlot0(bytes32)` /
    /// `getLiquidity(bytes32)` read a `pool_manager`'s pool state on-chain.
    /// Called once per `pool_manager` (the driver reads `state_view` from the
    /// `pool_managers` DB row) before V4 pools solve, so the solver-state
    /// accuracy gate can verify each V4 hop's state against the chain.
    ///
    /// Raises:
    ///     `ValueError`: If either address is malformed.
    fn register_v4_state_view(
        &self,
        py: Python<'_>,
        pool_manager: &str,
        state_view: &str,
    ) -> PyResult<()> {
        let pm = parse_address(pool_manager).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "register_v4_state_view: malformed pool_manager {pool_manager:?}: {e}"
            ))
        })?;
        let sv = parse_address(state_view).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "register_v4_state_view: malformed state_view {state_view:?}: {e}"
            ))
        })?;
        // The write guard is acquired inside the core method under this
        // py.detach (see `register_token`).
        py.detach(|| self.bot.register_v4_state_view(pm, sv));
        Ok(())
    }

    /// Register a Curve `StableSwap` pool by contract address.
    ///
    /// Returns the auto-assigned pool ID. The `tokens` / `rate_multipliers` /
    /// `balances` lists must all have the same length `N` (the coin count).
    ///
    /// Raises:
    ///     `ValueError`: If the pool address is malformed or already
    ///         registered, or the list lengths mismatch.
    #[expect(clippy::too_many_arguments)]
    // `y_variant` / `yd_variant` mirror the public Python kwargs and the
    // `resolve_y_variant` / `resolve_yd_variant` vocabulary in
    // `degenbot.curve._variant_groups`; they are intentional domain terms,
    // not a naming slip, so the `similar_names` nudge does not apply here.
    #[expect(clippy::similar_names)]
    #[pyo3(signature = (
        address,
        tokens,
        a_coefficient,
        a_precision,
        fee,
        admin_fee,
        rate_multipliers,
        balances,
        update_block,
        swap_style=0,
        lending_rate_style=0,
        d_variant=0,
        y_variant=0,
        yd_variant=0,
        base_pool=None,
        initial_a_coefficient=None,
        future_a_coefficient=None,
        initial_a_coefficient_time=None,
        future_a_coefficient_time=None,
        create_timestamp=None,
        fee_gamma=None,
        mid_fee=None,
        offpeg_fee_multiplier=None,
        out_fee=None,
        gamma=None,
        lp_token=None,
        use_lending=None,
        precision_multipliers=None,
        tokens_underlying=None,
        metapool_rate_style=1,
        metapool_underlying_style=1,
        data_provider=None
    ))]
    fn register_curve_pool(
        &self,
        py: Python<'_>,
        address: &str,
        tokens: &Bound<'_, PyList>,
        a_coefficient: u128,
        a_precision: u128,
        fee: u64,
        admin_fee: u64,
        rate_multipliers: &Bound<'_, PyList>,
        balances: &Bound<'_, PyList>,
        update_block: u64,
        swap_style: u8,
        lending_rate_style: u8,
        d_variant: u8,
        y_variant: u8,
        yd_variant: u8,
        base_pool: Option<&str>,
        initial_a_coefficient: Option<u128>,
        future_a_coefficient: Option<u128>,
        initial_a_coefficient_time: Option<u64>,
        future_a_coefficient_time: Option<u64>,
        create_timestamp: Option<u64>,
        fee_gamma: Option<u64>,
        mid_fee: Option<u64>,
        offpeg_fee_multiplier: Option<u64>,
        out_fee: Option<u64>,
        gamma: Option<u64>,
        lp_token: Option<&str>,
        use_lending: Option<&Bound<'_, PyList>>,
        precision_multipliers: Option<&Bound<'_, PyList>>,
        tokens_underlying: Option<&Bound<'_, PyList>>,
        metapool_rate_style: u8,
        metapool_underlying_style: u8,
        data_provider: Option<Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let token_addrs = parse_address_list(tokens)?;
        let rate_mults = extract_u256_list(rate_multipliers)?;
        let bal_vals = extract_u256_list(balances)?;
        let base = match base_pool {
            Some(s) => Some(parse_address(s)?),
            None => None,
        };
        let lp = match lp_token {
            Some(s) => Some(parse_address(s)?),
            None => None,
        };
        let use_lend: Vec<bool> = match use_lending {
            Some(l) => l.extract()?,
            None => Vec::new(),
        };
        let prec_mults: Vec<alloy::primitives::U256> = match precision_multipliers {
            Some(l) => extract_u256_list(l)?,
            None => Vec::new(),
        };
        let tokens_under = match tokens_underlying {
            Some(l) => Some(parse_address_list(l)?),
            None => None,
        };
        let p = RegisterCurvePoolParams {
            address: addr,
            tokens: token_addrs,
            a_coefficient,
            a_precision,
            fee,
            admin_fee,
            rate_multipliers: rate_mults,
            balances: bal_vals,
            update_block,
            swap_style,
            lending_rate_style,
            d_variant,
            y_variant,
            yd_variant,
            base_pool: base,
            initial_a_coefficient,
            future_a_coefficient,
            initial_a_coefficient_time,
            future_a_coefficient_time,
            create_timestamp,
            fee_gamma,
            mid_fee,
            offpeg_fee_multiplier,
            out_fee,
            gamma,
            lp_token: lp,
            use_lending: use_lend,
            precision_multipliers: prec_mults,
            tokens_underlying: tokens_under,
            metapool_rate_style,
            metapool_underlying_style,
            data_provider: data_provider
                .map(|b| crate::bot::pool::make_curve_data_provider(b.unbind())),
        };
        // The write guard is acquired inside the core method under this
        // py.detach — the GIL is released while parked on the BotState
        // write.
        Ok(py.detach(|| self.bot.register_curve_pool(&p)))
    }

    /// Build + register a Curve `StableSwap` pool through the Rust `PoolBuilder`
    /// — the Curve twin of [`Self::build_v2_pool`].
    /// The core `builder::build_curve_pool` runs the full detection
    /// choreography (coins + balances, `A`/`fee`/`admin_fee`, A-ramping,
    /// lending, crypto params, `lp_token`, metapool base + underlying coins,
    /// ERC20 decimals) and constructs a Rust `RpcCurveDataProvider` —
    /// so the pool is registered with a **Rust-native** on-chain data
    /// provider, not a Python `CurveDataProviderImpl`. Returns the `pool_id`
    /// (`get_pool(id)` after this). `registry_addresses` is the list of
    /// Curve registry/factory contract addresses consulted for the `lp_token`
    /// / metapool lookups.
    ///
    /// `block` defaults to the current chain head.
    ///
    /// Raises:
    ///     `RuntimeError`: No `ConstructionIo` attached (requires an alloy
    ///         provider), or a builder RPC/decode failure.
    #[pyo3(signature = (address, registry_addresses, block=None))]
    fn build_curve_pool(
        &self,
        py: Python<'_>,
        address: &str,
        registry_addresses: &Bound<'_, PyList>,
        block: Option<u64>,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let registry = parse_address_list(registry_addresses)?;
        // The fetch/detect/register choreography is core-owned
        // ([`Bot::build_and_register_curve_pool`]); the GIL is released
        // across the whole scope.
        py.detach(|| {
            self.bot
                .build_and_register_curve_pool(addr, &registry, block)
        })
        .map_err(|e| map_build_err("build_curve_pool", e))
    }

    /// Register a Balancer V2 weighted pool (ADR-005 slice 12a state port).
    ///
    /// Stores immutable pool config (`pool_id`, vault, tokens, weights,
    /// `scaling_factors`, `swap_fee`, `pow_version`) + registration balances + a
    /// genesis reorg journal delta. The slice 12b Python `BalancerV2Pool`
    /// companion is constructed over the returned `PyLiquidityPool` handle
    /// (call `get_pool(id)` after this).
    ///
    /// Returns the auto-assigned numeric pool ID. `tokens` / `weights` /
    /// `scaling_factors` / `balances` lists MUST all have length `N`.
    ///
    /// Raises:
    ///     `ValueError`: If an address is malformed, the pool is already
    ///         registered, the list lengths mismatch, or `pool_id_hex` is not
    ///         a 32-byte hex string.
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (
        address,
        vault,
        pool_id_hex,
        tokens,
        weights,
        scaling_factors,
        swap_fee,
        pow_version,
        balances,
        update_block
    ))]
    fn register_balancer_weighted_pool(
        &self,
        py: Python<'_>,
        address: &str,
        vault: &str,
        pool_id_hex: &str,
        tokens: &Bound<'_, PyList>,
        weights: &Bound<'_, PyList>,
        scaling_factors: &Bound<'_, PyList>,
        swap_fee: u128,
        pow_version: u8,
        balances: &Bound<'_, PyList>,
        update_block: u64,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let vault_addr = parse_address(vault)?;
        let pool_id_bytes = hex_string_to_pool_id(pool_id_hex)?;
        let token_addrs = parse_address_list(tokens)?;
        let weight_vals = extract_u256_list(weights)?;
        let scaling_vals = extract_u256_list(scaling_factors)?;
        let bal_vals = extract_u256_list(balances)?;
        let p = RegisterBalancerWeightedPoolParams {
            address: addr,
            vault: vault_addr,
            pool_id: pool_id_bytes,
            tokens: token_addrs,
            weights: weight_vals,
            scaling_factors: scaling_vals,
            swap_fee,
            pow_version,
            balances: bal_vals,
            update_block,
        };
        // The GIL is released across this py.detach — never hold the GIL
        // while parked on the BotState write.
        Ok(py.detach(|| self.bot.register_balancer_weighted_pool(&p)))
    }

    /// Register a Balancer V2 stable pool (ADR-005 slice 12c state port).
    ///
    /// Stores immutable pool config (`pool_id`, vault, tokens, amp,
    /// `scaling_factors`, `swap_fee`, `bpt_idx`, `invariant_version`) +
    /// registration balances + a genesis reorg journal delta. The slice 12d
    /// Python `BalancerV2StablePool` companion will be constructed over the
    /// returned `PyLiquidityPool` handle (call `get_pool(id)` after this).
    ///
    /// Returns the auto-assigned numeric pool ID. `tokens` / `scaling_factors`
    /// / `balances` lists MUST all have length `N`. `bpt_idx` is `None` for
    /// `MetaStablePools` and `Some(i)` (`i < N`) for `ComposableStablePools`.
    ///
    /// Raises:
    ///     `ValueError`: If an address is malformed, the pool is already
    ///         registered, the list lengths mismatch, `bpt_idx` is
    ///         out-of-range, or `pool_id_hex` is not a 32-byte hex string.
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (
        address,
        vault,
        pool_id_hex,
        tokens,
        amp,
        scaling_factors,
        swap_fee,
        bpt_idx,
        invariant_version,
        balances,
        update_block,
        rate_provider=None
    ))]
    fn register_balancer_stable_pool(
        &self,
        py: Python<'_>,
        address: &str,
        vault: &str,
        pool_id_hex: &str,
        tokens: &Bound<'_, PyList>,
        amp: u128,
        scaling_factors: &Bound<'_, PyList>,
        swap_fee: u128,
        bpt_idx: Option<usize>,
        invariant_version: u8,
        balances: &Bound<'_, PyList>,
        update_block: u64,
        rate_provider: Option<Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let vault_addr = parse_address(vault)?;
        let pool_id_bytes = hex_string_to_pool_id(pool_id_hex)?;
        let token_addrs = parse_address_list(tokens)?;
        let scaling_vals = extract_u256_list(scaling_factors)?;
        let bal_vals = extract_u256_list(balances)?;
        let provider =
            rate_provider.map(|b| crate::bot::pool::make_balancer_rate_provider(b.unbind()));
        let params = RegisterBalancerStablePoolParams {
            address: addr,
            vault: vault_addr,
            pool_id: pool_id_bytes,
            tokens: token_addrs,
            amp,
            scaling_factors: scaling_vals,
            swap_fee,
            bpt_idx,
            invariant_version,
            balances: bal_vals,
            update_block,
            rate_provider: provider,
        };
        // No GIL while parked on the BotState write (see `unregister_pool`).
        Ok(py.detach(|| self.bot.register_balancer_stable_pool(&params)))
    }

    /// Register an Aerodrome V2 pool by contract address (ADR-005 Aerodrome
    /// state port).
    ///
    /// Stores immutable identity (address, tokens, factory, variant, stable
    /// flag, unidirectional fee) + the registration reserves + a genesis
    /// reorg-journal anchor (mirror of V2's discipline). Returns the
    /// auto-assigned pool ID. Call `get_pool(id)` after this to obtain the
    /// `PyLiquidityPool` handle.
    ///
    /// Raises:
    ///     `PoolAlreadyRegisteredError`: If a pool at this address is
    ///         already registered (a `PoolRegistrationError`/`ValueError`
    ///         subclass — the typed twin of the V2/V3 duplicate rejection).
    ///     `ValueError`: If an address is malformed or `variant` is not a
    ///         recognized Aerodrome variant.
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (address, token0, token1, factory, variant, stable, fee_numer, fee_denom, token0_decimals, token1_decimals, reserve0, reserve1, update_block=0))]
    fn register_aerodrome_pool(
        &self,
        py: Python<'_>,
        address: &str,
        token0: &str,
        token1: &str,
        factory: &str,
        variant: &str,
        stable: bool,
        fee_numer: u64,
        fee_denom: u64,
        token0_decimals: u8,
        token1_decimals: u8,
        reserve0: &Bound<'_, PyAny>,
        reserve1: &Bound<'_, PyAny>,
        update_block: u64,
    ) -> PyResult<u64> {
        let addr = parse_address(address)?;
        let t0 = parse_address(token0)?;
        let t1 = parse_address(token1)?;
        let fac = parse_address(factory)?;
        let r0 = narrow_reserve(
            crate::conversion::alloy::extract_python_u256(reserve0)?,
            "reserve0",
        )?;
        let r1 = narrow_reserve(
            crate::conversion::alloy::extract_python_u256(reserve1)?,
            "reserve1",
        )?;
        let variant_enum = DexVariant::try_from(variant)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;

        // The EIP-1167 verify (skipped for non-JSON (chain, factory) rows) +
        // params assembly live inside the core method
        // ([`Bot::register_aerodrome_pool`]); the GIL is released across
        // this py.detach — never hold the GIL while parked on the BotState
        // write.
        py.detach(|| {
            self.bot
                .register_aerodrome_pool(
                    addr,
                    t0,
                    t1,
                    fac,
                    variant_enum,
                    stable,
                    fee_numer,
                    fee_denom,
                    token0_decimals,
                    token1_decimals,
                    r0,
                    r1,
                    update_block,
                )
                .map_err(map_aerodrome_registration_err)
        })
    }

    /// Update a V3 pool's state from a Swap event.
    ///
    /// No-op if the pool is not registered.
    #[pyo3(signature = (address, sqrt_price_x96, liquidity, tick, block_number))]
    fn update_v3_pool(
        &self,
        py: Python<'_>,
        address: &str,
        sqrt_price_x96: &Bound<'_, PyAny>,
        liquidity: &Bound<'_, PyAny>,
        tick: i32,
        block_number: u64,
    ) -> PyResult<()> {
        let addr = parse_address(address)?;
        let spx = crate::conversion::alloy::extract_python_u256(sqrt_price_x96)?;
        let liq = crate::conversion::alloy::extract_python_u256(liquidity)?.to::<u128>();

        // The write guard is core-owned ([`Bot::update_v3_pool`],
        // LockSite::Core) under this py.detach (inversion class).
        py.detach(|| self.bot.update_v3_pool(addr, spx, liq, tick, block_number));
        Ok(())
    }

    /// Get the number of deltas in the reorg journal for a V3 pool.
    ///
    /// Returns 0 if the pool ID is not registered or is not a V3 pool.
    fn v3_journal_len(&self, py: Python<'_>, pool_id: u64) -> usize {
        // GIL hygiene: the read guard is acquired inside the core method,
        // which runs under this py.detach (inversion class).
        py.detach(|| self.bot.v3_journal_len(pool_id))
    }

    /// Discard V3 reorg journal deltas earlier than the given block.
    /// Discard V3 reorg journal deltas earlier than the given block.
    ///
    /// No-op if the earliest delta is at/after the target; errors if the target
    /// is past the newest delta. Raises `ValueError` on error (ADR-005 slice 4).
    #[pyo3(signature = (pool_id, block))]
    fn v3_discard_before_block(&self, py: Python<'_>, pool_id: u64, block: u64) -> PyResult<()> {
        // The write guard is acquired inside the core method under this
        // py.detach — the GIL is released while parked behind a live reader.
        py.detach(|| self.bot.v3_discard_before_block(pool_id, block))
            .map_err(journal_err_to_py)
    }

    /// Restore V3 pool state prior to a target block.
    ///
    /// Returns `(sqrt_price_x96, liquidity, tick, block)` as Python ints,
    /// or `None` if the pool ID is not registered or not a V3 pool.
    #[pyo3(signature = (pool_id, block))]
    fn v3_restore_before_block(
        &self,
        py: Python<'_>,
        pool_id: u64,
        block: u64,
    ) -> PyResult<Option<Py<PyAny>>> {
        // Read-after-restore (ADR-016 D4): the trait `restore_pool_before_block`
        // returns `()`; the post-restore scalar fields ARE the before-values
        // the `V3RestoreResult.scalar_priors` previously carried. The core
        // copies the owned scalars out under its write guard; this shell
        // marshals them after release.
        // The write scope runs under the core method's py.detach — the GIL
        // is released while parked behind a live reader, and the !Send guard
        // never leaves the core.
        let restored = py
            .detach(|| self.bot.v3_restore_before_block(pool_id, block))
            .map_err(journal_err_to_py)?;
        let Some((sqrt_p, liq, tick, blk)) = restored else {
            return Ok(None);
        };
        let tuple = pyo3::types::PyTuple::new(
            py,
            [
                crate::conversion::alloy::u256_to_py(py, &sqrt_p)?.unbind(),
                liq.into_pyobject(py)?.into_any().unbind(),
                tick.into_pyobject(py)?.into_any().unbind(),
                blk.into_pyobject(py)?.into_any().unbind(),
            ],
        )?;
        Ok(Some(tuple.into_any().unbind()))
    }

    /// Register a token.
    ///
    /// Args:
    ///     `address`: Token contract address (hex string).
    ///     `name`: Token name.
    ///     `symbol`: Token symbol.
    ///     `decimals`: Token decimals.
    ///     `chain_id`: Chain ID.
    #[pyo3(signature = (address, name, symbol, decimals, chain_id))]
    fn register_token(
        &self,
        py: Python<'_>,
        address: &str,
        name: &str,
        symbol: &str,
        decimals: u8,
        chain_id: u64,
    ) -> PyResult<PyErc20Token> {
        let addr = parse_address(address)?;
        let name = name.to_string();
        let symbol = symbol.to_string();
        // release the GIL across the BotState write-lock acquisition +
        // insert so the live pump (tokio) + the asyncio loop keep making GIL
        // progress while the main thread awaits `core.write()`. Pre-fix no
        // registration seam released the GIL, so a parked `core.write()` /
        // `engine.lock()` carried the GIL with it — the startup-stall enabler
        // (the asyncio loop froze for the whole park; the pump's GIL-requiring
        // notify/log path could not progress). The lock + insertion live
        // inside the core method ([`Bot::register_token`]) under this
        // py.detach; the `PyErc20Token` handle is built afterward from the
        // parsed `addr`.
        py.detach(|| {
            self.bot
                .register_token(addr, name, symbol, decimals, chain_id);
        });
        Ok(PyErc20Token::new(self.bot.state_arc(), addr))
    }

    /// Get a thin `PyErc20Token` handle for the given address.
    ///
    /// Args:
    ///     `address`: Token contract address (hex string).
    ///
    /// Returns:
    ///     A `PyErc20Token` handle, or `None` if the address is not registered.
    fn get_token(&self, py: Python<'_>, address: &str) -> PyResult<Option<PyErc20Token>> {
        let addr = parse_address(address)?;
        // GIL hygiene: the has_token read runs under this py.detach via the core.
        if py.detach(|| self.bot.has_token(&addr)) {
            Ok(Some(PyErc20Token::new(self.bot.state_arc(), addr)))
        } else {
            Ok(None)
        }
    }

    /// Build + register an ERC-20 token, resolving metadata DB-first, then
    /// on-chain (the core twin of `Erc20Builder.build`).
    ///
    /// `builder::build_erc20_metadata` resolves `name`/`symbol`/`decimals`
    /// (DB row → on-chain batched read → alternate-prototype fallback →
    /// UNKNOWN sentinels, with a DB write-back), then this adapter registers
    /// the token into `BotState.tokens` (ADR-006) exactly like
    /// [`Self::register_token`], and returns a thin [`PyErc20Token`] handle.
    ///
    /// Args:
    ///     `address`: token contract address (hex string).
    ///     `chain_id`: chain ID.
    ///     `block`: optional block to read on-chain at (None = head).
    #[pyo3(signature = (address, chain_id, block=None))]
    fn build_erc20_token(
        &self,
        py: Python<'_>,
        address: &str,
        chain_id: u64,
        block: Option<u64>,
    ) -> PyResult<PyErc20Token> {
        let addr = parse_address(address)?;
        // The metadata resolution and token registration are core-owned
        // ([`Bot::build_and_register_erc20_token`]); the GIL is released
        // across the whole scope.
        let _registered = py
            .detach(|| {
                self.bot
                    .build_and_register_erc20_token(chain_id, addr, block)
            })
            .map_err(|e| map_build_err("build_erc20_token", e))?;
        Ok(PyErc20Token::new(self.bot.state_arc(), addr))
    }

    /// Encode a V2 swap call, returning `(to_address_hex, calldata_hex, value)`.
    ///
    /// Args:
    ///     `pool_id`: The pool ID returned by `register_v2_pool`.
    ///     `zero_for_one`: True for token0→token1, False for token1→token0.
    ///     `amount_out`: Output token amount (Python int).
    ///     `recipient`: Address to receive output tokens (hex string).
    ///
    /// Returns:
    ///     A tuple `(to_hex, calldata_hex, value)` or `None` if pool not found.
    #[pyo3(signature = (pool_id, zero_for_one, amount_out, recipient))]
    fn encode_swap(
        &self,
        py: Python<'_>,
        pool_id: u64,
        zero_for_one: bool,
        amount_out: &Bound<'_, PyAny>,
        recipient: &str,
    ) -> PyResult<Option<(String, String, u64)>> {
        let amount = crate::conversion::alloy::extract_python_u256(amount_out)?;
        let recip = parse_address(recipient)?;

        // GIL hygiene: the read guard is acquired inside the core method,
        // which runs under this py.detach (inversion class).
        py.detach(|| self.bot.encode_swap(pool_id, zero_for_one, amount, recip))
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("encode_swap: {e}")))
    }

    /// Get the number of deltas in the reorg journal for a V2 pool.
    ///
    /// Returns 0 if the pool ID is not registered.
    fn v2_journal_len(&self, py: Python<'_>, pool_id: u64) -> usize {
        // GIL hygiene: the read guard is acquired inside the core method,
        // which runs under this py.detach (inversion class).
        py.detach(|| self.bot.v2_journal_len(pool_id))
    }

    /// Discard V2 reorg journal deltas earlier than the given block.
    ///
    /// No-op if the earliest delta is at/after the target; errors if the
    /// target is past the newest delta (would remove every known state).
    ///
    /// Raises:
    ///     `ValueError`: If the target is past the newest delta.
    #[pyo3(signature = (pool_id, block))]
    fn v2_discard_before_block(&self, py: Python<'_>, pool_id: u64, block: u64) -> PyResult<()> {
        // The write guard is acquired inside the core method under this
        // py.detach — the GIL is released while parked behind a live reader.
        py.detach(|| self.bot.v2_discard_before_block(pool_id, block))
            .map_err(journal_err_to_py)
    }

    /// Restore V2 pool state prior to a target block.
    ///
    /// Pops reorg journal deltas at/after the target block and restores the
    /// landed-at state into the current fluid fields.
    ///
    /// Returns `(reserve0, reserve1, block)` as Python ints, or `None`
    /// if the pool ID is not registered.
    ///
    /// Raises:
    ///     `ValueError`: If the target is at or before the registration block
    ///         (no state exists before it) — rolling back past registration is
    ///         a hard error, not a silent no-op (ADR-005 slice 4 decision 3).
    #[pyo3(signature = (pool_id, block))]
    fn v2_restore_before_block(
        &self,
        py: Python<'_>,
        pool_id: u64,
        block: u64,
    ) -> PyResult<Option<Py<PyAny>>> {
        // Read-after-restore (ADR-016): the trait returns `()`; the
        // post-restore reserves ARE the before-values the per-family tuple
        // previously carried. The core copies the owned scalars out under its
        // write guard; this shell marshals them after release.
        // The write scope runs under the core method's py.detach — the GIL
        // is released while parked behind a live reader, and the !Send guard
        // never leaves the core.
        let restored = py
            .detach(|| self.bot.v2_restore_before_block(pool_id, block))
            .map_err(journal_err_to_py)?;
        let Some((r0, r1, blk)) = restored else {
            return Ok(None);
        };
        let tuple = pyo3::types::PyTuple::new(
            py,
            [
                crate::conversion::alloy::u256_to_py(py, &r0)?.unbind(),
                crate::conversion::alloy::u256_to_py(py, &r1)?.unbind(),
                blk.into_pyobject(py)?.into_any().unbind(),
            ],
        )?;
        Ok(Some(tuple.into_any().unbind()))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse one address string, raising the historical `ValueError`. The
/// `Invalid address '<input>': <source>` vocabulary is core-owned
/// ([`degenbot_bot::bot_core::registration::parse_address_str`]); this
/// adapter only crosses the error into Python.
fn parse_address(s: &str) -> PyResult<Address> {
    degenbot_bot::bot_core::registration::parse_address_str(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

/// Parse a Python list of address strings into `Vec<Address>`. The per-item
/// extract error (and its precedence over parse errors) stays here; the
/// string→address parse with the historical `Invalid address` message
/// vocabulary is core-owned
/// ([`degenbot_bot::bot_core::registration::parse_address_str`]).
fn parse_address_list(list: &Bound<'_, PyList>) -> PyResult<Vec<Address>> {
    list.iter()
        .map(|item| {
            let s: String = item.extract().map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("token address must be a str: {e}"))
            })?;
            degenbot_bot::bot_core::registration::parse_address_str(&s)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
        })
        .collect()
}

/// Extract a Python list of ints (or int-like) into `Vec<U256>`.
pub(crate) fn extract_u256_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<alloy::primitives::U256>> {
    list.iter()
        .map(|item| crate::conversion::alloy::extract_python_u256(&item))
        .collect()
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    /// PRG-1 registry unification: `build_v2_pool` answers an
    /// address that is already registered in `BotState` (the registry of
    /// record) WITHOUT replaying the RPC choreography — proven by giving it
    /// NO `ConstructionIo` at all: the legacy path raised
    /// "no `ConstructionIo` attached" here; the pre-check answers identity
    /// straight off the registered entry instead.
    #[test]
    fn build_v2_pool_answers_registered_address_from_the_registry_of_record() {
        pyo3::Python::attach(|py| {
            let bot = PyBot::new(1);
            let addr = "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa";
            let t0 = "0x1111111111111111111111111111111111111111";
            let t1 = "0x2222222222222222222222222222222222222222";
            let factory = "0x3333333333333333333333333333333333333333";
            let r0 = pyo3::types::PyInt::new(py, 1000i64);
            let r1 = pyo3::types::PyInt::new(py, 2000i64);
            let pool_id = bot
                .register_v2_pool(
                    py,
                    addr,
                    t0,
                    t1,
                    r0.as_any(),
                    r1.as_any(),
                    997,
                    1000,
                    997,
                    1000,
                    factory,
                    5,
                    "uniswap-v2",
                    false,
                    None,
                )
                .expect("test setup: V2 registration");

            // No ConstructionIo attached — the registry-of-record pre-check
            // must answer before the io fetch.
            let (pid, token0, token1, addr_ret, variant) = bot
                .build_v2_pool(py, addr, Some(5))
                .expect("registered address answers from the registry of record");
            assert_eq!(pid, pool_id);
            assert_eq!(token0.to_lowercase(), t0);
            assert_eq!(token1.to_lowercase(), t1);
            assert_eq!(addr_ret.to_lowercase(), addr.to_lowercase());
            assert_eq!(variant, "uniswap-v2");

            // And the single-flight twin: the peer path (claim window
            // closed) re-answers through the same pre-check.
            let (pid2, ..) = bot
                .build_v2_pool(py, addr, None)
                .expect("second build answers from the registry of record");
            assert_eq!(
                pid2, pool_id,
                "same registered pool id — no duplicate build"
            );
        });
    }

    /// PRG-1: an UNREGISTERED address without an attached `ConstructionIo`
    /// still raises the legacy `RuntimeError` (the pre-check only answers
    /// registry hits; a fresh build still requires the choreography io).
    #[test]
    fn build_v2_pool_unregistered_address_requires_construction_io() {
        pyo3::Python::attach(|py| {
            let bot = PyBot::new(1);
            let err = bot
                .build_v2_pool(py, "0xbBbBbBbbBbbbBbbBbbBBbbbBbBbbBBBbbBBBbbbb", None)
                .expect_err("no ConstructionIo attached");
            assert!(
                err.to_string().contains("no ConstructionIo attached"),
                "legacy io requirement preserved for fresh builds: {err}"
            );
        });
    }

    // ── the position seam's production wiring ────────────────────────────
    //
    // `attach_construction_io` is the method Python's `Bot.__init__` calls
    // with the resolved provider and the session's database path
    // (`src/degenbot/bot/_bot.py`), and it is the only point in a bot boot
    // holding both the session and the database handle a position read is
    // sourced from. The tests below therefore boot the way a real bot boots
    // and install nothing: the position reader they find is the one the boot
    // bound. Every other position test in the workspace installs its own
    // observer, so deleting the boot's install turns ONLY these red.

    /// A supplied-and-borrowed position in one Aave market, observed at
    /// [`POSITION_OBSERVED_AT`].
    #[cfg(feature = "aave-updater")]
    const POSITION_OBSERVED_AT: u64 = 21_000_000;

    #[cfg(feature = "aave-updater")]
    fn position_market() -> Address {
        Address::from([0x7bu8; 20])
    }

    #[cfg(feature = "aave-updater")]
    fn position_account() -> Address {
        Address::from([0x8bu8; 20])
    }

    /// A throwaway database path, unique per call so a parallel test run
    /// cannot read another test's rows.
    #[cfg(feature = "aave-updater")]
    fn position_db_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "degenbot_{tag}_{}_{:x}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ))
    }

    /// Write the rows an Aave updater would have left after advancing this
    /// market to [`POSITION_OBSERVED_AT`]: one market, one supplied-and-
    /// borrowed account. Seeded through the raw connection so the fixture
    /// depends on no crate the test target does not already name.
    #[cfg(feature = "aave-updater")]
    fn seed_aave_position(path: &std::path::Path) {
        let (db, _state) =
            degenbot_db::connection::DegenbotDb::open_for_writes(path).expect("open a temp DB");
        let user = position_account().to_checksum(None);
        db.lock()
            .execute_batch(&format!(
                "INSERT INTO aave_v3_markets (id, chain_id, name, active, last_update_block) \
                 VALUES (1, 1, 'Aave Ethereum Market', 1, {POSITION_OBSERVED_AT}); \
                 INSERT INTO erc20_tokens (id, chain, address) \
                 VALUES (1, 1, '0x0000000000000000000000000000000000000001'); \
                 INSERT INTO aave_v3_assets \
                 (id, market_id, underlying_asset_id, a_token_id, a_token_revision, \
                  v_token_id, v_token_revision, liquidity_index, liquidity_rate, \
                  borrow_index, borrow_rate) \
                 VALUES (1, 1, 1, 1, 1, 1, 1, '1000000000000000000000000000', '0', \
                 '1000000000000000000000000000', '0'); \
                 INSERT INTO aave_v3_asset_configs (id, asset_id, ltv, liquidation_threshold, \
                      liquidation_bonus, borrowing_enabled, stable_borrowing_enabled, \
                      flash_loan_enabled, isolation_mode, borrowable_in_isolation) \
                 VALUES (1, 1, 8000, 8500, 10500, 1, 1, 1, 0, 0); \
                 INSERT INTO aave_v3_users (id, market_id, address, e_mode, gho_discount, \
                  isolation_mode_debt) VALUES (1, 1, '{user}', 0, 0, '0'); \
                 INSERT INTO aave_v3_collateral_positions (id, user_id, asset_id, balance) \
                 VALUES (1, 1, 1, '1000000000000000000000'); \
                 INSERT INTO aave_v3_debt_positions (id, user_id, asset_id, balance) \
                 VALUES (1, 1, 1, '1000000000000000000000');"
            ))
            .expect("seed the Aave market and position rows");
        db.lock()
            .execute(
                "INSERT INTO aave_v3_contracts (id, market_id, name, address, revision) \
                 VALUES (1, 1, 'POOL', ?1, 1)",
                [position_market().to_checksum(None)],
            )
            .expect("seed the market's pool contract");
    }

    /// The production boot's provider argument: a `PyAlloyProvider` over an
    /// offline transport, which is the shape `extract_native_alloy` accepts
    /// and the shape a configured session resolves to.
    #[cfg(feature = "aave-updater")]
    fn boot_provider(py: pyo3::Python<'_>) -> pyo3::Bound<'_, pyo3::PyAny> {
        use degenbot_rpc::offline::OfflineProvider;
        use serde_json::json;

        let offline = OfflineProvider::from_json_str(
            &json!({
                "chain_id": 1u64,
                "block_number": POSITION_OBSERVED_AT,
                "timestamp": 0u64,
                "calls": {},
                "code": {},
            })
            .to_string(),
        )
        .expect("valid offline JSON")
        .as_alloy_provider();
        pyo3::Bound::new(
            py,
            crate::rpc::provider::PyAlloyProvider {
                provider: Arc::new(offline),
                max_blocks_per_request: 5000,
            },
        )
        .expect("PyAlloyProvider construction")
        .into_any()
    }

    /// A session booted exactly as `degenbot.bot.Bot.__init__` boots it, with
    /// the given database as the session's own.
    #[cfg(feature = "aave-updater")]
    fn booted_session(py: pyo3::Python<'_>, db_path: &std::path::Path) -> PyBot {
        let bot = PyBot::new(1);
        bot.attach_construction_io(
            py,
            boot_provider(py),
            Some(db_path.to_string_lossy().as_ref()),
        )
        .expect("the production boot attaches its construction I/O");
        bot
    }

    /// The regression that keeps the wiring: a session booted by the real
    /// production path, with no observer installed by this test, reads a
    /// position the updater stored. Delete the install in
    /// `attach_construction_io` and the first assertion fails here while
    /// every hand-installing position test in the workspace stays green.
    #[cfg(feature = "aave-updater")]
    #[test]
    fn a_booted_session_reads_a_position_no_test_installed_an_observer_for() {
        use degenbot_substrate::session_registry::Freshness;

        let db_path = position_db_path("position_boot_wiring");
        seed_aave_position(&db_path);

        pyo3::Python::attach(|py| {
            let bot = booted_session(py, &db_path);
            let registry = bot.bot.session_registry();

            assert!(
                registry.has_position_observer(),
                "a booted session reaches positions through the reader the boot bound"
            );

            let identity = registry.position_identity(position_market(), position_account());
            let read = registry.read_position(
                &identity,
                &Freshness::AtOrAfter {
                    block: POSITION_OBSERVED_AT,
                },
            );
            assert!(
                read.is_ok(),
                "a booted session reads the position its updater stored; refused {:?}",
                read.as_ref().err()
            );
            let reading = read.unwrap();
            assert_eq!(reading.identity(), &identity);
            assert_eq!(reading.observed_block(), POSITION_OBSERVED_AT);
        });

        let _ = std::fs::remove_file(&db_path);
    }

    /// The bound reader is the boot's LIVE handle, not a snapshot taken at
    /// boot: the updater advancing the market after the boot is visible to the
    /// next read. A boot that handed the observer a stale copy, or a registry
    /// that cached a reading, would fail the second read's freshness floor.
    #[cfg(feature = "aave-updater")]
    #[test]
    fn a_read_through_the_boot_bound_observer_sees_a_later_updater_commit() {
        use degenbot_substrate::session_registry::Freshness;

        let db_path = position_db_path("position_boot_live");
        seed_aave_position(&db_path);

        pyo3::Python::attach(|py| {
            let bot = booted_session(py, &db_path);
            let registry = bot.bot.session_registry();

            // The updater advances the market, on its own connection, while
            // the session is already booted and reading.
            {
                let (updater_db, _state) =
                    degenbot_db::connection::DegenbotDb::open_for_writes(&db_path)
                        .expect("the updater opens its own write connection");
                updater_db
                    .lock()
                    .execute(
                        "UPDATE aave_v3_markets SET last_update_block = ?1 WHERE id = 1",
                        [i64::try_from(POSITION_OBSERVED_AT + 5).expect("block fits i64")],
                    )
                    .expect("the updater commits a later block");
            }

            let identity = registry.position_identity(position_market(), position_account());
            let read = registry.read_position(
                &identity,
                &Freshness::AtOrAfter {
                    block: POSITION_OBSERVED_AT + 5,
                },
            );
            assert!(
                read.is_ok(),
                "the boot-bound reader sees the updater's later commit; refused {:?}",
                read.as_ref().err()
            );
            assert_eq!(read.unwrap().observed_block(), POSITION_OBSERVED_AT + 5);
        });

        let _ = std::fs::remove_file(&db_path);
    }

    /// The cross-strategy acceptance claim, resting on production wiring
    /// rather than on a hand-install: the settlement arm and the
    /// txpool-backrun arm each ask ONE booted session for the same position,
    /// agree on one canonical identity, and each receives its own fresh
    /// reading. Neither arm's configuration reaches the reading.
    #[cfg(all(feature = "aave-updater", feature = "submission"))]
    #[test]
    fn the_settlement_and_backrun_arms_share_one_position_through_the_real_boot() {
        use degenbot_config::BotConfig;
        use degenbot_strategy::{Settlement, Strategy, StrategyName, TxpoolBackrun};
        use degenbot_substrate::session_registry::Freshness;

        let db_path = position_db_path("position_cross_strategy");
        seed_aave_position(&db_path);

        let mut cfg = BotConfig::default();
        cfg.strategy.settlement.active = true;
        cfg.strategy.settlement.endpoints = Some(String::from("https://a.local,https://b.local"));
        cfg.strategy.txpool_backrun.bid_mode = true;
        cfg.strategy.txpool_backrun.budget_wei = 1_000_000_000_000_000_000u128;
        cfg.strategy.txpool_backrun.endpoints = Some(String::from("https://rpc.local"));

        pyo3::Python::attach(|py| {
            let bot = booted_session(py, &db_path);
            let registry = bot.bot.session_registry();
            let settlement = Settlement::from_config(&cfg);
            let backrun = TxpoolBackrun::from_config(&cfg, String::from("http://node.invalid"));
            assert_eq!(settlement.name(), StrategyName::Settlement);
            assert_eq!(
                <TxpoolBackrun as Strategy>::NAME,
                StrategyName::TxpoolBackrun
            );

            let identity = registry.position_identity(position_market(), position_account());
            let floor = Freshness::AtOrAfter {
                block: POSITION_OBSERVED_AT,
            };
            let from_settlement = registry.read_position(&identity, &floor);
            let from_backrun = registry.read_position(&identity, &floor);
            assert!(
                from_settlement.is_ok() && from_backrun.is_ok(),
                "both arms read through the one booted session; settlement refused {:?}, backrun refused {:?}",
                from_settlement.as_ref().err(),
                from_backrun.as_ref().err()
            );
            let from_settlement = from_settlement.unwrap();
            let from_backrun = from_backrun.unwrap();

            // One canonical identity, reached from either arm.
            assert_eq!(from_settlement.identity(), from_backrun.identity());
            assert_eq!(from_settlement.identity().market(), position_market());
            assert_eq!(from_settlement.identity().account(), position_account());
            assert_eq!(from_settlement.identity().chain_id(), 1);
            // Neither arm's policy reaches the reading: the same risk report
            // both arms act on, not a per-arm projection.
            assert_eq!(
                from_settlement.health_factor(),
                from_backrun.health_factor()
            );
            let rendered = format!("{from_settlement:?}");
            assert!(!rendered.contains("a.local"));
            assert!(!rendered.contains("rpc.local"));
            assert!(!format!("{settlement:?}{backrun:?}").contains(&rendered));
        });

        let _ = std::fs::remove_file(&db_path);
    }
}
