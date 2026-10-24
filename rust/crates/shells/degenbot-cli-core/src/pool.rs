//! The `pool` command arms (ADR-051 D1;).
//!
//! Ports `cli/pool.py` arm for arm:
//!
//! - `pool update` — the chunk-loop hand-off to
//!   [`run_pool_update`](degenbot_pool_updater::run_pool_update), with the
//!   `--chunk` and `--to-block` control catalog discovery.
//! - `pool verify` compares a block-pinned Erigon snapshot with ordinary getters.

use std::future::Future;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use alloy::primitives::B256;
use degenbot_core::runtime::get_runtime;
use degenbot_db::DegenbotDb;
use degenbot_pool_updater::{
    catalog_tick_map_request, run_pool_update, verify_tick_map_snapshot, NoProgress, RunError,
};

use degenbot_rpc::liquidity_verifier::LiquidityMapTarget;
use degenbot_rpc::tick_snapshot::TickMapRequest;

use crate::block::{parse_to_block, resolve_to_block};
use crate::cancel::CancelHandle;
use crate::context::CliContext;
use crate::error::{CliError, ExchangeResumeState, PoolUpdateFailure};
use crate::prompt::{PromptPlan, Prompter};
use crate::report::PoolReport;

/// The RPC retry budget the verify arm's spot reads use.
const RPC_MAX_RETRIES: u32 = 5;

/// The pool family `--family` selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolFamily {
    /// V3 (`ticks()`/`tickBitmap()`).
    V3,
    /// V4 (`PoolManager` `extsload`).
    V4,
}

impl PoolFamily {
    /// The wire spelling (`v3` / `v4`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V3 => "v3",
            Self::V4 => "v4",
        }
    }
}

/// The `pool` command group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolCommand {
    /// Advance every active exchange's liquidity state to `to_block`.
    Update {
        /// Max blocks per chunk before committing.
        chunk_size: u64,
        /// The raw `--to-block` identifier (tag / tag:offset / integer).
        to_block: String,
    },
    /// Verify a pool's complete Erigon snapshot against on-chain truth.
    Verify {
        /// The HTTP RPC endpoint.
        rpc_url: String,
        /// The chain the pool lives on.
        chain_id: i64,
        /// The block number to read on-chain truth at.
        block_number: u64,
        /// V3 pool address, or V4 `PoolId` (bytes32 hex).
        pool: String,
        /// The pool family.
        family: PoolFamily,
        /// (V4 only) the `PoolManager` singleton.
        pool_manager: Option<String>,
    },
}

impl PoolCommand {
    /// The arm's confirmation policy: neither pool arm prompts.
    #[must_use]
    pub const fn prompt_plan(&self, _ctx: &CliContext<'_>) -> PromptPlan {
        PromptPlan::None
    }
}

/// Execute a `pool` command.
///
/// # Errors
///
/// [`CliError::InvalidBlockTag`] / [`CliError::InvalidArgument`] /
/// [`CliError::InvalidAddress`] for malformed inputs, [`CliError::Config`] for
/// an unresolved driver-domain value, and [`CliError::PoolUpdate`] for a core
/// failure.
pub(crate) fn execute(
    command: &PoolCommand,
    ctx: &CliContext<'_>,
    _prompter: &dyn Prompter,
    cancel: &CancelHandle,
) -> Result<PoolReport, CliError> {
    match command {
        PoolCommand::Update {
            chunk_size,
            to_block,
        } => update(ctx, cancel, *chunk_size, to_block),
        PoolCommand::Verify {
            rpc_url,
            chain_id,
            block_number,
            pool,
            family,
            pool_manager,
        } => verify(
            ctx,
            rpc_url,
            *chain_id,
            *block_number,
            pool,
            *family,
            pool_manager.as_deref(),
        ),
    }
}

/// `pool update`.
fn update(
    ctx: &CliContext<'_>,
    cancel: &CancelHandle,
    chunk_size: u64,
    to_block: &str,
) -> Result<PoolReport, CliError> {
    let database_path = ctx.database_path()?.value;
    let chain_id = ctx.chain_id()?.value;
    let rpc_url = ctx.node_request_uri()?.value;
    // Self-serve registration: every supported exchange pair not found in the
    // DB registers inactive, so the update never depends on prior CREATEs.
    crate::registrations::ensure_supported_registrations(&database_path)?;
    let resolved = resolve_to_block(parse_to_block(to_block)?, &rpc_url)?;
    let chain = i64::try_from(chain_id)
        .map_err(|_| CliError::InvalidArgument(format!("chain id {chain_id} is out of range")))?;
    // The pre-run resume snapshot, for the failure line's requested-range
    // bound: it mirrors the core's own `initial_start_block` (the minimum
    // `last_update_block + 1` across the active exchanges). Read-only, so a
    // failed diagnostic read must not fail the run — it degrades to the
    // requested target.
    let cursors = read_exchange_cursors(database_path.as_path(), chain).unwrap_or_default();
    let from_block = initial_run_start_block(&cursors, resolved);
    // The thin live-provider wrapper (ADR-068 D5): the CLI owns the transport
    // construction — ONE build per run on the shared runtime, exactly where
    // the core's old internal `AlloyProvider::new` site sat — and the core
    // only injects it. A build failure flows through the SAME
    // [`PoolUpdateFailure`] report as a mid-run failure (endpoint + chain +
    // the committed resume cursors), since it IS the run's transport dying.
    let run = match shared_runtime_block_on(async {
        degenbot_rpc::provider::AlloyProvider::new(&rpc_url, RPC_MAX_RETRIES).await
    }) {
        Ok(Ok(provider)) => run_pool_update(
            &database_path,
            chain,
            resolved,
            chunk_size,
            provider,
            cancel.flag(),
            Arc::new(NoProgress),
        ),
        Ok(Err(err)) => Err(RunError::from(err)),
        Err(cli_err) => return Err(cli_err),
    };
    match run {
        Ok(report) => Ok(PoolReport::Updated {
            chain_id: report.chain_id,
            from_block: report.from_block,
            to_block: report.to_block,
            chunks_committed: report.chunks_committed,
            total_pools_written: report.total_pools_written,
        }),
        Err(RunError::Cancelled) => Ok(PoolReport::UpdateCancelled { chain_id: chain }),
        Err(err) => {
            // Post-failure resume snapshot: the committed per-chunk progress
            // (a chunk-boundary commit advances the exchange cursor), read
            // only — the outstanding work a rerun resumes from. `None` when
            // the snapshot read itself failed; the rendering says so.
            let resume = read_exchange_cursors(database_path.as_path(), chain).ok();
            Err(CliError::PoolUpdate(Box::new(PoolUpdateFailure {
                error: err,
                rpc_url: rpc_url.clone(),
                chain_id: chain,
                from_block,
                to_block: resolved,
                resume,
            })))
        }
    }
}

/// The read-only per-exchange cursor snapshot for `chain_id` — the resume
/// state a mid-run failure leaves committed. Plain `SELECT`, so it writes
/// nothing; `None` cursors are the never-updated exchanges.
fn read_exchange_cursors(
    database_path: &Path,
    chain_id: i64,
) -> Result<Vec<ExchangeResumeState>, rusqlite::Error> {
    let conn = rusqlite::Connection::open_with_flags(
        database_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut statement = conn.prepare(
        "SELECT name, last_update_block FROM exchanges \
         WHERE chain_id = ?1 AND active = 1 ORDER BY name",
    )?;
    let rows = statement.query_map([chain_id], |row| {
        Ok(ExchangeResumeState {
            name: row.get(0)?,
            last_update_block: row.get(1)?,
        })
    })?;
    rows.collect()
}

/// The run's intended start block, mirroring the core's own
/// `initial_start_block`: the minimum `last_update_block + 1` across the
/// active exchanges (a never-updated exchange starts at 1). An empty
/// registration set falls back to the requested target.
fn initial_run_start_block(cursors: &[ExchangeResumeState], to_block: Option<u64>) -> u64 {
    cursors
        .iter()
        .map(|row| {
            row.last_update_block
                .map_or(1, |block| u64::try_from(block).unwrap_or(0) + 1)
        })
        .min()
        .unwrap_or_else(|| to_block.unwrap_or(1))
        .max(1)
}

/// `pool verify`.
fn verify(
    ctx: &CliContext<'_>,
    rpc_url: &str,
    chain_id: i64,
    block_number: u64,
    pool: &str,
    family: PoolFamily,
    pool_manager: Option<&str>,
) -> Result<PoolReport, CliError> {
    let database_path = ctx.database_path()?.value;
    let request = {
        let (db, _state) = DegenbotDb::open(&database_path)?;
        let conn = db.lock();
        fetch_verify_state(&conn, chain_id, pool, family, pool_manager)?
    };
    // The arm resolves catalog identity for `chain_id`, so the
    // provider is bound to that chain: an endpoint serving another chain is
    // refused before any on-chain comparison reports phantom divergences.
    let chain = u64::try_from(chain_id).map_err(|_| {
        CliError::InvalidArgument(format!("chain id {chain_id} is not a valid chain"))
    })?;
    // ONE chain-verified provider for the whole arm, on the process-wide
    // shared runtime — the pattern `run_pool_update` uses for its chunk
    // loop — NOT a fresh runtime + transport built inside `block::block_on`
    // per target family (a per-family, per-verification build with its
    // connection tasks destroyed alongside the ad-hoc runtime is the churn
    // that intermittently surfaces as alloy's
    // `TransportErrorKind::BackendGone`). The construction is lazy (the
    // node is dialed only after the committed row above is resolved), the
    // chain binding still runs exactly once (`for_chain` refuses a
    // foreign-chain endpoint before any comparison), and no retry layer is
    // added: a transport failure surfaces as the typed run failure.
    let divergences = shared_runtime_block_on(async {
        note_verify_provider_build();
        let provider =
            degenbot_rpc::provider::AlloyProvider::for_chain(rpc_url, chain, RPC_MAX_RETRIES)
                .await
                .map_err(|err| CliError::BlockResolution(err.to_string()))?;
        verify_tick_map_snapshot(&provider, request, block_number)
            .await
            .map_err(|error| {
                CliError::PoolUpdate(Box::new(PoolUpdateFailure {
                    error,
                    rpc_url: rpc_url.to_string(),
                    chain_id,
                    from_block: block_number,
                    to_block: Some(block_number),
                    resume: None,
                }))
            })
    })??;

    Ok(PoolReport::Verified {
        pool: pool.to_string(),
        family,
        block_number,
        divergences,
    })
}

/// Drive `fut` on the process-wide shared runtime — the `&'static`
/// `degenbot_core::runtime::get_runtime()` singleton the updater cores
/// drive — instead of building a throwaway `Builder` runtime per call.
/// Same nesting constraint as [`crate::block::block_on`]: the arm must not
/// run from inside another runtime.
///
/// # Errors
///
/// [`CliError::RuntimeNested`] when called from inside an existing runtime.
pub(crate) fn shared_runtime_block_on<F: Future>(fut: F) -> Result<F::Output, CliError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(CliError::RuntimeNested);
    }
    Ok(get_runtime().block_on(fut))
}

/// The process-lifetime count of `pool verify` transport builds (the
/// `for_chain` site above). The lifecycle contract to pin: ONE build per
/// verify run — zero when the arm fails before the node is needed, and
/// never one per target family. The companion counter lives at the chunk
/// loop's own build site (`degenbot-pool-updater`'s
/// `run_provider_build_count`).
static VERIFY_PROVIDER_BUILDS: AtomicU64 = AtomicU64::new(0);

fn note_verify_provider_build() {
    VERIFY_PROVIDER_BUILDS.fetch_add(1, Ordering::Relaxed);
}

/// The instrumented transport-build count — a test-visible seam so the
/// one-build-per-run contract is pinnable offline (see the crate's
/// `pool_verify_lifecycle` test).
#[doc(hidden)]
#[must_use]
pub fn verify_provider_build_count() -> u64 {
    VERIFY_PROVIDER_BUILDS.load(Ordering::Relaxed)
}

/// Reset the transport-build counter to zero, returning the previous value.
#[doc(hidden)]
#[must_use]
pub fn reset_verify_provider_build_count() -> u64 {
    VERIFY_PROVIDER_BUILDS.swap(0, Ordering::Relaxed)
}

/// Resolve the contract identity and tick spacing from the catalog.
fn fetch_verify_state(
    conn: &rusqlite::Connection,
    chain_id: i64,
    pool: &str,
    family: PoolFamily,
    pool_manager: Option<&str>,
) -> Result<TickMapRequest, CliError> {
    let target = match family {
        PoolFamily::V3 => LiquidityMapTarget::V3(
            pool.parse()
                .map_err(|_| CliError::InvalidAddress(pool.to_string()))?,
        ),
        PoolFamily::V4 => {
            let manager = pool_manager.ok_or_else(|| {
                CliError::InvalidArgument(
                    "--pool-manager is required for --family v4 (the PoolManager singleton)."
                        .into(),
                )
            })?;
            LiquidityMapTarget::V4 {
                pool_manager: manager
                    .parse()
                    .map_err(|_| CliError::InvalidAddress(manager.to_string()))?,
                pool_id: B256::from_str(pool.strip_prefix("0x").unwrap_or(pool))
                    .map_err(|_| CliError::InvalidAddress(pool.to_string()))?,
            }
        }
    };
    catalog_tick_map_request(conn, chain_id, target).map_err(|error| match error {
        RunError::Db(degenbot_db::DbError::Sqlite(rusqlite::Error::QueryReturnedNoRows)) => {
            CliError::InvalidArgument(format!(
                "{} pool {pool} not found on chain {chain_id}",
                family.as_str()
            ))
        }
        RunError::Db(error) => CliError::Database(error),
        other => CliError::InvalidArgument(other.to_string()),
    })
}
