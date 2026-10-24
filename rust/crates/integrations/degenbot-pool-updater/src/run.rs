//! Historical pool discovery with atomic catalog writes and per-exchange checkpoints.
//!
//! State initialization belongs to the block-pinned Erigon tick-map RPC. Discovery
//! cursors describe catalog coverage and do not claim liquidity-map coverage.

use crate::fetch::{fetch_pool_created_logs_for_spec, DecodedPoolCreated};
use crate::spec::{load_active_exchange_specs, ExchangeSpec};
use alloy::primitives::{Address, B256};
use degenbot_core::errors::ProviderError;
use degenbot_core::op_info;
use degenbot_core::updater_telemetry::{UpdaterKind, UpdaterStage};
use degenbot_db::{DegenbotDb, V2PoolRowInput, V3PoolRowInput, V4PoolRowInput};
use degenbot_rpc::provider::{AlloyProvider, LogFetcher};
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(2);
const MARKER_RETRY_CAP: usize = 4;

/// The outcome and measured work of one catalog chunk.
#[derive(Debug, Clone, Copy)]
pub struct ChunkProgress {
    pub chain_id: i64,
    pub chunk_start: u64,
    pub chunk_end: u64,
    pub pools_written: usize,
    pub committed: bool,
    pub is_final: bool,
    pub fetch_time: Duration,
    pub apply_time: Duration,
    pub write_lock_hold_time: Duration,
}
/// The sink the chunk loop reports per-chunk progress to. Implementations:
/// `NoProgress` (silent) or a programmatic consumer's sink (a test harness
/// collecting per-chunk state).
///
/// `report_chunk` is synchronous (called between chunks, off the async path).
pub trait ProgressSink: Send + Sync {
    /// Report a chunk's outcome. Called once per chunk (chunk boundary).
    fn report_chunk(&self, progress: &ChunkProgress);
}

/// A no-op [`ProgressSink`] — silent runs (the default for `run_pool_update`
/// when no sink is supplied).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoProgress;

impl ProgressSink for NoProgress {
    fn report_chunk(&self, _progress: &ChunkProgress) {}
}

/// The percentage (0–100) of the run's block span covered through
/// `working_end_block`. The denominator is the run's actual range
/// (`initial_start_block..=last_block`), so the estimate is meaningful even
/// when the run starts well below the chain tip. Saturating integer math keeps
/// a single-block (or already-complete) run at 100.
fn progress_percent(initial_start_block: u64, working_end_block: u64, last_block: u64) -> u64 {
    let total = last_block
        .saturating_sub(initial_start_block)
        .saturating_add(1);
    if total == 0 {
        return 100;
    }
    let processed = working_end_block
        .saturating_sub(initial_start_block)
        .saturating_add(1)
        .min(total);
    processed.saturating_mul(100) / total
}

#[derive(Debug, Clone)]
pub enum PoolCreationToWrite {
    /// A V2 pool row (canonical `Uniswap V2` / `PancakeSwap V2` / `SushiSwap V2` /
    /// `Swapbased V2` — constant fee from the spec's `v2_fee_token`).
    V2 {
        exchange_id: i64,
        row: V2PoolRowInput,
    },
    /// A V3 pool row (`Uniswap V3` / `PancakeSwap V3` / `SushiSwap V3` /
    /// `Aerodrome V3` — fee + `tick_spacing` from the event).
    V3 {
        exchange_id: i64,
        row: V3PoolRowInput,
    },
    /// A V4 pool row (`Uniswap V4` — fee + `tick_spacing` + hooks from the
    /// `Initialize` event).
    V4 {
        exchange_id: i64,
        row: V4PoolRowInput,
    },
}

/// Catalog rows already resolved by the fetch phase.
#[derive(Debug, Default)]
pub struct ChunkInputs {
    pub pool_creations: Vec<PoolCreationToWrite>,
}

/// Writes performed by one catalog transaction.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ChunkWriteReport {
    pub pools_written: usize,
    pub apply_time: Duration,
    pub stamped_last_update_block: i64,
}
/// An error from [`map_pool_creation`] — the pure decode→row-input mapping.
#[derive(Debug, thiserror::Error)]
pub enum MapError {
    /// An Aerodrome V2 pool whose per-pool `getFee(address,bool)` RPC fee
    /// wasn't supplied. The outer loop MUST resolve the fee (an on-chain call)
    /// before mapping; passing `None` for `aerodrome_fee` yields this error so
    /// no pool is silently written with a fake/zero fee. The outer loop skips
    /// these pools (reporting via [`ProgressSink`]) until the fee-RPC path is
    /// wired (a follow-up; canonical V2/V3/V4 + Aerodrome V3 fees come from the
    /// event or a constant + are NOT affected).
    #[error(
        "Aerodrome V2 pool {pool_address} needs a per-pool getFee RPC resolution \
         (aerodrome_fee is None); the outer loop must resolve it before mapping"
    )]
    AerodromeFeeNeeded {
        /// The pool whose fee couldn't be resolved.
        pool_address: Address,
    },
}

/// Map a decoded `PoolCreated`/`Initialize` event to its DB `V*PoolRowInput`,
/// carrying the `exchange_id` context (which [`DecodedPoolCreated`] correctly
/// avoids — it's decode-pure). Pure (no RPC) EXCEPT the Aerodrome V2 fee,
/// which the caller supplies via `aerodrome_fee` (resolved by the outer loop's
/// on-chain `getFee(address, stable)` call — `None` only for non-Aerodrome V2
/// families, where this arg is ignored).
///
/// # Errors
///
/// Returns [`MapError::AerodromeFeeNeeded`] if the event is an Aerodrome V2
/// pool + `aerodrome_fee` is `None` (the caller forgot to resolve the fee).
pub fn map_pool_creation(
    spec: &ExchangeSpec,
    event: &DecodedPoolCreated,
    aerodrome_fee: Option<i64>,
) -> Result<PoolCreationToWrite, MapError> {
    match event {
        DecodedPoolCreated::V2(e) => {
            // Canonical V2: constant fee from the spec's `v2_fee_token`
            // (uniswap_v2: 3, pancakeswap_v2: 25, sushiswap_v2/swapbased_v2: 3).
            let fee = spec.v2_fee_token.unwrap_or(0);
            Ok(PoolCreationToWrite::V2 {
                exchange_id: spec.id,
                row: V2PoolRowInput {
                    address: e.pool_address,
                    token0_address: e.token0,
                    token1_address: e.token1,
                    fee_token0: fee,
                    fee_token1: fee,
                    stable: None,
                },
            })
        }
        DecodedPoolCreated::AerodromeV2(e) => {
            // Aerodrome V2: per-pool fee from on-chain `getFee(address, stable)`
            // (NOT a constant + NOT in the event). The outer loop resolves it.
            let fee = aerodrome_fee.ok_or(MapError::AerodromeFeeNeeded {
                pool_address: e.pool_address,
            })?;
            Ok(PoolCreationToWrite::V2 {
                exchange_id: spec.id,
                row: V2PoolRowInput {
                    address: e.pool_address,
                    token0_address: e.token0,
                    token1_address: e.token1,
                    fee_token0: fee,
                    fee_token1: fee,
                    stable: Some(e.stable),
                },
            })
        }
        DecodedPoolCreated::V3(e) => {
            // V3 fee + tick_spacing come from the event (applies to Uniswap V3,
            // PancakeSwap V3, SushiSwap V3, AND Aerodrome V3 — they all reuse
            // the V3 decode + carry fee/tick_spacing in the event).
            Ok(PoolCreationToWrite::V3 {
                exchange_id: spec.id,
                row: V3PoolRowInput {
                    address: e.pool_address,
                    token0_address: e.token0,
                    token1_address: e.token1,
                    fee: i64::from(e.fee),
                    tick_spacing: i64::from(e.tick_spacing),
                },
            })
        }
        DecodedPoolCreated::V4(e) => Ok(PoolCreationToWrite::V4 {
            exchange_id: spec.id,
            row: V4PoolRowInput {
                pool_hash: B256::from(e.pool_id).to_string(),
                hooks: e.hooks,
                currency0_address: e.currency0,
                currency1_address: e.currency1,
                fee: i64::from(e.fee),
                tick_spacing: i64::from(e.tick_spacing),
            },
        }),
    }
}

/// Write catalog rows and compare-and-set their exchange checkpoints together.
///
/// The caller owns the transaction so any failed checkpoint rolls back the rows.
///
/// # Errors
///
/// Returns a database error or [`RunError::MarkerMoved`] when a checkpoint changed.
pub fn apply_chunk_writes_on_conn(
    conn: &Connection,
    chain_id: i64,
    specs_to_update: &[ExchangeSpec],
    chunk_end: u64,
    inputs: &ChunkInputs,
) -> Result<ChunkWriteReport, RunError> {
    let started = Instant::now();
    let mut report = ChunkWriteReport::default();
    upsert_pool_creations_on_conn(
        conn,
        chain_id,
        specs_to_update,
        &inputs.pool_creations,
        &mut report,
    )?;
    let chunk_end = i64::try_from(chunk_end).unwrap_or(i64::MAX);
    for spec in specs_to_update {
        if !DegenbotDb::set_exchange_last_update_block_if_unchanged_on_conn(
            conn,
            chain_id,
            spec.id,
            chunk_end,
            spec.last_update_block,
        )? {
            return Err(RunError::MarkerMoved {
                exchange_id: spec.id,
            });
        }
    }
    report.stamped_last_update_block = chunk_end;
    report.apply_time = started.elapsed();
    Ok(report)
}

fn upsert_pool_creations_on_conn(
    conn: &Connection,
    chain_id: i64,
    specs_to_update: &[ExchangeSpec],
    pool_creations: &[PoolCreationToWrite],
    report: &mut ChunkWriteReport,
) -> Result<(), degenbot_db::DbError> {
    let mut v2_by_exchange: HashMap<i64, (String, i64, Vec<V2PoolRowInput>)> = HashMap::new();
    let mut v3_by_exchange: HashMap<i64, (String, i64, Vec<V3PoolRowInput>)> = HashMap::new();
    let mut v4_by_exchange: HashMap<i64, (String, i64, Vec<V4PoolRowInput>)> = HashMap::new();
    for creation in pool_creations {
        let Some(spec) = specs_to_update
            .iter()
            .find(|s| s.id == exchange_id_of(creation))
        else {
            continue;
        };
        match creation {
            PoolCreationToWrite::V2 { exchange_id, row } => {
                v2_by_exchange
                    .entry(*exchange_id)
                    .or_insert_with(|| (spec.name.clone(), spec.fee_denominator, Vec::new()))
                    .2
                    .push(row.clone());
            }
            PoolCreationToWrite::V3 { exchange_id, row } => {
                v3_by_exchange
                    .entry(*exchange_id)
                    .or_insert_with(|| (spec.name.clone(), spec.fee_denominator, Vec::new()))
                    .2
                    .push(row.clone());
            }
            PoolCreationToWrite::V4 { exchange_id, row } => {
                v4_by_exchange
                    .entry(*exchange_id)
                    .or_insert_with(|| (spec.name.clone(), spec.fee_denominator, Vec::new()))
                    .2
                    .push(row.clone());
            }
        }
    }
    for (exchange_id, (kind, fee_denominator, rows)) in v2_by_exchange {
        DegenbotDb::upsert_v2_pools_on_conn(
            conn,
            chain_id,
            &kind,
            exchange_id,
            fee_denominator,
            &rows,
        )?;
        report.pools_written += rows.len();
    }
    for (exchange_id, (kind, fee_denominator, rows)) in v3_by_exchange {
        DegenbotDb::upsert_v3_pools_on_conn(
            conn,
            chain_id,
            &kind,
            exchange_id,
            fee_denominator,
            &rows,
        )?;
        report.pools_written += rows.len();
    }
    for (exchange_id, (kind, fee_denominator, rows)) in v4_by_exchange {
        // V4 upserts `uniswap_v4_pools` keyed by the manager address (the
        // spec's `factory` is the PoolManager). The kind is always
        // `uniswap_v4`; `fee_denominator` is `1_000_000`.
        let _ = (kind, fee_denominator);
        DegenbotDb::upsert_v4_pools_on_conn(
            conn,
            chain_id,
            &specs_to_update
                .iter()
                .find(|s| s.id == exchange_id)
                .map(|s| s.factory.to_checksum(None))
                .unwrap_or_default(),
            1_000_000,
            &rows,
        )?;
        report.pools_written += rows.len();
    }
    Ok(())
}

/// Extract the `exchange_id` from a [`PoolCreationToWrite`] (a small helper
/// to keep the in-scope lookup in [`apply_chunk_writes_on_conn`] readable).
fn exchange_id_of(creation: &PoolCreationToWrite) -> i64 {
    match creation {
        PoolCreationToWrite::V2 { exchange_id, .. }
        | PoolCreationToWrite::V3 { exchange_id, .. }
        | PoolCreationToWrite::V4 { exchange_id, .. } => *exchange_id,
    }
}

fn in_scope_fetch_start(
    spec_last_update: Option<i64>,
    working_start: u64,
    working_end: u64,
) -> Option<u64> {
    let marker = spec_last_update.map_or(0, |b| u64::try_from(b).unwrap_or(0));
    (marker < working_end).then(|| (marker + 1).max(working_start))
}

/// Catalog coverage advanced by a run.
#[derive(Debug, Default, Clone, Copy)]
pub struct UpdateReport {
    pub chain_id: i64,
    pub from_block: u64,
    pub to_block: u64,
    pub chunks_committed: usize,
    pub total_pools_written: usize,
}

/// Discovery, persistence, or cooperative cancellation failure.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("database error: {0}")]
    Db(#[from] degenbot_db::DbError),
    #[error("rpc error: {0}")]
    Provider(#[from] ProviderError),
    #[error("runtime error: {0}")]
    Runtime(#[from] std::io::Error),
    #[error("cancelled by cancel flag at chunk boundary")]
    Cancelled,
    #[error(
        "exchange {exchange_id} marker moved under the chunk (optimistic stamp matched zero rows)"
    )]
    MarkerMoved { exchange_id: i64 },
}

/// Discover pools through the requested block using one transaction per chunk.
///
/// # Errors
///
/// Returns database, provider, invalid-range, cancellation, or checkpoint errors.
pub fn run_pool_update(
    database_path: &Path,
    chain_id: i64,
    to_block: Option<u64>,
    chunk_size: u64,
    provider: AlloyProvider,
    cancel: Arc<AtomicBool>,
    progress: Arc<dyn ProgressSink>,
) -> Result<UpdateReport, RunError> {
    let (db, _) = DegenbotDb::open_for_writes(database_path)?;
    run_pool_update_on_db(
        &db, chain_id, to_block, chunk_size, provider, cancel, progress,
    )
}

/// Run discovery using an existing writable handle, including traced replay handles.
///
/// # Errors
///
/// Returns database, provider, invalid-range, cancellation, or checkpoint errors.
#[expect(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run_pool_update_on_db(
    db: &DegenbotDb,
    chain_id: i64,
    to_block: Option<u64>,
    chunk_size: u64,
    provider: AlloyProvider,
    cancel: Arc<AtomicBool>,
    progress: Arc<dyn ProgressSink>,
) -> Result<UpdateReport, RunError> {
    if chunk_size == 0 {
        return Err(ProviderError::InvalidBlockRange { from: 1, to: 0 }.into());
    }
    let mut specs = load_active_exchange_specs(db, chain_id)?;
    if specs.is_empty() {
        return Ok(UpdateReport {
            chain_id,
            to_block: to_block.unwrap_or(0),
            ..Default::default()
        });
    }
    let rt = degenbot_core::runtime::get_runtime();
    let provider = Arc::new(provider);
    let last_block = match to_block {
        Some(block) => block,
        None => rt.block_on(provider.get_block_number())?,
    };
    let fetcher = LogFetcher::new(provider, chunk_size);
    specs.retain(|s| {
        s.last_update_block
            .is_none_or(|b| u64::try_from(b).unwrap_or(0) < last_block)
    });
    let start = specs
        .iter()
        .map(|s| {
            s.last_update_block
                .map_or(1, |b| u64::try_from(b).unwrap_or(0).saturating_add(1))
        })
        .min()
        .unwrap_or(last_block)
        .max(1);
    let mut report = UpdateReport {
        chain_id,
        from_block: start,
        to_block: last_block,
        ..Default::default()
    };
    let mut working_start = start;
    let mut retries = 0;
    let mut last_progress_log = Instant::now();
    while working_start <= last_block && !specs.is_empty() {
        if cancel.load(Ordering::Acquire) {
            return Err(RunError::Cancelled);
        }
        let end = last_block.min(working_start.saturating_add(chunk_size - 1));
        let chunk_span = tracing::info_span!(
            "degenbot.updater.pool.chunk",
            chain.id = chain_id,
            chunk.start = working_start,
            chunk.end = end
        );
        let _chunk_guard = chunk_span.enter();
        let rpc_started = degenbot_rpc::provider::rpc_round_trips();
        let scoped: Vec<_> = specs
            .iter()
            .filter_map(|s| {
                in_scope_fetch_start(s.last_update_block, working_start, end)
                    .map(|from| (s.clone(), from))
            })
            .collect();
        if scoped.is_empty() {
            working_start = end.saturating_add(1);
            continue;
        }
        let chunk_specs: Vec<_> = scoped.iter().map(|(s, _)| s.clone()).collect();
        let fetch_started = Instant::now();
        let fetch_span = tracing::info_span!("degenbot.updater.pool.fetch");
        let fetch_guard = fetch_span.enter();
        let inputs = ChunkInputs {
            pool_creations: rt.block_on(fetch_pool_creations(&fetcher, end, &scoped))?,
        };
        let fetch_time = fetch_started.elapsed();
        drop(fetch_guard);
        let apply_span = tracing::info_span!("degenbot.updater.pool.apply");
        let apply_guard = apply_span.enter();
        let mut guard = db.lock();
        let lock_started = Instant::now();
        let tx = guard.transaction().map_err(degenbot_db::DbError::from)?;
        let result = apply_chunk_writes_on_conn(&tx, chain_id, &chunk_specs, end, &inputs);
        let result = match result {
            Ok(writes) => tx
                .commit()
                .map(|()| writes)
                .map_err(degenbot_db::DbError::from)
                .map_err(RunError::from),
            Err(error) => {
                drop(tx);
                Err(error)
            }
        };
        let lock_time = lock_started.elapsed();
        drop(guard);
        drop(apply_guard);
        let outcome = ChunkProgress {
            chain_id,
            chunk_start: working_start,
            chunk_end: end,
            pools_written: result.as_ref().map_or(0, |r| r.pools_written),
            committed: result.is_ok(),
            is_final: result.is_ok() && end >= last_block,
            fetch_time,
            apply_time: result.as_ref().map_or(Duration::ZERO, |r| r.apply_time),
            write_lock_hold_time: lock_time,
        };
        progress.report_chunk(&outcome);
        if let Some(t) = degenbot_core::updater_telemetry::updaters() {
            t.observe_lock_hold(UpdaterKind::Pool, lock_time.as_secs_f64());
            if outcome.committed {
                t.add_rpc_round_trips(
                    UpdaterKind::Pool,
                    degenbot_rpc::provider::rpc_round_trips().saturating_sub(rpc_started),
                );
                t.observe_stage(
                    UpdaterKind::Pool,
                    UpdaterStage::Fetch,
                    fetch_time.as_secs_f64(),
                );
                t.observe_stage(
                    UpdaterKind::Pool,
                    UpdaterStage::Apply,
                    outcome.apply_time.as_secs_f64(),
                );
            }
        }
        let writes = match result {
            Err(RunError::MarkerMoved { .. }) if retries < MARKER_RETRY_CAP => {
                retries += 1;
                specs = load_active_exchange_specs(db, chain_id)?;
                continue;
            }
            result => result?,
        };
        for spec in &mut specs {
            if chunk_specs.iter().any(|s| s.id == spec.id) {
                spec.last_update_block = Some(writes.stamped_last_update_block);
            }
        }
        report.chunks_committed += 1;
        report.total_pools_written += writes.pools_written;
        if last_progress_log.elapsed() >= PROGRESS_LOG_INTERVAL || end >= last_block {
            op_info!(
                domain = ingest,
                chain_id,
                chunk_start = working_start,
                chunk_end = end,
                chunks_committed = report.chunks_committed,
                pools_written = writes.pools_written,
                progress_pct = progress_percent(start, end, last_block),
                "pool catalog update: chunk committed"
            );
            last_progress_log = Instant::now();
        }
        if end == last_block {
            break;
        }
        working_start = end + 1;
    }
    Ok(report)
}
async fn fetch_pool_creations(
    fetcher: &LogFetcher,
    to_block: u64,
    specs_with_start: &[(ExchangeSpec, u64)],
) -> Result<Vec<PoolCreationToWrite>, ProviderError> {
    let mut out = Vec::new();
    for (spec, from_block) in specs_with_start {
        let decoded =
            fetch_pool_created_logs_for_spec(fetcher, *from_block, to_block, spec).await?;
        for event in decoded {
            match map_pool_creation(spec, &event, /* aerodrome_fee */ None) {
                Ok(row) => out.push(row),
                Err(MapError::AerodromeFeeNeeded { pool_address }) => {
                    // Aerodrome V2 fee RPC not yet wired — skip this pool
                    // (a follow-up task resolves getFee(address, stable)).
                    // Logged via stderr (the chunk's progress sink doesn't carry
                    // per-pool warnings; a future sink revision might).
                    #[expect(clippy::print_stderr)] // deliberate per-pool stderr warning
                    {
                        eprintln!(
                            "degenbot-pool-updater: skipping Aerodrome V2 pool \
                             {pool_address} (exchange {}): per-pool getFee RPC not wired yet",
                            spec.name,
                        );
                    }
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use degenbot_db::DegenbotDb;

    /// An in-memory writeable DB with the head schema (the chunk-writer's
    /// substrate). Mirrors the discovery tests' `write_db` helper.
    fn write_db() -> DegenbotDb {
        let (db, _state) = DegenbotDb::open_for_writes(Path::new(":memory:")).unwrap();
        db
    }

    /// The progress estimate spans the run's actual range (not the chain
    /// tip) and saturates at 100 on the final chunk.
    #[test]
    fn progress_percent_spans_the_run_range() {
        assert_eq!(progress_percent(1, 1, 100), 1);
        assert_eq!(progress_percent(1, 50, 100), 50);
        assert_eq!(progress_percent(1, 100, 100), 100);
        // A non-1 start block: the denominator is the run span (50 blocks),
        // so halfway through the run reads 50%, not 75%.
        assert_eq!(progress_percent(51, 75, 100), 50);
        // A single-block run reports 100 on its only chunk.
        assert_eq!(progress_percent(100, 100, 100), 100);
    }

    #[test]
    fn apply_chunk_writes_on_conn_commits_pools_and_stamp_together() {
        let db = write_db();
        let factory = Address::from([0x1f; 20]);
        let exchange = db.upsert_exchange(1, "uniswap_v3", factory, None).unwrap();
        let spec = ExchangeSpec {
            id: exchange.id,
            chain_id: 1,
            name: "uniswap_v3".to_string(),
            factory,
            family: crate::fetch::PoolFamily::V3,
            event_topic: B256::ZERO,
            fee_denominator: 1_000_000,
            v2_fee_token: None,
            rpc_fee_call: None,
            last_update_block: None,
        };
        let specs = [spec.clone()];
        let pool = V3PoolRowInput {
            address: Address::from([0x11; 20]),
            token0_address: Address::from([0x22; 20]),
            token1_address: Address::from([0x33; 20]),
            fee: 500,
            tick_spacing: 10,
        };
        let inputs = ChunkInputs {
            pool_creations: vec![PoolCreationToWrite::V3 {
                exchange_id: exchange.id,
                row: pool.clone(),
            }],
        };

        // ONE transaction wraps the pool write + the stamp.
        let mut guard = db.lock();
        let tx = guard.transaction().unwrap();
        let report = apply_chunk_writes_on_conn(&tx, 1, &specs, 100, &inputs).unwrap();
        tx.commit().unwrap();
        drop(guard);

        assert_eq!(report.pools_written, 1);
        let graph = db
            .fetch_path_graph_edges(1, &[degenbot_db::PoolKind::V3])
            .unwrap();
        assert_eq!(graph.edges.len(), 1);
        let conn = db.lock();
        let stored_ticks: i64 = conn
            .query_row("SELECT count(*) FROM liquidity_positions", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(stored_ticks, 0);
        drop(conn);
        // Both the pool + the stamp landed (atomicity).
        assert!(db.fetch_pool_by_address(pool.address, 1).unwrap().is_some());
        let after = db.fetch_exchange(exchange.id).unwrap().unwrap();
        assert_eq!(after.last_update_block, Some(100));
    }

    #[test]
    fn apply_chunk_writes_on_conn_rolls_back_on_injected_duplicate_failure() {
        let db = write_db();
        let factory = Address::from([0x1f; 20]);
        let exchange = db.upsert_exchange(1, "uniswap_v3", factory, None).unwrap();
        let spec = ExchangeSpec {
            id: exchange.id,
            chain_id: 1,
            name: "uniswap_v3".to_string(),
            factory,
            family: crate::fetch::PoolFamily::V3,
            event_topic: B256::ZERO,
            fee_denominator: 1_000_000,
            v2_fee_token: None,
            rpc_fee_call: None,
            last_update_block: None,
        };
        let specs = [spec.clone()];
        let pool = V3PoolRowInput {
            address: Address::from([0x11; 20]),
            token0_address: Address::from([0x22; 20]),
            token1_address: Address::from([0x33; 20]),
            fee: 500,
            tick_spacing: 10,
        };
        let inputs_ok = ChunkInputs {
            pool_creations: vec![PoolCreationToWrite::V3 {
                exchange_id: exchange.id,
                row: pool.clone(),
            }],
        };
        let inputs_dup = ChunkInputs {
            pool_creations: vec![PoolCreationToWrite::V3 {
                exchange_id: exchange.id,
                row: pool.clone(),
            }],
        };

        // First chunk: commit cleanly (pool + stamp=100).
        {
            let mut guard = db.lock();
            let tx = guard.transaction().unwrap();
            apply_chunk_writes_on_conn(&tx, 1, &specs, 100, &inputs_ok).unwrap();
            tx.commit().unwrap();
        }

        // Second chunk: write the SAME pool (duplicate) → UNIQUE violation →
        // the inner fn returns Err → tx drops → rollback. The stamp advance to
        // 200 must NOT be durable (the chunk-atomicity + restart invariants).
        let err = {
            let mut guard = db.lock();
            let tx = guard.transaction().unwrap();
            let result = apply_chunk_writes_on_conn(&tx, 1, &specs, 200, &inputs_dup);
            // The duplicate insert must surface as an error (UNIQUE constraint).
            assert!(result.is_err(), "duplicate pool insert must error");
            // Drop tx without commit → rollback (the inner fn's `?` already returned).
            drop(tx);
            result.err()
        };

        // (a) The stamp stayed at 100 (the 200 advance rolled back).
        let after = db.fetch_exchange(exchange.id).unwrap().unwrap();
        assert_eq!(
            after.last_update_block,
            Some(100),
            "rolled-back chunk's stamp advance must not be durable (restart-safe)",
        );
        // (b) The pool is still the single committed row (not re-inserted).
        let _ = err;
        assert!(db.fetch_pool_by_address(pool.address, 1).unwrap().is_some());
        // (c) The error propagated (the caller would surface it).
        // (already asserted `result.is_err()` above)
    }

    /// The V4 exclusion bug: when exchange markers diverge (V3 forks stalled
    /// at `M` while V2/V4 advanced to `M + k`), the strict equality filter
    /// `marker + 1 == working_start` stranded every ahead-exchange — its
    /// unprocessed work in the chunk's tail was never fetched. The fix admits
    /// any spec that still has unprocessed work at or before `working_end`,
    /// scoped to its own marker + 1 so it does NOT re-fetch committed blocks.
    #[test]
    fn in_scope_fetch_start_admits_divergent_ahead_spec() {
        // V3 laggard at M (roots the chunk cursor at M + 1).
        const M: u64 = 25_508_130;
        // V4 ahead by k blocks (advanced in earlier committed chunks while
        // V3 was stuck on a verify rollback).
        const K: u64 = 2_593;
        const TIP: u64 = 25_510_816;
        let working_start = M + 1; // the min marker + 1 (V3 roots the cursor)
        let working_end = TIP;

        // 1. V3 laggard at the grid point — unchanged behavior.
        let v3 = in_scope_fetch_start(Some(i64::try_from(M).unwrap()), working_start, working_end);
        assert_eq!(
            v3,
            Some(working_start),
            "laggard at the grid point fetches the whole chunk",
        );

        // 2. V4 ahead of the grid but still behind `working_end` — MUST be
        //    in-scope, scoped to its own marker + 1 (NOT working_start, which
        //    would re-fetch V4's already-committed [M+1, M+k] range and
        //    double-apply liquidity in release builds where the per-pool
        //    event guard is a stripped `debug_assert!`).
        let v4_marker = M + K; // 25_510_723
        let v4 = in_scope_fetch_start(
            Some(i64::try_from(v4_marker).unwrap()),
            working_start,
            working_end,
        );
        assert_eq!(
            v4,
            Some(v4_marker + 1),
            "divergent-ahead spec is in-scope, scoped to its own marker + 1 \
             (no re-fetch of committed blocks, no double-apply)",
        );

        // 3. A spec already past this chunk's end is skipped (unchanged).
        let past = in_scope_fetch_start(
            Some(i64::try_from(working_end).unwrap()),
            working_start,
            working_end,
        );
        assert_eq!(
            past, None,
            "spec already at the chunk's end is skipped (nothing to do)",
        );

        // 4. A spec with `last_update_block = None` (never stamped) scans the
        //    whole chunk (backfill / first run — unchanged).
        let fresh = in_scope_fetch_start(None, working_start, working_end);
        assert_eq!(
            fresh,
            Some(working_start),
            "never-stamped spec scans the whole chunk",
        );

        // 5. A spec BEHIND the grid (divergent laggard — rarer, but the same
        //    bug class): its own marker + 1 is the unprocessed cursor, but
        //    clamped to `working_start` so it re-scans only the chunk's range,
        //    not everything since its stale marker.
        let behind_marker = M - 5_000; // a spec way behind the grid
        let behind = in_scope_fetch_start(
            Some(i64::try_from(behind_marker).unwrap()),
            working_start,
            working_end,
        );
        assert_eq!(
            behind,
            Some(working_start),
            "behind-spec is in-scope but clamped to the chunk's lower bound",
        );
    }
}
