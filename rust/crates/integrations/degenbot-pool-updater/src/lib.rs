//! Rust-owned pool catalog discovery and block-pinned tick-map verification.

pub mod fetch;
pub mod run;
pub mod spec;
pub mod verify;

pub use fetch::{
    decode_pool_created_log, fetch_pool_created_logs, fetch_pool_created_logs_for_spec,
    DecodedPoolCreated, PoolFamily,
};
pub use run::{
    apply_chunk_writes_on_conn, map_pool_creation, run_pool_update, run_pool_update_on_db,
    ChunkInputs, ChunkProgress, ChunkWriteReport, MapError, NoProgress, PoolCreationToWrite,
    ProgressSink, RunError, UpdateReport,
};
pub use spec::{load_active_exchange_specs, ExchangeSpec, RpcFeeCall};
pub use verify::{catalog_tick_map_request, verify_tick_map_snapshot, LiquidityDivergence};
