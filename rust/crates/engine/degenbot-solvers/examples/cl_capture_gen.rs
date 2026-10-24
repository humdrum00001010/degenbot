// Dev/example-only harness: an offline capture generator run by hand against live RPC.
// Pedantic + restriction lints that production code denies are relaxed here.
#![expect(
    clippy::doc_lazy_continuation,
    clippy::expect_used,
    clippy::if_same_then_else,
    clippy::manual_let_else,
    clippy::no_effect_underscore_binding,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::uninlined_format_args
)]

//! Offline generator for the all-CL capture fixture.
//!
//! For a curated set of REAL, liquid UNI V3 pools (heavy tick maps,
//! `tick_spacing` 1/5/10) at a pinned block, fetches complete Erigon tick-map
//! snapshots, builds each pool's `IntV3TickRangeSequence` via the same
//! `build_int_v3_sequence` the bot uses, forms valid 2-hop all-CL paths by
//! shared token, runs `solve_cl_piecewise` on each, and writes (input sequences
//! + golden) to a JSONL consumed by `cl_solve_replay`. Deterministic,
//! network-gated source of real heavy-CL data, no full bot soak.
//!
//!   `DEGENBOT_CLCAP_RPC=http://host.containers.internal:8545`,
//!   `DEGENBOT_CLCAP_BLOCK=<pinned block>`,
//!   `cargo run -p degenbot-solvers --example cl_capture_gen [-- <out.jsonl>`
//!   `cargo run -p degenbot-solvers --example cl_solve_replay <capture.jsonl>`
//!
//! This generator is the ONLY sanctioned producer of exact-wei goldens
//! (loop-18 T4): it writes sequences + the current solver's result from ONE
//! pinned block, so goldens and solver share an epoch. Production captures
//! (`DEGENBOT_SOLVER_CAPTURE*`) append working rows to NON-fixture paths —
//! never paste them into a fixture: their pool state and their recorded
//! answer come from different contexts. For stale epochs, refresh via this
//! generator (preferred) or `DR_REPLAY_REGEN=1` on `cl_solve_replay` for
//! over-shoot-only stale goldens.
//!
use std::io::Write;
use std::process::ExitCode;

use alloy::primitives::{Address, B256};
use degenbot_pools::v3_state::{PoolTickCoverage, RegisterV3PoolParams, V3PoolState};
use degenbot_rpc::provider::AlloyProvider;
use degenbot_rpc::{TickMapProtocol, TickMapRequest};
use degenbot_solvers::cl::IntV3TickRangeSequence;
use serde_json::{json, Value};

const PATH_CAP: usize = 12; // per-block cap, overridable via DEGENBOT_CLCAP_PATH_CAP

// (address, tick_spacing, fee_classic, token0, token1) — liquid UNI V3 pools
// from the static degenbot DB (block ~25826800). Shared USDC/USDT/DAI/WETH.
type Pool = (&'static str, i32, u32, &'static str, &'static str);
const POOLS: &[Pool] = &[
    (
        "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        10,
        500,
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    ),
    (
        "0x56534741CD8B152df6d48AdF7ac51f75169A83b2",
        10,
        500,
        "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    ),
    (
        "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0",
        10,
        500,
        "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    ),
    (
        "0x7858E59e0C01EA06Df3aF3D20aC7B0003275D4Bf",
        10,
        500,
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    ),
    (
        "0x6c6Bc977E13Df9b0de53b251522280BB72383700",
        10,
        500,
        "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    ),
    (
        "0x11b815efB8f581194ae79006d24E0d814B7697F6",
        10,
        500,
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    ),
    (
        "0x48DA0965ab2d2cbf1C17C09cFB5Cbe67Ad5B1406",
        1,
        100,
        "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    ),
    (
        "0x3416cF6C708Da44DB2624D63ea0AAef7113527C6",
        1,
        100,
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    ),
];

fn addr(s: &str) -> Address {
    s.parse().expect("address")
}

fn hop_ranges(s: &IntV3TickRangeSequence) -> Value {
    let ranges = s.ranges.iter().map(|r| json!({
        "liquidity": r.liquidity.to_string(),
        "sqrt_price_x96": r.sqrt_price_x96.to_string(),
        "sqrt_price_lower_x96": r.sqrt_price_lower_x96.to_string(),
        "sqrt_price_upper_x96": r.sqrt_price_upper_x96.to_string(),
        "gamma_numer": r.gamma_numer, "fee_denom": r.fee_denom,
        "zero_for_one": r.zero_for_one,
        "word_boundary_prices": r.word_boundary_prices.iter().map(std::string::ToString::to_string).collect::<Vec<_>>(),
    })).collect::<Vec<_>>();
    Value::Array(ranges)
}

fn main() -> ExitCode {
    let path_cap: usize = std::env::var("DEGENBOT_CLCAP_PATH_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(PATH_CAP);
    let rpc = if let Ok(v) = std::env::var("DEGENBOT_CLCAP_RPC") {
        v
    } else {
        eprintln!("DEGENBOT_CLCAP_RPC unset (network-gated). Aborting.");
        return ExitCode::FAILURE;
    };
    let block = std::env::var("DEGENBOT_CLCAP_BLOCK")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(25_826_800);
    let out = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "tests/fixtures/cl_capture_offline.jsonl".to_string());

    let snapshots = tokio::runtime::Runtime::new().expect("rt").block_on(async {
        let provider = AlloyProvider::new(&rpc, 10).await.expect("provider");
        let requests = POOLS
            .iter()
            .map(|(a, spacing, _, _, _)| TickMapRequest {
                address: addr(a),
                protocol: TickMapProtocol::UniswapV3,
                tick_spacing: *spacing,
                pool_id: None,
            })
            .collect::<Vec<_>>();
        provider
            .get_tick_maps(&requests, block.into())
            .await
            .expect("complete tick maps")
    });

    let mut seqs: Vec<(usize, IntV3TickRangeSequence)> = Vec::new();
    for (i, pool) in POOLS.iter().enumerate() {
        let (a, ts, fee, t0s, t1s) = pool;
        let snapshot = &snapshots.pools[i];
        let p = addr(a);
        let params = RegisterV3PoolParams {
            address: p,
            token0: addr(t0s),
            token1: addr(t1s),
            fee: *fee,
            tick_spacing: *ts,
            factory: Address::ZERO,
            sqrt_price_x96: snapshot.sqrt_price_x96,
            liquidity: snapshot.liquidity,
            tick: snapshot.tick,
            tick_data: snapshot.tick_map(block),
            update_block: block,
            tick_data_block: Some(block),
            coverage: PoolTickCoverage::Tracked,
            fetcher: None,
            deployer: Address::ZERO,
            init_hash: B256::ZERO,
            slot_layout: degenbot_pools::v3_state::ClSlotLayout::UniswapV3,
        };
        let (_identity, state) = V3PoolState::from_params(params, 8);

        match state.build_int_v3_sequence(*ts, *fee, false) {
            Some(seq) => {
                eprintln!(
                    "pool {i} {a}: {} ranges from complete snapshot",
                    seq.ranges.len()
                );
                seqs.push((i, seq));
            }
            None => eprintln!("pool {i} {a}: sequence unavailable from complete snapshot"),
        }
    }
    if seqs.len() < 2 {
        eprintln!("fewer than 2 sequences ({}); cannot form paths", seqs.len());
        return ExitCode::FAILURE;
    }

    let mut file = std::fs::File::create(&out).expect("create capture file");
    let mut n_paths = 0usize;
    let mut n_profitable = 0usize;
    'outer: for (ia, sa) in &seqs {
        let (a_t0, a_t1) = (addr(POOLS[*ia].3), addr(POOLS[*ia].4));
        for (ib, sb) in &seqs {
            if ia == ib {
                continue;
            }
            let (b_t0, b_t1) = (addr(POOLS[*ib].3), addr(POOLS[*ib].4));
            let shared = if a_t0 == b_t0 {
                Some(a_t0)
            } else if a_t0 == b_t1 {
                Some(a_t0)
            } else if a_t1 == b_t0 {
                Some(a_t1)
            } else if a_t1 == b_t1 {
                Some(a_t1)
            } else {
                None
            };
            let Some(s) = shared else {
                continue;
            };
            let other_a = if a_t0 == s { a_t1 } else { a_t0 };
            let other_b = if b_t0 == s { b_t1 } else { b_t0 };
            if other_a == other_b {
                continue;
            }
            let _zfo_a = other_a == a_t0;
            let _zfo_b = s == b_t0;
            let t0 = std::time::Instant::now();
            let out = degenbot_solvers::cl::derive_and_solve_cl_piecewise(
                &[sa, sb],
                &degenbot_solvers::runtime::SolveRuntimeConfig::default(),
            );
            let res = out.result;
            let micros = t0.elapsed().as_micros();
            let (pieces, sims) = (out.stats.pieces, out.stats.sims);
            let profitable = res.is_some();
            n_paths += 1;
            if profitable {
                n_profitable += 1;
            }
            let golden = match &res {
                None => Value::Null,
                Some((opt, _profit, hop_outputs)) => json!({
                    "optimal_input": opt.to_string(),
                    "hop_outputs": hop_outputs.iter().map(std::string::ToString::to_string).collect::<Vec<_>>(),
                    "n_hops": hop_outputs.len() }),
            };
            let na = sa.ranges.len();
            let nb = sb.ranges.len();
            let wbp = sa
                .ranges
                .iter()
                .map(|r| r.word_boundary_prices.len())
                .sum::<usize>()
                + sb.ranges
                    .iter()
                    .map(|r| r.word_boundary_prices.len())
                    .sum::<usize>();
            let line = json!({
                "block": block, "path_id": (ia * 100 + ib), "hops": 2,
                "n_ranges": [na, nb], "n_word_bounds": wbp,
                "measured": { "time_us": micros, "sims": sims, "pieces": pieces },
                "golden": golden,
                "hops": [hop_ranges(sa), hop_ranges(sb)],
                "pools": [POOLS[*ia].0, POOLS[*ib].0],
            });
            writeln!(file, "{}", line).expect("write line");
            eprintln!("path {} [{}x{}] ranges=({},{}) t={micros}us sims={sims} pieces={pieces} profitable={profitable}",
                ia * 100 + ib, POOLS[*ia].0, POOLS[*ib].0, na, nb);
            if n_paths >= path_cap {
                break 'outer;
            }
        }
    }
    eprintln!("---- wrote {n_paths} path(s), {n_profitable} profitable -> {out}");
    ExitCode::SUCCESS
}
