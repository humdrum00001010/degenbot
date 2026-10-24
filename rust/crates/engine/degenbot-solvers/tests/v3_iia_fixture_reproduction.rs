#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr
)]
//! Network-gated mainnet-fixture reproduction .
//!
//! The live `DEGENBOT_SIM_EXIT_ON_FAIL=1` trap captured a +13 wei V3-hop
//! over-prediction on pool `0x57D7…dF80` (UNI/DAI fee=500) at block 25647669.
//! The bias is **constant (+13/+14) across four paths with DIFFERENT
//! `amount_in`** — so it lives in the amount-INDEPENDENT `compute_crossing`
//! crossing-output accumulator, not the amount-dependent ending partial step.
//! `v3_crossing_solver_vs_sim_parity.rs` proves the solver's crossing math is
//! byte-exact with `v3_simulate_swap` on SYNTHETIC regular/symmetric tick
//! topologies down to 1e9 liquidity — so the divergence is topology-specific
//! (irregular initialized ticks far from the 1:1 price), reproducible only
//! against the real pool state.
//!
//! This test fetches the real `V3PoolState` at the captured block via the
//! archive RPC and runs `v3_simulate_swap` (the byte-exact twin the revm
//! sim's `actual` mirrors) on byte-identical on-chain state. The fork is
//! decided by comparing the result to the captured values:
//!
//! - sim == captured actual (150836781502) → the revm sim matches the Rust
//!   twin → the +13 is a SOLVER-MATH divergence (fix targets `compute_crossing`
//!   / `compute_tick_ranges`).
//! - sim == captured predicted (150836781515) → the Rust twin matches the
//!   solver → the revm sim's `actual` came from DIFFERENT state than on-chain@N
//!   → STALE ENGINE STATE (fix targets pump ordering).
//!
//! Gated by env `DEGENBOT_V3_FIXTURE_RPC` + `DEGENBOT_V3_FIXTURE_BLOCK` so CI
//! (no archive RPC) never hits the network. Run locally:
//!
//! ```text
//! DEGENBOT_V3_FIXTURE_RPC=http://host.containers.internal:8545/ \
//! DEGENBOT_V3_FIXTURE_BLOCK=25647669 \
//! cargo test -p degenbot-solvers --test v3_iia_fixture_reproduction -- --nocapture
//! ```

#![expect(clippy::too_many_lines, clippy::doc_markdown)]

use alloy::primitives::{Address, I256, U256};

use degenbot_pools::v3_state::{
    v3_simulate_swap, PoolTickCoverage, RegisterV3PoolParams, SimulateSwapError, V3PoolState,
    V3SwapOutcome,
};
use degenbot_rpc::provider::AlloyProvider;
use degenbot_rpc::{TickMapProtocol, TickMapRequest};
use degenbot_solvers::cl::{simulate_v3_range_swap, IntV3TickRangeSequence};

/// Hardcoded fixture from the captured `DEGENBOT_SIM_EXIT_ON_FAIL=1` trap
/// (`logs/bot_run.log` block 25647669, path 7724). Pool `0x57D7…dF80`,
/// UNI/DAI fee=500, tick_spacing=10, ofz (zero_for_one=false — token1 DAI in,
/// token0 UNI out). `amount_in` = hop[0]'s matched output.
const POOL_ADDRESS: &str = "0x57D7d040438730d4029794799dEEd8601E23fF80";
const FIXTURE_BLOCK: u64 = 25_647_669;
const FEE: u32 = 500;
const TICK_SPACING: i32 = 10;
const AMOUNT_IN: u128 = 50_868_891_135; // hop[0] matched output → hop[1] amount_in
const CAPTURED_ACTUAL: u128 = 150_836_781_502; // revm sim measured hop[1] output
const CAPTURED_PREDICTED: u128 = 150_836_781_515; // solver hop_outputs[1] (+13)

/// The solver's CL-hop output for `amount_in` (mirrors
/// `v3_crossing_solver_vs_sim_parity::solver_crossing_output`).
fn solver_crossing_output(amount_in: U256, seq: &IntV3TickRangeSequence) -> Option<U256> {
    let n = seq.ranges.len();
    let mut chosen_k = 0usize;
    for k in 0..n {
        let crossing = seq.compute_crossing(k)?;
        if crossing.crossing_gross_input <= amount_in {
            chosen_k = k;
        } else {
            break;
        }
    }
    let crossing = seq.compute_crossing(chosen_k)?;
    if amount_in < crossing.crossing_gross_input {
        return Some(U256::ZERO);
    }
    let remaining = amount_in - crossing.crossing_gross_input;
    let ending = simulate_v3_range_swap(remaining, &crossing.ending_range);
    Some(crossing.crossing_output.saturating_add(ending.output))
}

/// ofz exact-in → pool sends token0 (amount0).
fn v3_exact_in_output_ofz(outcome: &V3SwapOutcome) -> U256 {
    outcome.amount0
}

#[test]
fn v3_iia_fixture_reproduces_plus_thirteen_divergence() {
    let Ok(rpc_url) = std::env::var("DEGENBOT_V3_FIXTURE_RPC") else {
        eprintln!("DEGENBOT_V3_FIXTURE_RPC unset — skipping network-gated reproduction");
        return;
    };
    let block = std::env::var("DEGENBOT_V3_FIXTURE_BLOCK")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(FIXTURE_BLOCK);

    let pool: Address = POOL_ADDRESS.parse().expect("valid address");
    // Read a complete map at the fixture block; no later tick-word RPCs are
    // needed while simulating or constructing solver ranges.
    let snapshot = tokio::runtime::Runtime::new()
        .expect("setup runtime")
        .block_on(async {
            let provider = AlloyProvider::new(&rpc_url, 3).await.expect("provider");
            let request = TickMapRequest {
                address: pool,
                protocol: TickMapProtocol::UniswapV3,
                tick_spacing: TICK_SPACING,
                pool_id: None,
            };
            provider
                .get_tick_maps(&[request], block.into())
                .await
                .expect("complete tick map")
                .pools
                .into_iter()
                .next()
                .expect("one pool snapshot")
        });
    eprintln!(
        "block={block} sqrtPriceX96={} tick={} liquidity={}",
        snapshot.sqrt_price_x96, snapshot.tick, snapshot.liquidity
    );

    // Build tracked state directly from the complete snapshot.
    let params = RegisterV3PoolParams {
        address: pool,
        token0: Address::ZERO,
        token1: Address::ZERO,
        fee: FEE,
        tick_spacing: TICK_SPACING,
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
        init_hash: alloy::primitives::B256::ZERO,
        slot_layout: degenbot_pools::ClSlotLayout::UniswapV3,
    };
    let (_identity, state) = V3PoolState::from_params(params, 8);

    // Run the captured swap against the complete tracked state.
    let zero_for_one = false; // ofz
    let amount_specified = I256::try_from(U256::from(AMOUNT_IN)).unwrap();
    let limit = V3PoolState::default_sqrt_price_limit(zero_for_one);
    let sim_out = match v3_simulate_swap(
        &state,
        FEE,
        TICK_SPACING,
        zero_for_one,
        amount_specified,
        limit,
    ) {
        Ok(outcome) => v3_exact_in_output_ofz(&outcome),
        Err(SimulateSwapError::MissingTickWord(word)) => {
            panic!("complete snapshot missed tick word {word}");
        }
        Err(error) => panic!("v3_simulate_swap error: {error:?}"),
    };

    eprintln!("sim(v3_simulate_swap)={sim_out}");

    // Build the solver's sequence from the snapshot state and run
    //    the solver crossing path on the SAME amount_in.
    let Some(seq) = state.build_int_v3_sequence(TICK_SPACING, FEE, zero_for_one) else {
        eprintln!(
            "build_int_v3_sequence returned None for the complete snapshot; \
             sim_out alone is the decisive signal (see assertion below)."
        );
        assert_eq!(
            sim_out,
            U256::from(CAPTURED_ACTUAL),
            "sim did not reproduce captured actual even without solver side",
        );
        return;
    };
    let solver_out =
        solver_crossing_output(U256::from(AMOUNT_IN), &seq).expect("solver crossing output");

    eprintln!(
        "amount_in={AMOUNT_IN} sim(v3_simulate_swap)={sim_out} \
         solver(crossing)={solver_out} captured_actual={CAPTURED_ACTUAL} \
         captured_predicted={CAPTURED_PREDICTED}"
    );
    eprintln!(
        "sim_vs_captured_actual delta={}",
        if sim_out > U256::from(CAPTURED_ACTUAL) {
            sim_out - U256::from(CAPTURED_ACTUAL)
        } else {
            U256::from(CAPTURED_ACTUAL) - sim_out
        }
    );
    eprintln!(
        "solver_vs_sim delta={}",
        if solver_out > sim_out {
            solver_out - sim_out
        } else {
            sim_out - solver_out
        }
    );

    // DECISIVE ASSERTION. The revm sim's `actual` mirrors `v3_simulate_swap`
    // (the proven byte-exact twin). If `v3_simulate_swap` reproduces the
    // captured ACTUAL on byte-identical on-chain state, then the solver's +13
    // is a topology-specific `compute_crossing` divergence (the fix targets the
    // solver's crossing accumulator, not pump ordering). If instead it matches
    // the captured PREDICTED, the sim ran against different state than
    // on-chain@N (stale engine state — fix pump ordering).
    assert_eq!(
        sim_out,
        U256::from(CAPTURED_ACTUAL),
        "v3_simulate_swap on byte-identical on-chain@N state did NOT reproduce the \
         captured revm-sim actual ({CAPTURED_ACTUAL}) — got {sim_out}. This means the revm sim's \
         `actual` came from DIFFERENT state than on-chain@N → STALE ENGINE STATE (ph1 confirmed; \
         fix pump ordering). [conversely if it matched, the +13 is solver math]"
    );
}
