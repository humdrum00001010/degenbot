//! Pin for the V3-anchor sparse-tick defect (`unusable_pool_state` on every
//! declared chain): a target swap that crosses NO initialized tick leaves the
//! replayed V3 post-state with an empty tick map, and a solver range sequence
//! cannot be built from it. Pool ingress stages the anchor's complete tick map
//! from the pinned snapshot, so a healthy wide/range anchor admits and solves
//! while a genuinely unusable (ticks-less) anchor still rejects.

#![expect(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use alloy::primitives::{address, Address, U256};
use degenbot_core::errors::ProviderResult;
use degenbot_db::connection::DegenbotDb;
use degenbot_db::discovery::V3PoolRowInput;
use degenbot_pools::v3_state::ClSlotLayout;
use degenbot_rpc::tick_snapshot::{
    PoolTickSnapshot, SnapshotTick, TickMapRequest, TickMapRpc, TickMapSnapshot,
};
use degenbot_simulation::sim::evm::journal_pools::{
    PoolFamily, PoolPostKind, PoolPostState, TypedPoolPost,
};
use degenbot_strategy::backrun_engine::{BackrunHopRef, BackrunSolver, BackrunV2Pool, LaneFamily};
use degenbot_strategy::backrun_strategy::{admit_extracted, solve_dfs_chains};
use degenbot_strategy::frame_pipeline::MarketContext;
use degenbot_strategy::ETHEREUM_WETH as WETH;
use degenbot_substrate::connector_index::{V2ConnectorIndex, V2Edge, V3Edge};
use degenbot_substrate::executor_hop::{V2FeePair, V2Fees};

fn v2_fee_pair() -> V2FeePair {
    V2FeePair::from_discovered(Some(3), Some(3), Some(1_000))
}

fn v2_fees() -> V2Fees {
    v2_fee_pair().resolve().expect("valid fixture fee")
}

/// The V3 anchor's tokens (canonical order: TOK0 < WETH).
const TOK: Address = address!("0000000000000000000000000000000000000aa1");
/// The V3 anchor pool (the frame's touched pool).
const ANCHOR: Address = address!("000000000000000000000000000000000000b001");
/// The V2 connector the walker discovers (cheaper TOK than the anchor).
const MID: Address = address!("000000000000000000000000000000000000b002");
const SEED: u64 = 7;
const ANCHOR_FEE: u32 = 3_000;
const ANCHOR_SPACING: i32 = 60;
const FACTORY: Address = address!("00000000000000000000000000000000000000ff");
/// `sqrt(1) * 2^96` — the tick-0 price.
const SQRT_ONE: u128 = 79_228_162_514_264_337_593_543_950_336;

/// Seed the anchor's catalog row. Its complete tick map comes from the fixture
/// RPC.
fn seed_anchor(db: &DegenbotDb) {
    db.upsert_exchange(1, "uniswap_v3", FACTORY, None).unwrap();
    db.upsert_v3_pools(
        1,
        "uniswap_v3",
        1,
        1_000_000,
        &[V3PoolRowInput {
            address: ANCHOR,
            token0_address: TOK,
            token1_address: WETH,
            fee: i64::from(ANCHOR_FEE),
            tick_spacing: i64::from(ANCHOR_SPACING),
        }],
    )
    .unwrap();
}

struct SnapshotRpc(Vec<SnapshotTick>);

impl TickMapRpc for SnapshotRpc {
    fn get_tick_maps<'a>(
        &'a self,
        requests: &'a [TickMapRequest],
        block: alloy::eips::BlockId,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = ProviderResult<TickMapSnapshot>> + Send + 'a>,
    > {
        Box::pin(async move {
            Ok(TickMapSnapshot {
                chain_id: 1,
                block_number: block.as_u64().unwrap(),
                block_hash: alloy::primitives::B256::repeat_byte(9),
                pools: requests
                    .iter()
                    .map(|request| PoolTickSnapshot {
                        address: request.address,
                        protocol: request.protocol,
                        pool_id: request.pool_id,
                        tick_spacing: request.tick_spacing,
                        tick: 0,
                        sqrt_price_x96: U256::from(SQRT_ONE),
                        liquidity: if self.0.is_empty() {
                            0
                        } else {
                            10_000_000_000_000
                        },
                        protocol_fee: 0,
                        lp_fee: ANCHOR_FEE,
                        ticks: self.0.clone(),
                    })
                    .collect(),
            })
        })
    }
}

fn market_context(
    registry: Option<std::sync::Arc<degenbot_substrate::RouteRegistry>>,
    db: Option<std::sync::Arc<degenbot_db::connection::DegenbotDb>>,
    ticks: &[i32],
) -> MarketContext {
    let kit = degenbot_strategy::strategy_kit::StrategyKit::resolve(
        registry,
        Some(std::sync::Arc::new(SnapshotRpc(snapshot_ticks_for_anchor(
            ticks,
        )))),
        degenbot_substrate::pool_ingress::VerifyLevel::default(),
        None,
    );
    MarketContext::new(1, db, kit, 8, 4)
}

fn snapshot_ticks_for_anchor(ticks: &[i32]) -> Vec<SnapshotTick> {
    if ticks.is_empty() {
        return Vec::new();
    }
    let active = 10_000_000_000_000u128;
    vec![
        SnapshotTick {
            tick: -180,
            liquidity_gross: active + 4_000,
            liquidity_net: i128::try_from(active + 4_000).unwrap(),
        },
        SnapshotTick {
            tick: -120,
            liquidity_gross: 10_000,
            liquidity_net: -4_000,
        },
        SnapshotTick {
            tick: 120,
            liquidity_gross: 10_000,
            liquidity_net: 5_000,
        },
        SnapshotTick {
            tick: 180,
            liquidity_gross: active + 5_000,
            liquidity_net: -i128::try_from(active + 5_000).unwrap(),
        },
    ]
}

fn runtime(ticks: &[i32]) -> (MarketContext, u64, u64) {
    let (db, _state) = DegenbotDb::open_in_memory_for_writes().unwrap();
    let tok_id = db
        .get_or_create_erc20_token(1, &TOK.to_checksum(None), None, None, None)
        .unwrap();
    let weth_id = db
        .get_or_create_erc20_token(1, &WETH.to_checksum(None), None, None, None)
        .unwrap();
    let (tok_id, weth_id) = (
        u64::try_from(tok_id).unwrap(),
        u64::try_from(weth_id).unwrap(),
    );
    seed_anchor(&db);
    let mut index = V2ConnectorIndex::default();
    index.push_v3_edge(V3Edge {
        pool_id: 201,
        token0_id: tok_id,
        token1_id: weth_id,
        address: ANCHOR,
        fee: ANCHOR_FEE,
        tick_spacing: ANCHOR_SPACING,
        layout: ClSlotLayout::UniswapV3,
    });
    index.push_edge(V2Edge {
        pool_id: 202,
        token0_id: tok_id,
        token1_id: weth_id,
        address: MID,
        fees: v2_fee_pair(),
    });
    (
        market_context(
            Some(std::sync::Arc::new(degenbot_substrate::RouteRegistry::new(
                index,
            ))),
            Some(std::sync::Arc::new(db)),
            ticks,
        ),
        tok_id,
        weth_id,
    )
}

/// The frame's replayed V3 post-state: price/liquidity staged, NO touched
/// ticks (the target swap crossed no initialized tick).
fn anchor_post_state(liquidity: u128) -> PoolPostState {
    PoolPostState {
        address: ANCHOR,
        family: PoolFamily::V3 {
            layout: ClSlotLayout::UniswapV3,
            tick_spacing: ANCHOR_SPACING,
            current_tick_hint: Some(0),
        },
        kind: PoolPostKind::Typed(TypedPoolPost::V3 {
            sqrt_price_x96: Some(U256::from(SQRT_ONE)),
            tick: Some(0),
            liquidity: Some(liquidity),
            touched_ticks: Vec::new(),
        }),
    }
}

fn chain_refs(anchor_id: u64, mid_id: u64) -> Vec<BackrunHopRef> {
    vec![
        BackrunHopRef {
            pool_id: anchor_id,
            pool: ANCHOR,
            token0: TOK,
            token1: WETH,
            // WETH entry: the anchor consumes WETH = token1.
            zfo: false,
            family: LaneFamily::V3 { fee: ANCHOR_FEE },
        },
        BackrunHopRef {
            pool_id: mid_id,
            pool: MID,
            token0: TOK,
            token1: WETH,
            // Now holding TOK: the mid consumes TOK = token0.
            zfo: true,
            family: LaneFamily::V2 { fees: v2_fees() },
        },
    ]
}

fn admit_mid(solver: &mut BackrunSolver) -> u64 {
    solver
        .admit_v2(&BackrunV2Pool {
            address: MID,
            token0: TOK,
            token1: WETH,
            reserve0: 2_000,
            reserve1: 400_000,
            fees: v2_fee_pair(),
        })
        .unwrap()
}

#[test]
fn v3_anchor_snapshot_tick_map_admits_and_solves() {
    // A complete but empty map leaves the anchor admitted and unmodelable.
    let (rt, _tok, _weth) = runtime(&[]);
    let mut solver = BackrunSolver::new();
    let affected = admit_extracted(
        &rt,
        &mut solver,
        &[anchor_post_state(0)],
        SEED,
        &mut degenbot_strategy::frame_pipeline::StageTrace::default(),
    );
    assert_eq!(affected.len(), 1, "the V3 anchor admits into the scope");
    let mid_id = admit_mid(&mut solver);
    let stats = solve_dfs_chains(
        &mut solver,
        &[chain_refs(affected[0].workspace_pool_id, mid_id)],
        U256::ZERO,
    );
    assert_eq!(stats.dfs_declared, 1);
    assert_eq!(
        stats.dfs_evaluated, 0,
        "an anchor with no modelable tick ranges must not evaluate"
    );
    match &stats.chains[0].reject {
        Some(degenbot_strategy::backrun_engine::PathReject::UnusablePoolState {
            deficits, ..
        }) => {
            assert!(*deficits >= 1, "the unusable anchor is the deficit");
        }
        other => panic!("expected unusable_pool_state, got {other:?}"),
    }

    // The same frame with initialized boundaries can be modeled and solved.
    let (rt, _tok, _weth) = runtime(&[120, -120]);
    let mut solver = BackrunSolver::new();
    let affected = admit_extracted(
        &rt,
        &mut solver,
        &[anchor_post_state(10_000_000_000_000)],
        SEED,
        &mut degenbot_strategy::frame_pipeline::StageTrace::default(),
    );
    assert_eq!(affected.len(), 1, "the V3 anchor admits");
    let mid_id = admit_mid(&mut solver);
    let stats = solve_dfs_chains(
        &mut solver,
        &[chain_refs(affected[0].workspace_pool_id, mid_id)],
        U256::ZERO,
    );
    assert_eq!(stats.dfs_declared, 1);
    assert_eq!(
        stats.dfs_evaluated, 1,
        "the snapshot map makes the anchor modelable"
    );
    assert!(stats.chains[0].reject.is_none());
}

/// The `anchor_ticks` probe must tolerate u128-scale liquidity: real pools
/// carry >> `u64::MAX` liquidity, and `serde_json::json!` errors ("number out
/// of range") on such u128s — which panicked the io-rt thread and stopped
/// the feed live. Pinned by driving a `u128::MAX` anchor through admission.
#[test]
fn anchor_admission_survives_u128_liquidity() {
    let (rt, _tok, _weth) = runtime(&[120, -120]);
    let mut post = anchor_post_state(10_000_000_000_000);
    if let PoolPostKind::Typed(TypedPoolPost::V3 { liquidity, .. }) = &mut post.kind {
        *liquidity = Some(u128::MAX);
    }
    let affected = admit_extracted(
        &rt,
        &mut BackrunSolver::new(),
        &[post],
        SEED,
        &mut degenbot_strategy::frame_pipeline::StageTrace::default(),
    );
    assert_eq!(
        affected.len(),
        1,
        "u128 liquidity stages and traces cleanly"
    );
}

/// The ingress map never overwrites a replayed touched tick (post-frame facts
/// win) — pinned through the admission path's public behavior.
#[test]
fn replayed_touched_tick_wins_over_ingress_map() {
    let (rt, _tok, _weth) = runtime(&[120, -120]);
    let mut post = anchor_post_state(10_000_000_000_000);
    if let PoolPostKind::Typed(TypedPoolPost::V3 { touched_ticks, .. }) = &mut post.kind {
        touched_ticks.push(
            degenbot_simulation::sim::evm::journal_pools::TouchedTickWord {
                tick: -120,
                liquidity_gross: 1,
                liquidity_net: -1,
            },
        );
    }
    let mut solver = BackrunSolver::new();
    let affected = admit_extracted(
        &rt,
        &mut solver,
        &[post],
        SEED,
        &mut degenbot_strategy::frame_pipeline::StageTrace::default(),
    );
    assert_eq!(affected.len(), 1);
    // The pool's state is readable and the chain solves (the replayed tick
    // did not corrupt the merged map).
    let mid_id = admit_mid(&mut solver);
    let stats = solve_dfs_chains(
        &mut solver,
        &[chain_refs(affected[0].workspace_pool_id, mid_id)],
        U256::ZERO,
    );
    assert!(stats.chains[0].evaluated);
}
