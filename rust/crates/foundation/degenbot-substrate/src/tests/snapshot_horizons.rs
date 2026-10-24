use super::*;
use alloy::primitives::U128;
use degenbot_pools::registry::ConcentratedLiquidityPool;

#[derive(Clone, Copy)]
enum Family {
    V3,
    V4,
}

#[test]
fn startup_gap_horizon_is_the_minimum_registered_complete_snapshot() {
    let mut core = BotState::new();
    Family::V3.register(&mut core, 100, 80);
    assert_eq!(core.snapshot_seed_block(), Some(80));
    Family::V4.register(&mut core, 110, 60);
    assert_eq!(core.snapshot_seed_block(), Some(60));
    core.set_snapshot_seed_block(Some(105));
    assert_eq!(core.snapshot_seed_block(), Some(60));
    core.set_snapshot_seed_block(None);
    assert_eq!(core.snapshot_seed_block(), Some(60));
}

#[test]
fn empty_complete_snapshot_still_establishes_startup_gap_horizon() {
    let mut core = BotState::new();
    core.register_v3_pool(&RegisterV3PoolParams {
        address: make_pool_addr(),
        token0: make_token0(),
        token1: make_token1(),
        factory: make_factory(),
        fee: 3000,
        tick_spacing: 60,
        sqrt_price_x96: U256::from(1u64) << 96,
        liquidity: 0,
        tick: 0,
        tick_data: HashMap::new(),
        update_block: 100,
        tick_data_block: Some(80),
        coverage: PoolTickCoverage::Tracked,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(core.snapshot_seed_block(), Some(80));
}

impl Family {
    fn register(self, core: &mut BotState, price_block: u64, map_block: u64) -> u64 {
        let tick_data = HashMap::from([
            (
                -60,
                TickInfo {
                    liquidity_gross: U128::from(100),
                    liquidity_net: 100,
                    block: map_block,
                },
            ),
            (
                60,
                TickInfo {
                    liquidity_gross: U128::from(100),
                    liquidity_net: -100,
                    block: map_block,
                },
            ),
        ]);
        match self {
            Self::V3 => core
                .register_v3_pool(&RegisterV3PoolParams {
                    address: make_pool_addr(),
                    token0: make_token0(),
                    token1: make_token1(),
                    factory: make_factory(),
                    fee: 3000,
                    tick_spacing: 60,
                    sqrt_price_x96: U256::from(1u64) << 96,
                    liquidity: 100,
                    tick: 0,
                    tick_data,
                    update_block: price_block,
                    tick_data_block: Some(map_block),
                    coverage: PoolTickCoverage::Tracked,
                    ..Default::default()
                })
                .unwrap(),
            Self::V4 => core
                .register_v4_pool(&RegisterV4PoolParams {
                    pool_manager: make_pool_addr(),
                    pool_id: [0xee; 32],
                    pool_key: V4PoolKey {
                        currency0: make_token0(),
                        currency1: make_token1(),
                        fee: 500,
                        tick_spacing: 60,
                        hooks: Address::ZERO,
                    },
                    hook_flags: 0,
                    protocol_fee: 0,
                    sqrt_price_x96: U256::from(1u64) << 96,
                    liquidity: 100,
                    tick: 0,
                    tick_data,
                    update_block: price_block,
                    tick_data_block: Some(map_block),
                    coverage: PoolTickCoverage::Tracked,
                    fetcher: None,
                })
                .unwrap(),
        }
    }

    fn liquidity(self, core: &mut BotState, delta: i128, block: u64) {
        match self {
            Self::V3 => {
                core.apply_v3_liquidity_update(make_pool_addr(), -60, 60, delta, block);
            }
            Self::V4 => {
                core.apply_v4_liquidity_update(
                    make_pool_addr(),
                    [0xee; 32],
                    -60,
                    60,
                    I256::try_from(delta).unwrap(),
                    block,
                );
            }
        }
    }

    fn swap(self, core: &mut BotState, block: u64) {
        let sqrt_price_x96 = U256::from(2u64) << 96;
        match self {
            Self::V3 => {
                core.apply_v3_swap(make_pool_addr(), sqrt_price_x96, 90, 1, block, &[]);
            }
            Self::V4 => {
                core.apply_v4_swap(
                    &V4SwapUpdate {
                        pool_manager: make_pool_addr(),
                        pool_id: [0xee; 32],
                        sqrt_price_x96,
                        liquidity: 90,
                        tick: 1,
                        tick_priors: Box::default(),
                    },
                    block,
                );
            }
        }
    }

    fn drain_and_pin(self, core: &mut BotState) -> u64 {
        match self {
            Self::V3 => {
                core.apply_pump_buffer_v3(&make_pool_addr());
                core.pin_v3_post_drain_snapshot(make_pool_addr());
                core.take_v3_post_drain_snapshot(make_pool_addr())
                    .unwrap()
                    .1
            }
            Self::V4 => {
                core.apply_pump_buffer_v4(make_pool_addr(), [0xee; 32]);
                core.pin_v4_post_drain_snapshot(make_pool_addr(), &[0xee; 32]);
                core.take_v4_post_drain_snapshot(make_pool_addr(), &[0xee; 32])
                    .unwrap()
                    .1
            }
        }
    }

    fn live(self, core: &mut BotState) {
        match self {
            Self::V3 => core.set_v3_pool_live(make_pool_addr()),
            Self::V4 => core.set_v4_pool_live(make_pool_addr(), [0xee; 32]),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Observed {
    ticks: HashMap<i32, TickInfo>,
    price: U256,
    liquidity: u128,
    tick: i32,
    price_block: u64,
    map_block: u64,
    nonce: u64,
    journal_len: usize,
}

fn observe(core: &BotState, id: u64) -> Observed {
    let (state, journal_len): (&dyn ConcentratedLiquidityPool, _) =
        match core.registry.pools.get(&id).unwrap() {
            PoolEntry::V3(p) => (&p.1, p.1.journal.len()),
            PoolEntry::V4(p) => (&p.1, p.1.journal.len()),
            _ => unreachable!(),
        };
    Observed {
        ticks: state.tick_data().clone(),
        price: state.sqrt_price_x96(),
        liquidity: state.liquidity(),
        tick: state.tick(),
        price_block: state.update_block(),
        map_block: state.tick_data_block(),
        nonce: core.pool_state_nonce(id),
        journal_len,
    }
}

fn complete_snapshot_skips_covered_events(family: Family) {
    let mut core = BotState::new();
    let id = family.register(&mut core, 500, 500);
    let seed = observe(&core, id);
    family.liquidity(&mut core, 5, 499);
    family.swap(&mut core, 499);
    family.liquidity(&mut core, 7, 500);
    core.cl_mut().advance_pump_complete_cutoff(499);
    assert_eq!(family.drain_and_pin(&mut core), 500);
    assert_eq!(observe(&core, id), seed);
    family.live(&mut core);
    assert_eq!(observe(&core, id), seed);
    family.liquidity(&mut core, 11, 499);
    family.swap(&mut core, 499);
    assert_eq!(observe(&core, id), seed);
    family.liquidity(&mut core, 5, 501);
    family.liquidity(&mut core, 7, 501);
    let current = observe(&core, id);
    assert_eq!(current.ticks[&-60].liquidity_gross, U128::from(112));
    assert_eq!(current.liquidity, 112);
    assert_eq!((current.price_block, current.map_block), (501, 501));
    assert_eq!(current.journal_len, 1);
}

fn split_snapshot_replays_only_missing_liquidity(family: Family) {
    let mut core = BotState::new();
    let id = family.register(&mut core, 500, 400);
    family.liquidity(&mut core, 5, 400);
    family.liquidity(&mut core, 5, 450);
    family.liquidity(&mut core, 7, 450);
    family.swap(&mut core, 450);
    core.cl_mut().advance_pump_complete_cutoff(499);
    assert_eq!(family.drain_and_pin(&mut core), 450);
    let current = observe(&core, id);
    assert_eq!(current.ticks[&-60].liquidity_gross, U128::from(112));
    assert_eq!(current.liquidity, 100);
    assert_eq!(current.price, U256::from(1u64) << 96);
    assert_eq!((current.price_block, current.map_block), (500, 450));
    assert_eq!(current.journal_len, 1);
    family.live(&mut core);
    family.liquidity(&mut core, 9, 399);
    family.swap(&mut core, 499);
    assert_eq!(observe(&core, id), current);
}

#[test]
fn v3_complete_snapshot_horizons_cover_buffered_and_late_events() {
    complete_snapshot_skips_covered_events(Family::V3);
}

#[test]
fn v4_complete_snapshot_horizons_cover_buffered_and_late_events() {
    complete_snapshot_skips_covered_events(Family::V4);
}

#[test]
fn v3_split_snapshot_horizons_preserve_tick_only_replay_and_same_block_siblings() {
    split_snapshot_replays_only_missing_liquidity(Family::V3);
}

#[test]
fn v4_split_snapshot_horizons_preserve_tick_only_replay_and_same_block_siblings() {
    split_snapshot_replays_only_missing_liquidity(Family::V4);
}

fn snapshot_reorg_boundary(family: Family) {
    let mut core = BotState::new();
    let id = family.register(&mut core, 500, 500);
    core.seed_genesis_by_pool_id(id, 500).unwrap();
    family.live(&mut core);
    family.liquidity(&mut core, 5, 501);
    family.liquidity(&mut core, 7, 501);
    assert!(core.has_state_prior_to(id, 501));
    assert!(!core.has_state_prior_to(id, 500));
    core.restore_pool_before_block(id, 501).unwrap().unwrap();
    let restored = observe(&core, id);
    assert_eq!(restored.ticks[&-60].liquidity_gross, U128::from(100));
    assert_eq!((restored.price_block, restored.map_block), (500, 500));
    family.liquidity(&mut core, 9, 500);
    assert_eq!(observe(&core, id), restored);
    family.liquidity(&mut core, 5, 501);
    family.liquidity(&mut core, 7, 501);
    assert_eq!(
        observe(&core, id).ticks[&-60].liquidity_gross,
        U128::from(112)
    );
}

#[test]
fn v3_snapshot_horizons_reorg_rejects_seed_and_replays_post_seed_siblings() {
    snapshot_reorg_boundary(Family::V3);
}

#[test]
fn v4_snapshot_horizons_reorg_rejects_seed_and_replays_post_seed_siblings() {
    snapshot_reorg_boundary(Family::V4);
}
