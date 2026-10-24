#![expect(clippy::unwrap_used)]
use super::*;
use alloy::primitives::U128;
use degenbot_core::errors::ProviderResult;
use degenbot_pools::v3_state::PoolTickCoverage;
use degenbot_rpc::tick_snapshot::{PoolTickSnapshot, SnapshotTick, TickMapSnapshot};
use std::sync::atomic::{AtomicUsize, Ordering};

const POOL: Address = Address::repeat_byte(1);

fn params() -> IngressV3Params {
    IngressV3Params {
        address: POOL,
        token0: Address::repeat_byte(2),
        token1: Address::repeat_byte(3),
        fee: 3000,
        tick_spacing: 60,
        sqrt_price_x96: U256::from(1u128) << 96,
        liquidity: 1000,
        tick: 0,
        slot_layout: ClSlotLayout::UniswapV3,
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum SnapshotResponse {
    #[default]
    Complete,
    Empty,
    Malformed,
    WrongBlock,
    Failure,
}

#[derive(Default)]
struct SnapshotRpc {
    calls: AtomicUsize,
    requests: Mutex<Vec<TickMapRequest>>,
    response: SnapshotResponse,
    protocol_fee: u32,
    lp_fee: Option<u32>,
}

impl TickMapRpc for SnapshotRpc {
    fn get_tick_maps<'a>(
        &'a self,
        requests: &'a [TickMapRequest],
        block: BlockId,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = ProviderResult<TickMapSnapshot>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().extend_from_slice(requests);
            if self.response == SnapshotResponse::Failure {
                return Err(degenbot_core::errors::ProviderError::InvalidResponse {
                    message: "endpoint unavailable".into(),
                });
            }
            Ok(TickMapSnapshot {
                chain_id: 1,
                block_number: block.as_u64().unwrap()
                    + u64::from(self.response == SnapshotResponse::WrongBlock),
                block_hash: B256::repeat_byte(9),
                pools: requests
                    .iter()
                    .map(|request| PoolTickSnapshot {
                        address: request.address,
                        protocol: request.protocol,
                        pool_id: request.pool_id,
                        tick_spacing: request.tick_spacing,
                        tick: 0,
                        sqrt_price_x96: U256::from(1u128) << 96,
                        liquidity: if self.response == SnapshotResponse::Empty {
                            0
                        } else {
                            1000
                        },
                        protocol_fee: self.protocol_fee,
                        lp_fee: self.lp_fee.unwrap_or(3000),
                        ticks: if self.response == SnapshotResponse::Empty {
                            vec![]
                        } else {
                            vec![
                                SnapshotTick {
                                    tick: -60000,
                                    liquidity_gross: 1000,
                                    liquidity_net: 1000,
                                },
                                SnapshotTick {
                                    tick: 60000,
                                    liquidity_gross: 1000,
                                    liquidity_net: if self.response == SnapshotResponse::Malformed {
                                        -999
                                    } else {
                                        -1000
                                    },
                                },
                            ]
                        },
                    })
                    .collect(),
            })
        })
    }
}

#[derive(Default)]
struct Verifier {
    calls: AtomicUsize,
    fail: bool,
    seen: Mutex<Vec<(TickMapSampleTarget, RpcLiquidityMap)>>,
}

#[async_trait::async_trait]
impl TickMapSampleVerifier for Verifier {
    async fn verify(
        &self,
        target: TickMapSampleTarget,
        map: &RpcLiquidityMap,
        _block: u64,
    ) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen.lock().push((target, map.clone()));
        if self.fail {
            Err("sample mismatch".into())
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn default_admission_requires_complete_snapshot_transport() {
    let ingress = PoolIngress::new(None);
    let result = ingress
        .admit_v3_verified(&mut Workspace::new(), params(), 10)
        .await;
    assert!(matches!(result, Err(IngressDecline::TickMapFetch(_))));
}

#[tokio::test]
async fn complete_snapshot_stages_distant_boundaries_as_tracked_chain_truth() {
    let rpc = Arc::new(SnapshotRpc::default());
    let ingress = PoolIngress::new(Some(rpc.clone()));
    let seed = ingress.stage_v3_tick_map(params(), 100).await.unwrap();
    assert_eq!(seed.coverage, PoolTickCoverage::Tracked);
    assert_eq!(seed.source, TickMapSource::Chain);
    assert_eq!((seed.seed_block, seed.source_block), (100, Some(100)));
    assert_eq!(seed.ticks.len(), 2);
    assert_eq!(seed.bitmaps[&-4], U256::from(1) << 24);
    assert_eq!(seed.bitmaps[&3], U256::from(1) << 232);
    ingress
        .admit_v3_verified(&mut Workspace::new(), params(), 100)
        .await
        .unwrap();
    assert_eq!(rpc.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legitimate_empty_complete_snapshot_is_tracked_and_sampled() {
    let rpc = Arc::new(SnapshotRpc {
        response: SnapshotResponse::Empty,
        ..Default::default()
    });
    let verifier = Arc::new(Verifier::default());
    let mut ingress = PoolIngress::new(Some(rpc));
    ingress.set_verifier(verifier.clone());
    let seed = ingress.stage_v3_tick_map(params(), 100).await.unwrap();
    assert!(seed.ticks.is_empty() && seed.bitmaps.is_empty());
    assert_eq!(seed.coverage, PoolTickCoverage::Tracked);
    let mut empty_params = params();
    empty_params.liquidity = 0;
    ingress
        .admit_v3_verified(&mut Workspace::new(), empty_params, 100)
        .await
        .unwrap();
    assert_eq!(verifier.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn memo_is_pinned_to_block_layout_and_v4_manager_pool_id() {
    let rpc = Arc::new(SnapshotRpc::default());
    let ingress = PoolIngress::new(Some(rpc.clone()));
    let p = params();
    ingress.stage_v3_tick_map(p, 100).await.unwrap();
    ingress.stage_v3_tick_map(p, 100).await.unwrap();
    let mut pancake = p;
    pancake.slot_layout = ClSlotLayout::PancakeV3;
    ingress.stage_v3_tick_map(pancake, 100).await.unwrap();
    let v4 = IngressV4Params {
        manager: POOL,
        pool_id: B256::repeat_byte(4),
        token0: p.token0,
        token1: p.token1,
        fee: p.fee,
        tick_spacing: p.tick_spacing,
        hooks: Address::ZERO,
        sqrt_price_x96: p.sqrt_price_x96,
        liquidity: p.liquidity,
        tick: p.tick,
    };
    ingress.stage_v4_tick_map(v4, 100).await.unwrap();
    ingress
        .stage_v4_tick_map(
            IngressV4Params {
                manager: Address::repeat_byte(5),
                ..v4
            },
            100,
        )
        .await
        .unwrap();
    ingress
        .stage_v4_tick_map(
            IngressV4Params {
                pool_id: B256::repeat_byte(6),
                ..v4
            },
            100,
        )
        .await
        .unwrap();
    ingress.stage_v3_tick_map(p, 101).await.unwrap();
    assert_eq!(rpc.calls.load(Ordering::SeqCst), 6);
    let requests = rpc.requests.lock();
    assert_eq!(requests[1].protocol, TickMapProtocol::PancakeV3);
    assert_eq!(
        (requests[2].address, requests[2].pool_id),
        (v4.manager, Some(v4.pool_id))
    );
}

#[tokio::test]
async fn malformed_maps_wrong_blocks_and_zero_spacing_decline_even_with_sampling_off() {
    for rpc in [
        SnapshotRpc {
            response: SnapshotResponse::Malformed,
            ..Default::default()
        },
        SnapshotRpc {
            response: SnapshotResponse::WrongBlock,
            ..Default::default()
        },
        SnapshotRpc {
            response: SnapshotResponse::Failure,
            ..Default::default()
        },
    ] {
        let mut ingress = PoolIngress::new(Some(Arc::new(rpc)));
        ingress.set_verify_level(VerifyLevel::Off);
        assert!(matches!(
            ingress.stage_v3_tick_map(params(), 100).await,
            Err(IngressDecline::TickMapFetch(_))
        ));
    }
    let ingress = PoolIngress::new(Some(Arc::new(SnapshotRpc::default())));
    let mut invalid = params();
    invalid.tick_spacing = 0;
    assert!(matches!(
        ingress.stage_v3_tick_map(invalid, 100).await,
        Err(IngressDecline::TickMapFetch(_))
    ));
}

#[tokio::test]
async fn strict_bootstrap_off_preserve_sampling_policy_and_block_freshness() {
    for (level, expected) in [
        (VerifyLevel::Strict, 3),
        (VerifyLevel::Bootstrap, 2),
        (VerifyLevel::Off, 0),
    ] {
        let mut ingress = PoolIngress::new(Some(Arc::new(SnapshotRpc::default())));
        let verifier = Arc::new(Verifier::default());
        ingress.set_verify_level(level);
        ingress.set_verifier(verifier.clone());
        for block in [100, 100, 101] {
            ingress
                .admit_v3_verified(&mut Workspace::new(), params(), block)
                .await
                .unwrap();
        }
        assert_eq!(verifier.calls.load(Ordering::SeqCst), expected);
    }
}

#[tokio::test]
async fn verifier_failure_is_a_loud_decline_and_not_memoized() {
    let mut ingress = PoolIngress::new(Some(Arc::new(SnapshotRpc::default())));
    let verifier = Arc::new(Verifier {
        fail: true,
        ..Default::default()
    });
    ingress.set_verifier(verifier.clone());
    for _ in 0..2 {
        assert!(matches!(
            ingress
                .admit_v3_verified(&mut Workspace::new(), params(), 100)
                .await,
            Err(IngressDecline::Verify(_))
        ));
    }
    assert_eq!(verifier.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn replay_samples_canonical_seed_before_overlay_without_mutating_memo() {
    let mut ingress = PoolIngress::new(Some(Arc::new(SnapshotRpc::default())));
    let verifier = Arc::new(Verifier::default());
    ingress.set_verify_level(VerifyLevel::Strict);
    ingress.set_verifier(verifier.clone());
    let overlay = HashMap::from([(
        -60,
        TickInfo {
            liquidity_gross: U128::from(700),
            liquidity_net: 0,
            block: 100,
        },
    )]);
    ingress
        .admit_v3_replay(&mut Workspace::new(), params(), overlay, 100)
        .await
        .unwrap();
    {
        let seen = verifier.seen.lock();
        assert!(!seen[0].1.ticks.contains_key(&-60));
        assert!(!seen[0].1.bitmaps.contains_key(&-1));
        assert!(seen[0].1.ticks.contains_key(&60000));
    }
    assert!(!ingress
        .stage_v3_tick_map(params(), 100)
        .await
        .unwrap()
        .ticks
        .contains_key(&-60));
}

#[tokio::test]
async fn v4_replay_verification_targets_manager_and_preserves_distant_rows() {
    let mut ingress = PoolIngress::new(Some(Arc::new(SnapshotRpc::default())));
    let verifier = Arc::new(Verifier::default());
    ingress.set_verifier(verifier.clone());
    let p = params();
    let pool_id = B256::repeat_byte(7);
    let v4 = IngressV4Params {
        manager: POOL,
        pool_id,
        token0: p.token0,
        token1: p.token1,
        fee: p.fee,
        tick_spacing: p.tick_spacing,
        hooks: Address::ZERO,
        sqrt_price_x96: p.sqrt_price_x96,
        liquidity: p.liquidity,
        tick: p.tick,
    };
    ingress
        .admit_v4_replay(
            &mut Workspace::new(),
            v4,
            HashMap::from([(
                -60,
                TickInfo {
                    liquidity_gross: U128::from(7),
                    liquidity_net: 0,
                    block: 100,
                },
            )]),
            100,
        )
        .await
        .unwrap();
    let seen = verifier.seen.lock();
    assert_eq!(
        seen[0].0,
        TickMapSampleTarget::V4 {
            manager: POOL,
            pool_id
        }
    );
    assert!(seen[0].1.ticks.contains_key(&60000));
    assert!(!seen[0].1.ticks.contains_key(&-60));
    assert!(!seen[0].1.bitmaps.contains_key(&-1));
}

#[tokio::test]
async fn replay_removes_burned_boundaries_and_clears_their_bitmap_bits() {
    let ingress = PoolIngress::new(Some(Arc::new(SnapshotRpc::default())));
    let mut seed = ingress.stage_v3_tick_map(params(), 100).await.unwrap();
    merge_replay_overlay(
        &mut seed,
        HashMap::from([(
            -60000,
            TickInfo {
                liquidity_gross: U128::ZERO,
                liquidity_net: 0,
                block: 100,
            },
        )]),
        60,
        "V3",
    )
    .unwrap();
    assert!(!seed.ticks.contains_key(&-60000));
    assert!(!seed.bitmaps[&-4].bit(24));
    assert!(seed.ticks.contains_key(&60000));
}

#[tokio::test]
async fn v4_swap_state_uses_snapshot_fees_and_preserves_replayed_scalars() {
    let ingress = PoolIngress::new(Some(Arc::new(SnapshotRpc {
        protocol_fee: 1234,
        lp_fee: Some(500),
        ..Default::default()
    })));
    let p = params();
    let replay = IngressV4Params {
        manager: POOL,
        pool_id: B256::repeat_byte(7),
        token0: p.token0,
        token1: p.token1,
        fee: p.fee,
        tick_spacing: p.tick_spacing,
        hooks: Address::ZERO,
        sqrt_price_x96: p.sqrt_price_x96 + U256::from(1),
        liquidity: 777,
        tick: p.tick,
    };
    let staged = ingress.stage_v4_tick_map(replay, 100).await.unwrap();
    assert!(
        matches!(staged.into_v4_state(replay), ExplicitPoolState::V4 {
        fee: 500, protocol_fee: 1234, liquidity: 777, sqrt_price_x96, ..
    } if sqrt_price_x96 == replay.sqrt_price_x96)
    );
}

#[tokio::test]
async fn pending_mint_samples_empty_canonical_map_and_registers_the_minted_rows() {
    use crate::planning::{PathReject, PlanningHop};
    use alloy::primitives::aliases::U112;
    use degenbot_solvers::mixed::HopType;

    for level in [VerifyLevel::Strict, VerifyLevel::Bootstrap] {
        for v4 in [false, true] {
            let mut ingress = PoolIngress::new(Some(Arc::new(SnapshotRpc {
                response: SnapshotResponse::Empty,
                ..Default::default()
            })));
            let verifier = Arc::new(Verifier::default());
            ingress.set_verify_level(level);
            ingress.set_verifier(verifier.clone());
            let overlay = HashMap::from([
                (
                    -60,
                    TickInfo {
                        liquidity_gross: U128::from(1000),
                        liquidity_net: 1000,
                        block: 100,
                    },
                ),
                (
                    60,
                    TickInfo {
                        liquidity_gross: U128::from(1000),
                        liquidity_net: -1000,
                        block: 100,
                    },
                ),
            ]);
            let mut ws = Workspace::new();
            let p = params();
            let id = if v4 {
                ingress
                    .admit_v4_replay(
                        &mut ws,
                        IngressV4Params {
                            manager: POOL,
                            pool_id: B256::repeat_byte(7),
                            token0: p.token0,
                            token1: p.token1,
                            fee: p.fee,
                            tick_spacing: p.tick_spacing,
                            hooks: Address::ZERO,
                            sqrt_price_x96: p.sqrt_price_x96,
                            liquidity: p.liquidity,
                            tick: p.tick,
                        },
                        overlay,
                        100,
                    )
                    .await
                    .unwrap()
            } else {
                ingress
                    .admit_v3_replay(&mut ws, p, overlay, 100)
                    .await
                    .unwrap()
            };
            assert!(verifier.seen.lock()[0].1.ticks.is_empty());
            let connector = ws
                .register_with_state(
                    PlanningPoolParams {
                        address: Address::repeat_byte(9),
                        token0: p.token0,
                        token1: p.token1,
                    },
                    ExplicitPoolState::V2 {
                        reserve0: U112::from(2000),
                        reserve1: U112::from(1000),
                        fee_token0: (997, 1000),
                        fee_token1: (997, 1000),
                    },
                    100,
                )
                .unwrap();
            let path = ws.declare(&[
                PlanningHop {
                    pool_id: id,
                    hop_type: if v4 { HopType::V4 } else { HopType::V3 },
                    zero_for_one: true,
                },
                PlanningHop::v2(connector, false),
            ]);
            let verdict = ws.evaluate_verdict(path, U256::ZERO);
            assert!(
                !matches!(verdict, Err(PathReject::UnusablePoolState { .. })),
                "pending Mint rows must reach the solver: {verdict:?} (V4={v4}, policy={level:?})"
            );
        }
    }
}
