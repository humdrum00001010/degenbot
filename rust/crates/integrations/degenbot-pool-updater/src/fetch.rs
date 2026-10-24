// Pool creation discovery; live liquidity decoders are owned by degenbot-decoders.

use alloy::primitives::{Address, B256};
use alloy::rpc::types::Log;
use degenbot_core::errors::ProviderResult;
use degenbot_decoders::pool_created_decoder::{
    decode_aerodrome_v2_pool_created_log, decode_v2_pair_created_log, decode_v3_pool_created_log,
    decode_v4_initialize_log, AerodromeV2PoolCreatedEvent, V2PairCreatedEvent, V3PoolCreatedEvent,
    V4InitializeEvent, AERODROME_V2_POOL_CREATED_TOPIC, V2_PAIR_CREATED_TOPIC,
    V3_POOL_CREATED_TOPIC, V4_INITIALIZE_TOPIC,
};
use degenbot_pools::{ConcentratedLiquidityVariant, Identity, ReservePairVariant};
use degenbot_rpc::provider::LogFetcher;

use crate::spec::ExchangeSpec;

/// The Uniswap family a pool-creation event belongs to — drives the topic0
/// filter + the decode leaf the fetcher dispatches to.
///
/// Discriminators mirror the Python `kind` strings in
/// `src/degenbot/cli/pool.py` (the `POOL_UPDATER[chain_id, exchange.name]`
/// dispatch keys).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PoolFamily {
    /// Canonical Uniswap V2 `PairCreated` (no `stable` flag). Shared by
    /// `Uniswap` V2, `PancakeSwap` V2, `SushiSwap` V2, `Swapbased` V2.
    V2,
    /// Aerodrome V2 `PoolCreated` (the `stable`-flag variant).
    AerodromeV2,
    /// Uniswap V3 `PoolCreated`. Shared by `PancakeSwap` V3 + `SushiSwap` V3.
    V3,
    /// Uniswap V4 `Initialize` (the V4 `PoolCreated`).
    V4,
}

impl PoolFamily {
    /// Project the pool taxonomy onto the decode family (ADR-059 D1).
    ///
    /// The decode station LAGS the taxonomy on structure, not species: a
    /// balance-vector pool has no `PoolCreated`-style creation event here, so
    /// Curve/Balancer project to `None`. The species fork stays where it is —
    /// an `AerodromeV2` pair is its own family because its event carries the
    /// `stable` flag and a distinct topic0, while an Aerodrome V3 pool decodes
    /// through the shared `V3` structure (its `event_topic` differs per
    /// exchange, not per family).
    #[must_use]
    pub fn from_identity(identity: &Identity) -> Option<Self> {
        match identity {
            Identity::ReservePair { variant, .. } => match variant {
                ReservePairVariant::UniswapV2 => Some(Self::V2),
                ReservePairVariant::AerodromeV2 { .. } => Some(Self::AerodromeV2),
            },
            Identity::ConcentratedLiquidity { variant, .. } => match variant {
                ConcentratedLiquidityVariant::UniswapV3 => Some(Self::V3),
                ConcentratedLiquidityVariant::UniswapV4 => Some(Self::V4),
            },
            // The decode station carries neither a balance-vector creation
            // event nor an LBPair decode leaf: both LAG the taxonomy.
            Identity::BalanceVector { .. } | Identity::BinnedLiquidity { .. } => None,
        }
    }

    /// The topic0 hash for this family's pool-creation event.
    #[must_use]
    pub const fn topic0(&self) -> B256 {
        match self {
            Self::V2 => V2_PAIR_CREATED_TOPIC,
            Self::AerodromeV2 => AERODROME_V2_POOL_CREATED_TOPIC,
            Self::V3 => V3_POOL_CREATED_TOPIC,
            Self::V4 => V4_INITIALIZE_TOPIC,
        }
    }
}

/// A decoded `PoolCreated`/`Initialize` event, family-tagged.
///
/// The chunk loop maps each variant to the family-specific
/// `V*PoolRowInput` (carrying the `exchange_id`/`fee_denominator`/`kind`
/// context the leaf correctly avoids).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedPoolCreated {
    /// Canonical V2 `PairCreated` (no `stable` flag).
    V2(V2PairCreatedEvent),
    /// Aerodrome V2 `PoolCreated` (with `stable`).
    AerodromeV2(AerodromeV2PoolCreatedEvent),
    /// V3 `PoolCreated`.
    V3(V3PoolCreatedEvent),
    /// V4 `Initialize`.
    V4(V4InitializeEvent),
}

/// Decode a single `PoolCreated`/`Initialize` log for the given family.
///
/// Pure (no RPC). Returns `None` if the log's topic0 doesn't match the
/// family's signature, the data is malformed, or the indexed fields are
/// out of range (see each decode leaf's reject conditions).
#[must_use]
pub fn decode_pool_created_log(log: &Log, family: PoolFamily) -> Option<DecodedPoolCreated> {
    match family {
        PoolFamily::V2 => decode_v2_pair_created_log(log).map(DecodedPoolCreated::V2),
        PoolFamily::AerodromeV2 => {
            decode_aerodrome_v2_pool_created_log(log).map(DecodedPoolCreated::AerodromeV2)
        }
        PoolFamily::V3 => decode_v3_pool_created_log(log).map(DecodedPoolCreated::V3),
        PoolFamily::V4 => decode_v4_initialize_log(log).map(DecodedPoolCreated::V4),
    }
}

/// Fetch `PoolCreated`/`Initialize` logs for a single factory address across a
/// block range + decode each into a [`DecodedPoolCreated`].
///
/// The `factory_address` is the emitter (the `address` filter on
/// `eth_getLogs`) — for V2/V3 it's the factory contract; for V4 it's the
/// singleton `PoolManager`. The family drives the topic0 + the decode leaf.
///
/// # Errors
///
/// Returns [`ProviderError`] on RPC failure, a malformed filter (bad address
/// / topic), or an invalid block range.
pub async fn fetch_pool_created_logs(
    fetcher: &LogFetcher,
    from_block: u64,
    to_block: u64,
    factory_address: Address,
    family: PoolFamily,
) -> ProviderResult<Vec<DecodedPoolCreated>> {
    let topic0 = family.topic0();
    let logs = fetch_logs(fetcher, from_block, to_block, Some(factory_address), topic0).await?;
    Ok(logs
        .iter()
        .filter_map(|log| decode_pool_created_log(log, family))
        .collect())
}

/// Spec-aware `PoolCreated`/`Initialize` fetch: like
/// [`fetch_pool_created_logs`] but resolved from an [`ExchangeSpec`] — uses
/// `spec.event_topic` (NOT `spec.family.topic0()`) so Aerodrome V3 (which
/// shares the V3 decode structure but has its own
/// [`AERODROME_V3_POOL_CREATED_TOPIC`]) fetches the correct topic. The chunk
/// loop (`run_pool_update`) calls this per in-scope exchange.
///
/// # Errors
///
/// Returns [`ProviderError`](degenbot_core::errors::ProviderError) on RPC
/// failure or an invalid block range.
pub async fn fetch_pool_created_logs_for_spec(
    fetcher: &LogFetcher,
    from_block: u64,
    to_block: u64,
    spec: &ExchangeSpec,
) -> ProviderResult<Vec<DecodedPoolCreated>> {
    let logs = fetch_logs(
        fetcher,
        from_block,
        to_block,
        Some(spec.factory),
        spec.event_topic,
    )
    .await?;
    Ok(logs
        .iter()
        .filter_map(|log| decode_pool_created_log(log, spec.family))
        .collect())
}

async fn fetch_logs(
    fetcher: &LogFetcher,
    from_block: u64,
    to_block: u64,
    address: Option<Address>,
    topic0: B256,
) -> ProviderResult<Vec<Log>> {
    let addresses = address.map(|a| vec![a.to_string()]);
    let topic_filter = Some(vec![vec![topic0.to_string()]]);
    fetcher
        .fetch_logs_chunked(from_block, to_block, addresses, topic_filter)
        .await
}

#[expect(clippy::panic)]
#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloy::primitives::{Bytes, Log as AlloyLog, I256};

    /// Build a pool-creation log for decoder tests.
    fn make_log(
        address: Address,
        topics: Vec<B256>,
        data: Vec<u8>,
        block_number: u64,
        log_index: u64,
    ) -> Log {
        let inner = AlloyLog::new_unchecked(address, topics, Bytes::from(data));
        Log {
            inner,
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: Some(log_index),
            removed: false,
        }
    }

    fn addr_word(addr: Address) -> B256 {
        let mut word = [0u8; 32];
        word[12..32].copy_from_slice(addr.as_slice());
        B256::new(word)
    }

    fn int24_word(tick: i32) -> B256 {
        B256::from(I256::try_from(tick).unwrap().into_raw().to_be_bytes::<32>())
    }

    #[test]
    fn pool_family_topic0_roundtrips_to_constants() {
        assert_eq!(PoolFamily::V2.topic0(), V2_PAIR_CREATED_TOPIC);
        assert_eq!(
            PoolFamily::AerodromeV2.topic0(),
            AERODROME_V2_POOL_CREATED_TOPIC,
        );
        assert_eq!(PoolFamily::V3.topic0(), V3_POOL_CREATED_TOPIC);
        assert_eq!(PoolFamily::V4.topic0(), V4_INITIALIZE_TOPIC);
    }

    // ── decode_pool_created_log dispatch ─────────────────────────────────

    #[test]
    fn decode_pool_created_log_dispatches_v3() {
        let factory = Address::from([0xfa; 20]);
        let token0 = Address::from([0x11; 20]);
        let token1 = Address::from([0x22; 20]);
        let pool = Address::from([0x99; 20]);
        let fee: u32 = 500;
        let tick_spacing: i32 = 10;

        let mut data = vec![0u8; 64];
        data[0..32].copy_from_slice(&int24_word(tick_spacing).0);
        data[12 + 32..32 + 32].copy_from_slice(pool.as_slice());

        let mut fee_topic = [0u8; 32];
        fee_topic[29..32].copy_from_slice(&fee.to_be_bytes()[1..4]);
        let log = make_log(
            factory,
            vec![
                V3_POOL_CREATED_TOPIC,
                addr_word(token0),
                addr_word(token1),
                B256::new(fee_topic),
            ],
            data,
            100,
            0,
        );
        let decoded = decode_pool_created_log(&log, PoolFamily::V3).unwrap();
        match decoded {
            DecodedPoolCreated::V3(ev) => {
                assert_eq!(ev.factory_address, factory);
                assert_eq!(ev.token0, token0);
                assert_eq!(ev.token1, token1);
                assert_eq!(ev.fee, fee);
                assert_eq!(ev.tick_spacing, tick_spacing);
                assert_eq!(ev.pool_address, pool);
            }
            other => panic!("expected V3, got {other:?}"),
        }
    }

    #[test]
    fn decode_pool_created_log_wrong_family_returns_none() {
        // A V3 topic fed to the V2 family → must not decode.
        let log = make_log(
            Address::ZERO,
            vec![V3_POOL_CREATED_TOPIC, B256::ZERO, B256::ZERO, B256::ZERO],
            vec![0u8; 64],
            100,
            0,
        );
        assert!(decode_pool_created_log(&log, PoolFamily::V2).is_none());
    }

    // ── decode_v3_liquidity_log (Mint → positive, Burn → negative) ──────
}

#[cfg(test)]
mod taxonomy_tests {
    //! ADR-059 D1: the decode family is a projection of the pool taxonomy.
    //! Every structure x variant is pinned, including the balance-vector legs
    //! this station does not decode.
    use super::PoolFamily;
    use degenbot_pools::{
        BalanceVectorVariant, BinnedLiquidityVariant, ConcentratedLiquidityVariant, Identity,
        ReservePairVariant,
    };

    fn reserve_pair(variant: ReservePairVariant) -> Identity {
        Identity::ReservePair { variant, dex: None }
    }

    fn concentrated_liquidity(variant: ConcentratedLiquidityVariant) -> Identity {
        Identity::ConcentratedLiquidity { variant, dex: None }
    }

    fn balance_vector(variant: BalanceVectorVariant) -> Identity {
        Identity::BalanceVector { variant, dex: None }
    }

    #[test]
    fn identity_projection_admits_only_the_decoded_families() {
        assert_eq!(
            PoolFamily::from_identity(&reserve_pair(ReservePairVariant::UniswapV2)),
            Some(PoolFamily::V2),
        );
        assert_eq!(
            PoolFamily::from_identity(&reserve_pair(ReservePairVariant::AerodromeV2 {
                stable: true
            })),
            Some(PoolFamily::AerodromeV2),
        );
        assert_eq!(
            PoolFamily::from_identity(&reserve_pair(ReservePairVariant::AerodromeV2 {
                stable: false
            })),
            Some(PoolFamily::AerodromeV2),
        );
        assert_eq!(
            PoolFamily::from_identity(&concentrated_liquidity(
                ConcentratedLiquidityVariant::UniswapV3
            )),
            Some(PoolFamily::V3),
        );
        assert_eq!(
            PoolFamily::from_identity(&concentrated_liquidity(
                ConcentratedLiquidityVariant::UniswapV4
            )),
            Some(PoolFamily::V4),
        );
        assert_eq!(
            PoolFamily::from_identity(&balance_vector(BalanceVectorVariant::Curve)),
            None,
        );
        assert_eq!(
            PoolFamily::from_identity(&balance_vector(BalanceVectorVariant::BalancerWeighted)),
            None,
        );
        assert_eq!(
            PoolFamily::from_identity(&balance_vector(BalanceVectorVariant::BalancerStable)),
            None,
        );
        assert_eq!(
            PoolFamily::from_identity(&Identity::BinnedLiquidity {
                variant: BinnedLiquidityVariant::Lfj,
                dex: None,
            }),
            None,
            "no LBPair decode leaf yet: binned liquidity must lag, not guess",
        );
    }
}
