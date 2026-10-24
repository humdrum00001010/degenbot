//! Compare complete Erigon snapshots against independent contract getters.

use alloy::primitives::{U128, U256};
use degenbot_core::errors::ProviderError;
use degenbot_rpc::liquidity_verifier::{
    verify_liquidity_map, LiquidityMap, LiquidityMapDivergence, LiquidityMapTarget,
};
use degenbot_rpc::provider::AlloyProvider;
use degenbot_rpc::tick_snapshot::{TickMapProtocol, TickMapRequest};
use hashbrown::HashMap;

use crate::run::RunError;

/// A named, deterministic fact proving that an updater map differs from chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiquidityDivergence {
    /// A tick exists on only one side of the map.
    TickPresence {
        /// Tick index.
        tick: i32,
        /// Whether the updater map contains the tick.
        stored: bool,
        /// Whether chain state contains the tick.
        observed: bool,
    },
    /// Gross liquidity differs for a tick present on both sides.
    TickGross {
        /// Tick index.
        tick: i32,
        /// Supplied gross liquidity.
        expected: U128,
        /// Chain gross liquidity.
        actual: U128,
    },
    /// Net liquidity differs for a tick present on both sides.
    TickNet {
        /// Tick index.
        tick: i32,
        /// Supplied net liquidity.
        expected: alloy::primitives::I256,
        /// Chain net liquidity.
        actual: alloy::primitives::I256,
    },
    /// A bitmap word differs from chain state.
    BitmapWord {
        /// Signed bitmap word position.
        word: i32,
        /// Supplied bitmap.
        expected: U256,
        /// Chain bitmap.
        actual: U256,
    },
}

impl From<LiquidityMapDivergence> for LiquidityDivergence {
    fn from(value: LiquidityMapDivergence) -> Self {
        match value {
            LiquidityMapDivergence::TickPresence {
                tick,
                stored,
                observed,
            } => Self::TickPresence {
                tick,
                stored,
                observed,
            },
            LiquidityMapDivergence::TickGross {
                tick,
                expected,
                actual,
            } => Self::TickGross {
                tick,
                expected,
                actual,
            },
            LiquidityMapDivergence::TickNet {
                tick,
                expected,
                actual,
            } => Self::TickNet {
                tick,
                expected: alloy::primitives::I256::try_from(expected)
                    .unwrap_or(alloy::primitives::I256::ZERO),
                actual: alloy::primitives::I256::try_from(actual)
                    .unwrap_or(alloy::primitives::I256::ZERO),
            },
            LiquidityMapDivergence::BitmapWord {
                word,
                expected,
                actual,
            } => Self::BitmapWord {
                word,
                expected,
                actual,
            },
        }
    }
}

/// Resolve only immutable identity and spacing from the catalog.
///
/// # Errors
/// Returns a database error when the catalog row is absent or malformed.
pub fn catalog_tick_map_request(
    conn: &rusqlite::Connection,
    chain_id: i64,
    target: LiquidityMapTarget,
) -> Result<TickMapRequest, RunError> {
    let (address, protocol, pool_id, tick_spacing) = match target {
        LiquidityMapTarget::V3(address) => {
            let (id, kind): (i64, String) = conn
                .query_row(
                    "SELECT id, kind FROM pools WHERE chain = ?1 AND address = ?2",
                    rusqlite::params![chain_id, address.to_checksum(None)],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(degenbot_db::DbError::from)?;
            let table = degenbot_db::schema::table::v2_v3_subclass_table(&kind)
                .filter(|_| degenbot_db::schema::table::is_v3_kind(&kind))
                .ok_or_else(|| degenbot_db::DbError::Decode(format!("not a V3 pool: {kind}")))?;
            let spacing = conn
                .query_row(
                    &format!("SELECT tick_spacing FROM {table} WHERE pool_id = ?1"),
                    [id],
                    |row| row.get(0),
                )
                .map_err(degenbot_db::DbError::from)?;
            let protocol = if kind == "pancakeswap_v3" {
                TickMapProtocol::PancakeV3
            } else {
                TickMapProtocol::UniswapV3
            };
            (address, protocol, None, spacing)
        }
        LiquidityMapTarget::V4 {
            pool_manager,
            pool_id,
        } => {
            let spacing = conn.query_row(
                "SELECT v.tick_spacing FROM uniswap_v4_pools v JOIN managed_pools p ON p.id = v.managed_pool_id JOIN pool_managers m ON m.id = p.manager_id WHERE m.chain = ?1 AND m.address = ?2 AND v.pool_hash = ?3",
                rusqlite::params![chain_id, pool_manager.to_checksum(None), format!("{pool_id:#x}")],
                |row| row.get(0),
            ).map_err(degenbot_db::DbError::from)?;
            (
                pool_manager,
                TickMapProtocol::UniswapV4,
                Some(pool_id),
                spacing,
            )
        }
    };
    Ok(TickMapRequest {
        address,
        protocol,
        pool_id,
        tick_spacing,
    })
}

/// Compare the requested complete snapshot against ordinary contract getters.
///
/// # Errors
/// Returns RPC/decode errors without a persisted-map fallback.
pub async fn verify_tick_map_snapshot(
    provider: &AlloyProvider,
    request: TickMapRequest,
    block_number: u64,
) -> Result<Vec<LiquidityDivergence>, RunError> {
    let snapshot = provider
        .get_tick_maps(&[request], block_number.into())
        .await?;
    let pool = &snapshot.pools[0];
    let ticks = pool.tick_map(block_number);
    let mut bitmaps = HashMap::new();
    for &tick in ticks.keys() {
        let compressed = tick.div_euclid(request.tick_spacing);
        *bitmaps.entry(compressed >> 8).or_insert(U256::ZERO) |=
            U256::from(1) << (compressed & 255);
    }
    let target = request
        .pool_id
        .map_or(LiquidityMapTarget::V3(request.address), |pool_id| {
            LiquidityMapTarget::V4 {
                pool_manager: request.address,
                pool_id,
            }
        });
    verify_liquidity_map(
        provider,
        target,
        &LiquidityMap::tracked_with_spacing(ticks, bitmaps, request.tick_spacing),
        Some(block_number),
    )
    .await
    .map(|facts| facts.into_iter().map(LiquidityDivergence::from).collect())
    .map_err(|error| {
        RunError::Provider(ProviderError::Other {
            message: error.to_string(),
        })
    })
}
