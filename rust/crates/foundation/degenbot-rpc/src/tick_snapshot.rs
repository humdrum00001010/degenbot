//! Complete concentrated-liquidity tick snapshots from local Erigon state.

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, U256};
use degenbot_core::errors::{ProviderError, ProviderResult};
use degenbot_math::cl::tick_math::{
    get_sqrt_ratio_at_tick_internal, MAX_SQRT_RATIO, MAX_TICK, MIN_TICK,
};
use degenbot_pools::TickInfo;
use hashbrown::HashMap;
use serde::{Deserialize, Deserializer, Serialize};

use crate::provider::AlloyProvider;

/// The contract layout used to read a complete tick map.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum TickMapProtocol {
    #[serde(rename = "uniswapV3")]
    UniswapV3,
    #[serde(rename = "pancakeV3")]
    PancakeV3,
    #[serde(rename = "uniswapV4")]
    UniswapV4,
}

/// Pool identity and spacing required by the local state reader.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TickMapRequest {
    pub address: Address,
    pub protocol: TickMapProtocol,
    pub tick_spacing: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_id: Option<B256>,
}

/// Complete maps read atomically from a single chain and block.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TickMapSnapshot {
    #[serde(with = "alloy::serde::quantity")]
    pub chain_id: u64,
    #[serde(with = "alloy::serde::quantity")]
    pub block_number: u64,
    pub block_hash: B256,
    pub pools: Vec<PoolTickSnapshot>,
}

/// Pool state paired with every initialized tick at the snapshot block.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolTickSnapshot {
    pub address: Address,
    pub protocol: TickMapProtocol,
    pub pool_id: Option<B256>,
    pub tick_spacing: i32,
    pub tick: i32,
    pub sqrt_price_x96: U256,
    #[serde(with = "alloy::serde::quantity")]
    pub liquidity: u128,
    pub protocol_fee: u32,
    pub lp_fee: u32,
    pub ticks: Vec<SnapshotTick>,
}

/// Initialized boundary liquidity, ordered by ascending tick.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotTick {
    pub tick: i32,
    #[serde(with = "alloy::serde::quantity")]
    pub liquidity_gross: u128,
    #[serde(deserialize_with = "signed_decimal")]
    pub liquidity_net: i128,
}

fn signed_decimal<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
    let text = String::deserialize(deserializer)?;
    let value = text.parse::<i128>().map_err(serde::de::Error::custom)?;
    if value.to_string() != text {
        return Err(serde::de::Error::custom(
            "noncanonical signed decimal liquidity",
        ));
    }
    Ok(value)
}

fn invalid(message: impl Into<String>) -> ProviderError {
    ProviderError::InvalidResponse {
        message: format!("erigon_getTickMaps: {}", message.into()),
    }
}

impl PoolTickSnapshot {
    /// Materialize the pool core's tick records at the snapshot block.
    #[must_use]
    pub fn tick_map(&self, block: u64) -> HashMap<i32, TickInfo> {
        self.ticks
            .iter()
            .map(|tick| {
                (
                    tick.tick,
                    TickInfo {
                        liquidity_gross: alloy::primitives::U128::from(tick.liquidity_gross),
                        liquidity_net: tick.liquidity_net,
                        block,
                    },
                )
            })
            .collect()
    }

    fn validate(&self, request: &TickMapRequest) -> ProviderResult<()> {
        if !(1..=32_767).contains(&request.tick_spacing)
            || (request.protocol == TickMapProtocol::UniswapV4) != request.pool_id.is_some()
        {
            return Err(invalid("invalid spacing or pool identity"));
        }
        if (self.address, self.protocol, self.pool_id, self.tick_spacing)
            != (
                request.address,
                request.protocol,
                request.pool_id,
                request.tick_spacing,
            )
        {
            return Err(invalid("pool identity, order, or spacing mismatch"));
        }
        if !(MIN_TICK..MAX_TICK).contains(&self.tick) {
            return Err(invalid("head tick outside initialized pool range"));
        }
        let lower =
            get_sqrt_ratio_at_tick_internal(self.tick).map_err(|e| invalid(e.to_string()))?;
        let upper =
            get_sqrt_ratio_at_tick_internal(self.tick + 1).map_err(|e| invalid(e.to_string()))?;
        // A leftward crossing leaves slot0.tick one below the exact boundary price.
        if self.sqrt_price_x96 < U256::from(lower)
            || self.sqrt_price_x96 > U256::from(upper)
            || self.sqrt_price_x96 >= U256::from(MAX_SQRT_RATIO)
        {
            return Err(invalid("sqrt price and head tick disagree"));
        }
        let mut running = 0u128;
        let mut active = 0u128;
        let mut previous = None;
        for tick in &self.ticks {
            if !(MIN_TICK..=MAX_TICK).contains(&tick.tick)
                || tick.tick % self.tick_spacing != 0
                || previous.is_some_and(|previous| previous >= tick.tick)
                || tick.liquidity_gross == 0
                || tick.liquidity_net.unsigned_abs() > tick.liquidity_gross
            {
                return Err(invalid("invalid, duplicate, or unsorted initialized tick"));
            }
            running = running
                .checked_add_signed(tick.liquidity_net)
                .ok_or_else(|| invalid("liquidity prefix underflow or overflow"))?;
            if tick.tick <= self.tick {
                active = running;
            }
            previous = Some(tick.tick);
        }
        if running != 0 || active != self.liquidity {
            return Err(invalid(
                "incomplete tick map or inconsistent active liquidity",
            ));
        }
        Ok(())
    }
}

impl TickMapSnapshot {
    /// Validate completeness and identity even when a custom transport supplied the map.
    ///
    /// # Errors
    /// Returns an invalid-response error for malformed maps or mismatched identities and blocks.
    pub fn validate(&self, requests: &[TickMapRequest], block: BlockId) -> ProviderResult<()> {
        if requests.is_empty()
            || self.pools.len() != requests.len()
            || block
                .as_u64()
                .is_some_and(|number| self.block_number != number)
            || (block.is_earliest() && self.block_number != 0)
            || block
                .as_block_hash()
                .is_some_and(|hash| self.block_hash != hash)
        {
            return Err(invalid("pool count or pinned block mismatch"));
        }
        for (pool, request) in self.pools.iter().zip(requests) {
            pool.validate(request)?;
        }
        Ok(())
    }
}

/// Transport for complete, block-pinned concentrated-liquidity maps.
pub trait TickMapRpc: Send + Sync {
    /// Fetch every initialized tick at the requested block.
    fn get_tick_maps<'a>(
        &'a self,
        requests: &'a [TickMapRequest],
        block: BlockId,
    ) -> futures_util::future::BoxFuture<'a, ProviderResult<TickMapSnapshot>>;
}

impl TickMapRpc for AlloyProvider {
    fn get_tick_maps<'a>(
        &'a self,
        requests: &'a [TickMapRequest],
        block: BlockId,
    ) -> futures_util::future::BoxFuture<'a, ProviderResult<TickMapSnapshot>> {
        Box::pin(AlloyProvider::get_tick_maps(self, requests, block))
    }
}

impl AlloyProvider {
    /// Fetch complete maps from local Erigon without incremental bitmap RPC calls.
    ///
    /// # Errors
    /// Returns the RPC error for unsupported endpoints, or an invalid-response error
    /// for mismatched identity, partial maps, and inconsistent pool state.
    pub async fn get_tick_maps(
        &self,
        requests: &[TickMapRequest],
        block: BlockId,
    ) -> ProviderResult<TickMapSnapshot> {
        if requests.is_empty()
            || requests.iter().any(|request| {
                !(1..=32_767).contains(&request.tick_spacing)
                    || (request.protocol == TickMapProtocol::UniswapV4) != request.pool_id.is_some()
            })
        {
            return Err(ProviderError::InvalidParams {
                message: "tick maps require pools, valid spacing, and poolId only for V4".into(),
            });
        }
        let response = self
            .make_request("erigon_getTickMaps", serde_json::json!([requests, block]))
            .await?;
        let snapshot: TickMapSnapshot =
            serde_json::from_value(response).map_err(|e| invalid(e.to_string()))?;
        snapshot.validate(requests, block)?;
        if snapshot.chain_id != self.get_chain_id().await? {
            return Err(invalid("endpoint chain identity mismatch"));
        }
        Ok(snapshot)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloy::eips::RpcBlockHash;
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::rpc::client::ClientBuilder;
    use alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
    use alloy::transports::mock::{Asserter, MockTransport};
    use alloy::transports::{TransportError, TransportFut};
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tower::Service;

    #[derive(Clone)]
    struct RecordingTransport {
        inner: MockTransport,
        sent: Arc<Mutex<Vec<Value>>>,
    }

    impl Service<RequestPacket> for RecordingTransport {
        type Response = ResponsePacket;
        type Error = TransportError;
        type Future = TransportFut<'static>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, request: RequestPacket) -> Self::Future {
            self.sent
                .lock()
                .unwrap()
                .push(serde_json::to_value(&request).unwrap());
            self.inner.call(request)
        }
    }

    fn fixture() -> (TickMapRequest, Value) {
        let request = TickMapRequest {
            address: Address::repeat_byte(1),
            protocol: TickMapProtocol::UniswapV3,
            tick_spacing: 60,
            pool_id: None,
        };
        let response = json!({
            "chainId": "0x1", "blockNumber": "0x64", "blockHash": B256::repeat_byte(2),
            "pools": [{"address": request.address, "protocol": "uniswapV3", "tickSpacing": 60,
                "tick": 0, "sqrtPriceX96": "0x1000000000000000000000000", "liquidity": "0xa",
                "protocolFee": 0, "lpFee": 0,
                "ticks": [{"tick": -60, "liquidityGross": "0xa", "liquidityNet": "10"},
                          {"tick": 60, "liquidityGross": "0xa", "liquidityNet": "-10"}]}]
        });
        (request, response)
    }

    fn provider(response: &Value) -> (AlloyProvider, Arc<Mutex<Vec<Value>>>) {
        let asserter = Asserter::new();
        asserter.push_success(response);
        asserter.push_success(&"0x1");
        let sent = Arc::new(Mutex::new(Vec::new()));
        let transport = RecordingTransport {
            inner: MockTransport::new(asserter),
            sent: Arc::clone(&sent),
        };
        let client = ClientBuilder::default().transport(transport, true);
        let inner = ProviderBuilder::new().connect_client(client).erased();
        (AlloyProvider::from_provider(Arc::new(inner)), sent)
    }

    #[tokio::test]
    async fn complete_snapshot_uses_one_pinned_rpc_and_preserves_tick_values() {
        let (request, response) = fixture();
        let (provider, sent) = provider(&response);
        let block = BlockId::Hash(RpcBlockHash {
            block_hash: B256::repeat_byte(2),
            require_canonical: Some(true),
        });
        let snapshot = provider.get_tick_maps(&[request], block).await.unwrap();
        assert_eq!(snapshot.chain_id, 1);
        assert_eq!(snapshot.block_number, 100);
        assert_eq!(snapshot.pools[0].ticks[1].liquidity_net, -10);
        let ticks = snapshot.pools[0].tick_map(snapshot.block_number);
        assert_eq!(ticks[&-60].block, 100);
        assert_eq!(
            ticks[&60].liquidity_gross,
            alloy::primitives::U128::from(10)
        );
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0]["method"], "erigon_getTickMaps");
        assert_eq!(
            sent[0]["params"],
            json!([[request], {
                "blockHash": B256::repeat_byte(2), "requireCanonical": true
            }])
        );
    }

    #[tokio::test]
    async fn snapshots_reject_partial_mismatched_and_inconsistent_state() {
        let (request, response) = fixture();
        let mutations = [
            ("/chainId", json!("0x2")),
            ("/pools", json!([])),
            ("/blockNumber", json!("0x65")),
            ("/pools/0/address", json!(Address::ZERO)),
            ("/pools/0/protocol", json!("pancakeV3")),
            ("/pools/0/tickSpacing", json!(1)),
            ("/pools/0/tick", json!(887_272)),
            ("/pools/0/sqrtPriceX96", json!("0x0")),
            ("/pools/0/liquidity", json!("0xb")),
            ("/pools/0/ticks/0/tick", json!(-61)),
            ("/pools/0/ticks/1/tick", json!(-60)),
            ("/pools/0/ticks/0/liquidityGross", json!("0x0")),
            ("/pools/0/ticks/0/liquidityNet", json!("11")),
            ("/pools/0/ticks/1/liquidityNet", json!("-9")),
            ("/pools/0/ticks/0/liquidityNet", json!("0xa")),
            (
                "/pools/0/ticks/0/liquidityGross",
                json!("0x100000000000000000000000000000000"),
            ),
        ];
        for (path, value) in mutations {
            let mut invalid = response.clone();
            *invalid.pointer_mut(path).unwrap() = value;
            let (provider, _) = provider(&invalid);
            assert!(
                provider
                    .get_tick_maps(&[request], 100.into())
                    .await
                    .is_err(),
                "{path}"
            );
        }
        let (provider, _) = provider(&response);
        assert!(provider
            .get_tick_maps(
                &[request],
                BlockId::Hash(RpcBlockHash {
                    block_hash: B256::ZERO,
                    require_canonical: Some(true)
                })
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn all_protocols_and_exact_left_crossing_boundary_are_supported() {
        for protocol in [
            TickMapProtocol::UniswapV3,
            TickMapProtocol::PancakeV3,
            TickMapProtocol::UniswapV4,
        ] {
            let (mut request, mut response) = fixture();
            request.protocol = protocol;
            request.pool_id =
                (protocol == TickMapProtocol::UniswapV4).then_some(B256::repeat_byte(3));
            response["pools"][0]["protocol"] = serde_json::to_value(protocol).unwrap();
            if let Some(pool_id) = request.pool_id {
                response["pools"][0]["poolId"] = json!(pool_id);
            }
            response["pools"][0]["tick"] = json!(-1);
            let (provider, _) = provider(&response);
            provider
                .get_tick_maps(&[request], 100.into())
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn unsigned_liquidity_prefixes_can_exceed_i128_and_cannot_overflow_u128() {
        let (request, mut response) = fixture();
        let net = i128::MAX;
        let gross = format!("0x{:x}", net.unsigned_abs());
        let boundary = |tick, net: i128| {
            json!({
                "tick": tick, "liquidityGross": gross, "liquidityNet": net.to_string()
            })
        };
        response["pools"][0]["ticks"] = json!([
            boundary(-120, net),
            boundary(-60, net),
            boundary(60, -net),
            boundary(120, -net)
        ]);
        response["pools"][0]["liquidity"] = json!(format!("0x{:x}", u128::MAX - 1));
        let (valid_provider, _) = provider(&response);
        valid_provider
            .get_tick_maps(&[request], 100.into())
            .await
            .unwrap();
        response["pools"][0]["ticks"]
            .as_array_mut()
            .unwrap()
            .insert(0, boundary(-180, net));
        let (invalid_provider, _) = provider(&response);
        assert!(invalid_provider
            .get_tick_maps(&[request], 100.into())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn invalid_protocol_descriptor_never_reaches_the_transport() {
        let (mut request, response) = fixture();
        request.pool_id = Some(B256::ZERO);
        let (provider, sent) = provider(&response);
        assert!(provider
            .get_tick_maps(&[request], 100.into())
            .await
            .is_err());
        assert!(sent.lock().unwrap().is_empty());
    }
}
