//! Complete, block-pinned concentrated-liquidity pool admission.

use std::sync::Arc;

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, U256};
use degenbot_pools::v3_state::ClSlotLayout;
use degenbot_pools::TickInfo;
use degenbot_rpc::liquidity_verifier::{
    verify_liquidity_map, LiquidityMap as RpcLiquidityMap, LiquidityMapTarget,
};
use degenbot_rpc::provider::AlloyProvider;
use hashbrown::HashMap;
use parking_lot::Mutex;

use crate::planning::{ExplicitPoolState, PlanningPoolParams, Workspace};
use degenbot_rpc::tick_snapshot::{TickMapProtocol, TickMapRequest, TickMapRpc};

// Provenance follows the seed across the planning boundary.
pub use crate::planning::{TickMapPoolIdentity, TickMapSeed, TickMapSource};

/// The chain-sample verification policy for concentrated-liquidity admission.
///
/// Sampling is independent of complete-snapshot validation, which always runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VerifyLevel {
    /// Chain-sample-verify every admission.
    Strict,
    /// Verify the first admission per pool and freshness block, then memoize.
    #[default]
    Bootstrap,
    /// No chain-sample verification; the operator has declared confidence.
    /// Integrity checks are unaffected.
    Off,
}

impl VerifyLevel {
    /// The stable label (`strict`/`bootstrap`/`off`).
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Bootstrap => "bootstrap",
            Self::Off => "off",
        }
    }
}

/// The contract identity sampled for one complete tick map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickMapSampleTarget {
    /// A V3 pool contract.
    V3(Address),
    /// A V4 pool's full-map storage owner and pool id.
    V4 { manager: Address, pool_id: B256 },
}

/// A chain-sample verifier for a staged CL tick map. The concrete
/// [`AlloySampleVerifier`] delegates both families to the shared full-map
/// verifier; this seam exists only for ingress policy and test doubles.
#[async_trait::async_trait]
pub trait TickMapSampleVerifier: Send + Sync {
    /// Verify one complete map against its contract target at `block`.
    ///
    /// # Errors
    ///
    /// A genuine mismatch or a transport failure, as a loud string.
    async fn verify(
        &self,
        target: TickMapSampleTarget,
        map: &RpcLiquidityMap,
        block: u64,
    ) -> Result<(), String>;
}

/// The `AlloyProvider`-backed sample verifier: delegates to the snapshot-block
/// [`verify_liquidity_map`] so the ingress and the settlement pump share one
/// verification implementation.
pub struct AlloySampleVerifier {
    provider: Arc<AlloyProvider>,
}

impl AlloySampleVerifier {
    #[must_use]
    pub fn new(provider: Arc<AlloyProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait::async_trait]
impl TickMapSampleVerifier for AlloySampleVerifier {
    async fn verify(
        &self,
        target: TickMapSampleTarget,
        map: &RpcLiquidityMap,
        block: u64,
    ) -> Result<(), String> {
        let target = match target {
            TickMapSampleTarget::V3(address) => LiquidityMapTarget::V3(address),
            TickMapSampleTarget::V4 { manager, pool_id } => LiquidityMapTarget::V4 {
                pool_manager: manager,
                pool_id,
            },
        };
        let facts = verify_liquidity_map(self.provider.as_ref(), target, map, Some(block))
            .await
            .map_err(|e| e.to_string())?;
        if facts.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "liquidity map verification returned {} divergences",
                facts.len()
            ))
        }
    }
}

/// Why the ingress could not admit or stage a V3 pool. Stages map 1:1 onto
/// the per-hop JSONL witness labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressDecline {
    /// The `slot0`/`liquidity` `eth_call` failed.
    Slot0Fetch(String),
    /// A slot0/liquidity value did not fit its wire type.
    Slot0Width,
    /// The complete tick-map snapshot failed (transport/revert or a
    /// malformed return).
    TickMapFetch(String),
    /// The workspace refused the fetched state (spec violation,
    /// `AlreadyRegistered`, ...).
    Register(String),
    /// The chain-sample verification refused the staged map (mismatch or
    /// transport failure). A loud decline, never a silent fallback.
    Verify(String),
}

impl IngressDecline {
    /// The stable JSONL `stage` label for the refused step.
    #[must_use]
    pub fn stage(&self) -> &'static str {
        match self {
            Self::Slot0Fetch(_) => "admit-v3-slot0-fetch",
            Self::Slot0Width => "admit-v3-slot0-width",
            Self::TickMapFetch(_) => "admit-v3-tick-map-fetch",
            Self::Register(_) => "admit-v3-register",
            Self::Verify(_) => "admit-v3-verify",
        }
    }

    /// The step's error detail (empty for width refusals, whose identity is
    /// the stage itself).
    #[must_use]
    pub fn detail(&self) -> String {
        match self {
            Self::Slot0Fetch(d) | Self::TickMapFetch(d) | Self::Register(d) | Self::Verify(d) => {
                d.clone()
            }
            Self::Slot0Width => "slot0/liquidity value out of type range".into(),
        }
    }
}

/// A V3 pool's admission payload. `slot_layout` is mandatory — the fork the
/// caller READ the `slot0`/`liquidity` scalars through; the ingress passes it
/// to [`ExplicitPoolState::V3`] verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressV3Params {
    pub address: Address,
    pub token0: Address,
    pub token1: Address,
    pub fee: u32,
    pub tick_spacing: i32,
    pub sqrt_price_x96: U256,
    pub liquidity: u128,
    pub tick: i32,
    pub slot_layout: ClSlotLayout,
}

/// A V4 pool's admission payload. Complete map reads and verification target
/// the manager and pool id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressV4Params {
    pub manager: Address,
    pub pool_id: B256,
    pub token0: Address,
    pub token1: Address,
    pub fee: u32,
    pub tick_spacing: i32,
    pub hooks: Address,
    pub sqrt_price_x96: U256,
    pub liquidity: u128,
    pub tick: i32,
}

#[derive(Clone)]
struct StagedTickMap {
    seed: TickMapSeed,
    protocol_fee: u32,
    lp_fee: u32,
}

impl StagedTickMap {
    fn into_v4_state(self, params: IngressV4Params) -> ExplicitPoolState {
        ExplicitPoolState::V4 {
            pool_id: params.pool_id.0,
            fee: self.lp_fee,
            tick_spacing: params.tick_spacing,
            hooks: params.hooks,
            hook_flags: derive_hook_flags(params.hooks),
            protocol_fee: self.protocol_fee,
            sqrt_price_x96: params.sqrt_price_x96,
            liquidity: params.liquidity,
            tick: params.tick,
            seed: self.seed,
        }
    }
}

/// Complete snapshot admission with one frozen map per pool request and block.
pub struct PoolIngress {
    chain: Option<Arc<dyn TickMapRpc>>,
    memo: Mutex<(u64, HashMap<TickMapRequest, StagedTickMap>)>,
    verify_level: VerifyLevel,
    verified: Mutex<HashMap<TickMapPoolIdentity, u64>>,
    verifier: Option<Arc<dyn TickMapSampleVerifier>>,
}

impl PoolIngress {
    #[must_use]
    pub fn new(chain: Option<Arc<dyn TickMapRpc>>) -> Self {
        Self {
            chain,
            memo: Mutex::new((u64::MAX, HashMap::new())),
            verify_level: VerifyLevel::default(),
            verified: Mutex::new(HashMap::new()),
            verifier: None,
        }
    }

    /// Attach the snapshot transport resolved by the driver.
    pub fn set_chain(&mut self, chain: Arc<dyn TickMapRpc>) {
        self.chain = Some(chain);
    }

    /// Set the chain-sample verification policy. Setting [`VerifyLevel::Off`]
    /// emits one loud journal entry: the operator has declared confidence, and
    /// the unconditional integrity checks still run.
    pub fn set_verify_level(&mut self, level: VerifyLevel) {
        if level == VerifyLevel::Off {
            tracing::warn!(
                "pool ingress verify_level=off: chain-sample verification of staged V3 tick maps is disabled by operator declaration; self-contradictory Tracked maps and the two-stamp clock still abort"
            );
        }
        self.verify_level = level;
    }

    /// The active chain-sample policy.
    #[must_use]
    pub fn verify_level(&self) -> VerifyLevel {
        self.verify_level
    }

    /// Attach the chain-sample verifier (the driver wires the provider-backed
    /// adapter after the runtime is built).
    pub fn set_verifier(&mut self, verifier: Arc<dyn TickMapSampleVerifier>) {
        self.verifier = Some(verifier);
    }

    async fn stage_tick_map(
        &self,
        request: TickMapRequest,
        identity: TickMapPoolIdentity,
        block: u64,
    ) -> Result<StagedTickMap, IngressDecline> {
        {
            let mut memo = self.memo.lock();
            if memo.0 != block {
                *memo = (block, HashMap::new());
            }
            if let Some(seed) = memo.1.get(&request) {
                return Ok(seed.clone());
            }
        }
        let chain = self.chain.as_ref().ok_or_else(|| {
            IngressDecline::TickMapFetch("complete tick-map RPC is not configured".into())
        })?;
        let pinned = BlockId::from(block);
        let snapshot = chain
            .get_tick_maps(&[request], pinned)
            .await
            .map_err(|error| IngressDecline::TickMapFetch(error.to_string()))?;
        snapshot
            .validate(&[request], pinned)
            .map_err(|error| IngressDecline::TickMapFetch(error.to_string()))?;
        let ticks = snapshot.pools[0].tick_map(block);
        let mut bitmaps = HashMap::new();
        for &tick in ticks.keys() {
            let compressed = tick.div_euclid(request.tick_spacing);
            let bit = usize::try_from(compressed.rem_euclid(256))
                .map_err(|error| IngressDecline::TickMapFetch(error.to_string()))?;
            bitmaps
                .entry(compressed >> 8)
                .or_insert(U256::ZERO)
                .set_bit(bit, true);
        }
        let mut seed = TickMapSeed::chain(
            ticks,
            bitmaps,
            degenbot_pools::v3_state::PoolTickCoverage::Tracked,
            block,
            identity,
        );
        seed.source_block = Some(block);
        let staged = StagedTickMap {
            seed,
            protocol_fee: snapshot.pools[0].protocol_fee,
            lp_fee: snapshot.pools[0].lp_fee,
        };
        let mut memo = self.memo.lock();
        if memo.0 == block {
            memo.1.insert(request, staged.clone());
        }
        Ok(staged)
    }

    async fn stage_v3_tick_map(
        &self,
        params: IngressV3Params,
        block: u64,
    ) -> Result<TickMapSeed, IngressDecline> {
        self.stage_tick_map(
            TickMapRequest {
                address: params.address,
                protocol: match params.slot_layout {
                    ClSlotLayout::UniswapV3 => TickMapProtocol::UniswapV3,
                    ClSlotLayout::PancakeV3 => TickMapProtocol::PancakeV3,
                },
                tick_spacing: params.tick_spacing,
                pool_id: None,
            },
            TickMapPoolIdentity::V3(params.address),
            block,
        )
        .await
        .map(|staged| staged.seed)
    }

    async fn stage_v4_tick_map(
        &self,
        params: IngressV4Params,
        block: u64,
    ) -> Result<StagedTickMap, IngressDecline> {
        self.stage_tick_map(
            TickMapRequest {
                address: params.manager,
                protocol: TickMapProtocol::UniswapV4,
                tick_spacing: params.tick_spacing,
                pool_id: Some(params.pool_id),
            },
            TickMapPoolIdentity::V4 {
                manager: params.manager,
                pool_id: params.pool_id,
            },
            block,
        )
        .await
    }

    /// Stage, chain-sample-verify (per [`VerifyLevel`]), then register a V3
    /// pool. The verification composes the pump's snapshot-block machinery
    /// through [`TickMapSampleVerifier`]; a mismatch or transport failure is a
    /// loud [`IngressDecline::Verify`], never a silent fallback.
    ///
    /// # Errors
    ///
    /// [`IngressDecline`] naming the refused staging, verification, or
    /// registration step.
    pub async fn admit_v3_verified(
        &self,
        ws: &mut Workspace,
        params: IngressV3Params,
        head: u64,
    ) -> Result<u64, IngressDecline> {
        let seed = self.stage_v3_tick_map(params, head).await?;
        self.verify_staged(
            TickMapPoolIdentity::V3(params.address),
            params.tick_spacing,
            &seed,
        )
        .await?;
        Self::register_v3(ws, params, seed, head)
    }

    /// Sample canonical state before applying pending replay rows, then admit
    /// the journal's post-state scalars and tick overlay.
    ///
    /// # Errors
    ///
    /// Returns the same typed [`IngressDecline`] stages as cold-hop admission.
    pub async fn admit_v3_replay(
        &self,
        ws: &mut Workspace,
        params: IngressV3Params,
        overlay: HashMap<i32, TickInfo>,
        head: u64,
    ) -> Result<u64, IngressDecline> {
        let mut seed = self.stage_v3_tick_map(params, head).await?;
        self.verify_staged(
            TickMapPoolIdentity::V3(params.address),
            params.tick_spacing,
            &seed,
        )
        .await?;
        merge_replay_overlay(&mut seed, overlay, params.tick_spacing, "V3")?;
        Self::register_v3(ws, params, seed, head)
    }

    /// Register a staged V3 seed into `ws`, naming the typed rejection on
    /// refusal.
    fn register_v3(
        ws: &mut Workspace,
        params: IngressV3Params,
        seed: TickMapSeed,
        head: u64,
    ) -> Result<u64, IngressDecline> {
        ws.register_with_state(
            PlanningPoolParams {
                address: params.address,
                token0: params.token0,
                token1: params.token1,
            },
            ExplicitPoolState::V3 {
                sqrt_price_x96: params.sqrt_price_x96,
                liquidity: params.liquidity,
                tick: params.tick,
                fee: params.fee,
                tick_spacing: params.tick_spacing,
                seed,
                slot_layout: params.slot_layout,
            },
            head,
        )
        .map_err(|e| IngressDecline::Register(format!("{e:?}")))
    }

    /// Apply the sampling policy after unconditional complete-map validation.
    async fn verify_staged(
        &self,
        identity: TickMapPoolIdentity,
        tick_spacing: i32,
        seed: &TickMapSeed,
    ) -> Result<(), IngressDecline> {
        if self.verify_level == VerifyLevel::Off {
            return Ok(());
        }
        let Some(verifier) = self.verifier.as_ref() else {
            return Ok(());
        };
        if seed.coverage != degenbot_pools::v3_state::PoolTickCoverage::Tracked {
            return Ok(());
        }
        if self.verify_level == VerifyLevel::Bootstrap
            && self.verified.lock().get(&identity) == Some(&seed.seed_block)
        {
            return Ok(());
        }
        let target = match identity {
            TickMapPoolIdentity::V3(address) => TickMapSampleTarget::V3(address),
            TickMapPoolIdentity::V4 { manager, pool_id } => {
                TickMapSampleTarget::V4 { manager, pool_id }
            }
        };
        let map = RpcLiquidityMap::tracked_with_spacing(
            seed.ticks.clone(),
            seed.bitmaps.clone(),
            tick_spacing,
        );
        verifier
            .verify(target, &map, seed.seed_block)
            .await
            .map_err(IngressDecline::Verify)?;
        self.verified.lock().insert(identity, seed.seed_block);
        Ok(())
    }

    /// Stage, policy-sample, and register explicit V4 state without replay
    /// overlay. This is the cold-hop/explicit-state ingress behavior.
    ///
    /// # Errors
    ///
    /// Returns a typed staging, verification, or registration decline.
    pub async fn admit_v4_verified(
        &self,
        ws: &mut Workspace,
        params: IngressV4Params,
        head: u64,
    ) -> Result<u64, IngressDecline> {
        let staged = self.stage_v4_tick_map(params, head).await?;
        self.verify_staged(
            TickMapPoolIdentity::V4 {
                manager: params.manager,
                pool_id: params.pool_id,
            },
            params.tick_spacing,
            &staged.seed,
        )
        .await?;
        Self::register_v4(ws, params, staged, head)
    }

    /// Sample the canonical V4 seed, then apply pending replay rows before registration.
    ///
    /// # Errors
    ///
    /// Returns the same typed staging, verification, or registration declines
    /// as explicit V4 admission.
    pub async fn admit_v4_replay(
        &self,
        ws: &mut Workspace,
        params: IngressV4Params,
        overlay: HashMap<i32, TickInfo>,
        head: u64,
    ) -> Result<u64, IngressDecline> {
        let mut staged = self.stage_v4_tick_map(params, head).await?;
        self.verify_staged(
            TickMapPoolIdentity::V4 {
                manager: params.manager,
                pool_id: params.pool_id,
            },
            params.tick_spacing,
            &staged.seed,
        )
        .await?;
        merge_replay_overlay(&mut staged.seed, overlay, params.tick_spacing, "V4")?;
        Self::register_v4(ws, params, staged, head)
    }

    fn register_v4(
        ws: &mut Workspace,
        params: IngressV4Params,
        staged: StagedTickMap,
        head: u64,
    ) -> Result<u64, IngressDecline> {
        ws.register_with_state(
            PlanningPoolParams {
                address: params.manager,
                token0: params.token0,
                token1: params.token1,
            },
            staged.into_v4_state(params),
            head,
        )
        .map_err(|error| IngressDecline::Register(format!("{error:?}")))
    }
}

fn merge_replay_overlay(
    seed: &mut TickMapSeed,
    overlay: HashMap<i32, TickInfo>,
    tick_spacing: i32,
    family: &str,
) -> Result<(), IngressDecline> {
    for (tick, info) in overlay {
        if tick_spacing <= 0 || tick % tick_spacing != 0 {
            return Err(IngressDecline::TickMapFetch(format!(
                "unaligned {family} replay tick {tick}"
            )));
        }
        let compressed = tick.div_euclid(tick_spacing);
        let word = compressed >> 8;
        let bit = usize::try_from(compressed.rem_euclid(256)).map_err(|_| {
            IngressDecline::TickMapFetch(format!("tick {tick} has an invalid {family} bitmap bit"))
        })?;
        seed.bitmaps
            .entry(word)
            .or_insert(U256::ZERO)
            .set_bit(bit, info.liquidity_gross != alloy::primitives::U128::ZERO);
        if info.liquidity_gross == alloy::primitives::U128::ZERO {
            seed.ticks.remove(&tick);
        } else {
            seed.ticks.insert(tick, info);
        }
    }
    Ok(())
}

/// The V4 hook-capability flags encoded in the hook address's last two bytes.
#[must_use]
pub fn derive_hook_flags(hook_address: alloy::primitives::Address) -> u16 {
    u16::from_be_bytes([hook_address[18], hook_address[19]])
}

#[cfg(test)]
#[path = "tests/complete_ingress.rs"]
mod complete_ingress;
