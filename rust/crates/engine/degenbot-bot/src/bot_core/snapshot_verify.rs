//! Typed failures shared by registration verification and retry handling.

/// A verify-pipeline error — the non-pyo3 analogue of `PyRuntimeError`.
///
/// `py_binding.rs` maps this to typed Python exceptions at the seam
/// (`map_verify_err`):
/// - `Snapshot` → `VerificationMismatchError` (genuine mismatch — fatal)
/// - `Provider` → `VerificationRpcError` (verify-provider construction failure)
/// - `Rpc` → `VerificationRpcError` (per-call RPC transport failure)
/// - `Other` → `PyRuntimeError` (any other fatal classification failure)
///
#[derive(Debug)]
pub enum VerifyError {
    /// A verify-phase mismatch (snapshot block or backfill block). The message
    /// carries the phase + pool label + mismatch detail (mirrors the on-chain
    /// `VerificationMismatch` text the `py_binding` impl formats at the seam).
    Snapshot(String),
    /// A verify-provider construction failure (RPC init / connection error).
    Provider(String),
    /// A per-call RPC transport failure (an `eth_call` / `getTickBitmap` /
    /// `getTickLiquidity` read failed to reach the node). Transient — a
    /// retry/backoff candidate, NOT a genuine on-chain mismatch. Distinct from
    /// `Snapshot` (fatal) so the seam routes it to `VerificationRpcError`
    /// instead of `VerificationMismatchError`.
    Rpc(String),
    /// A verification operation has no required provider configuration.
    NotConfigured(String),
    /// A fatal verify-pipeline failure outside the specific categories above
    /// (a driver failure surfacing through a lifecycle call site). Distinct
    /// from `Rpc`/`Provider` — which are transient — so the retry dance never
    /// re-attempts it.
    Other(String),
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Snapshot(message) => write!(f, "snapshot verify mismatch: {message}"),
            Self::Provider(message) => write!(f, "verify provider failure: {message}"),
            Self::Rpc(message) => write!(f, "verify RPC failure: {message}"),
            Self::NotConfigured(message) => write!(f, "verify not configured: {message}"),
            Self::Other(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for VerifyError {}
