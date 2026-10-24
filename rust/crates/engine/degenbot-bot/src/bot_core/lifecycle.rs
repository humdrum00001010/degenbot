//! Bot pump lifecycle, verification configuration, and block streams.

use crate::arb_engine::{DriverError, EngineDriver};
use crate::bot_core::bot::Bot;

/// Error from [`Bot::block_stream`].
#[derive(Debug)]
pub enum BlockStreamError {
    /// No engine was constructed against the bot (the shell's "no pump
    /// state" `RuntimeError`).
    NoPumpState,
    /// The receiver was already handed out (the shell's "can only be called
    /// once" `RuntimeError`).
    AlreadyTaken,
}

impl Bot {
    /// Subscribe to the WS `newHeads` + logs streams — the `block_on` wrapper
    /// over [`EngineDriver::subscribe`] the `PyO3` shells (`PyBot`,
    /// `PyArbEngine`) drive detached from the GIL. Blocks (sync, via the
    /// shared tokio runtime) until the first block is observed, then returns
    /// the first WS block number (the backfill target).
    ///
    /// # Errors
    /// [`DriverError`] if the pump is already started/subscribed, the phase
    /// is wrong, or the WS subscribe fails.
    pub fn subscribe(driver: &EngineDriver, rpc_url: &str) -> Result<u64, DriverError> {
        degenbot_core::runtime::get_runtime().block_on(driver.subscribe(rpc_url))
    }

    /// Resume the pump — begin normal WS processing, including the driver's
    /// synchronous `S+1..W` auto-backfill. The `block_on` wrapper over
    /// [`EngineDriver::resume`].
    ///
    /// # Errors
    /// [`DriverError`] if the phase is wrong, subscribe wasn't called, the
    /// driver is stopped, or it was already resumed.
    pub fn resume(driver: &EngineDriver) -> Result<(), DriverError> {
        degenbot_core::runtime::get_runtime().block_on(driver.resume())
    }

    /// Stop the pump — the driver's any-phase, idempotent stop.
    ///
    /// # Errors
    /// Currently always `Ok`; the typed result keeps the surface symmetric.
    pub fn stop(driver: &EngineDriver) -> Result<(), DriverError> {
        driver.stop()
    }

    /// Set the HTTP RPC URL used for verification. `None` (no engine
    /// constructed against the bot) is a no-op — the guard the `PyO3` shell
    /// held as a swallowed `pump_state` error.
    pub fn set_verify_rpc_url(driver: Option<&EngineDriver>, rpc_url: &str) {
        if let Some(pump) = driver {
            pump.set_verify_rpc_url(rpc_url);
        }
    }

    /// Set the `StateView` contract address for V4 verification. `None` (no
    /// engine constructed against the bot) is a no-op.
    pub fn set_verify_state_view(driver: Option<&EngineDriver>, state_view_address: &str) {
        if let Some(pump) = driver {
            pump.set_verify_state_view(state_view_address);
        }
    }

    /// Take the block-clock receiver for the block stream — the once-only
    /// hand-off from the attached engine's block source channel. `None`
    /// (no engine constructed against the bot) is
    /// [`BlockStreamError::NoPumpState`]; a second take is
    /// [`BlockStreamError::AlreadyTaken`] (the receiver is moved into the
    /// iterator at the shell).
    ///
    /// # Errors
    /// [`BlockStreamError`] as above.
    pub fn block_stream(
        driver: Option<&EngineDriver>,
    ) -> Result<
        degenbot_eventhub::NamedReceiver<crate::bot_core::BlockNotification>,
        BlockStreamError,
    > {
        let pump = driver.ok_or(BlockStreamError::NoPumpState)?;
        pump.take_block_receiver()
            .ok_or(BlockStreamError::AlreadyTaken)
    }
}

#[expect(clippy::expect_used)]
#[cfg(test)]
mod tests {
    use crate::bot_core::bot::Bot;

    /// The block-clock receiver is handed out exactly once: a missing driver
    /// is the typed `NoPumpState` (the shell's "no pump state" `RuntimeError`)
    /// and a second take is `AlreadyTaken` ("can only be called once").
    #[test]
    fn block_stream_hands_the_receiver_out_exactly_once() {
        let bot = std::sync::Arc::new(Bot::new(8453));
        let driver =
            crate::arb_engine::EngineDriver::new(bot, degenbot_config::holder::config_arc());

        assert!(matches!(
            Bot::block_stream(None),
            Err(super::BlockStreamError::NoPumpState)
        ));

        let rx = Bot::block_stream(Some(&driver)).expect("first hand-off");
        drop(rx);
        assert!(matches!(
            Bot::block_stream(Some(&driver)),
            Err(super::BlockStreamError::AlreadyTaken)
        ));
    }

    /// The verify-config setters no-op without an attached engine (the shell's
    /// swallowed `pump_state` error), and accept one without side effects the
    /// shell could observe.
    #[test]
    fn set_verify_config_noops_without_an_attached_driver() {
        Bot::set_verify_rpc_url(None, "http://127.0.0.1:1");
        Bot::set_verify_state_view(None, "0x0000000000000000000000000000000000000001");

        let bot = std::sync::Arc::new(Bot::new(8453));
        let driver =
            crate::arb_engine::EngineDriver::new(bot, degenbot_config::holder::config_arc());
        Bot::set_verify_rpc_url(Some(&driver), "http://127.0.0.1:1");
        Bot::set_verify_state_view(Some(&driver), "0x0000000000000000000000000000000000000001");
    }

    /// `Bot::stop` drives the driver's any-phase idempotent stop (the shell
    /// keeps only the error map).
    #[test]
    fn stop_drives_the_driver_stop() {
        let bot = std::sync::Arc::new(Bot::new(8453));
        let driver =
            crate::arb_engine::EngineDriver::new(bot, degenbot_config::holder::config_arc());
        Bot::stop(&driver).expect("any-phase stop is Ok");
        Bot::stop(&driver).expect("stop is idempotent");
    }
}
