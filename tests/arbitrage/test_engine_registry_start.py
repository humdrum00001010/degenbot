"""Integration tests for EngineRegistry.start — the two-phase Layer A facade.

`start(...)` runs the pre-pump startup ritual (subscribe → stream snapshots →
backfill → verify config) and stops at `PumpPhase::Backfilled`, BEFORE
`resume()`. This is the consumer-safety invariant: between subscribe and
resume (including stream/backfill/verify) zero result batches are emitted,
so the caller can attach its consumer any time before `resume()` without
risk of unbounded backlog or stale-batch dispatch.
"""

from __future__ import annotations

import pytest

import degenbot.arbitrage.engine_registry as runner
from degenbot._ffi import Bot as CoreBot
from degenbot._ffi import BotIo
from tests.fakes.engine import FakeEngine


def test_start_no_snapshots_calls_subscribe_then_verify_never_resume() -> None:
    """Tracer: with no snapshots, start() subscribes + sets verify config in
    order and never calls resume(). Skip stream/backfill (no snapshots)."""
    fake = FakeEngine(backfill_target=18_000_000)
    registry = runner.EngineRegistry(bot=None, engine=fake)

    backfill_target = registry.start(
        "http://localhost:8545",
        "ws://localhost:8546",
    )

    assert fake.calls == [
        "subscribe",
        "set_verify_rpc_url",
    ]
    assert "resume" not in fake.calls
    assert backfill_target == 18_000_000


def test_start_passes_verify_state_view_when_supplied() -> None:
    """verify_state_view is set on the engine only when supplied."""
    fake = FakeEngine(backfill_target=18_000_000)
    registry = runner.EngineRegistry(bot=None, engine=fake)
    state_view = "0x0000000000000000000000000000000000000abc"

    registry.start(
        "http://localhost:8545",
        "ws://localhost:8546",
        verify_state_view=state_view,
    )

    assert "set_verify_state_view" in fake.calls


def test_start_skips_set_verify_state_view_when_none() -> None:
    """When verify_state_view is None, set_verify_state_view is not called."""
    fake = FakeEngine(backfill_target=18_000_000)
    registry = runner.EngineRegistry(bot=None, engine=fake)

    registry.start("http://localhost:8545", "ws://localhost:8546")

    assert "set_verify_state_view" not in fake.calls


def test_pybot_exposes_verify_methods_after_engine_attach() -> None:
    """T4 (ADR-006 D4): Bot exposes the verify CONFIG + per-pool two-step
    lifecycle entry points once an ArbitrageEngine is constructed against it.
    The whole-batch `verify_liquidity_maps` (and V3/V4 twins) was REMOVED as
    redundant + racy (the per-pool two-step lifecycle is the verify authority),
    so those must NOT be present."""
    from degenbot._ffi import Bot
    from degenbot.arbitrage.engine_registry import ArbitrageEngine

    bot = Bot()
    ArbitrageEngine(py_bot=bot)  # attaches shared PumpState
    for method in (
        "set_verify_rpc_url",
        "set_verify_state_view",
    ):
        assert hasattr(bot, method), f"Bot must expose {method} after engine attach"
    for method in (
        "verify_liquidity_maps",
        "verify_v3_liquidity_maps",
        "verify_v4_liquidity_maps",
    ):
        assert not hasattr(bot, method), f"removed batch verify {method} must not exist"


def test_pybot_exposes_pump_lifecycle_methods_after_engine_attach() -> None:
    """T3 (ADR-006 D4): Bot exposes subscribe/resume as delegating entry
     points once a ArbitrageEngine is constructed against it (which attaches
     the shared PumpState). The Bot is the D4 pump owner; these methods drive
     the SAME PumpState the engine reads.

     `backfill_from_snapshot` is retired — the snapshot→WS gap is
     closed automatically inside the core `BlockPump::resume_from_subscribe`
    . The non-DB path uses the `snapshot_seed_block` setter to record
     `S` so the core auto-backfill picks it up.
    """
    from degenbot._ffi import Bot
    from degenbot.arbitrage.engine_registry import ArbitrageEngine

    bot = Bot()
    # Constructing the engine against the bot attaches the shared PumpState.
    engine = ArbitrageEngine(py_bot=bot)
    for method in ("subscribe", "resume"):
        assert hasattr(bot, method), f"Bot must expose {method} after engine attach"
    # backfill_from_snapshot is retired.
    assert not hasattr(bot, "backfill_from_snapshot"), "Bot::backfill_from_snapshot retired"
    # The engine still exposes subscribe/resume too (reads the same shared state).
    for method in ("subscribe", "resume"):
        assert hasattr(engine, method)
    assert not hasattr(engine, "backfill_from_snapshot"), (
        "ArbitrageEngine::backfill_from_snapshot retired"
    )
    # The non-DB path uses the snapshot_seed_block setter.
    assert hasattr(engine, "snapshot_seed_block")  # getter+setter


def test_start_does_not_store_global_snapshot_horizons() -> None:
    fake = FakeEngine(backfill_target=18_000_000)
    registry = runner.EngineRegistry(bot=None, engine=fake)
    registry.start("http://localhost:8545", "ws://localhost:8546")
    assert not hasattr(registry, "_verify_snapshot_block")
    assert not hasattr(registry, "_verify_backfill_block")


def test_start_delegates_the_ritual_to_one_engine_call() -> None:
    """EngineRegistry.start authors the subscribe -> verify ordering exactly
    once, through the engine's one-call ``start`` (the Rust
    ``EngineDriver::start`` mirror) — not three separate FFI crossings.
    """
    fake = FakeEngine(backfill_target=18_000_000)
    registry = runner.EngineRegistry(bot=None, engine=fake)
    state_view = "0x0000000000000000000000000000000000000abc"

    target = registry.start(
        "http://localhost:8545",
        "ws://localhost:8546",
        verify_state_view=state_view,
    )

    assert fake.start_calls == [
        ("http://localhost:8545", "ws://localhost:8546", state_view),
    ]
    assert target == 18_000_000
    assert "resume" not in fake.calls


def test_start_propagates_driver_session_error_on_double_start() -> None:
    """A re-entry surfaces the shared driver's typed session refusal (the
    engine owns the phase gate) rather than the registry re-authoring the
    three-call sequence.
    """
    fake = FakeEngine(backfill_target=18_000_000)
    registry = runner.EngineRegistry(bot=None, engine=fake)
    registry.start("http://localhost:8545", "ws://localhost:8546")

    with pytest.raises(RuntimeError, match="already subscribed"):
        registry.start("http://localhost:8545", "ws://localhost:8546")


def test_pybot_has_no_reconstruction_bootstrap_surface() -> None:
    for name in (
        "assemble_v3_tick_map",
        "assemble_v4_tick_map",
        "load_snapshot_from_db",
        "close_snapshot_tx",
    ):
        assert not hasattr(CoreBot, name)
    for name in (
        "fetch_tick_bitmap",
        "fetch_tick_data",
        "fetch_v4_tick_bitmap",
        "fetch_v4_tick_data",
    ):
        assert not hasattr(BotIo, name)
