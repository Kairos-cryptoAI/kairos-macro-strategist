"""Network-free runtime tests for Macro Strategist."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from kairos_core.bus import BusEnvelope, MessageBus
from kairos_core.contracts import (
    AccountSnapshot,
    DerivativesMetrics,
    MarketSnapshot,
    OrderBookSummary,
    PositionSnapshot,
    TechnicalIndicators,
)
from kairos_core.enums import Side, StrategicTrigger, SystemMode
from kairos_core.topics import Topics

from kairos_macro.config import MacroSettings
from kairos_macro.service import MacroService
from kairos_macro.strategist import AllocationOutput


class _FakeGateway:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.closed = False

    async def complete(self, *, system, user, effort, schema=None):
        self.calls.append({"system": system, "user": user, "effort": effort, "schema": schema})
        return SimpleNamespace(
            parsed=AllocationOutput(
                regime="BEAR",
                stable_reserve_pct=0.5,
                strategy_weights={"delta_neutral": 0.5},
                max_gross_leverage=1.2,
                rationale="real context",
            )
        )

    async def close(self):
        self.closed = True


class _FakeBus(MessageBus):
    def __init__(self, messages: dict[str, list[BusEnvelope]] | None = None) -> None:
        self.messages = messages or {}
        self.operations: list[tuple[str, str, str]] = []
        self.published: list[tuple[str, dict]] = []
        self.fail_publish_once = False
        self.closed = False

    async def publish(self, topic, message):
        payload = self._to_payload(message)
        self.operations.append(("publish", topic, payload.get("message_id", "")))
        if self.fail_publish_once:
            self.fail_publish_once = False
            raise RuntimeError("publish failed")
        self.published.append((topic, payload))
        return "published-1"

    async def subscribe(
        self,
        topic: str,
        *,
        group: str | None = None,
        consumer: str | None = None,
    ) -> AsyncIterator[BusEnvelope]:
        for envelope in self.messages.get(topic, []):
            yield envelope

    async def ack(self, topic, envelope, *, group=None):
        self.operations.append(("ack", topic, envelope.id))

    async def close(self):
        self.closed = True


def _settings(**changes) -> MacroSettings:
    return MacroSettings(
        bus_backend="memory",
        trading_symbols=["BTCUSDT", "ETHUSDT"],
        crash_pct_1h=10,
        shock_cooldown_s=3600,
        **changes,
    )


def _envelope(topic: str, payload: dict, envelope_id: str) -> BusEnvelope:
    return BusEnvelope(id=envelope_id, topic=topic, payload=payload)


def _account(captured_at: datetime) -> AccountSnapshot:
    return AccountSnapshot(
        message_id="account-1",
        source="execution",
        exchange="evedex",
        account_id="primary",
        equity_usd=12_000,
        available_balance_usd=7_000,
        margin_used_usd=5_000,
        peak_equity_usd=13_000,
        daily_pnl_pct=-1.5,
        realized_pnl_usd=100,
        unrealized_pnl_usd=-250,
        captured_at=captured_at,
        reconciled=True,
        positions=[
            PositionSnapshot(
                source="execution",
                exchange="evedex",
                account_id="primary",
                symbol="BTCUSDT",
                signed_quantity=0.2,
                entry_price=100,
                mark_price=95,
            )
        ],
    )


def _market(
    price: float,
    produced_at: datetime,
    *,
    message_id: str,
    bias: Side = Side.SHORT,
) -> MarketSnapshot:
    return MarketSnapshot(
        message_id=message_id,
        source="quant-scouts",
        symbol="BTCUSDT",
        produced_at=produced_at,
        mid_price=price,
        volume_usd=1_000_000,
        order_book=OrderBookSummary(
            best_bid=price - 0.5,
            best_ask=price + 0.5,
            spread_bps=100,
            imbalance=-0.2,
            depth_usd=500_000,
        ),
        derivatives=DerivativesMetrics(
            funding_rate=-0.0001,
            open_interest=2_000_000,
            oi_change_pct_1h=-5,
            long_liquidations_usd=100_000,
        ),
        indicators=TechnicalIndicators(rsi_14=25, macd=-2, macd_signal=-1, macd_hist=-1),
        quant_bias=bias,
    )


def _service() -> tuple[MacroService, _FakeGateway, _FakeBus]:
    gateway = _FakeGateway()
    bus = _FakeBus()
    return MacroService(_settings(), gateway=gateway, bus=bus), gateway, bus


def test_gateway_health_hook_is_wired_for_production_gateway():
    service = MacroService(MacroSettings(bus_backend="memory"))
    assert service.strategist.gateway._on_health is not None


async def test_run_once_uses_real_account_and_market_context():
    service, gateway, bus = _service()
    now = datetime(2026, 8, 12, tzinfo=UTC)
    service._ingest_account(_envelope(Topics.ACCOUNT_SNAPSHOT, _account(now).to_payload(), "account"))
    service._ingest_market(_market(95, now, message_id="market-1"))

    allocation = await service.run_once(
        StrategicTrigger.SCHEDULE,
        trigger_id="schedule:2026-08-12:00",
        trigger_detail={"kind": "schedule"},
    )

    context = json.loads(gateway.calls[0]["user"])
    assert context["portfolio"]["equity_usd"] == 12_000
    assert context["portfolio"]["positions"][0]["symbol"] == "BTCUSDT"
    assert context["performance"]["sample_count"] == 1
    assert context["performance"]["full_window"] is False
    assert context["macro"]["markets"]["BTCUSDT"]["mid_price"] == 95
    assert gateway.calls[0]["schema"] is AllocationOutput
    assert allocation.message_id == "macro:schedule:2026-08-12:00"
    assert bus.published[0][0] == Topics.STRATEGIC_ALLOCATION


def test_newer_reconciliation_failure_revokes_context_and_old_success_cannot_restore_it():
    service, _, _ = _service()
    now = datetime(2026, 8, 12, tzinfo=UTC)
    current = _account(now)
    failure = _account(now + timedelta(seconds=2)).model_copy(
        update={"reconciled": False, "reconciliation_detail": "positions unavailable"}
    )
    delayed = _account(now + timedelta(seconds=1))

    service._ingest_account(_envelope(Topics.ACCOUNT_SNAPSHOT, current.to_payload(), "current"))
    service._ingest_account(_envelope(Topics.ACCOUNT_SNAPSHOT, failure.to_payload(), "failure"))
    service._ingest_account(_envelope(Topics.ACCOUNT_SNAPSHOT, delayed.to_payload(), "delayed"))

    assert service._latest_account is None
    assert service._latest_account_captured_at == failure.captured_at


def test_regime_hint_requires_a_true_majority():
    service, _, _ = _service()
    now = datetime(2026, 8, 12, tzinfo=UTC)
    long_market = _market(100, now, message_id="btc", bias=Side.LONG)
    flat_eth = _market(100, now, message_id="eth", bias=Side.FLAT).model_copy(update={"symbol": "ETHUSDT"})
    flat_sol = _market(100, now, message_id="sol", bias=Side.FLAT).model_copy(update={"symbol": "SOLUSDT"})
    for snapshot in (long_market, flat_eth, flat_sol):
        service._ingest_market(snapshot)

    assert service._regime_hint() == "CHOP"

    service._ingest_market(
        _market(101, now + timedelta(seconds=1), message_id="eth-long", bias=Side.LONG).model_copy(
            update={"symbol": "ETHUSDT"}
        )
    )
    assert service._regime_hint() == "BULL"


async def test_reconciled_account_recovers_a_schedule_that_ran_without_context():
    now = datetime(2026, 8, 12, tzinfo=UTC)
    account = _account(now)
    envelope = _envelope(Topics.ACCOUNT_SNAPSHOT, account.to_payload(), "account-env")
    bus = _FakeBus({Topics.ACCOUNT_SNAPSHOT: [envelope]})
    gateway = _FakeGateway()
    service = MacroService(_settings(), gateway=gateway, bus=bus)
    service._pending_schedule_key = "schedule:2026-08-12:00"

    await service._consume_accounts()

    assert len(gateway.calls) == 1
    assert bus.published[0][1]["message_id"].startswith("macro:schedule:2026-08-12:00:context:")
    assert service._pending_schedule_key is None
    assert bus.operations[-1] == ("ack", Topics.ACCOUNT_SNAPSHOT, "account-env")


async def test_real_market_snapshots_trigger_shock_allocation():
    service, gateway, bus = _service()
    now = datetime(2026, 8, 12, 12, tzinfo=UTC)
    service._ingest_account(_envelope(Topics.ACCOUNT_SNAPSHOT, _account(now).to_payload(), "account"))
    baseline = _market(100, now - timedelta(minutes=61), message_id="baseline")
    crash = _market(88, now, message_id="crash")
    await service._process_market(_envelope(Topics.MARKET_SNAPSHOT, baseline.to_payload(), "base-env"))

    await service._process_market(_envelope(Topics.MARKET_SNAPSHOT, crash.to_payload(), "crash-env"))

    assert len(gateway.calls) == 1
    assert bus.published[0][1]["triggered_by"] == "shock_event"
    trigger = json.loads(gateway.calls[0]["user"])["trigger"]
    assert trigger["kind"] == "price_crash"
    assert trigger["symbol"] == "BTCUSDT"


async def test_failed_shock_publish_is_unacked_and_reuses_cached_llm_output():
    now = datetime(2026, 8, 12, 12, tzinfo=UTC)
    baseline = _market(100, now - timedelta(minutes=61), message_id="baseline")
    crash = _market(88, now, message_id="crash")
    crash_envelope = _envelope(Topics.MARKET_SNAPSHOT, crash.to_payload(), "crash-env")
    bus = _FakeBus({Topics.MARKET_SNAPSHOT: [crash_envelope]})
    bus.fail_publish_once = True
    gateway = _FakeGateway()
    service = MacroService(_settings(), gateway=gateway, bus=bus)
    service._ingest_account(_envelope(Topics.ACCOUNT_SNAPSHOT, _account(now).to_payload(), "account"))
    await service._process_market(_envelope(Topics.MARKET_SNAPSHOT, baseline.to_payload(), "base-env"))

    await service._consume_markets()
    assert not any(operation[0] == "ack" for operation in bus.operations)
    assert len(gateway.calls) == 1

    await service._consume_markets()
    assert len(gateway.calls) == 1
    assert bus.published[0][1]["message_id"] == "macro:shock:crash"
    assert bus.operations[-1] == ("ack", Topics.MARKET_SNAPSHOT, "crash-env")


async def test_degraded_control_publishes_defensive_allocation_then_acks():
    control = _envelope(
        Topics.SYSTEM_CONTROL,
        {"message_id": "control-1", "mode": "CONFLICT_SAFE"},
        "control-env",
    )
    bus = _FakeBus({Topics.SYSTEM_CONTROL: [control]})
    gateway = _FakeGateway()
    service = MacroService(_settings(), gateway=gateway, bus=bus)

    await service._consume_control()

    assert service.system_mode is SystemMode.CONFLICT_SAFE
    assert gateway.calls == []
    assert bus.published[0][1]["stable_reserve_pct"] == 0.6
    assert [operation[0] for operation in bus.operations] == ["publish", "ack"]


async def test_invalid_control_is_acked_as_poison_message():
    control = _envelope(Topics.SYSTEM_CONTROL, {"mode": "UNKNOWN"}, "control-env")
    bus = _FakeBus({Topics.SYSTEM_CONTROL: [control]})
    service = MacroService(_settings(), gateway=_FakeGateway(), bus=bus)

    await service._consume_control()

    assert bus.operations == [("ack", Topics.SYSTEM_CONTROL, "control-env")]
    assert service.system_mode is SystemMode.NORMAL


async def test_run_closes_gateway_and_bus(monkeypatch):
    service, gateway, bus = _service()

    async def finite_scheduler():
        return None

    monkeypatch.setattr(service, "_scheduler", finite_scheduler)
    await service.run()

    assert gateway.closed is True
    assert bus.closed is True


async def test_close_runs_when_taskgroup_fails(monkeypatch):
    service, gateway, bus = _service()

    async def failed_scheduler():
        raise RuntimeError("scheduler failed")

    monkeypatch.setattr(service, "_scheduler", failed_scheduler)
    with pytest.raises(ExceptionGroup):
        await service.run()

    assert gateway.closed is True
    assert bus.closed is True
