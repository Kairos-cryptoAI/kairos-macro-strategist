"""Hermetic restart, bounded evidence, identity and fail-closed coverage."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from kairos_core.contracts import AccountSnapshotV2
from kairos_core.enums import StrategicTrigger, SystemMode
from kairos_core.topics import Topics

from kairos_macro.history import load_audit_history, load_prior_allocation, validate_audit_rows

from .test_service import _account, _envelope, _FakeBus, _FakeGateway, _market, _service, _settings

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)


def audit_row(topic, payload):
    return {
        "topic": topic,
        "payload": payload,
        "produced_at": datetime.fromisoformat(payload["produced_at"]),
        **{
            key: payload.get(key)
            for key in ("message_id", "source", "schema_version", "correlation_id", "causation_id")
        },
    }


def account_row(at, *, name=None, reconciled=True):
    account = _account(at).model_copy(update={"account_id": name or "primary", "reconciled": reconciled})
    return audit_row(Topics.ACCOUNT_SNAPSHOT, account.to_payload())


def market_row(at, *, price=100, identity=None):
    market = _market(price, at, message_id=identity or f"market:{at.isoformat()}")
    return audit_row(Topics.MARKET_SNAPSHOT, market.to_payload())


def control_row(at, mode):
    return audit_row(
        Topics.SYSTEM_CONTROL,
        {
            "message_id": f"control:{at.isoformat()}:{mode}",
            "source": "risk",
            "schema_version": "1.0",
            "produced_at": at.isoformat(),
            "mode": mode,
        },
    )


def v2_row(at, *, mode="PAPER", profile="DEV"):
    account = AccountSnapshotV2(
        source="execution",
        account_id="primary",
        trading_mode=mode,
        evedex_profile=profile,
        equity_usd=1000,
        available_balance_usd=1000,
        margin_used_usd=0,
        durable_day_start_equity_usd=1000,
        durable_peak_equity_usd=1000,
        captured_at_ms=int(at.timestamp() * 1000),
        reconciliation_seq=1,
        reconciled=True,
    )
    return audit_row(Topics.ACCOUNT_SNAPSHOT_V2, account.to_payload())


def restore(service, rows):
    service.restore_facts(validate_audit_rows(rows, NOW), NOW)


async def test_restart_restores_exact_account_price_history_without_effects():
    first, _, _ = _service(NOW)
    rows = [account_row(NOW - timedelta(seconds=30 * step)) for step in range(4, -1, -1)]
    rows += [market_row(NOW - timedelta(minutes=step)) for step in range(61, -1, -1)]
    facts = validate_audit_rows(rows, NOW)
    for fact in facts:
        if fact.topic == Topics.ACCOUNT_SNAPSHOT:
            first._ingest_account(_envelope(fact.topic, fact.payload, fact.payload["message_id"]))
        else:
            from kairos_core.contracts import MarketSnapshot

            first._ingest_market(MarketSnapshot.model_validate(fact.payload))
    restarted, gateway, bus = _service(NOW)
    restarted.restore_facts(facts, NOW)
    assert restarted._account_history == first._account_history
    assert restarted._price_history == first._price_history
    assert restarted._latest_account == first._latest_account
    assert restarted._portfolio_context(NOW) == first._portfolio_context(NOW)
    assert restarted._context_readiness_issue(NOW) is None
    assert restarted.history_status["restored_rows"] == len(rows)
    assert not gateway.calls and not bus.published and not bus.operations


@pytest.mark.parametrize("other", ["different-account", "different-version", "different-environment"])
async def test_mixed_account_scope_fails_restart_and_prevents_model(other):
    service, gateway, bus = _service(NOW)
    if other == "different-account":
        rows = [account_row(NOW - timedelta(seconds=1)), account_row(NOW, name="other")]
    elif other == "different-version":
        rows = [account_row(NOW - timedelta(seconds=1)), v2_row(NOW)]
    else:
        rows = [v2_row(NOW - timedelta(seconds=1)), v2_row(NOW, mode="LIVE", profile="PROD")]
    with pytest.raises(ValueError, match="mixed account/environment/version"):
        restore(service, rows)
    assert service.history_status["state"] == "failed"
    with pytest.raises(RuntimeError, match="restoration"):
        await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="blocked")
    assert not gateway.calls and not bus.published


def test_explicit_account_and_version_filter_ignores_other_scope():
    service, _, _ = _service(NOW)
    service.settings = _settings(account_history_account_id="primary", account_history_version="v2")
    restore(service, [account_row(NOW, name="other"), v2_row(NOW)])
    assert isinstance(service._latest_account, AccountSnapshotV2)
    assert len(service._account_history) == 1


async def test_restored_stale_account_remains_stale_without_paid_call():
    service, gateway, _ = _service(NOW)
    restore(service, [account_row(NOW - timedelta(minutes=5)), market_row(NOW)])
    allocation = await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="stale")
    assert "stale" in allocation.rationale
    assert allocation.stable_reserve_pct == 1 and not gateway.calls


def test_gap_and_row_bound_cannot_manufacture_full_window_or_one_hour_shock():
    service, _, _ = _service(NOW)
    rows = [account_row(NOW - timedelta(days=7)), account_row(NOW)]
    rows += [market_row(NOW - timedelta(hours=1)), market_row(NOW, price=80)]
    restore(service, rows)
    assert len(service._account_history) == 1
    assert service._performance_context()["full_window"] is False
    assert service._price_shock(service._latest_markets["BTCUSDT"]) is None
    assert service.history_status["account_gaps"] == 1
    assert service.history_status["market_gaps"] == 1

    service, _, _ = _service(NOW)
    service.settings = _settings(history_sample_limit=2)
    restore(service, [account_row(NOW - timedelta(seconds=step)) for step in range(3, -1, -1)])
    assert len(service._account_history) == 2
    assert service.history_status["sample_evictions"] == 2


def test_reconciliation_failure_resets_history_and_reordered_success_cannot_revive_it():
    service, _, _ = _service(NOW)
    row = account_row(NOW - timedelta(seconds=1))
    row["payload"]["produced_at"] = NOW.isoformat()
    row["produced_at"] = NOW
    restore(
        service,
        [
            account_row(NOW - timedelta(seconds=3)),
            account_row(NOW - timedelta(seconds=2), reconciled=False),
            row,
        ],
    )
    # This capture is newer than failure and is a genuine fresh recovery, not a stale success.
    assert len(service._account_history) == 1
    stale = _account(NOW - timedelta(seconds=4))
    service._ingest_account(_envelope(Topics.ACCOUNT_SNAPSHOT, stale.to_payload(), "reordered"))
    assert service.history_status["account_reorders"] == 1
    assert len(service._account_history) == 1


def test_exact_audit_duplicate_is_idempotent_and_identity_mutation_rejected():
    row = market_row(NOW)
    assert len(validate_audit_rows([row, row], NOW)) == 1
    mutation = {**row, "payload": {**row["payload"], "mid_price": 101}}
    with pytest.raises(ValueError, match="conflicting duplicate"):
        validate_audit_rows([row, mutation], NOW)


@pytest.mark.parametrize("change", ["source", "produced_at", "non_finite", "future"])
def test_invalid_audit_evidence_is_not_repaired(change):
    row = market_row(NOW)
    if change == "source":
        row["source"] = "other"
    elif change == "produced_at":
        row["produced_at"] = NOW - timedelta(seconds=1)
    elif change == "non_finite":
        row["payload"]["mid_price"] = float("nan")
    else:
        row = market_row(NOW + timedelta(seconds=1))
    with pytest.raises(ValueError):
        validate_audit_rows([row], NOW)


@pytest.mark.parametrize("topic", ["market", "account", "control"])
def test_same_timestamp_conflicting_observations_stop_recovery(topic):
    service, _, _ = _service(NOW)
    if topic == "market":
        rows = [market_row(NOW, identity="a"), market_row(NOW, price=90, identity="b")]
    elif topic == "account":
        a = account_row(NOW)
        b = account_row(NOW)
        b["payload"]["message_id"] = b["message_id"] = "different"
        b["payload"]["equity_usd"] = 13000
        rows = [a, b]
    else:
        rows = [control_row(NOW, "NORMAL"), control_row(NOW, "CONFLICT_SAFE")]
    with pytest.raises(ValueError, match="conflicting"):
        restore(service, rows)
    assert not service._history_restored


async def test_restored_control_blocks_call_and_stale_normal_cannot_override_it():
    service, gateway, _ = _service(NOW)
    restore(service, [account_row(NOW), market_row(NOW), control_row(NOW, "CONFLICT_SAFE")])
    await service._process_control(
        _envelope(Topics.SYSTEM_CONTROL, control_row(NOW - timedelta(seconds=1), "NORMAL")["payload"], "old")
    )
    allocation = await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="control-guard")
    assert service.system_mode is SystemMode.CONFLICT_SAFE
    assert not gateway.calls and allocation.stable_reserve_pct == 1


async def test_recovered_allocation_is_republished_byte_identically_without_call():
    service, gateway, bus = _service(NOW)
    allocation = service.strategist.defensive(
        StrategicTrigger.SCHEDULE, message_id="macro:schedule:2026-09-12:00"
    )
    allocation = allocation.model_copy(update={"produced_at": NOW})
    restore(service, [audit_row(Topics.STRATEGIC_ALLOCATION, allocation.to_payload())])
    replayed = await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="schedule:2026-09-12:00")
    assert replayed.to_payload() == allocation.to_payload() == bus.published[0][1]
    assert service._last_schedule_key == "schedule:2026-09-12:00"
    assert service._pending_schedule_key == service._last_schedule_key
    assert not gateway.calls


async def test_removed_strategy_in_historical_allocation_is_not_republished_or_replaced():
    service, gateway, bus = _service(NOW)
    allocation = service.strategist.defensive(StrategicTrigger.SCHEDULE, message_id="macro:old")
    allocation = allocation.model_copy(
        update={"produced_at": NOW, "stable_reserve_pct": 0.6, "strategy_weights": {"delta_neutral": 0.4}}
    )
    restore(service, [audit_row(Topics.STRATEGIC_ALLOCATION, allocation.to_payload())])
    with pytest.raises(ValueError, match="outside the configured allowlist"):
        await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="old")
    assert not gateway.calls and not bus.published


def test_recovery_retains_shock_cooldown_even_when_allocation_cache_evicts():
    service, gateway, _ = _service(NOW)
    service.settings = _settings(replay_cache_size=1)
    rows = [market_row(NOW - timedelta(seconds=1), price=80, identity="crash")]
    for identity in ("macro:shock:crash", "macro:z-other"):
        allocation = service.strategist.defensive(StrategicTrigger.SHOCK_EVENT, message_id=identity)
        rows.append(
            audit_row(
                Topics.STRATEGIC_ALLOCATION, allocation.model_copy(update={"produced_at": NOW}).to_payload()
            )
        )
    restore(service, rows)
    assert service._last_shock_at["BTCUSDT"] == NOW - timedelta(seconds=1)
    assert not gateway.calls


class FakePool:
    def __init__(self, pages):
        self.pages = list(pages)
        self.queries = []
        self.transaction_options = None

    @asynccontextmanager
    async def acquire(self):
        yield self

    @asynccontextmanager
    async def transaction(self, **options):
        self.transaction_options = options
        yield self

    async def fetch(self, query, *args):
        self.queries.append((query, args))
        return self.pages.pop(0)


async def test_loader_has_consistent_readonly_snapshot_bounded_queries_and_exact_scope():
    pool = FakePool([[account_row(NOW)], [market_row(NOW)], [], []])
    settings = _settings(
        account_history_account_id="primary", account_history_version="legacy", history_restore_max_rows=3
    )
    facts = await load_audit_history(pool, settings, NOW)
    assert len(facts) == 2
    assert pool.transaction_options == {"isolation": "repeatable_read", "readonly": True}
    assert pool.queries[0][1][0] == [Topics.ACCOUNT_SNAPSHOT]
    assert pool.queries[0][1][3] == "primary"
    assert [args[-1] for _, args in pool.queries] == [4, 3, 2, 2]
    assert all("LIMIT" in query and "persisted_at" in query for query, _ in pool.queries)


async def test_loader_row_overflow_refuses_silent_partial_restoration():
    pool = FakePool([[account_row(NOW), account_row(NOW - timedelta(seconds=1))]])
    with pytest.raises(ValueError, match="restore row limit"):
        await load_audit_history(pool, _settings(history_restore_max_rows=1), NOW)


async def test_prior_allocation_lookup_resolves_old_trigger_without_cache_or_age_reset():
    service, _, _ = _service(NOW)
    allocation = service.strategist.defensive(StrategicTrigger.SCHEDULE, message_id="macro:old")
    allocation = allocation.model_copy(update={"produced_at": NOW - timedelta(days=30)})
    row = audit_row(Topics.STRATEGIC_ALLOCATION, allocation.to_payload())
    row["payload"] = json.dumps(row["payload"])
    pool = FakePool([[row]])
    fact = await load_prior_allocation(pool, allocation.message_id, NOW)
    assert fact is not None and fact.payload == allocation.to_payload()
    assert pool.queries[0][1] == ("macro:old",)


@pytest.mark.parametrize("overflow", [False, True])
async def test_durable_startup_finishes_recovery_before_scheduler_and_closes_on_failure(
    monkeypatch, overflow
):
    import kairos_macro.service as service_module

    class StubDurableBus(_FakeBus):
        def __init__(self):
            super().__init__()
            pages = (
                [[account_row(NOW), account_row(NOW - timedelta(seconds=1))]]
                if overflow
                else [[], [], [], []]
            )
            self.database = SimpleNamespace(pool=FakePool(pages))

        async def start(self):
            self.operations.append(("start", "", ""))

    monkeypatch.setattr(service_module, "DurableMessageBus", StubDurableBus)
    bus, gateway = StubDurableBus(), _FakeGateway()
    service = service_module.MacroService(
        _settings(history_restore_max_rows=1), gateway=gateway, bus=bus, clock=lambda: NOW
    )
    assert not service._history_restored

    async def finite_scheduler():
        assert service._history_restored
        bus.operations.append(("scheduled", "", ""))

    monkeypatch.setattr(service, "_scheduler", finite_scheduler)
    if overflow:
        with pytest.raises(ValueError, match="restore row limit"):
            await service.run()
        assert service.history_status["state"] == "failed"
        assert [item[0] for item in bus.operations] == ["start"]
    else:
        await service.run()
        assert [item[0] for item in bus.operations] == ["start", "scheduled"]
    assert bus.closed and gateway.closed and not gateway.calls


async def test_default_service_allowlist_blocks_paid_call_with_complete_fresh_context():
    from kairos_macro.config import MacroSettings
    from kairos_macro.service import MacroService

    gateway, bus = _FakeGateway(), _FakeBus()
    service = MacroService(MacroSettings(bus_backend="memory"), gateway=gateway, bus=bus, clock=lambda: NOW)
    restore(service, [account_row(NOW), market_row(NOW)])
    allocation = await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="no-approved-alpha")
    assert service._context_readiness_issue(NOW) is None
    assert allocation.stable_reserve_pct == 1 and allocation.strategy_weights == {} and not gateway.calls
