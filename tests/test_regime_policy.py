"""Network-free engineering fixtures for the real opt-in Macro subscriber."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from kairos_core.contracts import AccountSnapshotV2, ExitPlanV1, StrategyIntentV1, StrategyProvenanceV1
from kairos_core.contracts.regime_capability import (
    REGIME_BOUND_ALLOCATION_TOPIC,
    REGIME_OBSERVATION_TOPIC,
    CapabilityRegime,
    RegimeCapabilityPolicyV1,
    RegimeObservationV1,
    StrategyRegimeCapabilityV1,
)
from kairos_core.enums import EvedexProfile, Side, StrategicTrigger, SystemMode, TradingMode
from kairos_core.topics import Topics
from pydantic import ValidationError

from kairos_macro.config import MacroSettings
from kairos_macro.service import MacroService
from tests.test_service import _envelope, _FakeBus, _FakeGateway, _market

SHA_A, SHA_B, SHA_C, SHA_D = (letter * 64 for letter in "abcd")


def _policy() -> RegimeCapabilityPolicyV1:
    return RegimeCapabilityPolicyV1(
        source_set_sha256=SHA_C,
        observation_source="engineering-detector-fixture",
        detector_code_sha256=SHA_D,
        detector_config_sha256=SHA_A,
        capabilities=(
            StrategyRegimeCapabilityV1(
                strategy_id="fixture_macro_strategy_v1",
                strategy_revision="fixture-1",
                strategy_code_sha256=SHA_A,
                config_sha256=SHA_B,
                regime=CapabilityRegime.RANGE,
                sides=(Side.LONG, Side.SHORT),
            ),
        ),
    )


def _settings(tmp_path, **changes) -> MacroSettings:
    policy = _policy()
    artifact = tmp_path / "engineering-regime-policy.json"
    artifact.write_text(policy.model_dump_json(), encoding="utf-8")
    values = dict(
        bus_backend="memory",
        allowed_strategy_ids=("fixture_macro_strategy_v1",),
        account_history_account_id="engineering-paper-dev",
        account_history_version="v2",
        regime_policy_profile="adaptive-research-v1",
        regime_policy_file=artifact,
        regime_policy_sha256=policy.policy_sha256,
        regime_source_set_sha256=SHA_C,
        regime_strategy_revisions=("fixture_macro_strategy_v1@fixture-1",),
        account_snapshot_max_age_s=120,
    )
    values.update(changes)
    return MacroSettings(**values)


def _service(tmp_path):
    now = datetime.now(UTC)
    clock = [now]
    gateway, bus = _FakeGateway(), _FakeBus()
    service = MacroService(_settings(tmp_path), gateway=gateway, bus=bus, clock=lambda: clock[0])
    account = AccountSnapshotV2(
        source="execution",
        trading_mode=TradingMode.PAPER,
        evedex_profile=EvedexProfile.DEV,
        account_id="engineering-paper-dev",
        equity_usd=10_000,
        available_balance_usd=10_000,
        margin_used_usd=0,
        durable_day_start_equity_usd=10_000,
        durable_peak_equity_usd=10_000,
        total_open_risk_usd=0,
        captured_at_ms=int((now - timedelta(milliseconds=500)).timestamp() * 1000),
        reconciliation_seq=1,
        reconciled=True,
    )
    service._ingest_account(_envelope(Topics.ACCOUNT_SNAPSHOT_V2, account.to_payload(), "account"))
    service._ingest_market(_market(100, now, message_id="market:engineering"))
    return service, gateway, bus, clock


def _observation(service, clock, **changes):
    basis = next(iter(service._capital_bases.values()))
    capital_time = int(basis.allocation.produced_at.timestamp() * 1000)
    decision_time = (capital_time // 60_000 + 1) * 60_000 - 1
    intent = StrategyIntentV1(
        source="engineering-strategy-fixture",
        strategy_id="fixture_macro_strategy_v1",
        strategy_revision="fixture-1",
        symbol="BTCUSDT",
        side=Side.LONG,
        decision_ts_ms=decision_time,
        entry_eligible_ts_ms=decision_time + 1,
        entry_expires_ts_ms=decision_time + 120_000,
        reference_price=100,
        signal_strength=0.5,
        gross_reward_bps=500,
        exit_plan=ExitPlanV1(stop_price=95, target_price=105, max_holding_ms=180_000),
        provenance=StrategyProvenanceV1(
            strategy_code_sha256=SHA_A,
            config_sha256=SHA_B,
            input_window_sha256=SHA_C,
            features_sha256=SHA_D,
            input_bar_sha256s=(SHA_A, SHA_B),
        ),
    )
    values = dict(
        source="engineering-detector-fixture",
        source_set_sha256=SHA_C,
        detector_code_sha256=SHA_D,
        detector_config_sha256=SHA_A,
        intent=intent,
        regime=CapabilityRegime.RANGE,
        event_as_of_ms=decision_time,
        observed_at_ms=decision_time + 100,
        expires_at_ms=intent.entry_expires_ts_ms,
    )
    values.update(changes)
    observation = RegimeObservationV1(**values)
    clock[0] = datetime.fromtimestamp((decision_time + 150) / 1000, UTC)
    return observation


def test_default_legacy_profile_has_no_capability_policy_and_explicit_opt_in_is_required(tmp_path):
    assert MacroSettings().load_regime_policy() is None
    with pytest.raises(ValidationError):
        MacroSettings(regime_policy_profile="adaptive-research-v1")
    with pytest.raises(ValidationError):
        MacroSettings(regime_policy_file=tmp_path / "unused.json")
    with pytest.raises(ValueError):
        _settings(tmp_path, regime_policy_sha256=SHA_A).load_regime_policy()
    with pytest.raises(ValueError):
        _settings(
            tmp_path, regime_strategy_revisions=("fixture_macro_strategy_v1@fixture-2",)
        ).load_regime_policy()
    with pytest.raises(ValidationError):
        _settings(tmp_path, regime_strategy_revisions=("foreign_strategy@fixture-1",))


@pytest.mark.asyncio
async def test_real_opted_in_subscriber_publishes_bound_capital_then_ack_without_new_gateway_call(tmp_path):
    service, gateway, bus, clock = _service(tmp_path)
    allocation = await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="engineering-new-capital")
    assert len(gateway.calls) == 1
    observation = _observation(service, clock)
    bus.messages[REGIME_OBSERVATION_TOPIC] = [
        _envelope(REGIME_OBSERVATION_TOPIC, observation.to_payload(), "regime")
    ]
    await service._consume_regime_observations()
    assert len(gateway.calls) == 1
    topic, payload = bus.published[-1]
    assert topic == REGIME_BOUND_ALLOCATION_TOPIC
    assert payload["capital_basis"]["allocation"] == allocation.to_payload()
    assert payload["observation"]["regime"] == "RANGE"
    assert payload["capital_basis"]["allocation"]["regime"] == "BEAR"
    assert bus.operations[-2][0:2] == ("publish", REGIME_BOUND_ALLOCATION_TOPIC)
    assert bus.operations[-1] == ("ack", REGIME_OBSERVATION_TOPIC, "regime")


@pytest.mark.asyncio
async def test_macro_and_onchain_unavailable_stay_honest_but_are_not_capital_permission_signals(tmp_path):
    service, gateway, _, _ = _service(tmp_path)
    await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="engineering-capital")
    context = json.loads(gateway.calls[0]["user"])
    assert context["macro_factors"]["status"] == "unavailable"
    assert context["onchain_factors"]["status"] == "unavailable"
    assert service._capital_bases


@pytest.mark.asyncio
async def test_binding_publish_retry_preserves_exact_payload_and_does_not_recall_model(tmp_path):
    service, gateway, bus, clock = _service(tmp_path)
    await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="engineering-capital")
    observation = _observation(service, clock)
    envelope = _envelope(REGIME_OBSERVATION_TOPIC, observation.to_payload(), "regime")
    bus.fail_publish_once = True
    with pytest.raises(RuntimeError, match="publish failed"):
        await service.handle_regime_observation(envelope)
    expected = service._bound_allocations[observation.observation_id].model_dump(mode="json")
    clock[0] += timedelta(milliseconds=1)
    actual = await service.handle_regime_observation(envelope)
    assert actual.to_payload() == expected
    assert len(gateway.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"source": "foreign-detector"},
        {"source_set_sha256": SHA_A},
        {"detector_code_sha256": SHA_A},
        {"detector_config_sha256": SHA_B},
        {"regime": CapabilityRegime.UNCERTAIN},
    ],
)
async def test_unknown_detector_mapping_and_source_drift_do_not_publish(tmp_path, change):
    service, gateway, bus, clock = _service(tmp_path)
    await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="engineering-capital")
    observation = _observation(service, clock, **change)
    with pytest.raises(ValueError):
        await service.handle_regime_observation(
            _envelope(REGIME_OBSERVATION_TOPIC, observation.to_payload(), "bad")
        )
    assert len(gateway.calls) == 1
    assert all(topic != REGIME_BOUND_ALLOCATION_TOPIC for topic, _ in bus.published)


@pytest.mark.asyncio
async def test_nested_legacy_capital_mutation_fails_before_first_binding(tmp_path):
    service, _, bus, clock = _service(tmp_path)
    allocation = await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="engineering-capital")
    observation = _observation(service, clock)
    allocation.strategy_weights["fixture_macro_strategy_v1"] = 0.4
    with pytest.raises(ValueError, match="capital basis identity"):
        await service.handle_regime_observation(
            _envelope(REGIME_OBSERVATION_TOPIC, observation.to_payload(), "regime")
        )
    assert all(topic != REGIME_BOUND_ALLOCATION_TOPIC for topic, _ in bus.published)


@pytest.mark.asyncio
async def test_historical_unscoped_cache_cannot_be_adopted_as_opted_in_capital(tmp_path):
    service, gateway, _, clock = _service(tmp_path)
    allocation = await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="engineering-capital")
    observation = _observation(service, clock)
    service._capital_bases.clear()
    cached = await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="engineering-capital")
    assert cached is allocation and not service._capital_bases
    with pytest.raises(ValueError, match="no causal capital"):
        await service.handle_regime_observation(
            _envelope(REGIME_OBSERVATION_TOPIC, observation.to_payload(), "regime")
        )
    assert len(gateway.calls) == 1


@pytest.mark.asyncio
async def test_future_or_expired_evidence_and_revoked_readiness_fail_at_trusted_receipt(tmp_path):
    service, _, bus, clock = _service(tmp_path)
    await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="engineering-capital")
    observation = _observation(service, clock)
    future = observation.model_dump(mode="json")
    future["produced_at"] = (clock[0] + timedelta(milliseconds=1)).isoformat()
    with pytest.raises(ValueError, match="trusted local receipt"):
        await service.handle_regime_observation(_envelope(REGIME_OBSERVATION_TOPIC, future, "future"))
    clock[0] = datetime.fromtimestamp((observation.expires_at_ms + 1) / 1000, UTC)
    with pytest.raises(ValueError, match="trusted local receipt"):
        await service.handle_regime_observation(
            _envelope(REGIME_OBSERVATION_TOPIC, observation.to_payload(), "expired")
        )
    clock[0] = datetime.fromtimestamp((observation.observed_at_ms + 100) / 1000, UTC)
    service.system_mode = SystemMode.CONFLICT_SAFE
    with pytest.raises(ValueError, match="readiness"):
        await service.handle_regime_observation(
            _envelope(REGIME_OBSERVATION_TOPIC, observation.to_payload(), "safe")
        )
    assert all(topic != REGIME_BOUND_ALLOCATION_TOPIC for topic, _ in bus.published)


@pytest.mark.asyncio
async def test_wrong_topic_cannot_be_consumed_as_deterministic_observation(tmp_path):
    service, _, bus, clock = _service(tmp_path)
    await service.run_once(StrategicTrigger.SCHEDULE, trigger_id="engineering-capital")
    observation = _observation(service, clock)
    with pytest.raises(ValueError, match="wrong versioned topic"):
        await service.handle_regime_observation(
            _envelope(Topics.STRATEGIC_ALLOCATION, observation.to_payload(), "wrong")
        )
    assert all(topic != REGIME_BOUND_ALLOCATION_TOPIC for topic, _ in bus.published)
