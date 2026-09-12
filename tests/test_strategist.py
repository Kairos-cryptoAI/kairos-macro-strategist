from types import SimpleNamespace

import pytest
from kairos_core.enums import MarketRegime, StrategicTrigger
from kairos_llm import LLMWorkload

from kairos_macro.config import MacroSettings
from kairos_macro.strategist import AllocationOutput, MacroStrategist


class FakeGateway:
    def __init__(self, parsed):
        self.parsed = parsed
        self.schema = None

    async def complete(self, *, system, user, workload, schema=None):
        self.workload = workload
        self.schema = schema
        return SimpleNamespace(parsed=self.parsed)


async def test_parses_allocation_through_strict_schema():
    gateway = FakeGateway(
        {
            "regime": "BEAR",
            "stable_reserve_pct": 0.6,
            "strategy_weights": [{"strategy_name": "fixture_macro_strategy_v1", "weight": 0.4}],
            "max_gross_leverage": 1.5,
            "rationale": "risk off",
        }
    )
    allocation = await MacroStrategist(gateway, allowed_strategy_ids=("fixture_macro_strategy_v1",)).allocate(
        "{}",
        trigger=StrategicTrigger.SHOCK_EVENT,
        message_id="macro:shock-1",
        correlation_id="trace-1",
        causation_id="snapshot-1",
    )

    assert gateway.schema is AllocationOutput
    assert gateway.workload is LLMWorkload.MACRO_STRATEGIST
    assert allocation.regime is MarketRegime.BEAR
    assert allocation.stable_reserve_pct == 0.6
    assert allocation.triggered_by is StrategicTrigger.SHOCK_EVENT
    assert allocation.message_id == "macro:shock-1"
    assert allocation.causation_id == "snapshot-1"


async def test_invalid_weights_trigger_defensive_fallback():
    gateway = FakeGateway(
        {
            "regime": "BULL",
            "stable_reserve_pct": 0.8,
            "strategy_weights": [{"strategy_name": "grid", "weight": 0.5}],
            "max_gross_leverage": 3,
        }
    )

    allocation = await MacroStrategist(gateway, allowed_strategy_ids=("fixture_macro_strategy_v1",)).allocate(
        "{}", trigger=StrategicTrigger.SCHEDULE, message_id="macro:schedule-1"
    )

    assert allocation.message_id == "macro:schedule-1"
    assert allocation.stable_reserve_pct == 1.0
    assert allocation.max_gross_leverage == 1.0


async def test_extra_model_fields_are_rejected():
    gateway = FakeGateway(
        {
            "regime": "CHOP",
            "stable_reserve_pct": 0.5,
            "strategy_weights": [{"strategy_name": "delta_neutral", "weight": 0.5}],
            "max_gross_leverage": 1,
            "rationale": "safe",
            "unexpected": "not allowed",
        }
    )

    allocation = await MacroStrategist(gateway, allowed_strategy_ids=("fixture_macro_strategy_v1",)).allocate(
        "{}", trigger=StrategicTrigger.SCHEDULE
    )

    assert allocation.rationale.startswith("defensive fallback")


async def test_unallocated_capital_triggers_defensive_fallback():
    gateway = FakeGateway(
        {
            "regime": "CHOP",
            "stable_reserve_pct": 0.5,
            "strategy_weights": [{"strategy_name": "delta_neutral", "weight": 0.2}],
            "max_gross_leverage": 1,
            "rationale": "leave the rest unspecified",
        }
    )

    allocation = await MacroStrategist(gateway, allowed_strategy_ids=("fixture_macro_strategy_v1",)).allocate(
        "{}", trigger=StrategicTrigger.SCHEDULE
    )

    assert allocation.stable_reserve_pct == 1.0
    assert allocation.strategy_weights == {}
    assert allocation.rationale.startswith("defensive fallback")


def test_provider_schema_has_fixed_strategy_weight_items() -> None:
    schema = AllocationOutput.model_json_schema()
    weights = schema["properties"]["strategy_weights"]

    assert weights["type"] == "array"
    assert weights["items"]["$ref"].endswith("/$defs/StrategyWeightOutput")
    item = schema["$defs"]["StrategyWeightOutput"]
    assert item["additionalProperties"] is False
    assert set(item["required"]) == {"strategy_name", "weight"}


async def test_empty_allowlist_blocks_model_and_preserves_all_capital():
    gateway = FakeGateway({})
    allocation = await MacroStrategist(gateway).allocate("{}", trigger=StrategicTrigger.SCHEDULE)
    assert gateway.schema is None
    assert allocation.stable_reserve_pct == 1.0
    assert allocation.strategy_weights == {}


@pytest.mark.parametrize(
    "returned_id",
    [
        "delta_neutral",
        "fixture_macro_strategy",
        "Fixture_macro_strategy_v1",
        "fixture_macro_strategy_v1:revision",
    ],
)
@pytest.mark.parametrize("weight", [0.0, 0.4])
async def test_unknown_ids_and_aliases_fail_closed_even_at_zero_weight(returned_id, weight):
    gateway = FakeGateway(
        {
            "regime": "BULL",
            "stable_reserve_pct": 1 - weight,
            "strategy_weights": [{"strategy_name": returned_id, "weight": weight}],
            "max_gross_leverage": 1,
            "rationale": "invented strategy",
        }
    )
    allocation = await MacroStrategist(gateway, allowed_strategy_ids=("fixture_macro_strategy_v1",)).allocate(
        "{}", trigger=StrategicTrigger.SCHEDULE
    )
    assert gateway.schema is AllocationOutput
    assert allocation.stable_reserve_pct == 1.0
    assert allocation.strategy_weights == {}


@pytest.mark.parametrize(
    "ids", [(" x",), ("x ",), ("x", "x"), ("technical-canary",), ("technical-canary:session",)]
)
def test_allowlist_is_exact_and_never_includes_technical_canary(ids):
    with pytest.raises(ValueError):
        MacroSettings(allowed_strategy_ids=ids)
    with pytest.raises(ValueError):
        MacroStrategist(FakeGateway({}), allowed_strategy_ids=ids)
