from types import SimpleNamespace

from kairos_core.enums import MarketRegime, StrategicTrigger
from kairos_llm import LLMWorkload

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
            "strategy_weights": {"delta_neutral": 0.4},
            "max_gross_leverage": 1.5,
            "rationale": "risk off",
        }
    )
    allocation = await MacroStrategist(gateway).allocate(
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
            "strategy_weights": {"grid": 0.5},
            "max_gross_leverage": 3,
        }
    )

    allocation = await MacroStrategist(gateway).allocate(
        "{}", trigger=StrategicTrigger.SCHEDULE, message_id="macro:schedule-1"
    )

    assert allocation.message_id == "macro:schedule-1"
    assert allocation.stable_reserve_pct == 0.6
    assert allocation.max_gross_leverage == 1.0


async def test_extra_model_fields_are_rejected():
    gateway = FakeGateway(
        {
            "regime": "CHOP",
            "stable_reserve_pct": 0.5,
            "strategy_weights": {"delta_neutral": 0.5},
            "max_gross_leverage": 1,
            "rationale": "safe",
            "unexpected": "not allowed",
        }
    )

    allocation = await MacroStrategist(gateway).allocate("{}", trigger=StrategicTrigger.SCHEDULE)

    assert allocation.rationale.startswith("defensive fallback")
