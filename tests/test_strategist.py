import asyncio
from types import SimpleNamespace

from kairos_macro.strategist import MacroStrategist
from kairos_core.enums import MarketRegime, StrategicTrigger


class FakeGateway:
    def __init__(self, parsed): self._p = parsed
    async def complete(self, *, system, user, effort, schema=None):
        return SimpleNamespace(parsed=self._p)


def test_parses_allocation():
    gw = FakeGateway({"regime": "BEAR", "stable_reserve_pct": 0.6,
                      "strategy_weights": {"delta_neutral": 0.4}, "max_gross_leverage": 1.5,
                      "rationale": "risk off"})
    alloc = asyncio.run(MacroStrategist(gw).allocate("{}", trigger=StrategicTrigger.SHOCK_EVENT))
    assert alloc.regime is MarketRegime.BEAR
    assert alloc.stable_reserve_pct == 0.6
    assert alloc.triggered_by is StrategicTrigger.SHOCK_EVENT


def test_invalid_weights_trigger_defensive_fallback():
    # weights sum > 1 -> contract raises -> defensive fallback
    gw = FakeGateway({"regime": "BULL", "stable_reserve_pct": 0.8,
                      "strategy_weights": {"grid": 0.5}, "max_gross_leverage": 3})
    alloc = asyncio.run(MacroStrategist(gw).allocate("{}", trigger=StrategicTrigger.SCHEDULE))
    assert alloc.stable_reserve_pct == 0.6  # fell back
    assert alloc.max_gross_leverage == 1.0
