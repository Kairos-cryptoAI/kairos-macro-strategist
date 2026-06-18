"""Call the xhigh model and parse a StrategicAllocation."""
from __future__ import annotations

from kairos_core.contracts import StrategicAllocation
from kairos_core.enums import MarketRegime, ReasoningEffort, StrategicTrigger

from .prompts import MACRO_SYSTEM


class MacroStrategist:
    def __init__(self, gateway, *, source: str = "macro-strategist") -> None:
        self.gateway = gateway
        self.source = source

    async def allocate(self, context_json: str, *, trigger: StrategicTrigger) -> StrategicAllocation:
        try:
            res = await self.gateway.complete(system=MACRO_SYSTEM, user=context_json, effort=ReasoningEffort.XHIGH)
            data = res.parsed if isinstance(res.parsed, dict) else {}
            return StrategicAllocation(
                source=self.source,
                regime=MarketRegime(data["regime"]),
                stable_reserve_pct=float(data["stable_reserve_pct"]),
                strategy_weights={k: float(v) for k, v in data.get("strategy_weights", {}).items()},
                max_gross_leverage=float(data.get("max_gross_leverage", 2.0)),
                triggered_by=trigger,
                rationale=str(data.get("rationale", ""))[:400],
            )
        except Exception:
            return self._defensive(trigger)

    def _defensive(self, trigger: StrategicTrigger) -> StrategicAllocation:
        # Safe fallback: mostly stables, delta-neutral only (spec's black-swan posture).
        return StrategicAllocation(
            source=self.source, regime=MarketRegime.CHOP, stable_reserve_pct=0.6,
            strategy_weights={"delta_neutral": 0.4}, max_gross_leverage=1.0,
            triggered_by=trigger, rationale="defensive fallback on invalid/failed model output",
        )
