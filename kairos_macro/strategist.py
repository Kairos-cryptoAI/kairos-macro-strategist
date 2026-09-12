"""Strictly validate xhigh model output as a strategic allocation."""

from __future__ import annotations

from typing import Annotated, Any

from kairos_core.contracts import StrategicAllocation
from kairos_core.enums import MarketRegime, StrategicTrigger
from kairos_llm import LLMWorkload
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .config import STRATEGY_ID_PATTERN, validate_strategy_ids
from .prompts import allocation_system_prompt

Weight = Annotated[float, Field(ge=0.0, le=1.0)]


class StrategyWeightOutput(BaseModel):
    """One fixed-shape allocation item accepted by OpenAI Structured Outputs."""

    model_config = ConfigDict(extra="forbid")

    strategy_name: str = Field(min_length=1, max_length=128, pattern=STRATEGY_ID_PATTERN)
    weight: Weight


class AllocationOutput(BaseModel):
    """Provider-neutral Structured Outputs schema for the Macro call."""

    model_config = ConfigDict(extra="forbid")

    regime: MarketRegime
    stable_reserve_pct: float = Field(ge=0.0, le=1.0)
    strategy_weights: tuple[StrategyWeightOutput, ...] = Field(..., max_length=64)
    max_gross_leverage: float = Field(gt=0.0, le=20.0)
    rationale: str = Field(..., max_length=400)

    @model_validator(mode="after")
    def validate_total_allocation(self) -> AllocationOutput:
        total = self.stable_reserve_pct + sum(item.weight for item in self.strategy_weights)
        if not 0.9999 <= total <= 1.0001:
            raise ValueError(f"total allocation must equal 1.0: {total:.4f}")
        names = [item.strategy_name for item in self.strategy_weights]
        if len(names) != len(set(names)):
            raise ValueError("strategy names must be unique")
        return self

    def strategy_weight_map(self, allowed_strategy_ids: tuple[str, ...]) -> dict[str, float]:
        """Convert the provider-safe list into the canonical domain mapping."""

        weights = {item.strategy_name: item.weight for item in self.strategy_weights}
        if set(weights) - set(allowed_strategy_ids):
            raise ValueError("model returned an unconfigured strategy ID")
        return weights


class MacroStrategist:
    def __init__(
        self, gateway: Any, *, source: str = "macro-strategist", allowed_strategy_ids: tuple[str, ...] = ()
    ) -> None:
        self.gateway = gateway
        self.source = source
        self.allowed_strategy_ids = validate_strategy_ids(allowed_strategy_ids)

    async def allocate(
        self,
        context_json: str,
        *,
        trigger: StrategicTrigger,
        message_id: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> StrategicAllocation:
        identity = self._identity_fields(message_id, correlation_id, causation_id)
        if not self.allowed_strategy_ids:
            return self.defensive(trigger, **identity, detail="no configured strategy IDs")
        try:
            result = await self.gateway.complete(
                system=allocation_system_prompt(self.allowed_strategy_ids),
                user=context_json,
                workload=LLMWorkload.MACRO_STRATEGIST,
                schema=AllocationOutput,
            )
            output = (
                result.parsed
                if isinstance(result.parsed, AllocationOutput)
                else AllocationOutput.model_validate(result.parsed)
            )
            return StrategicAllocation(
                **identity,
                source=self.source,
                regime=output.regime,
                stable_reserve_pct=output.stable_reserve_pct,
                strategy_weights=output.strategy_weight_map(self.allowed_strategy_ids),
                max_gross_leverage=output.max_gross_leverage,
                triggered_by=trigger,
                rationale=output.rationale,
            )
        except Exception:
            return self.defensive(
                trigger,
                message_id=message_id,
                correlation_id=correlation_id,
                causation_id=causation_id,
                detail="invalid or failed model output",
            )

    def defensive(
        self,
        trigger: StrategicTrigger,
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        detail: str = "model unavailable",
    ) -> StrategicAllocation:
        """Return the deterministic capital-preservation allocation."""
        return StrategicAllocation(
            **self._identity_fields(message_id, correlation_id, causation_id),
            source=self.source,
            regime=MarketRegime.CHOP,
            stable_reserve_pct=1.0,
            strategy_weights={},
            max_gross_leverage=1.0,
            triggered_by=trigger,
            rationale=f"defensive fallback: {detail}"[:400],
        )

    @staticmethod
    def _identity_fields(
        message_id: str | None,
        correlation_id: str | None,
        causation_id: str | None,
    ) -> dict[str, str]:
        return {
            key: value
            for key, value in {
                "message_id": message_id,
                "correlation_id": correlation_id,
                "causation_id": causation_id,
            }.items()
            if value is not None
        }
