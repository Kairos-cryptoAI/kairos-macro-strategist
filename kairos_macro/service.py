"""Macro service: schedule + shock triggers -> xhigh allocation -> bus."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from kairos_core.bus import build_bus
from kairos_core.enums import StrategicTrigger
from kairos_core.contracts import LLMHealthEvent
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics

from .config import MacroSettings
from .context import build_macro_context
from .strategist import MacroStrategist
from .triggers import ShockDetector

log = get_logger("macro")


class MacroService:
    def __init__(self, settings: MacroSettings | None = None, *, gateway=None) -> None:
        self.settings = settings or MacroSettings()
        self.bus = build_bus(self.settings)
        self.detector = ShockDetector(self.settings.crash_pct_1h)
        if gateway is None:
            from kairos_llm import LLMGateway
            gateway = LLMGateway(on_health=self._publish_health)
        self.strategist = MacroStrategist(gateway, source=self.settings.service_name)

    async def _publish_health(self, model: str, provider: str, ok: bool, kind: str, latency_s: float) -> None:
        await self.bus.publish(Topics.LLM_HEALTH, LLMHealthEvent(
            source=self.settings.service_name, provider=provider, model=model,
            ok=ok, kind=kind, latency_s=latency_s))

    async def run_once(self, trigger: StrategicTrigger) -> None:
        # In production these come from TimescaleDB + macro/on-chain feeds.
        ctx = build_macro_context(
            portfolio={"equity_usd": 10_000, "stable_pct": 0.3, "positions": []},
            week_pnl_pct=0.0, regime_hint="unknown", macro={}, onchain={},
        )
        alloc = await self.strategist.allocate(ctx, trigger=trigger)
        await self.bus.publish(Topics.STRATEGIC_ALLOCATION, alloc)
        log.info("macro.allocation", regime=alloc.regime.value, stable=alloc.stable_reserve_pct,
                max_lev=alloc.max_gross_leverage, trigger=trigger.value)

    async def _scheduler(self) -> None:  # pragma: no cover - timing
        while True:
            now = datetime.now(timezone.utc)
            if now.hour == self.settings.run_cron_hour_utc and now.minute == 0:
                await self.run_once(StrategicTrigger.SCHEDULE)
                await asyncio.sleep(61)
            await asyncio.sleep(30)

    async def run(self) -> None:  # pragma: no cover - network
        configure_logging(self.settings.log_level, json_logs=self.settings.log_json, service=self.settings.service_name)
        log.info("macro.start")
        await self._scheduler()


def main() -> None:  # pragma: no cover
    asyncio.run(MacroService().run())


if __name__ == "__main__":
    main()
