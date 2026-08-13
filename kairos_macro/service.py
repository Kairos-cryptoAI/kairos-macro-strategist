"""Macro service driven by real account, market, schedule, and control inputs."""

from __future__ import annotations

import asyncio
import inspect
from collections import OrderedDict, defaultdict, deque
from datetime import UTC, datetime, timedelta
from typing import Any

from kairos_core.bus import BusEnvelope, MessageBus, build_bus
from kairos_core.contracts import AccountSnapshot, LLMHealthEvent, MarketSnapshot, StrategicAllocation
from kairos_core.enums import Side, StrategicTrigger, SystemMode
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics

from .config import MacroSettings
from .context import build_macro_context
from .strategist import MacroStrategist
from .triggers import ShockDetector, ShockEvent

log = get_logger("macro")


class MacroService:
    def __init__(
        self,
        settings: MacroSettings | None = None,
        *,
        gateway: Any | None = None,
        bus: MessageBus | None = None,
    ) -> None:
        self.settings = settings or MacroSettings()
        self.bus = bus if bus is not None else build_bus(self.settings)
        self.detector = ShockDetector(self.settings.crash_pct_1h)
        if gateway is None:
            from kairos_llm import LLMGateway

            gateway = LLMGateway(on_health=self._publish_health)
        self.strategist = MacroStrategist(gateway, source=self.settings.service_name)

        self.system_mode = SystemMode.NORMAL
        self._latest_account: AccountSnapshot | None = None
        self._latest_account_captured_at: datetime | None = None
        self._account_history: deque[tuple[datetime, float]] = deque()
        self._latest_markets: dict[str, MarketSnapshot] = {}
        self._price_history: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)
        self._last_shock_at: dict[str, datetime] = {}
        self._allocation_cache: OrderedDict[str, StrategicAllocation] = OrderedDict()
        self._handled_market_ids: OrderedDict[str, None] = OrderedDict()
        self._handled_control_ids: OrderedDict[str, None] = OrderedDict()
        self._ingested_market_ids: OrderedDict[str, None] = OrderedDict()
        self._allocation_lock = asyncio.Lock()
        self._last_schedule_key: str | None = None
        self._pending_schedule_key: str | None = None

    async def _publish_health(
        self,
        model: str,
        provider: str,
        ok: bool,
        kind: str,
        latency_s: float,
    ) -> None:
        await self.bus.publish(
            Topics.LLM_HEALTH,
            LLMHealthEvent(
                source=self.settings.service_name,
                provider=provider,
                model=model,
                ok=ok,
                kind=kind,
                latency_s=latency_s,
            ),
        )

    def _remember(self, cache: OrderedDict[str, Any], key: str, value: Any = None) -> None:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > self.settings.replay_cache_size:
            cache.popitem(last=False)

    def _portfolio_context(self) -> dict[str, Any]:
        account = self._latest_account
        if account is None:
            raise RuntimeError("reconciled account context is unavailable")
        return {
            "exchange": account.exchange,
            "account_id": account.account_id,
            "equity_usd": account.equity_usd,
            "available_balance_usd": account.available_balance_usd,
            "liquid_pct": round(min(1.0, account.available_balance_usd / account.equity_usd), 6),
            "margin_used_usd": account.margin_used_usd,
            "peak_equity_usd": account.peak_equity_usd,
            "daily_pnl_pct": account.daily_pnl_pct,
            "realized_pnl_usd": account.realized_pnl_usd,
            "unrealized_pnl_usd": account.unrealized_pnl_usd,
            "captured_at": account.captured_at.isoformat(),
            "positions": [
                position.model_dump(
                    mode="json",
                    include={
                        "symbol",
                        "signed_quantity",
                        "entry_price",
                        "mark_price",
                        "leverage",
                        "liquidation_price",
                        "unrealized_pnl_usd",
                        "protective_stop_order_id",
                    },
                )
                for position in account.positions
            ],
        }

    def _performance_context(self) -> dict[str, Any]:
        account = self._latest_account
        if account is None:
            raise RuntimeError("reconciled account context is unavailable")
        first_at, baseline = self._account_history[0]
        observed_window_s = max(0.0, (account.captured_at - first_at).total_seconds())
        observed_pnl_pct = ((account.equity_usd / baseline) - 1.0) * 100.0 if baseline > 0 else 0.0
        return {
            "observed_pnl_pct": round(observed_pnl_pct, 2),
            "observed_window_s": observed_window_s,
            "target_window_s": self.settings.account_history_window_s,
            "full_window": observed_window_s >= self.settings.account_history_window_s * 0.99,
            "sample_count": len(self._account_history),
            "daily_pnl_pct": account.daily_pnl_pct,
        }

    def _regime_hint(self) -> str:
        biases = [snapshot.quant_bias for snapshot in self._latest_markets.values()]
        long_count = biases.count(Side.LONG)
        short_count = biases.count(Side.SHORT)
        majority = len(biases) // 2 + 1
        if long_count >= majority:
            return "BULL"
        if short_count >= majority:
            return "BEAR"
        return "CHOP"

    def _market_context(self) -> dict[str, Any]:
        return {
            "markets": {
                symbol: {
                    "mid_price": snapshot.mid_price,
                    "volume_usd": snapshot.volume_usd,
                    "funding_rate": snapshot.derivatives.funding_rate,
                    "open_interest": snapshot.derivatives.open_interest,
                    "oi_change_pct_1h": snapshot.derivatives.oi_change_pct_1h,
                    "long_liquidations_usd": snapshot.derivatives.long_liquidations_usd,
                    "short_liquidations_usd": snapshot.derivatives.short_liquidations_usd,
                    "rsi_14": snapshot.indicators.rsi_14,
                    "quant_bias": snapshot.quant_bias.value,
                    "produced_at": snapshot.produced_at.isoformat(),
                }
                for symbol, snapshot in sorted(self._latest_markets.items())
            }
        }

    def _context(self, trigger_detail: dict[str, Any]) -> str:
        return build_macro_context(
            portfolio=self._portfolio_context(),
            performance=self._performance_context(),
            regime_hint=self._regime_hint(),
            macro=self._market_context(),
            onchain={"status": "not_available_on_current_bus"},
            trigger=trigger_detail,
        )

    async def run_once(
        self,
        trigger: StrategicTrigger,
        *,
        trigger_id: str,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        trigger_detail: dict[str, Any] | None = None,
    ) -> StrategicAllocation:
        """Create and publish one replay-stable allocation for ``trigger_id``."""
        async with self._allocation_lock:
            allocation = self._allocation_cache.get(trigger_id)
            if allocation is None:
                message_id = f"macro:{trigger_id}"
                correlation_id = correlation_id or trigger_id
                detail = trigger_detail or {"kind": trigger.value}
                if self.system_mode in {SystemMode.CONFLICT_SAFE, SystemMode.LOCAL_QUANT_MODE}:
                    allocation = self.strategist.defensive(
                        trigger,
                        message_id=message_id,
                        correlation_id=correlation_id,
                        causation_id=causation_id,
                        detail=f"system mode {self.system_mode.value}",
                    )
                elif self._latest_account is None:
                    allocation = self.strategist.defensive(
                        trigger,
                        message_id=message_id,
                        correlation_id=correlation_id,
                        causation_id=causation_id,
                        detail="missing reconciled account context",
                    )
                else:
                    allocation = await self.strategist.allocate(
                        self._context(detail),
                        trigger=trigger,
                        message_id=message_id,
                        correlation_id=correlation_id,
                        causation_id=causation_id,
                    )
                self._remember(self._allocation_cache, trigger_id, allocation)

            await self.bus.publish(Topics.STRATEGIC_ALLOCATION, allocation)
            log.info(
                "macro.allocation",
                regime=allocation.regime.value,
                stable=allocation.stable_reserve_pct,
                max_lev=allocation.max_gross_leverage,
                trigger=trigger.value,
                trigger_id=trigger_id,
            )
            return allocation

    def _ingest_account(self, envelope: BusEnvelope) -> None:
        account = AccountSnapshot.model_validate(envelope.payload)
        if account.captured_at.utcoffset() is None:
            raise ValueError("account snapshot captured_at must be timezone-aware")
        latest_at = self._latest_account_captured_at
        if latest_at is not None and account.captured_at < latest_at:
            return
        if latest_at is not None and account.captured_at == latest_at:
            if not account.reconciled:
                self._latest_account = None
            return

        self._latest_account_captured_at = account.captured_at
        if not account.reconciled:
            self._latest_account = None
            log.warning("macro.account_unreconciled", account_id=account.account_id)
            return

        self._latest_account = account
        self._account_history.append((account.captured_at, account.equity_usd))
        cutoff = account.captured_at - timedelta(seconds=self.settings.account_history_window_s)
        while self._account_history and self._account_history[0][0] < cutoff:
            self._account_history.popleft()

    async def _consume_accounts(self) -> None:
        async for envelope in self.bus.subscribe(Topics.ACCOUNT_SNAPSHOT, group="macro", consumer="accounts"):
            try:
                self._ingest_account(envelope)
                await self._recover_pending_schedule()
                await self.bus.ack(Topics.ACCOUNT_SNAPSHOT, envelope, group="macro")
            except Exception:
                log.exception("macro.account_processing_failed", envelope_id=envelope.id)

    async def _recover_pending_schedule(self) -> None:
        schedule_key = self._pending_schedule_key
        account = self._latest_account
        if schedule_key is None or account is None or self.system_mode is not SystemMode.NORMAL:
            return
        await self.run_once(
            StrategicTrigger.SCHEDULE,
            trigger_id=f"{schedule_key}:context:{account.message_id}",
            correlation_id=account.correlation_id or account.message_id,
            causation_id=account.message_id,
            trigger_detail={"kind": "schedule_context_recovery", "schedule_key": schedule_key},
        )
        self._pending_schedule_key = None

    def _ingest_market(self, snapshot: MarketSnapshot) -> None:
        if snapshot.message_id in self._ingested_market_ids:
            return
        self._remember(self._ingested_market_ids, snapshot.message_id)

        current = self._latest_markets.get(snapshot.symbol)
        if current is not None and snapshot.produced_at < current.produced_at:
            return
        self._latest_markets[snapshot.symbol] = snapshot
        history = self._price_history[snapshot.symbol]
        history.append((snapshot.produced_at, snapshot.mid_price))
        cutoff = snapshot.produced_at - timedelta(seconds=self.settings.price_history_window_s)
        while history and history[0][0] < cutoff:
            history.popleft()

    def _price_shock(self, snapshot: MarketSnapshot) -> ShockEvent | None:
        cutoff = snapshot.produced_at - timedelta(hours=1)
        baselines = [point for point in self._price_history[snapshot.symbol] if point[0] <= cutoff]
        if not baselines:
            return None
        _, baseline_price = baselines[-1]
        pct_change = ((snapshot.mid_price / baseline_price) - 1.0) * 100.0
        return self.detector.check_price(pct_change)

    async def _process_market(self, envelope: BusEnvelope) -> None:
        snapshot = MarketSnapshot.model_validate(envelope.payload)
        if not self.settings.symbol_allowed(snapshot.symbol):
            log.warning("macro.symbol_rejected", symbol=snapshot.symbol)
            return

        self._ingest_market(snapshot)
        shock = self._price_shock(snapshot)
        if shock is None:
            return
        previous = self._last_shock_at.get(snapshot.symbol)
        if previous is not None:
            elapsed = (snapshot.produced_at - previous).total_seconds()
            if elapsed < self.settings.shock_cooldown_s:
                return

        await self.run_once(
            StrategicTrigger.SHOCK_EVENT,
            trigger_id=f"shock:{snapshot.message_id}",
            correlation_id=snapshot.correlation_id or snapshot.message_id,
            causation_id=snapshot.message_id,
            trigger_detail={
                "kind": shock.kind,
                "symbol": snapshot.symbol,
                "detail": shock.detail,
                "severity": shock.severity,
            },
        )
        self._last_shock_at[snapshot.symbol] = snapshot.produced_at

    async def _consume_markets(self) -> None:
        async for envelope in self.bus.subscribe(Topics.MARKET_SNAPSHOT, group="macro", consumer="markets"):
            try:
                if envelope.id not in self._handled_market_ids:
                    await self._process_market(envelope)
                    self._remember(self._handled_market_ids, envelope.id)
                await self.bus.ack(Topics.MARKET_SNAPSHOT, envelope, group="macro")
            except Exception:
                log.exception("macro.market_processing_failed", envelope_id=envelope.id)

    async def _process_control(self, envelope: BusEnvelope) -> None:
        raw_mode = envelope.payload.get("mode")
        if not isinstance(raw_mode, str):
            raise ValueError(f"invalid system mode: {raw_mode!r}")
        try:
            mode = SystemMode(raw_mode)
        except ValueError as exc:
            raise ValueError(f"invalid system mode: {raw_mode!r}") from exc

        previous = self.system_mode
        self.system_mode = mode
        if mode is not previous:
            log.warning("macro.mode_change", previous=previous.value, mode=mode.value)
        if mode in {SystemMode.CONFLICT_SAFE, SystemMode.LOCAL_QUANT_MODE}:
            upstream_id = envelope.payload.get("message_id")
            causation_id = upstream_id if isinstance(upstream_id, str) else envelope.id
            await self.run_once(
                StrategicTrigger.SHOCK_EVENT,
                trigger_id=f"control:{causation_id}",
                correlation_id=causation_id,
                causation_id=causation_id,
                trigger_detail={"kind": "system_mode", "mode": mode.value},
            )
        elif mode is SystemMode.NORMAL:
            await self._recover_pending_schedule()

    async def _consume_control(self) -> None:
        async for envelope in self.bus.subscribe(Topics.SYSTEM_CONTROL, group="macro", consumer="control"):
            try:
                if envelope.id not in self._handled_control_ids:
                    await self._process_control(envelope)
                    self._remember(self._handled_control_ids, envelope.id)
                await self.bus.ack(Topics.SYSTEM_CONTROL, envelope, group="macro")
            except ValueError:
                log.exception("macro.invalid_control", envelope_id=envelope.id)
                await self.bus.ack(Topics.SYSTEM_CONTROL, envelope, group="macro")
            except Exception:
                log.exception("macro.control_processing_failed", envelope_id=envelope.id)

    async def _scheduler(self) -> None:  # pragma: no cover - wall-clock loop
        while True:
            now = datetime.now(UTC)
            schedule_key = f"schedule:{now.date().isoformat()}:{self.settings.run_cron_hour_utc:02d}"
            if (
                now.hour == self.settings.run_cron_hour_utc
                and now.minute == 0
                and schedule_key != self._last_schedule_key
            ):
                if self._latest_account is None and self.system_mode is SystemMode.NORMAL:
                    self._pending_schedule_key = schedule_key
                await self.run_once(
                    StrategicTrigger.SCHEDULE,
                    trigger_id=schedule_key,
                    trigger_detail={"kind": "schedule", "scheduled_at": now.isoformat()},
                )
                self._last_schedule_key = schedule_key
            await asyncio.sleep(self.settings.scheduler_poll_s)

    async def close(self) -> None:
        try:
            close_gateway = getattr(self.strategist.gateway, "close", None)
            if close_gateway is not None:
                result = close_gateway()
                if inspect.isawaitable(result):
                    await result
        finally:
            await self.bus.close()

    async def run(self) -> None:  # pragma: no cover - production consumers are unbounded
        configure_logging(
            self.settings.log_level,
            json_logs=self.settings.log_json,
            service=self.settings.service_name,
        )
        log.info("macro.start", system_mode=self.system_mode.value)
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self._scheduler(), name="schedule")
                tasks.create_task(self._consume_accounts(), name="account-snapshots")
                tasks.create_task(self._consume_markets(), name="market-snapshots")
                tasks.create_task(self._consume_control(), name="system-control")
        finally:
            await self.close()


def main() -> None:  # pragma: no cover
    asyncio.run(MacroService().run())


if __name__ == "__main__":
    main()
