"""Macro service driven by real account, market, schedule, and control inputs."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
from collections import OrderedDict, defaultdict, deque
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from kairos_core.bus import BusEnvelope, MessageBus, build_bus
from kairos_core.contracts import (
    AccountSnapshot,
    AccountSnapshotV2,
    LLMHealthEvent,
    MarketSnapshot,
    StrategicAllocation,
)
from kairos_core.enums import Side, StrategicTrigger, SystemMode
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics
from kairos_persistence import DurableLLMUsageBudget, DurableMessageBus

from .config import MacroSettings
from .context import build_macro_context
from .history import AuditFact, load_audit_history, load_prior_allocation, payload_digest_text, timestamp
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
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings or MacroSettings()
        if bus is not None:
            self.bus = bus
        else:
            transport = build_bus(self.settings)
            self.bus = (
                transport
                if self.settings.bus_backend == "memory"
                else DurableMessageBus(transport, service_name=self.settings.service_name)
            )
        self.detector = ShockDetector(self.settings.crash_pct_1h)
        if gateway is None:
            from kairos_llm import (
                BudgetedLLMGateway,
                DenyLLMUsageBudget,
                LLMGateway,
                LLMSettings,
                Provider,
            )

            budget = (
                DurableLLMUsageBudget(self.bus)
                if isinstance(self.bus, DurableMessageBus)
                else DenyLLMUsageBudget()
            )
            gateway = BudgetedLLMGateway(
                LLMGateway(settings=LLMSettings(max_retries=0), on_health=self._publish_health),
                budget,
                monthly_budgets_microusd={
                    Provider.OPENAI: 12_000_000,
                    Provider.DEEPSEEK: 1_000_000,
                },
            )
        self.strategist = MacroStrategist(
            gateway,
            source=self.settings.service_name,
            allowed_strategy_ids=self.settings.allowed_strategy_ids,
        )
        self._clock = clock or (lambda: datetime.now(UTC))

        self.system_mode = SystemMode.NORMAL
        self._latest_account: AccountSnapshot | AccountSnapshotV2 | None = None
        self._latest_account_captured_at: datetime | None = None
        self._account_history: deque[tuple[datetime, float]] = deque()
        self._latest_markets: dict[str, MarketSnapshot] = {}
        self._price_history: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)
        self._last_shock_at: dict[str, datetime] = {}
        self._allocation_cache: OrderedDict[str, StrategicAllocation] = OrderedDict()
        self._handled_market_ids: OrderedDict[str, None] = OrderedDict()
        self._handled_control_ids: OrderedDict[str, None] = OrderedDict()
        self._ingested_market_ids: OrderedDict[str, str] = OrderedDict()
        self._ingested_account_ids: OrderedDict[str, str] = OrderedDict()
        self._account_scope: tuple[str, ...] | None = None
        self._latest_account_digest: str | None = None
        self._latest_control_at: datetime | None = None
        self._latest_control_digest: str | None = None
        self._integrity_issue: str | None = None
        self._history_restored = not isinstance(self.bus, DurableMessageBus)
        self.history_status: dict[str, Any] = {
            "state": "memory_only" if self._history_restored else "pending",
            "restored_rows": 0,
            "account_gaps": 0,
            "market_gaps": 0,
            "account_reorders": 0,
            "market_reorders": 0,
            "sample_evictions": 0,
        }
        self._allocation_lock = asyncio.Lock()
        self._last_schedule_key: str | None = None
        self._pending_schedule_key: str | None = None

    @staticmethod
    def _require_aware(value: datetime, *, field: str) -> None:
        if value.utcoffset() is None:
            raise ValueError(f"{field} must be timezone-aware")

    def _now(self) -> datetime:
        now = self._clock()
        self._require_aware(now, field="clock")
        return now.astimezone(UTC)

    def _freshness_issue(
        self,
        *,
        label: str,
        observed_at: datetime,
        reference: datetime,
        ttl_s: float,
    ) -> str | None:
        age_s = (reference - observed_at).total_seconds()
        if age_s < -self.settings.max_future_skew_s:
            return f"{label} is {abs(age_s):.3f}s in the future"
        if age_s > ttl_s:
            return f"{label} is stale by age {age_s:.3f}s (ttl {ttl_s:.3f}s)"
        return None

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

    @staticmethod
    def _market_digest(snapshot: MarketSnapshot) -> str:
        payload = payload_digest_text(snapshot.to_payload())
        return hashlib.sha256(payload.encode()).hexdigest()

    @staticmethod
    def _account_captured_at(account: AccountSnapshot | AccountSnapshotV2) -> datetime:
        return (
            account.captured_at
            if isinstance(account, AccountSnapshot)
            else datetime.fromtimestamp(account.captured_at_ms / 1_000, UTC)
        )

    def _reject_integrity(self, detail: str) -> None:
        self._integrity_issue = detail
        raise ValueError(detail)

    def _append_history(
        self,
        history: deque[tuple[datetime, float]],
        point: tuple[datetime, float],
        *,
        window_s: float,
        gap_s: float,
        label: str,
    ) -> None:
        if history and (point[0] - history[-1][0]).total_seconds() > gap_s:
            history.clear()
            self.history_status[f"{label}_gaps"] += 1
        history.append(point)
        cutoff = point[0] - timedelta(seconds=window_s)
        while history and history[0][0] < cutoff:
            history.popleft()
        while len(history) > self.settings.history_sample_limit:
            history.popleft()
            self.history_status["sample_evictions"] += 1

    @staticmethod
    def _account_daily_pnl_pct(account: AccountSnapshot | AccountSnapshotV2) -> float:
        if isinstance(account, AccountSnapshot):
            return account.daily_pnl_pct
        return ((account.equity_usd / account.durable_day_start_equity_usd) - 1.0) * 100.0

    def _portfolio_context(self, reference: datetime) -> dict[str, Any]:
        account = self._latest_account
        if account is None:
            raise RuntimeError("reconciled account context is unavailable")
        captured_at = self._account_captured_at(account)
        if isinstance(account, AccountSnapshotV2):
            positions = [
                {
                    "symbol": position.venue_symbol,
                    "signed_quantity": position.signed_quantity,
                    "entry_price": position.entry_price,
                    "mark_price": position.mark_price,
                    "leverage": position.leverage,
                    "liquidation_price": position.liquidation_price,
                    "unrealized_pnl_usd": position.unrealized_pnl_usd,
                    "protective_stop_order_id": position.stop_client_order_id,
                    "strategy_id": position.strategy_id,
                    "trade_id": position.trade_id,
                    "lifecycle_state": position.lifecycle_state.value,
                }
                for position in account.positions
            ]
            peak_equity_usd = account.durable_peak_equity_usd
            realized_pnl_usd = account.daily_realized_pnl_usd
        else:
            positions = [
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
            ]
            peak_equity_usd = account.peak_equity_usd
            realized_pnl_usd = account.realized_pnl_usd
        return {
            "message_id": account.message_id,
            "source": account.source,
            "exchange": account.exchange,
            "account_id": account.account_id,
            "equity_usd": account.equity_usd,
            "available_balance_usd": account.available_balance_usd,
            "liquid_pct": round(min(1.0, account.available_balance_usd / account.equity_usd), 6),
            "margin_used_usd": account.margin_used_usd,
            "peak_equity_usd": peak_equity_usd,
            "daily_pnl_pct": self._account_daily_pnl_pct(account),
            "realized_pnl_usd": realized_pnl_usd,
            "unrealized_pnl_usd": account.unrealized_pnl_usd,
            "captured_at": captured_at.isoformat(),
            "age_s": round(max(0.0, (reference - captured_at).total_seconds()), 3),
            "positions": positions,
        }

    def _performance_context(self) -> dict[str, Any]:
        account = self._latest_account
        if account is None:
            raise RuntimeError("reconciled account context is unavailable")
        first_at, baseline = self._account_history[0]
        captured_at = self._account_captured_at(account)
        observed_window_s = max(0.0, (captured_at - first_at).total_seconds())
        observed_pnl_pct = ((account.equity_usd / baseline) - 1.0) * 100.0 if baseline > 0 else 0.0
        return {
            "observed_pnl_pct": round(observed_pnl_pct, 2),
            "observed_window_s": observed_window_s,
            "target_window_s": self.settings.account_history_window_s,
            "full_window": observed_window_s >= self.settings.account_history_window_s * 0.99,
            "sample_count": len(self._account_history),
            "history_integrity": dict(self.history_status),
            "daily_pnl_pct": self._account_daily_pnl_pct(account),
        }

    def _fresh_markets(self, reference: datetime) -> dict[str, MarketSnapshot]:
        return {
            symbol: snapshot
            for symbol, snapshot in sorted(self._latest_markets.items())
            if snapshot.produced_at <= reference
            and self._freshness_issue(
                label=f"market snapshot {symbol}",
                observed_at=snapshot.produced_at,
                reference=reference,
                ttl_s=self.settings.market_snapshot_max_age_s,
            )
            is None
        }

    def _regime_hint(self, reference: datetime | None = None) -> str:
        current_time = reference or self._now()
        biases = [snapshot.quant_bias for snapshot in self._fresh_markets(current_time).values()]
        long_count = biases.count(Side.LONG)
        short_count = biases.count(Side.SHORT)
        majority = len(biases) // 2 + 1
        if long_count >= majority:
            return "BULL"
        if short_count >= majority:
            return "BEAR"
        return "CHOP"

    def _regime_evidence(self, reference: datetime) -> dict[str, Any]:
        fresh = self._fresh_markets(reference)
        counts = {
            side.value: sum(snapshot.quant_bias is side for snapshot in fresh.values()) for side in Side
        }
        return {
            "method": "strict_majority_of_fresh_quant_biases",
            "counts": counts,
            "fresh_market_count": len(fresh),
            "required_majority": len(fresh) // 2 + 1 if fresh else 1,
        }

    def _market_context(self, reference: datetime) -> dict[str, Any]:
        fresh = self._fresh_markets(reference)
        configured_symbols = sorted(self.settings.trading_symbols)
        return {
            "status": "available" if fresh else "unavailable",
            "source_topic": Topics.MARKET_SNAPSHOT,
            "coverage": {
                "fresh_symbols": sorted(fresh),
                "configured_symbols": configured_symbols,
                "fresh_fraction": round(len(fresh) / len(configured_symbols), 4)
                if configured_symbols
                else 0.0,
                "max_age_s": self.settings.market_snapshot_max_age_s,
            },
            "units": {
                "mid_price": "quote_currency_per_base_unit",
                "volume_usd": "USD",
                "funding_rate": "fraction",
                "open_interest": "provider_native_notional",
                "oi_change_pct_1h": "percent",
                "long_liquidations_usd": "USD",
                "short_liquidations_usd": "USD",
                "rsi_14": "index_0_100",
                "quant_bias": "categorical_side",
            },
            "values": {
                symbol: {
                    "message_id": snapshot.message_id,
                    "source": snapshot.source,
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
                    "age_s": round((reference - snapshot.produced_at).total_seconds(), 3),
                }
                for symbol, snapshot in fresh.items()
            },
        }

    def _context(self, trigger_detail: dict[str, Any], reference: datetime) -> str:
        return build_macro_context(
            portfolio=self._portfolio_context(reference),
            performance=self._performance_context(),
            regime_hint=self._regime_hint(reference),
            regime_evidence=self._regime_evidence(reference),
            market_factors=self._market_context(reference),
            macro_factors={
                "status": "unavailable",
                "reason": "no structured macro-release topic exists on the current bus",
            },
            onchain_factors={
                "status": "unavailable",
                "reason": "no structured on-chain topic exists on the current bus",
            },
            trigger=trigger_detail,
        )

    def _context_readiness_issue(self, reference: datetime) -> str | None:
        if not self._history_restored:
            return "durable history restoration has not completed"
        if self._integrity_issue is not None:
            return f"history integrity failure: {self._integrity_issue}"
        account = self._latest_account
        if account is None:
            return "missing reconciled account context"
        captured_at = self._account_captured_at(account)
        if captured_at > reference:
            return "account snapshot postdates the allocation evaluation time"
        issue = self._freshness_issue(
            label="account snapshot",
            observed_at=captured_at,
            reference=reference,
            ttl_s=self.settings.account_snapshot_max_age_s,
        )
        if issue is not None:
            return issue
        fresh_market_count = len(self._fresh_markets(reference))
        if fresh_market_count < self.settings.minimum_fresh_markets:
            return (
                f"only {fresh_market_count} fresh market snapshots; "
                f"minimum is {self.settings.minimum_fresh_markets}"
            )
        return None

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
            if not self._history_restored:
                raise RuntimeError("durable history restoration has not completed")
            allocation = self._allocation_cache.get(trigger_id)
            if allocation is None and isinstance(self.bus, DurableMessageBus) and self._history_restored:
                fact = await load_prior_allocation(self.bus.database.pool, f"macro:{trigger_id}", self._now())
                if fact is not None:
                    allocation = self._allocation_fact(fact)
            if allocation is not None:
                self._validate_replayed_allocation(allocation)
            if allocation is None:
                message_id = f"macro:{trigger_id}"
                correlation_id = correlation_id or trigger_id
                detail = trigger_detail or {"kind": trigger.value}
                reference = self._now()
                if self.system_mode in {SystemMode.CONFLICT_SAFE, SystemMode.LOCAL_QUANT_MODE}:
                    allocation = self.strategist.defensive(
                        trigger,
                        message_id=message_id,
                        correlation_id=correlation_id,
                        causation_id=causation_id,
                        detail=f"system mode {self.system_mode.value}",
                    )
                elif (readiness_issue := self._context_readiness_issue(reference)) is not None:
                    allocation = self.strategist.defensive(
                        trigger,
                        message_id=message_id,
                        correlation_id=correlation_id,
                        causation_id=causation_id,
                        detail=readiness_issue,
                    )
                else:
                    allocation = await self.strategist.allocate(
                        self._context(detail, reference),
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
        account: AccountSnapshot | AccountSnapshotV2
        if envelope.topic == Topics.ACCOUNT_SNAPSHOT_V2:
            account = AccountSnapshotV2.model_validate(envelope.payload)
        else:
            account = AccountSnapshot.model_validate(envelope.payload)
        version = "v2" if isinstance(account, AccountSnapshotV2) else "legacy"
        if self.settings.account_history_version not in {
            None,
            version,
        } or self.settings.account_history_account_id not in {None, account.account_id}:
            return
        scope: tuple[str, ...] = (version, account.exchange, account.account_id)
        if isinstance(account, AccountSnapshotV2):
            scope += (account.trading_mode.value, account.evedex_profile.value)
        if self._account_scope is not None and scope != self._account_scope:
            self._reject_integrity("account history contains mixed account/environment/version scopes")
        captured_at = self._account_captured_at(account)
        self._require_aware(captured_at, field="account snapshot captured_at")
        future_issue = self._freshness_issue(
            label="account snapshot",
            observed_at=captured_at,
            reference=self._now(),
            ttl_s=float("inf"),
        )
        if future_issue is not None:
            raise ValueError(future_issue)
        digest = payload_digest_text(account.to_payload())
        prior_digest = self._ingested_account_ids.get(account.message_id)
        if prior_digest is not None:
            if prior_digest != digest:
                self._reject_integrity("account snapshot message_id was reused")
            return
        latest_at = self._latest_account_captured_at
        if latest_at is not None and captured_at < latest_at:
            self.history_status["account_reorders"] += 1
            return
        if latest_at is not None and captured_at == latest_at:
            if digest != self._latest_account_digest:
                self._reject_integrity("conflicting account snapshots at the same captured_at")
            return

        self._account_scope = scope
        self._remember(self._ingested_account_ids, account.message_id, digest)
        self._latest_account_digest = digest
        self._latest_account_captured_at = captured_at
        if not account.reconciled:
            self._latest_account = None
            self._account_history.clear()
            log.warning("macro.account_unreconciled", account_id=account.account_id)
            return

        self._latest_account = account
        self._append_history(
            self._account_history,
            (captured_at, account.equity_usd),
            window_s=self.settings.account_history_window_s,
            gap_s=self.settings.account_history_max_gap_s,
            label="account",
        )

    async def _consume_accounts(self) -> None:
        async for envelope in self.bus.subscribe(Topics.ACCOUNT_SNAPSHOT, group="macro", consumer="accounts"):
            try:
                self._ingest_account(envelope)
                await self._recover_pending_schedule()
                await self.bus.ack(Topics.ACCOUNT_SNAPSHOT, envelope, group="macro")
            except Exception:
                log.exception("macro.account_processing_failed", envelope_id=envelope.id)

    async def _consume_accounts_v2(self) -> None:
        async for envelope in self.bus.subscribe(
            Topics.ACCOUNT_SNAPSHOT_V2,
            group="macro-v2",
            consumer="accounts-v2",
        ):
            try:
                self._ingest_account(envelope)
                await self._recover_pending_schedule()
                await self.bus.ack(Topics.ACCOUNT_SNAPSHOT_V2, envelope, group="macro-v2")
            except Exception:
                log.exception("macro.account_v2_processing_failed", envelope_id=envelope.id)

    async def _recover_pending_schedule(self) -> None:
        schedule_key = self._pending_schedule_key
        account = self._latest_account
        if (
            schedule_key is None
            or account is None
            or self.system_mode is not SystemMode.NORMAL
            or self._context_readiness_issue(self._now()) is not None
        ):
            return
        await self.run_once(
            StrategicTrigger.SCHEDULE,
            trigger_id=f"{schedule_key}:context:{account.message_id}",
            correlation_id=account.correlation_id or account.message_id,
            causation_id=account.message_id,
            trigger_detail={"kind": "schedule_context_recovery", "schedule_key": schedule_key},
        )
        self._pending_schedule_key = None

    def _ingest_market(self, snapshot: MarketSnapshot) -> bool:
        self._require_aware(snapshot.produced_at, field="market snapshot produced_at")
        future_issue = self._freshness_issue(
            label="market snapshot",
            observed_at=snapshot.produced_at,
            reference=self._now(),
            ttl_s=float("inf"),
        )
        if future_issue is not None:
            raise ValueError(future_issue)
        digest = self._market_digest(snapshot)
        prior_digest = self._ingested_market_ids.get(snapshot.message_id)
        if prior_digest is not None:
            if prior_digest != digest:
                self._reject_integrity(f"market snapshot message_id {snapshot.message_id!r} was reused")
            # Exact replay remains eligible so an allocation cached before a
            # failed publish can be delivered without rerunning the model.
            return True
        self._remember(self._ingested_market_ids, snapshot.message_id, digest)

        current = self._latest_markets.get(snapshot.symbol)
        if current is not None and snapshot.produced_at < current.produced_at:
            self.history_status["market_reorders"] += 1
            return False
        if current is not None and snapshot.produced_at == current.produced_at:
            if self._market_digest(current) != digest:
                self._reject_integrity("conflicting market snapshots at the same produced_at")
            return True
        self._latest_markets[snapshot.symbol] = snapshot
        history = self._price_history[snapshot.symbol]
        self._append_history(
            history,
            (snapshot.produced_at, snapshot.mid_price),
            window_s=self.settings.price_history_window_s,
            gap_s=self.settings.market_history_max_gap_s,
            label="market",
        )
        return True

    def _price_shock(self, snapshot: MarketSnapshot) -> ShockEvent | None:
        cutoff = snapshot.produced_at - timedelta(hours=1)
        earliest = cutoff - timedelta(seconds=self.settings.shock_baseline_tolerance_s)
        baselines = [
            point for point in self._price_history[snapshot.symbol] if earliest <= point[0] <= cutoff
        ]
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

        if not self._ingest_market(snapshot):
            return
        await self._recover_pending_schedule()
        now = self._now()
        if snapshot.produced_at > now:
            return
        if (
            self._freshness_issue(
                label="market snapshot",
                observed_at=snapshot.produced_at,
                reference=now,
                ttl_s=self.settings.market_snapshot_max_age_s,
            )
            is not None
        ):
            return
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
        if not self._ingest_control(envelope.payload):
            return
        mode = self.system_mode
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

    def _ingest_control(self, payload: dict[str, Any]) -> bool:
        raw_mode = payload.get("mode")
        if not isinstance(raw_mode, str):
            raise ValueError(f"invalid system mode: {raw_mode!r}")
        try:
            mode = SystemMode(raw_mode)
        except ValueError as exc:
            raise ValueError(f"invalid system mode: {raw_mode!r}") from exc
        observed_at = timestamp(payload.get("produced_at", self._now()))
        if observed_at > self._now():
            raise ValueError("system control postdates evaluation time")
        digest = payload_digest_text(payload)
        if self._latest_control_at is not None:
            if observed_at < self._latest_control_at:
                return False
            if observed_at == self._latest_control_at and digest != self._latest_control_digest:
                self._reject_integrity("conflicting system controls at the same produced_at")
        previous = self.system_mode
        self.system_mode = mode
        self._latest_control_at = observed_at
        self._latest_control_digest = digest
        if mode is not previous:
            log.warning("macro.mode_change", previous=previous.value, mode=mode.value)
        return True

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
            now = self._now()
            schedule_key = f"schedule:{now.date().isoformat()}:{self.settings.run_cron_hour_utc:02d}"
            if (
                now.hour == self.settings.run_cron_hour_utc
                and now.minute == 0
                and schedule_key != self._last_schedule_key
            ):
                if (
                    self.system_mode is not SystemMode.NORMAL
                    or self._context_readiness_issue(now) is not None
                ):
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

    def _allocation_fact(self, fact: AuditFact) -> StrategicAllocation:
        if (
            fact.topic != Topics.STRATEGIC_ALLOCATION
            or fact.payload.get("source") != self.settings.service_name
        ):
            raise ValueError("allocation identity belongs to a different topic/source")
        allocation = StrategicAllocation.model_validate(fact.payload)
        if not allocation.message_id.startswith("macro:"):
            raise ValueError("historical Macro allocation has an unknown trigger identity")
        return allocation

    def _validate_replayed_allocation(self, allocation: StrategicAllocation) -> None:
        if self._integrity_issue is not None:
            raise ValueError(f"history integrity failure: {self._integrity_issue}")
        if set(allocation.strategy_weights) - set(self.settings.allowed_strategy_ids):
            self._reject_integrity("historical allocation uses a strategy outside the configured allowlist")
        total = allocation.stable_reserve_pct + sum(allocation.strategy_weights.values())
        if not 0.9999 <= total <= 1.0001 or any(
            not 0 <= weight <= 1 for weight in allocation.strategy_weights.values()
        ):
            self._reject_integrity("historical allocation does not conserve capital")

    def restore_facts(self, facts: tuple[AuditFact, ...], reference: datetime) -> None:
        """Replay input facts only: no gateway calls, publishes, ACKs or shock triggers."""
        self._history_restored = False
        self.history_status["state"] = "restoring"
        try:
            if len(facts) > self.settings.history_restore_max_rows:
                raise ValueError("Macro audit history exceeds configured restore row limit")
            previous_key: tuple[datetime, str] | None = None
            for fact in facts:
                key = (fact.produced_at, fact.payload["message_id"])
                if previous_key is not None and key < previous_key:
                    raise ValueError("restore facts are not in deterministic chronological order")
                previous_key = key
                if fact.produced_at > reference:
                    raise ValueError("restored event postdates the recovery cutoff")
                if fact.topic in {Topics.ACCOUNT_SNAPSHOT, Topics.ACCOUNT_SNAPSHOT_V2}:
                    self._ingest_account(
                        BusEnvelope(id=fact.payload["message_id"], topic=fact.topic, payload=fact.payload)
                    )
                    if (
                        self._latest_account_captured_at is not None
                        and self._latest_account_captured_at > reference
                    ):
                        raise ValueError("restored account capture postdates the recovery cutoff")
                elif fact.topic == Topics.MARKET_SNAPSHOT:
                    market = MarketSnapshot.model_validate(fact.payload)
                    if self.settings.symbol_allowed(market.symbol):
                        self._ingest_market(market)
                elif fact.topic == Topics.SYSTEM_CONTROL:
                    self._ingest_control(fact.payload)
                elif fact.topic == Topics.STRATEGIC_ALLOCATION:
                    allocation = self._allocation_fact(fact)
                    trigger_id = allocation.message_id.removeprefix("macro:")
                    self._remember(self._allocation_cache, trigger_id, allocation)
                    if trigger_id.startswith("schedule:"):
                        schedule_key = trigger_id.split(":context:", 1)[0]
                        if self._last_schedule_key is not None and schedule_key < self._last_schedule_key:
                            continue
                        self._last_schedule_key = schedule_key
                        self._pending_schedule_key = (
                            schedule_key
                            if ":context:" not in trigger_id
                            and allocation.rationale.startswith("defensive fallback:")
                            else None
                        )
            # Restore shock cooldown from actual prior effects, never replay historical triggers.
            shock_ids = {
                fact.payload["message_id"].removeprefix("macro:")
                for fact in facts
                if fact.topic == Topics.STRATEGIC_ALLOCATION
            }
            for fact in facts:
                if fact.topic == Topics.MARKET_SNAPSHOT:
                    trigger_id = f"shock:{fact.payload['message_id']}"
                    if trigger_id in shock_ids:
                        self._last_shock_at[fact.payload["symbol"]] = fact.produced_at
            self._history_restored = True
            self.history_status.update(
                state="restored", restored_rows=len(facts), cutoff=reference.isoformat()
            )
        except Exception:
            self.history_status["state"] = "failed"
            raise

    async def restore_history(self) -> None:
        if not isinstance(self.bus, DurableMessageBus):
            return
        self._history_restored = False
        try:
            await self.bus.start()
            reference = self._now()
            facts = await load_audit_history(self.bus.database.pool, self.settings, reference)
            self.restore_facts(facts, reference)
        except Exception:
            self.history_status["state"] = "failed"
            raise
        log.info("macro.history_restored", **self.history_status)

    async def run(self) -> None:  # pragma: no cover - production consumers are unbounded
        configure_logging(
            self.settings.log_level,
            json_logs=self.settings.log_json,
            service=self.settings.service_name,
        )
        log.info("macro.start", system_mode=self.system_mode.value)
        try:
            await self.restore_history()
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self._scheduler(), name="schedule")
                tasks.create_task(self._consume_accounts(), name="account-snapshots")
                tasks.create_task(self._consume_accounts_v2(), name="account-snapshots-v2")
                tasks.create_task(self._consume_markets(), name="market-snapshots")
                tasks.create_task(self._consume_control(), name="system-control")
        finally:
            await self.close()


def main() -> None:  # pragma: no cover
    asyncio.run(MacroService().run())


if __name__ == "__main__":
    main()
