"""Bounded, read-only startup evidence from the existing durable event audit."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from kairos_core.topics import Topics

from .config import MacroSettings


def payload_digest_text(payload: Mapping[str, Any]) -> str:
    return json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), allow_nan=False)


def timestamp(value: object) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("audit timestamp must be timezone-aware")
    return value


@dataclass(frozen=True)
class AuditFact:
    topic: str
    produced_at: datetime
    payload: dict[str, Any]


def validate_audit_rows(rows: Sequence[Mapping[str, Any]], reference: datetime) -> tuple[AuditFact, ...]:
    """Require causal audit/payload identity and immutable IDs; never repair evidence."""
    facts: list[AuditFact] = []
    seen: dict[str, tuple[str, str]] = {}
    for row in rows:
        raw = row["payload"]
        payload = json.loads(raw) if isinstance(raw, str) else dict(raw)
        produced_at = timestamp(row["produced_at"])
        if row.get("persisted_at") is not None and timestamp(row["persisted_at"]) > reference:
            raise ValueError("audit event was persisted after the recovery cutoff")
        if produced_at > reference or timestamp(payload.get("produced_at")) != produced_at:
            raise ValueError("audit event is future-dated or has a conflicting produced_at")
        for key in ("message_id", "source", "schema_version", "correlation_id", "causation_id"):
            if row.get(key) != payload.get(key):
                raise ValueError(f"audit envelope/payload mismatch: {key}")
        message_id = payload.get("message_id")
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("audit message_id is missing")
        identity = (str(row["topic"]), payload_digest_text(payload))
        prior = seen.get(message_id)
        if prior is not None:
            if prior != identity:
                raise ValueError("audit contains a conflicting duplicate message_id")
            continue
        seen[message_id] = identity
        facts.append(AuditFact(str(row["topic"]), produced_at, payload))
    return tuple(sorted(facts, key=lambda fact: (fact.produced_at, fact.payload["message_id"])))


async def load_audit_history(
    pool: Any, settings: MacroSettings, reference: datetime
) -> tuple[AuditFact, ...]:
    """One consistent snapshot; row overflow is a startup failure, never silent truncation."""
    account_topics = {
        "legacy": [Topics.ACCOUNT_SNAPSHOT],
        "v2": [Topics.ACCOUNT_SNAPSHOT_V2],
        None: [Topics.ACCOUNT_SNAPSHOT, Topics.ACCOUNT_SNAPSHOT_V2],
    }[settings.account_history_version]
    queries = (
        (
            """SELECT * FROM event_audit
               WHERE topic = ANY($1::text[]) AND produced_at >= $2 AND produced_at <= $3
                 AND persisted_at <= $3
                 AND ($4::text IS NULL OR payload->>'account_id' = $4)
               ORDER BY produced_at, message_id LIMIT $5""",
            (
                account_topics,
                reference - timedelta(seconds=settings.account_history_window_s),
                reference,
                settings.account_history_account_id,
            ),
        ),
        (
            """SELECT * FROM event_audit
               WHERE topic = $1 AND produced_at >= $2 AND produced_at <= $3 AND persisted_at <= $3
                 AND payload->>'symbol' = ANY($4::text[])
               ORDER BY produced_at, message_id LIMIT $5""",
            (
                Topics.MARKET_SNAPSHOT,
                reference - timedelta(seconds=settings.price_history_window_s),
                reference,
                list(settings.trading_symbols),
            ),
        ),
        (
            """SELECT * FROM event_audit
               WHERE topic = $1 AND produced_at >= $2 AND produced_at <= $3
                 AND persisted_at <= $3 AND source = $4
               ORDER BY produced_at, message_id LIMIT $5""",
            (
                Topics.STRATEGIC_ALLOCATION,
                reference
                - timedelta(seconds=max(settings.account_history_window_s, settings.shock_cooldown_s)),
                reference,
                settings.service_name,
            ),
        ),
        (
            """SELECT * FROM event_audit WHERE topic = $1 AND persisted_at <= $2 AND produced_at = (
                   SELECT MAX(produced_at) FROM event_audit
                   WHERE topic = $1 AND produced_at <= $2 AND persisted_at <= $2
               ) ORDER BY produced_at, message_id LIMIT $3""",
            (Topics.SYSTEM_CONTROL, reference),
        ),
    )
    rows: list[Any] = []
    async with pool.acquire() as connection:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            for query, parameters in queries:
                remaining = settings.history_restore_max_rows - len(rows)
                page = await connection.fetch(query, *parameters, remaining + 1)
                if len(page) > remaining:
                    raise ValueError("Macro audit history exceeds configured restore row limit")
                rows.extend(page)
    return validate_audit_rows(rows, reference)


async def load_prior_allocation(pool: Any, message_id: str, reference: datetime) -> AuditFact | None:
    """Resolve old trigger redelivery beyond the bounded in-memory replay cache."""
    rows = await pool.fetch(
        "SELECT * FROM event_audit WHERE message_id = $1 ORDER BY produced_at LIMIT 3", message_id
    )
    if len(rows) > 2:
        raise ValueError("allocation message_id has multiple audit identities")
    facts = validate_audit_rows(rows, reference)
    if len(facts) > 1:
        raise ValueError("allocation message_id has conflicting audit facts")
    return facts[0] if facts else None
