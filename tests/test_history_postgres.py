"""Opt-in real SQL drill; writes synthetic facts only to one explicit disposable DB."""

from __future__ import annotations

import json
import os
from datetime import timedelta

import pytest

from kairos_macro.history import load_audit_history, load_prior_allocation

from .test_history import NOW, account_row, control_row, market_row
from .test_service import _service, _settings

EXPECTED_DATABASE = "kairos_macro_test_20260912"
DATABASE_URL_ENV = "KAIROS_MACRO_TEST_DATABASE_URL"

pytestmark = pytest.mark.skipif(not os.getenv(DATABASE_URL_ENV), reason="explicit isolated SQL drill only")


async def test_real_sql_bounded_causal_restart_and_truncation():
    from kairos_persistence import Database, PersistenceSettings
    from kairos_persistence.database_target import connect_verified_database, require_database_target_url

    url = os.environ[DATABASE_URL_ENV]
    require_database_target_url(url, EXPECTED_DATABASE, local_only=True)
    database = Database(PersistenceSettings(database_url=url))
    await connect_verified_database(database, EXPECTED_DATABASE, local_only=True)
    try:
        # No migration, no DROP/TRUNCATE, no writes before resolved target verification.
        assert await database.pool.fetchval("SELECT current_database()") == EXPECTED_DATABASE
        existing = await database.pool.fetchval("SELECT to_regclass('public.event_audit')")
        if existing is not None:
            raise ValueError("disposable Macro database already contains evidence; preserve it")
        await database.pool.execute("""CREATE TABLE event_audit (
            produced_at TIMESTAMPTZ NOT NULL, message_id TEXT NOT NULL, topic TEXT NOT NULL,
            source TEXT NOT NULL, schema_version TEXT NOT NULL, correlation_id TEXT,
            causation_id TEXT, payload JSONB NOT NULL, persisted_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (produced_at, message_id)
        )""")
        rows = [account_row(NOW - timedelta(seconds=30)), account_row(NOW)]
        rows += [market_row(NOW - timedelta(minutes=step)) for step in range(61, -1, -1)]
        rows += [control_row(NOW - timedelta(days=30), "CONFLICT_SAFE")]
        # Causally unavailable rows and an out-of-scope account must not be restored.
        rows += [account_row(NOW, name="out-of-scope")]
        rows[-1]["message_id"] = rows[-1]["payload"]["message_id"] = "different-account"
        rows += [market_row(NOW + timedelta(seconds=1), identity="future-produced")]
        rows += [market_row(NOW, identity="not-yet-persisted")]
        for row in rows:
            persisted = NOW + timedelta(seconds=1) if row["message_id"] == "not-yet-persisted" else NOW
            await database.pool.execute(
                """INSERT INTO event_audit
                   (produced_at,message_id,topic,source,schema_version,correlation_id,causation_id,payload,persisted_at)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)""",
                row["produced_at"],
                row["message_id"],
                row["topic"],
                row["source"],
                row["schema_version"],
                row["correlation_id"],
                row["causation_id"],
                json.dumps(row["payload"]),
                persisted,
            )
        settings = _settings(account_history_account_id="primary", account_history_version="legacy")
        facts = await load_audit_history(database.pool, settings, NOW)
        assert len(facts) == 65
        assert not {"future-produced", "not-yet-persisted", "different-account"} & {
            fact.payload["message_id"] for fact in facts
        }
        first, gateway, bus = _service(NOW)
        first.restore_facts(facts, NOW)
        assert len(first._account_history) == 2 and len(first._price_history["BTCUSDT"]) == 62
        assert first.system_mode.value == "CONFLICT_SAFE"
        assert not gateway.calls and not bus.operations
        with pytest.raises(ValueError, match="restore row limit"):
            await load_audit_history(database.pool, _settings(history_restore_max_rows=1), NOW)
        assert await load_prior_allocation(database.pool, "does-not-exist", NOW) is None
        await database.close()
        await connect_verified_database(database, EXPECTED_DATABASE, local_only=True)
        again = await load_audit_history(database.pool, settings, NOW)
        assert again == facts
        assert await database.pool.fetchval("SELECT count(*) FROM event_audit") == 68
    finally:
        await database.close()
