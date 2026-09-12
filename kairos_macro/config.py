from __future__ import annotations

import re
from typing import Literal

from kairos_core.config import CoreSettings
from pydantic import Field, field_validator

STRATEGY_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$"


def validate_strategy_ids(values: tuple[str, ...]) -> tuple[str, ...]:
    """Validate exact identifiers without renaming or approving a strategy."""
    if len(values) > 64 or len(values) != len(set(values)):
        raise ValueError("allowed strategy IDs must be unique, at most 64")
    if any(not re.fullmatch(STRATEGY_ID_PATTERN, value) for value in values):
        raise ValueError("allowed strategy IDs must be exact non-whitespace identifiers")
    if any(value.casefold().startswith("technical-canary") for value in values):
        raise ValueError("technical-canary must never receive a Macro LLM allocation")
    return tuple(sorted(values))


class MacroSettings(CoreSettings):
    service_name: str = "kairos-macro-strategist"
    allowed_strategy_ids: tuple[str, ...] = ()
    account_history_account_id: str | None = Field(default=None, min_length=1)
    account_history_version: Literal["legacy", "v2"] | None = None
    run_cron_hour_utc: int = Field(default=0, ge=0, le=23)
    crash_pct_1h: float = Field(default=10.0, gt=0)
    shock_cooldown_s: float = Field(default=3600.0, gt=0)
    price_history_window_s: float = Field(default=7200.0, ge=3600.0)
    shock_baseline_tolerance_s: float = Field(default=300.0, gt=0)
    account_history_window_s: float = Field(default=604800.0, gt=0)
    account_snapshot_max_age_s: float = Field(default=60.0, gt=0)
    market_snapshot_max_age_s: float = Field(default=120.0, gt=0)
    max_future_skew_s: float = Field(default=5.0, ge=0)
    minimum_fresh_markets: int = Field(default=1, ge=1)
    scheduler_poll_s: float = Field(default=30.0, gt=0)
    replay_cache_size: int = Field(default=256, ge=1)
    history_restore_max_rows: int = Field(default=100_000, ge=1, le=1_000_000)
    history_sample_limit: int = Field(default=100_000, ge=2, le=1_000_000)
    account_history_max_gap_s: float = Field(default=120.0, gt=0)
    market_history_max_gap_s: float = Field(default=120.0, gt=0)

    @field_validator("allowed_strategy_ids")
    @classmethod
    def exact_strategy_ids(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return validate_strategy_ids(values)
