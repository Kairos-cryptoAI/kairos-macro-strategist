from __future__ import annotations

from kairos_core.config import CoreSettings
from pydantic import Field


class MacroSettings(CoreSettings):
    service_name: str = "kairos-macro-strategist"
    run_cron_hour_utc: int = Field(default=0, ge=0, le=23)
    crash_pct_1h: float = Field(default=10.0, gt=0)
    shock_cooldown_s: float = Field(default=3600.0, gt=0)
    price_history_window_s: float = Field(default=7200.0, ge=3600.0)
    account_history_window_s: float = Field(default=604800.0, gt=0)
    scheduler_poll_s: float = Field(default=30.0, gt=0)
    replay_cache_size: int = Field(default=256, ge=1)
