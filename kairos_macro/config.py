from __future__ import annotations

from kairos_core.config import CoreSettings


class MacroSettings(CoreSettings):
    service_name: str = "kairos-macro-strategist"
    run_cron_hour_utc: int = 0          # daily run at 00:00 UTC
    crash_pct_1h: float = 10.0
