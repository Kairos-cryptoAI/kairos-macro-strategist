"""Decide WHEN the Macro-Strategist should run.

Two triggers (spec): a schedule (daily/weekly rebalancing) and a shock event
(e.g. the market drops 10% in an hour, or a shocking inflation print).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ShockEvent:
    kind: str          # "price_crash" | "macro_print"
    detail: str
    severity: float    # 0..1


class ShockDetector:
    def __init__(self, crash_pct_1h: float = 10.0) -> None:
        self.crash_pct_1h = crash_pct_1h

    def check_price(self, pct_change_1h: float) -> ShockEvent | None:
        """``pct_change_1h`` is signed (negative == drop)."""
        if pct_change_1h <= -self.crash_pct_1h:
            sev = min(1.0, abs(pct_change_1h) / (self.crash_pct_1h * 2))
            return ShockEvent("price_crash", f"market {pct_change_1h:.1f}% in 1h", sev)
        return None

    def check_macro(self, *, indicator: str, surprise_sigma: float) -> ShockEvent | None:
        """Fire when a macro print deviates strongly from expectations."""
        if abs(surprise_sigma) >= 2.0:
            return ShockEvent("macro_print", f"{indicator} surprise {surprise_sigma:+.1f}sigma",
                              min(1.0, abs(surprise_sigma) / 4.0))
        return None
