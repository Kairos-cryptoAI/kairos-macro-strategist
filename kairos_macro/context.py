"""Assemble the large strategic context fed to the xhigh model."""
from __future__ import annotations

import json
from typing import Any, Dict


def build_macro_context(*, portfolio: Dict[str, Any], week_pnl_pct: float,
                        regime_hint: str, macro: Dict[str, Any],
                        onchain: Dict[str, Any] | None = None) -> str:
    """Unlike the Aggregator's tiny context, the Macro layer gets a rich brief:
    a week of performance, portfolio state, macro reports and on-chain metrics.
    """
    ctx = {
        "portfolio": portfolio,                # {equity, stable_pct, positions:[...]}
        "performance": {"week_pnl_pct": round(week_pnl_pct, 2)},
        "regime_hint": regime_hint,
        "macro": macro,                        # {cpi, fed_rate, dxy, ...}
        "onchain": onchain or {},              # {active_addresses, exchange_netflow, ...}
    }
    return json.dumps(ctx, separators=(",", ":"))
