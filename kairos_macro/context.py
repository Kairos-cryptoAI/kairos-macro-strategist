"""Assemble the strategic context fed to the xhigh model."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


def build_macro_context(
    *,
    portfolio: Mapping[str, Any],
    performance: Mapping[str, Any],
    regime_hint: str,
    regime_evidence: Mapping[str, Any],
    market_factors: Mapping[str, Any],
    macro_factors: Mapping[str, Any],
    onchain_factors: Mapping[str, Any],
    trigger: Mapping[str, Any] | None = None,
) -> str:
    """Serialize a bounded, real-data strategic brief for the model."""
    context = {
        "portfolio": dict(portfolio),
        "performance": dict(performance),
        "regime_hint": regime_hint,
        "regime_evidence": dict(regime_evidence),
        "market_factors": dict(market_factors),
        "macro_factors": dict(macro_factors),
        "onchain_factors": dict(onchain_factors),
        "trigger": dict(trigger or {}),
    }
    return json.dumps(context, separators=(",", ":"), sort_keys=True)
