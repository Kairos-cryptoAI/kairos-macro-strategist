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
    macro: Mapping[str, Any],
    onchain: Mapping[str, Any] | None = None,
    trigger: Mapping[str, Any] | None = None,
) -> str:
    """Serialize a bounded, real-data strategic brief for the model."""
    context = {
        "portfolio": dict(portfolio),
        "performance": dict(performance),
        "regime_hint": regime_hint,
        "macro": dict(macro),
        "onchain": dict(onchain or {}),
        "trigger": dict(trigger or {}),
    }
    return json.dumps(context, separators=(",", ":"), sort_keys=True)
