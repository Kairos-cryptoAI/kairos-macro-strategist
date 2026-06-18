"""Kairos Layer 4 — Macro-Strategist.

The slow, careful strategic layer (xhigh reasoning). It does not trade minute to
minute; instead it sets global capital allocation, defends against black swans
and adapts to regime changes (bull / bear / hard chop). Runs on a schedule
(daily/weekly) or when a shock event fires.
"""
from __future__ import annotations

__version__ = "0.1.0"

from .triggers import ShockDetector, ShockEvent
from .strategist import MacroStrategist
from .context import build_macro_context

__all__ = ["ShockDetector", "ShockEvent", "MacroStrategist", "build_macro_context", "__version__"]
