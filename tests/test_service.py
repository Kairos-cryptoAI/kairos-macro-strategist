"""The production gateway must publish LLM health events back to the bus."""
from kairos_macro.config import MacroSettings
from kairos_macro.service import MacroService


def test_gateway_health_hook_is_wired():
    svc = MacroService(MacroSettings(bus_backend="memory"))
    assert svc.strategist.gateway._on_health is not None
