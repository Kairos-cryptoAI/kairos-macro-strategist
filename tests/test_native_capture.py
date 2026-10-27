"""Synthetic no-network capture checks; not source or producer qualification."""

import base64
import hashlib
import json
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from kairos_core.contracts import canonical_json_bytes
from kairos_core.enums import StrategicTrigger

from kairos_macro.capture import MacroCaptureError, NativeMacroCapture
from kairos_macro.strategist import MacroStrategist


class Sink:
    def __init__(self, *, fail_at=None):
        self.rows = {}
        self.fail_at = fail_at

    def create_only(self, identity, raw):
        if len(self.rows) == self.fail_at:
            raise RuntimeError("private persistence details")
        assert identity not in self.rows
        self.rows[identity] = raw


class Gateway:
    def __init__(self, *, fail=False):
        self.calls = []
        self.fail = fail

    async def complete(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("private provider details")
        return SimpleNamespace(
            parsed={
                "regime": "BEAR",
                "stable_reserve_pct": 0.6,
                "strategy_weights": [{"strategy_name": "fixture_macro_strategy_v1", "weight": 0.4}],
                "max_gross_leverage": 1,
                "rationale": "synthetic fixture",
            }
        )


def strategist(sink, gateway, *, allowed=True):
    return MacroStrategist(
        gateway,
        allowed_strategy_ids=("fixture_macro_strategy_v1",) if allowed else (),
        capture=NativeMacroCapture(sink),
    )


@pytest.mark.asyncio
async def test_original_context_and_output_are_bound_without_rewriting_clocks_or_provider_claims():
    sink, gateway = Sink(), Gateway()
    macro = strategist(sink, gateway)
    context = '{ "native": "Событие", "unknown_macro": null }\n'
    allocation = await macro.allocate(
        context, trigger=StrategicTrigger.SCHEDULE, message_id="macro:synthetic-capture"
    )
    before, after = [json.loads(raw) for raw in sink.rows.values()]
    assert len(gateway.calls) == 1 and gateway.calls[0]["user"] == context
    assert base64.b64decode(before["context_utf8"]["base64"]) == context.encode("utf-8")
    assert before["context_utf8"]["sha256"] == hashlib.sha256(context.encode("utf-8")).hexdigest()
    assert before["capture_scope"] == "EXACT_APPLICATION_CONTEXT_NOT_PROVIDER_HTTP"
    assert after["input_record_sha256"] == hashlib.sha256(next(iter(sink.rows.values()))).hexdigest()
    assert after["allocation"] == allocation.model_dump(mode="json")
    assert after["allocation_sha256"] == hashlib.sha256(canonical_json_bytes(allocation)).hexdigest()
    assert after["output_origin"] == "NATIVE_MODEL"
    assert (
        before["accepted_at_ms"] <= int(allocation.produced_at.timestamp() * 1000) <= after["observed_at_ms"]
    )
    assert "provider" not in before and "provider" not in after


@pytest.mark.asyncio
@pytest.mark.parametrize("allowed", [False, True])
async def test_original_fallback_is_captured_as_fallback_not_a_model_answer(allowed):
    sink, gateway = Sink(), Gateway(fail=True)
    allocation = await strategist(sink, gateway, allowed=allowed).allocate(
        "{}", trigger=StrategicTrigger.SCHEDULE, message_id="macro:synthetic-fallback"
    )
    assert allocation.stable_reserve_pct == 1
    assert json.loads(list(sink.rows.values())[1])["output_origin"] == "NATIVE_DETERMINISTIC_FALLBACK"
    assert len(gateway.calls) == (1 if allowed else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", [0, 1])
async def test_persistence_failure_escapes_and_original_identity_cannot_trigger_a_second_call(fail_at):
    sink, gateway = Sink(fail_at=fail_at), Gateway()
    macro = strategist(sink, gateway)
    with pytest.raises(MacroCaptureError, match="persistence") as failed:
        await macro.allocate("{}", trigger=StrategicTrigger.SCHEDULE, message_id="macro:synthetic-failure")
    assert "private" not in str(failed.value)
    assert len(gateway.calls) == fail_at
    with pytest.raises(MacroCaptureError, match="already attempted"):
        await macro.allocate("{}", trigger=StrategicTrigger.SCHEDULE, message_id="macro:synthetic-failure")
    assert len(gateway.calls) == fail_at and len(sink.rows) == fail_at


@pytest.mark.asyncio
async def test_captured_mode_requires_original_message_identity_before_spending():
    sink, gateway = Sink(), Gateway()
    with pytest.raises(MacroCaptureError, match="original message ID"):
        await strategist(sink, gateway).allocate("{}", trigger=StrategicTrigger.SCHEDULE)
    assert not gateway.calls and not sink.rows


def test_foreign_tokens_and_identity_substitution_cannot_finish_original_capture():
    sink = Sink()
    capture = NativeMacroCapture(sink)
    identity = {"message_id": "macro:synthetic-token"}
    token = capture.begin(
        "{}", trigger=StrategicTrigger.SCHEDULE, source="macro-strategist", identity=identity
    )
    allocation = MacroStrategist(Gateway()).defensive(StrategicTrigger.SCHEDULE, **identity)
    with pytest.raises(MacroCaptureError, match="exact original"):
        capture.finish(replace(token), allocation, output_origin="NATIVE_DETERMINISTIC_FALLBACK")
    with pytest.raises(MacroCaptureError, match="native identity"):
        capture.finish(
            token,
            allocation.model_copy(update={"source": "foreign"}),
            output_origin="NATIVE_DETERMINISTIC_FALLBACK",
        )
    capture.finish(token, allocation, output_origin="NATIVE_DETERMINISTIC_FALLBACK")
    with pytest.raises(MacroCaptureError, match="unconsumed"):
        capture.finish(token, allocation, output_origin="NATIVE_DETERMINISTIC_FALLBACK")
    assert len(sink.rows) == 2


@pytest.mark.parametrize("invalid", [True, -1, float("inf"), 2**63])
def test_capture_clock_refuses_invalid_and_backwards_times(invalid):
    capture = NativeMacroCapture(Sink(), clock_ms=lambda: invalid)
    with pytest.raises(MacroCaptureError, match="clock"):
        capture.begin(
            "{}", trigger=StrategicTrigger.SCHEDULE, source="macro-strategist", identity={"message_id": "x"}
        )


def test_capture_clock_cannot_go_backwards_after_original_input():
    values = iter([int(time.time() * 1000), 0])
    capture = NativeMacroCapture(Sink(), clock_ms=lambda: next(values))
    token = capture.begin(
        "{}", trigger=StrategicTrigger.SCHEDULE, source="macro-strategist", identity={"message_id": "x"}
    )
    allocation = MacroStrategist(Gateway()).defensive(StrategicTrigger.SCHEDULE, message_id="x")
    with pytest.raises(MacroCaptureError, match="clock"):
        capture.finish(token, allocation, output_origin="NATIVE_DETERMINISTIC_FALLBACK")
