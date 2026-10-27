"""Optional original Macro application-input/output capture, not source authority.

The exact native context accepted by Macro is retained before a model can be
called. This is NOT an HTTP response, an external macroeconomic dataset or
provider authentication. Original upstream witnesses and the gateway's separate
budgeted call records are still required by a production input producer.
"""

from __future__ import annotations

import base64
import hashlib
import inspect
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol, cast
from uuid import uuid4

from kairos_core.contracts import StrategicAllocation, canonical_json_bytes
from kairos_core.enums import StrategicTrigger


class MacroCaptureError(ValueError):
    """No uncaptured allocation may be published by an opted-in caller."""


class MacroCaptureSink(Protocol):
    def create_only(self, observation_id: str, record: bytes) -> None: ...


@dataclass(frozen=True, slots=True)
class MacroInputCapture:
    capture_id: str
    record: bytes
    accepted_at_ms: int
    identity: tuple[tuple[str, str], ...]
    source: str
    trigger: StrategicTrigger


class NativeMacroCapture:
    """Use the existing two-argument create-only sink without owning its files.

    Capture failure is not a model failure and must not become a fresh defensive
    allocation. No retry, credential access or producer registration exists here.
    """

    def __init__(
        self,
        sink: MacroCaptureSink,
        *,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if not callable(getattr(sink, "create_only", None)):
            raise TypeError("an explicit synchronous Macro capture sink is required")
        if clock_ms is not None and not callable(clock_ms):
            raise TypeError("an explicit capture clock must be callable")
        self.sink = sink
        self._clock = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._last_ms: int | None = None
        self._pending: dict[str, MacroInputCapture] = {}
        self._finished: set[str] = set()
        self._seen_messages: set[str] = set()

    def _now(self) -> int:
        instant = self._clock()
        if inspect.iscoroutine(instant):
            instant.close()
        if (
            type(instant) is not int
            or not 0 <= instant < 2**63
            or (self._last_ms is not None and instant < self._last_ms)
        ):
            raise MacroCaptureError("Macro capture requires a monotone integer millisecond clock")
        self._last_ms = instant
        return instant

    def _write(self, observation_id: str, record: bytes) -> None:
        try:
            writer = cast(Callable[[str, bytes], object], self.sink.create_only)
            result = writer(observation_id, record)
            if inspect.iscoroutine(result):
                result.close()
            if result is not None:
                raise ValueError
        except Exception:
            raise MacroCaptureError("Macro capture persistence was not synchronously acknowledged") from None

    def begin(
        self,
        context_json: str,
        *,
        trigger: StrategicTrigger,
        identity: Mapping[str, str],
        source: str,
    ) -> MacroInputCapture:
        if not isinstance(context_json, str) or not isinstance(trigger, StrategicTrigger):
            raise MacroCaptureError("the exact native context and trigger are required")
        try:
            raw = context_json.encode("utf-8", errors="strict")
        except UnicodeError:
            raise MacroCaptureError("Macro context is not valid UTF-8") from None
        if not raw or len(raw) > 1_000_000:
            raise MacroCaptureError("Macro context exceeds the original-byte capture bound")
        if len(self._pending) + len(self._finished) >= 10_000:
            raise MacroCaptureError("Macro capture session capacity exhausted; no identities are evicted")
        if set(identity) - {"message_id", "correlation_id", "causation_id"} or any(
            not isinstance(value, str) or not value or len(value) > 512 for value in identity.values()
        ):
            raise MacroCaptureError("native Macro identity metadata is malformed")
        message_id = identity.get("message_id")
        if (
            not isinstance(message_id, str)
            or not message_id
            or not isinstance(source, str)
            or not source
            or len(source) > 160
            or source != source.strip()
        ):
            raise MacroCaptureError("captured Macro requires an original message ID and source")
        if message_id in self._seen_messages:
            raise MacroCaptureError("Macro input identity already attempted; no blind capture or model retry")
        capture_id = uuid4().hex
        at_ms = self._now()
        record = canonical_json_bytes(
            {
                "version": "kairos.native-macro-application-input.v1",
                "capture_scope": "EXACT_APPLICATION_CONTEXT_NOT_PROVIDER_HTTP",
                "capture_id": capture_id,
                "accepted_at_ms": at_ms,
                "trigger": trigger.value,
                "source": source,
                "identity": dict(identity),
                "context_utf8": {
                    "base64": base64.b64encode(raw).decode("ascii"),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                },
            }
        )
        token = MacroInputCapture(capture_id, record, at_ms, tuple(sorted(identity.items())), source, trigger)
        # Fence before persistence: partial writes are never retried or adopted.
        self._pending[capture_id] = token
        self._seen_messages.add(message_id)
        self._write(capture_id, record)
        return token

    def finish(
        self,
        token: MacroInputCapture,
        allocation: StrategicAllocation,
        *,
        output_origin: Literal["NATIVE_MODEL", "NATIVE_DETERMINISTIC_FALLBACK"],
    ) -> bytes:
        if (
            type(token) is not MacroInputCapture
            or self._pending.get(token.capture_id) is not token
            or token.capture_id in self._finished
            or output_origin not in {"NATIVE_MODEL", "NATIVE_DETERMINISTIC_FALLBACK"}
        ):
            raise MacroCaptureError("Macro output must bind one exact original unconsumed input capture")
        checked = StrategicAllocation.model_validate_json(allocation.model_dump_json())
        if (
            any(getattr(checked, key) != value for key, value in token.identity)
            or checked.source != token.source
            or checked.triggered_by is not token.trigger
        ):
            raise MacroCaptureError("Macro output changed its original native identity")
        at_ms = self._now()
        if int(checked.produced_at.timestamp() * 1_000) > at_ms:
            raise MacroCaptureError("Macro output is future-dated at capture")
        raw = canonical_json_bytes(checked)
        record = canonical_json_bytes(
            {
                "version": "kairos.native-macro-application-output.v1",
                "capture_scope": "EXACT_APPLICATION_OUTPUT_NOT_PROVIDER_HTTP",
                "capture_id": token.capture_id,
                "input_record_sha256": hashlib.sha256(token.record).hexdigest(),
                "observed_at_ms": at_ms,
                "output_origin": output_origin,
                "allocation": checked.model_dump(mode="json"),
                "allocation_sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
        self._finished.add(token.capture_id)
        self._pending.pop(token.capture_id)
        output_id = hashlib.sha256((token.capture_id + ":allocation").encode("ascii")).hexdigest()[:32]
        self._write(output_id, record)
        return record
