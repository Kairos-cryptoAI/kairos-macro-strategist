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
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol, cast
from uuid import uuid4

from kairos_core.contracts import StrategicAllocation, canonical_json_bytes
from kairos_core.enums import StrategicTrigger


class MacroCaptureError(ValueError):
    """No uncaptured allocation may be published by an opted-in caller."""


class MacroCaptureSink(Protocol):
    def create_only(self, observation_id: str, record: bytes) -> None: ...


class MacroProcessingClaims(Protocol):
    """Durable event fence with the existing news-processing claim semantics."""

    async def claim_batch(
        self, scope_key: str, event_keys: tuple[str, ...], attempt_id: str
    ) -> tuple[str, ...]: ...

    async def complete_batch(self, scope_key: str, event_keys: tuple[str, ...], attempt_id: str) -> None: ...


@dataclass(frozen=True, slots=True)
class MacroInputCapture:
    capture_id: str
    record: bytes
    accepted_at_ms: int
    identity: tuple[tuple[str, str], ...]
    source: str
    trigger: StrategicTrigger
    claim_scope: str | None = None
    claim_event_key: str | None = None
    claim_attempt_id: str | None = None


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
        processing_claims: MacroProcessingClaims | None = None,
        processing_scope_sha256: str | None = None,
    ) -> None:
        if not callable(getattr(sink, "create_only", None)):
            raise TypeError("an explicit synchronous Macro capture sink is required")
        if clock_ms is not None and not callable(clock_ms):
            raise TypeError("an explicit capture clock must be callable")
        if processing_claims is None and processing_scope_sha256 is not None:
            raise TypeError("a processing scope requires an explicit claims repository")
        if processing_claims is not None and (
            type(processing_scope_sha256) is not str
            or len(processing_scope_sha256) != 64
            or any(char not in "0123456789abcdef" for char in processing_scope_sha256)
        ):
            raise ValueError("durable Macro claims require one fixed lowercase SHA-256 scope")
        if processing_claims is not None and (
            not callable(getattr(processing_claims, "claim_batch", None))
            or not callable(getattr(processing_claims, "complete_batch", None))
        ):
            raise TypeError("an explicit durable Macro processing-claims repository is required")
        self.sink = sink
        self.processing_claims = processing_claims
        self.processing_scope_sha256 = processing_scope_sha256
        self._clock = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._last_ms: int | None = None
        self._pending: dict[str, MacroInputCapture] = {}
        self._finished: set[str] = set()
        self._seen_messages: set[str] = set()
        self._tokens: dict[str, MacroInputCapture] = {}
        self._claim_bindings: dict[str, tuple[str, str, str]] = {}
        self._output_acknowledged: set[str] = set()
        self._claim_completion_attempted: set[str] = set()

    @property
    def has_durable_claims(self) -> bool:
        return self.processing_claims is not None

    async def begin_claimed(
        self,
        context_json: str,
        *,
        trigger: StrategicTrigger,
        identity: Mapping[str, str],
        source: str,
    ) -> MacroInputCapture:
        """Durably fence the source identity before any capture or provider call.

        A claim is never reset/released. A crash or uncertain persistence therefore
        remains pending and prevents a second model call from another capture object.
        """
        repository = self.processing_claims
        scope = self.processing_scope_sha256
        if repository is None or scope is None:
            raise MacroCaptureError("capture-mode allocation requires a durable processing-claims source")
        message_id = identity.get("message_id")
        if not isinstance(message_id, str) or not message_id or len(message_id) > 512:
            raise MacroCaptureError("durable Macro processing requires an original message ID")
        if not isinstance(source, str) or not source or len(source) > 160 or source != source.strip():
            raise MacroCaptureError("durable Macro processing requires a normalized original source")
        event_key = hashlib.sha256((source + "\0" + message_id).encode("utf-8")).hexdigest()
        attempt_id = hashlib.sha256(uuid4().bytes).hexdigest()
        try:
            claimed = await repository.claim_batch(scope, (event_key,), attempt_id)
        except Exception:
            raise MacroCaptureError("durable Macro processing claim was not acknowledged") from None
        if claimed != (event_key,):
            raise MacroCaptureError("original Macro message was already claimed; provider retry is forbidden")
        token = self.begin(
            context_json,
            trigger=trigger,
            identity=identity,
            source=source,
            _claim=(scope, event_key, attempt_id),
        )
        self._claim_bindings[token.capture_id] = (scope, event_key, attempt_id)
        return token

    async def complete_claim(self, token: MacroInputCapture) -> None:
        repository = self.processing_claims
        binding = self._claim_bindings.get(token.capture_id) if type(token) is MacroInputCapture else None
        if (
            type(token) is not MacroInputCapture
            or repository is None
            or self._tokens.get(token.capture_id) is not token
            or token.capture_id not in self._output_acknowledged
            or binding is None
            or (token.claim_scope, token.claim_event_key, token.claim_attempt_id) != binding
            or binding[0] != self.processing_scope_sha256
            or binding[1]
            != hashlib.sha256(
                (token.source + "\0" + dict(token.identity)["message_id"]).encode("utf-8")
            ).hexdigest()
            or token.capture_id in self._claim_completion_attempted
        ):
            raise MacroCaptureError("captured Macro input has no durable processing claim")
        self._claim_completion_attempted.add(token.capture_id)
        try:
            completion = cast(
                Awaitable[object], repository.complete_batch(binding[0], (binding[1],), binding[2])
            )
            result = await completion
            if result is not None:
                raise ValueError("unexpected processing completion result")
        except Exception:
            raise MacroCaptureError("durable Macro processing completion was not acknowledged") from None

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
        _claim: tuple[str, str, str] | None = None,
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
        if _claim is not None:
            scope, event_key, attempt_id = _claim
            if (
                self.processing_claims is None
                or scope != self.processing_scope_sha256
                or event_key != hashlib.sha256((source + "\0" + message_id).encode("utf-8")).hexdigest()
                or len(attempt_id) != 64
                or any(char not in "0123456789abcdef" for char in attempt_id)
            ):
                raise MacroCaptureError("durable Macro claim does not match its original input")
        token = MacroInputCapture(
            capture_id,
            record,
            at_ms,
            tuple(sorted(identity.items())),
            source,
            trigger,
            *(_claim if _claim is not None else (None, None, None)),
        )
        # Fence before persistence: partial writes are never retried or adopted.
        self._pending[capture_id] = token
        self._tokens[capture_id] = token
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
        self._output_acknowledged.add(token.capture_id)
        return record
