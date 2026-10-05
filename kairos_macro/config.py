from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from kairos_core.config import CoreSettings
from pydantic import Field, field_validator, model_validator

if TYPE_CHECKING:
    from kairos_core.contracts.regime_capability import RegimeCapabilityPolicyV1

STRATEGY_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$"


def validate_strategy_ids(values: tuple[str, ...]) -> tuple[str, ...]:
    """Validate exact identifiers without renaming or approving a strategy."""
    if len(values) > 64 or len(values) != len(set(values)):
        raise ValueError("allowed strategy IDs must be unique, at most 64")
    if any(not re.fullmatch(STRATEGY_ID_PATTERN, value) for value in values):
        raise ValueError("allowed strategy IDs must be exact non-whitespace identifiers")
    if any(value.casefold().startswith("technical-canary") for value in values):
        raise ValueError("technical-canary must never receive a Macro LLM allocation")
    return tuple(sorted(values))


class MacroSettings(CoreSettings):
    service_name: str = "kairos-macro-strategist"
    allowed_strategy_ids: tuple[str, ...] = ()
    account_history_account_id: str | None = Field(default=None, min_length=1)
    account_history_version: Literal["legacy", "v2"] | None = None
    run_cron_hour_utc: int = Field(default=0, ge=0, le=23)
    crash_pct_1h: float = Field(default=10.0, gt=0)
    shock_cooldown_s: float = Field(default=3600.0, gt=0)
    price_history_window_s: float = Field(default=7200.0, ge=3600.0)
    shock_baseline_tolerance_s: float = Field(default=300.0, gt=0)
    account_history_window_s: float = Field(default=604800.0, gt=0)
    account_snapshot_max_age_s: float = Field(default=60.0, gt=0)
    market_snapshot_max_age_s: float = Field(default=120.0, gt=0)
    max_future_skew_s: float = Field(default=5.0, ge=0)
    minimum_fresh_markets: int = Field(default=1, ge=1)
    scheduler_poll_s: float = Field(default=30.0, gt=0)
    replay_cache_size: int = Field(default=256, ge=1)
    history_restore_max_rows: int = Field(default=100_000, ge=1, le=1_000_000)
    history_sample_limit: int = Field(default=100_000, ge=2, le=1_000_000)
    account_history_max_gap_s: float = Field(default=120.0, gt=0)
    market_history_max_gap_s: float = Field(default=120.0, gt=0)
    regime_policy_profile: Literal["legacy-v1", "adaptive-research-v1"] = "legacy-v1"
    regime_policy_file: Path | None = None
    regime_policy_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    regime_source_set_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    regime_strategy_revisions: tuple[str, ...] = ()
    regime_capital_max_age_s: float = Field(default=26 * 60 * 60, gt=0, allow_inf_nan=False)

    @field_validator("allowed_strategy_ids")
    @classmethod
    def exact_strategy_ids(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return validate_strategy_ids(values)

    @field_validator("regime_strategy_revisions")
    @classmethod
    def exact_regime_revisions(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) != len(set(values)):
            raise ValueError("adaptive strategy revisions must be unique")
        for value in values:
            if value.count("@") != 1:
                raise ValueError("adaptive strategy revisions require exact '<strategy_id>@<revision>'")
            strategy_id, revision = value.split("@", maxsplit=1)
            validate_strategy_ids((strategy_id,))
            if not re.fullmatch(STRATEGY_ID_PATTERN, revision):
                raise ValueError("adaptive strategy revision must be exact and normalized")
        return tuple(sorted(values))

    @model_validator(mode="after")
    def explicit_regime_opt_in(self) -> MacroSettings:
        inputs = (self.regime_policy_file, self.regime_policy_sha256, self.regime_source_set_sha256)
        if self.regime_policy_profile == "legacy-v1":
            if any(value is not None for value in inputs) or self.regime_strategy_revisions:
                raise ValueError("regime artifacts require an explicit adaptive-research-v1 opt-in")
        else:
            if any(value is None for value in inputs) or not self.regime_strategy_revisions:
                raise ValueError(
                    "adaptive regime policy requires independently frozen file/hash/source set/revisions"
                )
            if self.account_history_version != "v2" or self.account_history_account_id is None:
                raise ValueError("adaptive capital requires an exact full v2 account scope")
            if any(
                value.split("@", maxsplit=1)[0] not in self.allowed_strategy_ids
                for value in self.regime_strategy_revisions
            ):
                raise ValueError("adaptive revisions cannot widen the existing Macro strategy allowlist")
        return self

    def load_regime_policy(self) -> RegimeCapabilityPolicyV1 | None:
        if self.regime_policy_profile == "legacy-v1":
            return None
        from kairos_core.contracts.regime_capability import RegimeCapabilityPolicyV1, validate_policy_binding

        if (
            self.regime_policy_file is None
            or self.regime_policy_sha256 is None
            or self.regime_source_set_sha256 is None
        ):
            raise ValueError("adaptive regime policy requires independently frozen file/hash/source set")
        policy = RegimeCapabilityPolicyV1.model_validate_json(self.regime_policy_file.read_bytes())
        allowed: set[tuple[str, str]] = set()
        for value in self.regime_strategy_revisions:
            strategy_id, revision = value.split("@", maxsplit=1)
            allowed.add((strategy_id, revision))
        validate_policy_binding(
            policy,
            expected_sha256=self.regime_policy_sha256,
            source_set_sha256=self.regime_source_set_sha256,
            allowed_strategy_revisions=allowed,
        )
        return policy
