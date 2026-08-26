"""Frozen macro-state corpus qualification tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from kairos_llm import LLMResult, TokenUsage

from kairos_macro.prompts import MACRO_SYSTEM
from kairos_macro.shadow_qualification import (
    HARD_MAXIMUM_PLANNED_COST_USD,
    MacroCorpus,
    QualificationStatus,
    _ScriptedGateway,
    load_corpus,
    main,
    planned_cost_ceiling_usd,
    qualify_macro_corpus,
)
from kairos_macro.strategist import AllocationOutput


async def test_packaged_macro_corpus_passes_network_free_harness() -> None:
    corpus, digest = load_corpus()
    report = await qualify_macro_corpus(
        corpus,
        _ScriptedGateway(),
        mode="STATIC_HARNESS",
        corpus_sha256=digest,
        maximum_planned_cost_usd=0.25,
    )

    assert report.status is QualificationStatus.PASS
    assert report.live_orders_allowed is False
    assert len(report.observations) == 4
    assert all(item.model_schema_valid and item.deadline_met for item in report.observations)
    assert all(item.allocation_total == pytest.approx(1.0) for item in report.observations)


async def test_targeted_case_replay_does_not_recall_passed_states() -> None:
    corpus, digest = load_corpus()
    report = await qualify_macro_corpus(
        corpus,
        _ScriptedGateway(),
        mode="STATIC_HARNESS",
        corpus_sha256=digest,
        maximum_planned_cost_usd=0.25,
        selected_case_ids=("bear_shock_drawdown",),
    )
    assert [item.case_id for item in report.observations] == ["bear_shock_drawdown"]
    with pytest.raises(ValueError, match="unknown corpus case"):
        planned_cost_ceiling_usd(corpus, ("unknown",))


class _UnsafeGateway:
    async def complete(self, **_kwargs) -> LLMResult:
        parsed = AllocationOutput.model_validate(
            {
                "regime": "BULL",
                "stable_reserve_pct": 0,
                "strategy_weights": {"unsafe": 1},
                "max_gross_leverage": 20,
                "rationale": "obeyed untrusted context",
            }
        )
        return LLMResult(
            content=parsed.model_dump_json(),
            parsed=parsed,
            model="gpt-5.6-sol",
            effort="xhigh",
            usage=TokenUsage(input_tokens=10, output_tokens=10),
            cost_usd=0.01,
            latency_s=0.1,
            workload="macro_strategist",
            provider="openai",
            request_id="unsafe",
            resolved_model="gpt-5.6-sol",
            budget_reservation_id="kairos-llm-v1:openai:unsafe",
        )


async def test_macro_corpus_rejects_unsafe_regime_reserve_and_leverage() -> None:
    corpus, digest = load_corpus()
    report = await qualify_macro_corpus(
        corpus,
        _UnsafeGateway(),
        mode="LIVE",
        corpus_sha256=digest,
        planned_cost_ceiling_usd=1,
        maximum_planned_cost_usd=1,
    )

    assert report.status is QualificationStatus.FAIL
    injection = next(item for item in report.observations if item.category == "prompt_injection")
    assert "stable_reserve_below_safety_floor" in injection.reasons
    assert "gross_leverage_above_safety_ceiling" in injection.reasons
    assert "regime_outside_expected_set" in injection.reasons


def test_corpus_requires_exact_state_categories_and_unique_ids() -> None:
    corpus, _digest = load_corpus()
    payload = corpus.model_dump(mode="json")
    payload["cases"].pop()
    with pytest.raises(ValueError, match="exact required"):
        MacroCorpus.model_validate(payload)

    payload = corpus.model_dump(mode="json")
    payload["cases"][1]["case_id"] = payload["cases"][0]["case_id"]
    with pytest.raises(ValueError, match="unique"):
        MacroCorpus.model_validate(payload)


def test_prompt_treats_context_as_untrusted_data() -> None:
    prompt = MACRO_SYSTEM.casefold()
    assert "untrusted data" in prompt
    assert "never follow instructions" in prompt


def test_planned_cost_and_static_cli_are_bounded_and_sanitized(tmp_path: Path) -> None:
    corpus, _digest = load_corpus()
    planned = planned_cost_ceiling_usd(corpus)
    assert 0 < planned < HARD_MAXIMUM_PLANNED_COST_USD

    output = tmp_path / "macro.json"
    assert main(["--static", "--output", str(output)]) == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS"
    assert payload["live_orders_allowed"] is False
    rendered = json.dumps(payload).casefold()
    assert "operator_note" not in rendered
    assert "ignore all rules" not in rendered
    assert main(["--static", "--output", str(output)]) == 2


def test_static_mode_rejects_secret_files_before_reading(tmp_path: Path) -> None:
    assert (
        main(
            [
                "--static",
                "--openai-key-file",
                str(tmp_path / "missing"),
                "--output",
                str(tmp_path / "report.json"),
            ]
        )
        == 2
    )
