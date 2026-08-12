# kairos-macro-strategist

**Layer 4 — Macro Strategist.** The slow strategic layer uses xhigh reasoning to
set global capital allocation. It never creates individual exchange orders.

## Real inputs and triggers

The service consumes:

- `kairos.account.snapshot` for reconciled equity, balances, PnL, and positions;
- `kairos.market.snapshot` for price history, derivatives, indicators, and regime bias;
- `kairos.system.control` for LLM circuit-breaker state.

A daily schedule or a price decline of at least 10% across an observed one-hour
window produces a `kairos.macro.allocation`. Shock events have a configurable
per-symbol cooldown. The current core contract has no structured macro-release topic,
so CPI and similar surprise-sigma triggers remain available in `ShockDetector` but are
not fabricated from news text.

## Safety and replay behavior

- The model receives a strict Pydantic Structured Outputs schema.
- Missing reconciled account data yields a deterministic defensive allocation instead
  of sending an empty or invented portfolio to the model.
- If the daily schedule fires before account reconciliation, the defensive allocation
  is followed by a separately identified context-recovery allocation as soon as a full
  account snapshot arrives in normal system mode.
- `CONFLICT_SAFE` and `LOCAL_QUANT_MODE` bypass GPT and publish the same defensive
  capital-preservation allocation immediately.
- Every trigger has a deterministic allocation message ID. Completed LLM output is
  cached by trigger so a failed publish retries the identical result.
- Input messages are acknowledged only after validation and all required publishing.
- TaskGroup cancellation always closes both the LLM gateway and message bus.

## Local development

Install [uv](https://docs.astral.sh/uv/) once. The repository pins uv 0.12.3,
Python 3.11, all transitive dependencies, and compatible `kairos-core`/`kairos-llm`
Git revisions:

```powershell
winget install --id astral-sh.uv --exact
uv sync --locked
uv run --locked python -m kairos_macro
```

## Checks

```powershell
uv run --locked ruff check kairos_macro tests
uv run --locked ruff format --check kairos_macro tests
uv run --locked mypy kairos_macro
uv run --locked bandit -q -r kairos_macro -x tests
uv run --locked pytest -q --tb=short
uv build --no-sources
```

CI runs the blocking suite on Linux with Python 3.11 and 3.14, plus Windows with
Python 3.11.

---

Part of the [Kairos](https://github.com/Kairos-cryptoAI/kairos) system. MIT licensed.
