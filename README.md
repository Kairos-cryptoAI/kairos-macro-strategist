# kairos-macro-strategist

**Layer 4 — Macro Strategist.** The slow strategic layer selects the explicit
`MACRO_STRATEGIST` LLM workload for its xhigh capital-allocation analysis. Model
and provider selection remain centralized in `kairos-llm`; this service never
creates individual exchange orders.

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

The model context keeps factor families explicit. `market_factors` contains only
fresh technical and derivatives observations together with units and symbol coverage;
`macro_factors` and `onchain_factors` are marked `unavailable` until dedicated
structured bus topics exist. Missing evidence is uncertainty, never a neutral print.
The `regime_hint` is a deterministic strict majority of fresh `quant_bias` values and
the exact counts/method are included as `regime_evidence`.

## Safety and replay behavior

- The model receives a strict Pydantic Structured Outputs schema.
- `KAIROS_ALLOWED_STRATEGY_IDS` is an explicit list of exact `StrategyIntent.strategy_id`
  values. Risk uses those exact dictionary keys; Macro never normalizes aliases, invents
  strategy families, or maps a revision to a different ID. Unknown IDs are rejected even
  at zero weight. Empty configuration bypasses the model. Every defensive fallback is
  **100% stable reserve and no strategy weights**, not a fabricated delta-neutral sleeve.
  The default is `[]`: no strategy is currently PAPER_APPROVED. Adding a name here does
  not grant alpha approval, bypass the Strategy Engine registry, or permit trading.
  `technical-canary` is prohibited; technical DEV scenarios must remain entirely LLM-free.
- Missing reconciled account data yields a deterministic defensive allocation instead
  of sending an empty or invented portfolio to the model.
- Stale/future account data or fewer than `KAIROS_MINIMUM_FRESH_MARKETS` fresh market
  snapshots also yield the defensive allocation without an LLM call. A frozen clock is
  injectable in tests, so all freshness decisions are reproducible offline.
- A one-hour crash requires a baseline within the configured tolerance around the
  one-hour mark; an arbitrary older price is never mislabeled as a 1h move.
- Model allocations must assign exactly 100% of capital across stable reserve and
  named strategies; under-allocation is rejected as ambiguous.
- If the daily schedule fires before account reconciliation, the defensive allocation
  is followed by a separately identified context-recovery allocation as soon as a full
  account snapshot arrives in normal system mode.
- `CONFLICT_SAFE` and `LOCAL_QUANT_MODE` bypass the `MACRO_STRATEGIST` workload and
  publish the same defensive capital-preservation allocation immediately.
- Every trigger has a deterministic allocation message ID. Completed LLM output is
  cached by trigger so a failed publish retries the identical result.
- Input messages are acknowledged only after validation and all required publishing.
- TaskGroup cancellation always closes both the LLM gateway and message bus.

Sol calls reserve capacity in the shared PostgreSQL campaign
`kairos-dev-qualification-v1` before contacting OpenAI. The cumulative qualification
ceiling is `$12` for OpenAI and `$1` for DeepSeek, including prior committed and
outstanding reservations across services and billing months. One authoritative
paid-shadow database must be used by every paid caller; independent databases do not
share a cap. Campaign adoption requires the persistence operator's explicit historical
receipt; a fresh/unregistered database does not grant a fresh allowance.
An in-memory runtime denies paid calls and falls
back defensively. Macro accepts both legacy DRY_RUN account snapshots and strict,
reconciled `AccountSnapshotV2` messages from the isolated PAPER contour.

## Frozen macro-state qualification

`kairos-macro-qualify` replays a versioned bull, bear-shock, mixed-uncertainty and
prompt-injection corpus through the strict Sol allocation schema. Each state defines
an allowed regime set, a minimum stable reserve and a maximum gross-leverage ceiling.
The gate also requires full allocation, complete paid-call provenance and deadline
completion. It never publishes the resulting allocation.

```sh
uv run --locked kairos-macro-qualify --static \
  --output /tmp/kairos-macro-harness.json
```

The network-free mode validates the harness without cost and explicitly permits only
`fixture_macro_strategy_v1`; that fixture is not a tradable or approved strategy.
A live shadow run requires repeatable `--strategy-id EXACT_ID` arguments before
reading any secret file, as well as OpenAI,
Redis and PostgreSQL one-value secret files, reserves every Sol call in the shared
durable `kairos-llm-v1/openai` ledger and refuses a planned run above `$0.25` by
default (hard maximum `$0.50`). Every report sets `live_orders_allowed=false`.
Use repeatable `--case CASE_ID` selectors after a failure so passed Sol cases are
not recalled. The qualification envelope reserves up to 1024 output tokens for
xhigh reasoning and commits only the provider-reported actual cost.
The planned token ceiling includes the trusted allowlist added to the system prompt.
Schema-valid output with an unknown strategy still fails qualification; a defensive
fallback is not counted as a successful model response.

## Startup history recovery

Before scheduling or consuming new input, the durable service reads existing
`event_audit` facts in one PostgreSQL repeatable-read, read-only transaction. No new
schema migration or historical rewrite is introduced. The normal durable-bus startup
still applies its registered migrations and dispatches already-committed outbox effects.
History restoration itself never calls a model, triggers historical shocks, publishes a
new allocation, or ACKs transport messages.

The recovery window is bounded by `KAIROS_ACCOUNT_HISTORY_WINDOW_S` (seven days) and
`KAIROS_PRICE_HISTORY_WINDOW_S` (two hours). It restores reconciled account/equity
history, per-symbol prices, the latest previously known control state, prior allocation
identities, schedule recovery state and observed shock cooldowns. Produced/capture
timestamps are preserved; old data must still pass the normal freshness checks.
Queries exclude facts produced or persisted after the recovery cutoff. A causal snapshot
does not imply that missing observations existed, or that a full week was observed.

- `KAIROS_HISTORY_RESTORE_MAX_ROWS` limits the total audit load; exceeding it fails
  startup rather than silently returning a partial window. Query/validation failures
  prevent all new model calls. Restore service connectivity or adjust a justified bound
  and restart; do not delete conflicting evidence or reset it to an empty history.
- Use the optional exact `KAIROS_ACCOUNT_HISTORY_ACCOUNT_ID` and
  `KAIROS_ACCOUNT_HISTORY_VERSION=legacy|v2` filters for a shared audit. Without them,
  multiple account/exchange/version/environment identities fail closed. A V2 account's
  trading mode and EVEDEX profile must also remain identical throughout its history;
  DEV and PROD equity are never combined. The isolated paid-shadow database should
  contain only the intended account/environment.
- Audit envelope/payload identity, finite values, conflicting message IDs and conflicting
  same-timestamp observations are checked. Exact replays do not add samples. Older
  account/market deliveries do not replace newer state and are counted as reorders.
  Reconciliation failure invalidates account context and clears its continuous segment.
- `KAIROS_ACCOUNT_HISTORY_MAX_GAP_S` and `KAIROS_MARKET_HISTORY_MAX_GAP_S` are explicit
  operational tolerances, both defaulting to 120 seconds. These snapshots are not a
  guaranteed-period bar stream: the thresholds are not inferred producer intervals.
  An observed gap starts a new continuous segment. A shock cannot span the missing
  interval and account coverage cannot claim the discarded period as a full week.
  `KAIROS_HISTORY_SAMPLE_LIMIT` separately bounds each in-memory time series.
- `history_status` exposes state/cutoff/row count and gap/reorder/eviction counters without
  portfolio PnL. Integrity failure remains fail-closed until a clean restart. Market
  and account status still describe only observed coverage, not missing macro/on-chain feeds.
- Old trigger redelivery beyond the RAM cache resolves the original allocation by its
  durable message ID. It republishes the identical safe result without another model
  call. If an old allocation names a now-unconfigured strategy, replay stops; the old
  allocation is neither rewritten under the same ID nor replaced by another paid result.

The SQL drill in `tests/test_history_postgres.py` is opt-in and refuses every database
except the exact disposable `kairos_macro_test_20260912`. It checks the parsed URL and
resolved `current_database()` before its first write, refuses existing evidence, inserts
only synthetic audit rows, then verifies read-only causal reload, scope filtering, row
overflow refusal and reconnect parity. It never migrates or deletes a database. The
test database and its 68 fixture rows remain available as drill evidence. The optional
`tests/Dockerfile.history` reuses the named local runtime/test images without downloading
dependencies or including credentials; it is not a deployment image.

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

## Runtime delivery durability

With Redis, consumed IDs, handler outputs and completion are committed through
`kairos-persistence`; Redis is ACKed only after PostgreSQL commits. Configure
`KAIROS_PERSISTENCE_DATABASE_URL` through the deployment secret provider. The
in-memory backend intentionally bypasses persistence for local tests.

---

Part of the [Kairos](https://github.com/Kairos-cryptoAI/kairos) system. MIT licensed.
