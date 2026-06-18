# kairos-macro-strategist

**Layer 4 — Macro-Strategist.** The slow, careful strategic layer (**xhigh** reasoning).
It never trades minute-to-minute; it sets global capital allocation, defends against black
swans and adapts to regime changes.

## When it runs
- **Schedule** — daily / weekly portfolio rebalancing.
- **Shock event** — e.g. the market drops **10% in an hour**, or a macro print lands
  **>2σ** from expectations (`ShockDetector`).

## What it decides
A `StrategicAllocation`: market `regime`, `stable_reserve_pct`, per-strategy `weights` and
`max_gross_leverage`. Example defensive posture from the spec: *60% to stablecoins, 40% on
low-risk delta-neutral strategies*. Invalid model output degrades to exactly that defensive
fallback. Model calls go through [`kairos-llm`](https://github.com/TheLitis/kairos-llm) at
`xhigh` effort.

## Run
```bash
pip install -e ../kairos-core -e ../kairos-llm && pip install -e ".[dev]"
make test
python -m kairos_macro
```
Emits `kairos.macro.allocation`.

---
Part of the [Kairos](https://github.com/TheLitis/kairos) system. MIT licensed.
