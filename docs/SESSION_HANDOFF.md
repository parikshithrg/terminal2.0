# Workspace 2.0 — session handoff

Updated: 05 November 2026

## Latest work — point-in-time universe foundation

- Analytics hardening step 5 is in progress.
- Added `universe_membership_events` and `universe_classification_snapshots` to
  the Workspace-owned market database.
- Seeded 123 reviewed NIFTY 500 inclusion/exclusion events from seven official
  NSE Indices notices: three broad-market reconstitutions plus four reviewed
  demerger/amalgamation changes inside the active window.
- Added four source-linked Vedanta lineage intervals. Reconstruction now
  distinguishes non-investable zero-price dummies, listed resulting companies,
  and their staggered official index exits.
- Added `GET /api/workspace/removed-constituent-history-audit`. The first live
  audit covers 67 reviewed exits: 4 complete histories, 3 partial histories and
  60 missing instruments. The point-in-time coverage flag now uses this audit
  rather than a hard-coded incomplete value.
- Added a fourth Kite EOD refresh stage that uses the audit to resolve still-
  listed symbols from the full NSE instrument master and backfill only the
  required pre-exit windows. Delisted/unavailable symbols are returned explicitly.
- Added `GET /api/workspace/point-in-time-universe?as_of=YYYY-MM-DD`.
- The contract reconstructs membership backward from the latest validated
  official snapshot, but earlier dates remain `partial_official_reconstruction`
  and are not ranking-admissible.
- Backtest evidence now reports the partial point-in-time status and keeps the
  survivorship-bias warning. Do not mark milestone 5 complete until all ad-hoc
  notices, removed/delisted histories, symbol lineage, and dated sector/size
  classifications are covered.
- Next action: fetch the 60 missing and 3 partial removed-constituent histories,
  while continuing the intervening ad-hoc NIFTY 500 notice review.

## Current restart point

Resume analytics hardening step 5, point-in-time universes. Workspace 2.0 is
running on port 8520, but Kite is disconnected after the server restart. First,
log in to Kite and run **Refresh data** so the new fourth EOD stage can attempt
the 60 missing and 3 partial removed-constituent histories. Re-run
`/api/workspace/removed-constituent-history-audit`, record which symbols remain
unavailable from Kite, then continue the intervening ad-hoc NIFTY 500 notice
review and historical sector/size snapshots. Keep the survivorship gate partial.

Verification checkpoint: 38 automated tests pass. The latest audit covers 67
reviewed exits with 4 complete histories and 489 of 28,434 required sessions
stored. The live server correctly rejects the remediation endpoint with
`kite_not_connected` until a fresh memory-only Kite session is established.

## Completed today

- Added profit factor, average trade edge and current invested/cash state to the
  NIFTY 500 research table.
- Preserved the adaptive table height and selected-stock evidence panel.
- Kept the original long-or-cash 20/50-session simple-moving-average crossover.
- Added an independent long-or-cash 55/20-session Donchian trend model:
  - enter above the prior 55-session completed-close high;
  - exit below the prior 20-session completed-close low;
  - apply every signal from the following completed session;
  - report return, excess return, annualised return and volatility, drawdown,
    exposure, completed trades, win rate, profit factor and trade expectancy.
- Added a model selector inside the collapsible selected-stock backtest panel.
- Added `/api/backtests/donchian-breakout` with optional `symbol`, `entry` and
  `exit` parameters.
- Added 20/50 EMA crossover, 63-session absolute momentum and 20/50/200
  triple-SMA alignment models to the same selected-stock evidence panel.
- Added three independent API routes and expanded the model selector from two to
  five trend-following strategies.
- Added five mean-reversion strategies to the same engine and selector: RSI(2),
  Bollinger re-entry, 10-session price z-score, 5/200 pullback in uptrend, and
  three-day selloff recovery.
- Expanded the automated backtest suite to twenty-four passing tests.
- Added a universe-level backtest ranking strip to Stock Research. Any of the ten
  engines can populate the table's profit-factor evidence column and sort it
  highest-first or lowest-first without rerunning cached models.
- Added completed-trade p-values to all ten engines, the research table and the
  selected-stock evidence panel using a two-sided one-sample Student t-test of
  mean trade return versus zero. Results with fewer than two trades remain blank.

## Verified example

For RELIANCE through 04 November 2026, the 55/20 Donchian model displayed a cash
state, 2.54x profit factor and +4.08% average completed-trade edge. These are gross
historical observations and not a recommendation.

## Next working decision

Decide how the two strategy results should contribute to the Stock research
evidence hierarchy. Sensible next options are:

1. retain the SMA result in the table and keep Donchian in the detail selector;
2. show a compact two-model agreement state in the table; or
3. build a composite trend-evidence score only after thresholds and validation
   rules are explicitly approved.

Do not connect these short-term research models to the long-term Portfolio page,
and do not add order placement. Portfolio analysis and trade research remain
separate.
