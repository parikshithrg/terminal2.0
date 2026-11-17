# Workspace 2.0

Workspace 2.0 is an independent Koyfin-inspired EOD market-intelligence workspace.
Its server, data contracts, Kite authentication, calculations and SQLite stores
all live inside this project. PG-terminal does not need to be installed or running.

## Current scope

- Dashboard, market state, F&O positioning, stock research and long-term Portfolio.
- Compact equal-card information layout; no chart dependency.
- Dark and light themes.
- Honest missing-data states.
- Direct Zerodha Kite EOD collection plus official public-data collectors.
- Workspace-owned global-risk EOD collection for Brent crude, India VIX,
  USD/INR, S&P 500, U.S. VIX and the U.S. Dollar Index. Every series stores its
  latest completed close, 20-session change, source symbol, retrieval timestamp
  and independently dated observation. The evidence gate requires fresh Brent,
  India VIX, USD/INR and S&P 500 observations; optional series may fail without
  being silently treated as core coverage.
- Independent `workspace_market.sqlite3` and `workspace_snapshots.sqlite3` stores.
- Official NSE historical F&O bhavcopy backfill covering both legacy and UDiFF
  formats. The importer caches every source archive with its checksum, retains
  the nearest unexpired futures contract per symbol/session and counts only
  positive-OI same-contract comparisons separated by no more than seven calendar
  days. The refresh API is
  `/api/workspace/fno-archive/refresh?from=YYYY-MM-DD&to=YYYY-MM-DD`; rollover
  baselines are stored but never counted as directional confirmation.
- Background stock-research calculations, beginning with a long-only 20/50-session
  simple-moving-average crossover. The API is available at
  `/api/backtests/moving-average-crossover` and accepts optional `symbol`, `fast`
  and `slow` query parameters. A second long-or-cash trend model uses a 55-session
  Donchian entry breakout with a 20-session exit channel. It is available at
  `/api/backtests/donchian-breakout` with optional `symbol`, `entry` and `exit`
  parameters. Three additional long-or-cash models are available at
  `/api/backtests/ema-crossover`, `/api/backtests/time-series-momentum` and
  `/api/backtests/triple-sma-alignment`. Every completed-close state change is
  executed only at the next valid session open. The prior position owns the
  overnight gap, the new position owns the subsequent open-to-close move, and a
  transition is rejected when its required opening price is unavailable. The
  five-model suite is research evidence rather than an order system. Five
  long-or-cash mean-reversion models use the same accounting
  contract: RSI(2) oversold recovery, Bollinger-band re-entry, price z-score
  reversal, a 5/200 pullback-in-uptrend rule, and three-day selloff recovery.
  Their APIs are `/api/backtests/rsi-mean-reversion`,
  `/api/backtests/bollinger-mean-reversion`,
  `/api/backtests/zscore-mean-reversion`, `/api/backtests/pullback-uptrend` and
  `/api/backtests/selloff-recovery`. Every model also reports a completed-trade
  edge p-value from a two-sided one-sample Student t-test against a zero mean;
  fewer than two completed trades are reported as unavailable rather than
  statistically significant.

### Trading-cost model

Every strategy reports gross and estimated net evidence. The net calculation uses
the published Zerodha/NSE equity-delivery schedule effective 01-Mar-2026: zero
retail delivery brokerage, 0.1% STT on both sides, NSE cash-market transaction and
IPFT charges, SEBI turnover charges, 18% GST on service charges, 0.015% buy-side
stamp duty and the published Zerodha sell-side DP charge. The default research
assumption is a ₹50,000 position, 5 bps slippage on each side and zero additional
market impact. The current schedule is applied uniformly to history and does not
reconstruct historical rate changes.

`/api/costs/equity-delivery` exposes the auditable calculator. Query parameters are
`buy`, `sell`, `capital`, `slippage_bps`, `impact_bps` and `include_dp`. Rates and
source URLs are returned with every response.

### Backtest comparability contract

All ten backtest engines publish v8 results over the same 252 completed NIFTY 50
sessions, ending on the latest session with at least 95% stored-stock coverage.
Each model receives its required warm-up immediately before that common window.
Universe rankings retain that comparable 252-session contract. Selected-stock
detail requests use `history=full` and evaluate every continuous stored session
through the same common cutoff after strategy warm-up, so multi-year regimes are
included whenever local history permits.
Stocks with stale data, missing reference sessions, inadequate history, invalid
OHLC rows, or an unexplained close jump above 50% remain visible but are marked
quality-gated and excluded from profit-factor rankings. Every result also reports
candle sources, retrieval timestamps and corporate-action adjustment provenance;
adjustment provenance is an enforced ranking gate.

Selected-stock v8 results publish `chronological_validation`, a first
60/20/20 time-ordered holdout contract. Strategy parameters are frozen before the
validation and untouched-test periods, each boundary resets the position, and the
untouched test is named as the primary published period. This is deliberately a
single chronological split. They now also publish `rolling_walk_forward`: an
expanding minimum 504-session training window followed by fixed 126-session
validation and 126-session untouched-test blocks, advanced 126 sessions per fold.
Every fold keeps the baseline strategy parameters frozen. Test folds ending inside
the reviewed point-in-time NIFTY 500 window carry their dated membership and
classification gate; earlier folds are explicitly labelled outside reviewed
universe coverage. No holdout or fold result is yet used to rank the full stock
universe. Each selected-stock walk-forward response now includes a conservative
`walk-forward-stability-gate-v1` admission decision. It requires at least three
point-in-time-admitted test folds, 15 completed trades, at least 60% positive test
folds, median net profit factor of 1.10 or more, positive median net trade edge and
no admitted test-fold drawdown worse than 25%. A failed check keeps the observation
out of future out-of-sample rankings; the existing 252-session table ordering is
explicitly labelled exploratory.

### Corporate-action contract

`/api/workspace/corporate-actions` exposes the local event ledger and provider
policy. `/api/workspace/corporate-actions/refresh` retrieves an official NSE
equity corporate-action window and merges its coverage into the local audit
record. The normal manual **Refresh data** action updates the most recent window.

Kite historical candles are recorded as provider-reported adjusted for splits,
bonuses, rights issues, mergers/demergers and extraordinary dividends. NSE events
requiring manual symbol-lineage, delisting or unclassified treatment block a
backtest rank until reviewed. Ordinary dividends are preserved as cash events but
are not added to price returns, so all published strategy and buy-and-hold figures
remain price-return rather than total-return evidence.

### Point-in-time universe contract

`/api/workspace/point-in-time-universe` exposes the local NIFTY 500 membership
ledger. An optional `as_of=YYYY-MM-DD` query reconstructs membership backward
from the latest official constituent snapshot by reversing reviewed NSE Indices
replacement notices. Version 2 stores 163 events across ten official notices,
including the 23-Sep-2025 and 30-Sep-2025 changes, the 31-Dec-2025, 30-Mar-2026
and 30-Sep-2026 broad-market reviews, and reviewed corporate-action changes. The dated
press-release catalog review covers NIFTY 500 membership changes through
12-Nov-2026.

Temporary corporate-action constituents use a separate lineage interval ledger.
For the Vedanta demerger, the contract preserves all four zero-price dummy symbols,
their 15-Jun-2026 transition to listed securities, their individual official exit
dates, and whether the active identity was investable on the requested date.

Historical classification is now evidence-gated rather than copied backward from
the present. Six archived copies of the official NIFTY 500 constituent CSV provide
dated sector labels, while the official 30-Sep-2025, 30-Mar-2026 and 30-Sep-2026 Nifty 100,
Midcap 150 and Smallcap 250 reconstitution ledgers reconstruct the size bucket in
force on the requested date. Constituents admitted between archived snapshots use
an explicit official-event ledger; the next official constituent file confirms the
sector and the response labels that provenance as subsequent confirmation. The
ledger also reconstructs the HEG-to-HEGAM and AKZOINDIA-to-JSWDULUX symbol changes
and the HEG Graphite, Advent, SKF India Industrial and KWIL corporate-action
intervals. Every audited transition date from 15-Jul-2025 through the 12-Nov-2026
anchor passes membership, sector and size coverage, and the anchor resolves to the
official 501 members. All 34,903 required sessions for 90 reviewed historical exits
are stored from official NSE cash archives. Current-
universe strategy rankings still carry a survivorship-bias warning until rolling
walk-forward evaluation consumes the dated membership contract for every window.

`/api/workspace/removed-constituent-history-audit` reports the candle coverage for
every reviewed exit. Ordinary removals require up to 451 completed sessions: the
252-session comparison window plus the longest 199-session strategy warm-up.
Listed demerger entities require only their actual index-eligible interval, while
zero-price dummy phases are deliberately excluded from the candle requirement.
After Kite login, Refresh data also runs the removed-constituent remediation
stage. It uses the audit windows, retains existing adjustment provenance, and
returns unavailable symbols rather than silently substituting current members.
The independent NSE archive remediation endpoint,
`/api/workspace/removed-constituent-history-backfill`, caches official full cash
bhavcopies and records every requested symbol/session as present or absent. The
current audit is complete for all 90 reviewed exits: 34,903 of 34,903 required
tradable sessions are stored. Confirmed pre-listing and other non-trading sessions
are excluded only after the official daily archive verifies the absence.

### Portfolio contract

Portfolio is a local-only long-term holdings ledger, deliberately separated from
the trading-research engine. It records completed buy/sell transactions, explicit
cash flows and duplicate-safe broker CSV imports; it cannot create or submit an
order. `/api/portfolio/overview` publishes completed-session valuation and
reconciliation. `/api/portfolio/performance` publishes cash-flow-neutral TWR,
annualised TWR, dated XIRR and a Nifty 50 comparison. `/api/portfolio/risk-analysis`
publishes allocation, HHI/effective-position concentration, drawdown and P&L
contribution. Missing cash history, benchmark sessions or held-stock prices gate
the relevant result rather than generating an estimate.

To begin, open **Portfolio**, record contributions and withdrawals, then enter
completed transactions or import a compatible equity tradebook CSV. Record
dividends, interest, taxes and account fees separately. Review the reconciliation
panel before relying on performance or risk evidence. All portfolio records remain
inside `data/workspace_market.sqlite3`; back up that file before reinstalling or
moving the workspace.

## Run locally

1. From this folder, run `python3 server.py`.
2. Open `http://127.0.0.1:8520/`.
3. Use **Kite login** when a new completed-session refresh is required.

The Kite access token and API secret remain memory-only. Refresh writes admitted
completed-session candles directly to `data/workspace_market.sqlite3`.

The normal **Refresh data** action also updates the public global-risk series via
`/api/workspace/global-risk/refresh`. These observations are stored in
`global_risk_observations`; they do not require a Kite session.

## Refresh local snapshots

Run `python3 snapshot_store.py update` to store the current NIFTY 500 membership,
official Nifty 100 / Midcap 150 / Smallcap 250 size classifications, the latest
NSE integrated-financial-filing metadata for current constituents, and the latest
completed-session India, US, Germany, UK and Japan sovereign curves.
The updater records each source independently in
`data/workspace_snapshots.sqlite3`; one source failure does not discard the others.

The filing intake stores source timestamps and direct NSE XBRL links. Its normalized
fact layer supports the NSE industrial, bank, life-insurance and general-insurance
taxonomies while preserving failed downloads in an audit table. Value/Core/Growth
cells remain blank until comparable prior-period facts and completed-session prices
pass the documented style coverage checks; the dashboard never fills them by proxy.

### NIFTY 500 size × style methodology

- Size is the dated official Nifty 100, Nifty Midcap 150 and Nifty Smallcap 250 membership.
- Value is quarterly earnings yield using reported EPS (or reported profit and paid-up capital when safe).
- Growth is the mean of available year-over-year revenue, profit and EPS growth, with extreme ratios capped.
- Value/Core/Growth are relative terciles of growth-versus-value rank inside each size universe.
- Each displayed cell is the equal-weight one-session return from the latest official NSE bhavcopy.
- A filing is eligible only when its public filing timestamp is on or before that bhavcopy session.
- Publication requires at least 70% classification coverage and at least five admitted stocks per cell.

### Sovereign-yield range indicators

Each country/tenor is compared only with its own trailing five-year history. Below
the 65th percentile is shown as normal (green), the 65th–85th percentile as elevated
(amber), and the 85th percentile or higher as high (red). A cell remains limited
instead of being judged when fewer than 30 historical observations are stored.
These are relative level indicators, not claims that lower yields are always better.

Run `python3 snapshot_store.py status` to inspect coverage and the most recent
source-level update results without contacting any external service.

## Project rule

New pages are built here one at a time. Every data contract must be produced by
Workspace-owned code from an identified official source, a direct Kite EOD call,
or an auditable locally stored calculation. Runtime proxies to another application
are not permitted.
