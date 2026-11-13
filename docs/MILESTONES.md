# Workspace 2.0 milestones

## Current progress

The independent Workspace data foundation and NIFTY 500 equity-factor drill-down
are complete. Every size × style grid cell
is an accessible action button. Selecting a cell opens its admitted-stock list in
place, preserves the dashboard context, and shows the evidence explaining why each
stock belongs in that cell. No order placement is in scope.

0. **Independent data engine — operational.** Removed the PG-terminal runtime,
   HTTP-proxy and database-path dependencies. Workspace now owns its market-history
   database, official-source snapshot database, API contracts, calculations and
   memory-only Kite login. Future EOD refreshes write directly to Workspace storage.
   The pre-existing validated history was migrated once as the starting dataset;
   no live link to its former project remains.

1. **Dashboard — active.** The Koyfin-inspired executive EOD dashboard and its
   official-source snapshot foundation are operational. The nine point-in-time
   NIFTY 500 size × style cells are published, global sovereign curves are stored,
   and each yield tenor is placed in its own trailing five-year normal/elevated/high
   range. All nine factor cells now open a searchable, sortable stock list with
   symbol, company, industry, completed-session return, value score, growth score,
   reporting periods and evidence timestamp without navigating away or placing orders.
   Current breadth is constrained to the latest official NIFTY 500 snapshot;
   removed constituents remain stored for point-in-time backtests but cannot enter
   the current 20-session, 50DMA or 200DMA denominators.
2. **Market state — operational.** Sentiment, NIFTY 500 breadth, realised
   volatility, derivatives context, historical regime analogues, watchpoints and
   evidence audits are now available in a compact overview-first page. The page
   uses the existing completed-session dashboard contract, keeps missing evidence
   explicit and does not publish forecasts or trade instructions.
3. **F&O positioning — operational.** The dedicated page now presents current
   same-contract price/OI quadrants, explicit coverage and history gates, ranked
   price and open-interest movers, and a sortable, filterable 208-contract detail
   table. The official NSE derivatives bhavcopy archive backfill now supports both
   legacy and UDiFF formats, caches each source file with a checksum, stores only
   the historical front-month futures contract and excludes rollover baselines
   from comparable-session counts. The completed continuous load wrote 173,832 rows
   from 848 official files spanning 01-Jun-2023 through 09-Nov-2026 and produced
   825 validated same-contract sessions from 849 stored sessions; 24 rollover or
   invalid-baseline sessions remain stored but excluded. The 252-session history
   gate now passes. Current observations remain visible separately and regime
   scoring remains disabled until a directional model is validated.
4. **Stock research — operational foundation.** NIFTY 500 and F&O universes are
   selectable, sortable and filterable; NIFTY 500 evidence is persisted locally
   between Kite sessions. A background backtest engine now exposes a basic,
   long-only 20/50-session simple-moving-average crossover for the stored stock
   universe or an individual symbol. A second, independent 55/20-session Donchian
   breakout model enters above the prior channel high and exits below the shorter
   prior channel low. The suite also includes a 20/50 EMA crossover, 63-session
   absolute momentum and 20/50/200 triple-SMA alignment. All five use shifted completed-session signals, report coverage,
   drawdown, trade statistics and explicit limitations, and are not presented as a
   separate page. The calculations are selectable beside the selected stock's
   evidence state and open in a collapsible inline evidence panel without implying
   an order instruction. Five mean-reversion models now sit beside the five trend
   models: RSI oversold recovery, Bollinger re-entry, price z-score reversal,
   pullback-in-uptrend and three-day selloff recovery. All ten models remain
   descriptive background research with next-session signal attribution. Stock
   Research now supports selecting any of the ten engines and ranking either the
   NIFTY 500 or F&O universe by that engine's profit factor in ascending or
   descending order; computed universe payloads are cached for the browser session.
   The table and selected-stock panel also expose a two-sided one-sample t-test
   p-value for average completed-trade return versus zero. This is descriptive
   evidence only and does not correct for multiple strategy or stock comparisons.

5. **Analytics-engine hardening — next workstream.** Freeze the current ten
   strategies as the baseline suite and improve their research validity before
   adding any further models. Complete the following steps in order:

   1. **Executable signal convention — complete.** Every model now observes its
      signal only after a completed EOD close and executes a state change at the
      next valid session open. The prior position owns the overnight gap and the
      new position owns the open-to-close move. Missing opening prices reject the
      transition, final-session signals remain explicitly pending, and every v3
      result contract reports the executed position, target signal, execution
      price, execution date and rejected/pending counts.
   2. **Trading-cost model — complete.** Added a versioned Zerodha/NSE equity-
      delivery calculator covering brokerage, STT, NSE transaction/IPFT charges,
      SEBI turnover charges, GST, buy-side stamp duty and sell-side DP charges.
      Position capital, slippage, market impact and DP inclusion are configurable
      through the calculator API. All ten strategy contracts now publish gross and
      estimated net return, gross and net profit factor, gross and net trade edge,
      net significance, buy-and-hold costs, charge breakdown, turnover events and
      cost drag. Stock Research rankings and evidence tiles use net evidence while
      retaining gross evidence for comparison. The current published schedule is
      applied uniformly to history and is explicitly not a historical-rate model.
   3. **Data-quality and comparability gates — complete.** All ten models now use
      a common 252-completed-session NIFTY 50 window ending on the latest session
      with at least 95% stock coverage, preceded by each strategy's own warm-up.
      This remains the universe-ranking contract. Selected-stock evidence now
      additionally runs over every continuous stored session through that common
      cutoff, producing a multi-year full-history study when sufficient history
      is available.
      Before calculation, the v4 contract audits stale closes, missing reference
      sessions, minimum history, invalid OHLC relationships, unaudited price jumps
      above 50%, candle sources and retrieval timestamps. Adjustment provenance is
      reported but deliberately becomes a hard gate in step 4. Gated stocks remain
      visible in Stock Research with the failed checks, while profit factor, edge
      and p-value stay absent so they cannot enter the ranking.
   4. **Corporate-action contract — complete.** Added a local NSE event ledger
      covering the full common evaluation and longest strategy warm-up window,
      with source/retrieval timestamps and explicit classification for splits,
      bonuses, rights issues, ordinary and special dividends, buybacks, mergers,
      demergers, symbol changes and delistings. The v5 strategy contract enforces
      Kite's provider-reported adjusted-history provenance, reports every event in
      the selected window, and blocks unverified adjustment sources, incomplete
      ledger coverage, manual lineage/delisting review and unclassified actions.
      Ordinary dividends remain admitted but are explicitly excluded from the
      price-return result; buybacks require no historical price adjustment.
   5. **Point-in-time universes — complete.** Added a local, source-linked
      membership-event ledger and a `nifty500-point-in-time-universe-v2` contract.
      It anchors to the official dated constituent snapshot and reverses reviewed
      official broad-market and ad-hoc notices (163 inclusion/exclusion events)
      covering the 23/30-Sep-2025, 31-Dec-2025, 30-Mar-2026 and 30-Sep-2026
      reconstitutions plus reviewed corporate-action changes inside the active window.
      Temporary corporate-action membership is stored separately from normal
      replacements: the four Vedanta demerger entities retain their zero-price
      dummy symbols from 30-Apr-2026, change to listed symbols on 15-Jun-2026,
      and exit on their official 19/23/24-Jun dates. Dummy rows are explicitly
      non-investable and cannot enter a stock ranking or backtest universe.
      Added a removed-constituent candle audit covering the 252-session comparison
      window plus the longest 199-session strategy warm-up. The first live audit
      now covers 90 reviewed exits and drives the history gate.
      The standard Kite EOD refresh now consumes that audit, resolves available
      symbols from the full NSE instrument master and fetches only each stock's
      required pre-exit window; delisted/unavailable symbols remain explicitly
      reported for a separate archival source. The archival remediation is now
      operational: 651 official NSE full cash bhavcopies are cached locally,
      10,336 EOD candles are sourced from the archive, and 2,574 symbol/session absences
      were verified against the daily files rather than assumed. The refreshed
      audit is 90/90 complete with 34,903 of 34,903 required tradable sessions;
      recent IPO pre-listing dates and other confirmed non-trading dates no longer
      appear as false data gaps. The endpoint is
      `/api/workspace/removed-constituent-history-backfill` and is resumable from
      the local archive cache and presence ledger.
      A dated NSE Indices press-release catalog review covers the reconstruction
      window through 12-Nov-2026, including the previously missing 17-Jul-2026
      J.B. Chemicals / Prime Focus amalgamation replacement. The ad-hoc notice-
      review flag now passes.
      Historical classification is now source-dated and audited. Six archived
      copies of the official NIFTY 500 constituent CSV are stored locally for
      sector provenance. Official 30-Sep-2025, 30-Mar-2026 and 30-Sep-2026 Nifty 100, Midcap
      150 and Smallcap 250 change ledgers are reversed from the latest validated
      snapshot to recover the size bucket in force on an earlier date; five ad-hoc
      replacements outside those reviews retain their documented small-cap slot.
      The contract no longer copies a present sector into the past. Constituents
      admitted between archived snapshots use an explicit official-event ledger,
      with their sector confirmed by the next official constituent file and that
      later confirmation disclosed in provenance. It also reconstructs the
      HEG-to-HEGAM and AKZOINDIA-to-JSWDULUX symbol changes and the HEG Graphite,
      Advent, SKF India Industrial and KWIL temporary corporate-action lineages.
      Every audited transition date from 15-Jul-2025 through the 12-Nov-2026 anchor
      now has complete membership,
      sector and size coverage; the current anchor returns the official 501 members.
      Current-universe strategy rankings still retain a survivorship-bias warning
      until step 6 evaluates each historical window with its dated membership.
   6. **Walk-forward validation — complete.** Selected-stock v8 results now
      separate full-history evidence into chronological 60% training, 20%
      validation and 20% untouched-test periods. Parameters are frozen before the
      two out-of-sample periods, positions reset at boundaries, and the untouched
      test is explicitly identified as the primary published period. The second
      increment adds expanding 504-session training folds with fixed 126-session
      validation and untouched-test blocks, advancing 126 sessions at a time.
      Every test fold now consumes the dated point-in-time membership contract
      when it falls inside the reviewed window; older folds disclose that universe
      admission is unavailable. A versioned stability gate now requires at least
      three point-in-time-admitted test folds, 15 completed trades, a 60% positive-
      fold ratio, median net profit factor of at least 1.10, positive median net
      trade edge and no test-fold drawdown worse than 25%. Stock Research displays
      the admission decision and failed checks; its existing 252-session ordering
      is now explicitly labelled exploratory. The reviewed history now begins on
      15-Jul-2025. Official NSE cash archives populate all 34,903 required sessions
      for 90 reviewed exits, and the dated universe admits three RELIANCE test folds.
      That satisfies the coverage threshold without relaxing strategy-quality checks:
      the sample moving-average study remains gated on trade count, median net profit
      factor and median net expectancy rather than being silently promoted.
   7. **Statistical evidence upgrade — complete.** The selected-stock study
      retains the present t-test as explicitly exploratory and now requires at
      least 15 completed net trades before publishing deterministic 95% ordinary-
      bootstrap and circular moving-block-bootstrap confidence intervals. The
      intervals cover net trade expectancy and profit-factor uncertainty; the
      block length scales with the square root of the trade sample and preserves
      adjacent-trade runs as a serial-dependence check. Stock Research displays
      the intervals or the exact sample shortfall. The second increment adds
      Benjamini-Hochberg false-discovery-rate q-values across every comparable
      stock within each strategy family. Stock Research shows both raw p-values
      and adjusted q-values, reports the number of 5% FDR discoveries, and keeps
      the correction boundary explicit. A cached combined contract now applies
      the same correction across every comparable stock × default-strategy
      hypothesis, and publishes both the within-strategy and all-strategy
      q-values. Selected-stock evidence publishes the frozen
      training, untouched chronological test and point-in-time-admitted rolling
      test significance separately.
   8. **Evidence-gated ranking.** Replace raw profit-factor-only ordering with a
      composite research score using out-of-sample profit factor, net trade edge,
      sample size, confidence, drawdown, excess return, exposure, turnover, costs
      and period stability. Do not rank observations that fail evidence gates.
   9. **Regime-conditioned results.** Partition evidence across bull, bear and
      sideways markets; high- and low-volatility states; strong and weak breadth;
      rate environments; and sector-leadership conditions. Show whether the
      current regime resembles the conditions in which a model historically held.
   10. **Engine modularisation and performance.** Split data quality, execution,
       metrics, statistics, ranking, regimes and strategy families into focused
       modules. Add versioned result caches, incremental EOD recalculation,
       background progress, SQLite WAL/index tuning, frozen integration fixtures
       and regression tests for published results.

   The existing EOD-only design, independent Workspace-owned SQLite stores,
   completed-session evidence, official-source provenance, explicit missing-data
   states, separation of trend and mean-reversion families, absence of order
   placement and separation of long-term portfolio analysis from trading research
   remain unchanged throughout this workstream.

6. **Portfolio.** Build long-term capital analysis independently from trading
   research.
7. **Historical regimes.** Present policy cycles, analogues and regime evidence.
8. **Cross-page consolidation and release audit.** Validate responsive behavior,
   accessibility, data-state honesty, performance and operational instructions.
9. **Expense analyser — final milestone.** Build only after the core terminal is
   stable, retaining local-only storage and reconciliation.
