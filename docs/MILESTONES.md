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
   8. **Evidence-gated ranking — complete.** Raw profit-factor ordering is
      replaced by a versioned 100-point full-history OOS score spanning median
      net profit factor, net trade edge, completed-trade sample, adjusted OOS
      confidence, worst drawdown, excess return, exposure, turnover, estimated
      costs and positive-fold stability. Every walk-forward test fold now checks
      both the completeness of its dated NIFTY 500 reconstruction and whether the
      specific stock was actually a constituent on that date. Within-strategy OOS
      p-values receive Benjamini-Hochberg correction. Strategy endpoints populate
      the ten-strategy OOS family incrementally as models are opened, and the
      combined FDR is published only after all strategy caches are complete.
      Ordinary page reads never launch the complete CPU-heavy family inside the
      request server; an explicit maintenance request can still opt into that
      build. Browser data contracts also have bounded request deadlines, so a slow
      source cannot leave the refresh control permanently busy.
      A result is ranked only when every OOS stability, within-strategy and combined
      all-strategy gate passes. Failed observations remain visible as `Not ranked`,
      with their diagnostic score and exact failed checks disclosed rather than
      hidden. Cached strategy families are keyed to the Workspace database version
      and automatically invalidate after data changes.
   9. **Top-down opportunity engine — next.** Build a trader-focused research
      hierarchy that identifies sectors and stocks primed for outsized movement:
      market regime → sector trend → stock leadership → entry trigger → F&O state
      → full-history OOS evidence. Implement it in dependency order:

      1. **Forward-move label contract — complete.** The versioned
         `forward-move-labels-v1` backend contract freezes 5/10/20/60-session
         forward return, exact-session sector and NIFTY 50 relative return,
         MFE, MAE, time-to-extreme and a 1.5-sigma volatility-normalised
         `large_move` label. The feature candle is excluded from every outcome
         path, ex-ante volatility uses only closes known at the feature close,
         and incomplete future tails or missing references are disclosed rather
         than inferred. The symbol-level API is
         `/api/research/forward-move-labels`;
      2. **Point-in-time feature ledger — complete.** Added the persistent
         `opportunity-feature-ledger-v1` contract with one dated row per
         investable NIFTY 500 member. It records only information available at
         the completed close: dated membership and classification provenance,
         5/20/60/120-session trailing returns, MA distance and slope, realised
         volatility, volume participation, market breadth, equal-weight sector
         returns/breadth/ranks, stock ranks and same-contract F&O state. Future-
         dated classifications remain null with an explicit missing reason, and
         the schema contains no forward-label fields. Build and query endpoints
         are `/api/research/feature-ledger/build` and
         `/api/research/feature-ledger`. The first live 13-Nov-2026 sample stores
         500 investable rows: 495 priced, 20 sectors and 217 F&O observations;
      3. **Market-regime layer — complete.** Added the frozen
         `market-regime-classification-v1` contract and persistent regime ledger.
         It classifies trend direction/strength, range/transition/expansion,
         annualised volatility and 50DMA/200DMA breadth using completed-session
         feature rows only. A 95% priced and breadth coverage gate is enforced;
         strategy results, trades and forward labels are excluded by schema and
         test. Build/history endpoints are `/api/research/market-regimes/build`
         and `/api/research/market-regimes`. A bounded maintenance backfill—not
         callable by ordinary page reads—produced 20 audited sessions from
         16-Oct through 13-Nov-2026: 8 bearish-trend, 5 transition and 7 bullish-
         trend. Trade counts stay explicitly unjoined until crossover
         conditioning. The latest state is moderate bullish/transition, normal
         volatility and positive breadth, but remains descriptive-only while the
         point-in-time universe review gate is incomplete;
      4. **Sector-trend layer — complete.** Added the frozen
         `sector-trend-classification-v1` contract and persistent sector ledger.
         It ranks dated sectors on equal-weight 20/60/120-session relative
         strength versus NIFTY 50, 50DMA breadth, three-MA alignment, 20DMA
         slope, five-session persistence/acceleration and ten-session realised-
         volatility expansion. An 80% constituent/input coverage gate withholds
         the state instead of filling missing evidence. States are rule-based
         `leading`, `improving`, `neutral`, `weakening` or `lagging`; forward
         outcomes and strategy results are excluded by schema and test. Build
         and history endpoints are `/api/research/sector-trends/build` and
         `/api/research/sector-trends`. The bounded 20-session audit stores 20
         sectors per date; the latest session classifies 19 sectors and gates
         Realty at 78.6% coverage. Published ranks remain descriptive-only while
         the point-in-time universe review gate is incomplete;
      5. **Stock leadership and pre-trigger priming — complete.** Added the
         persistent `stock-leadership-priming-v1` contract. Each dated stock is
         ranked inside its sector and versus NIFTY 50 on 20/60/120-session
         relative strength, with five-session persistence and acceleration,
         three-MA alignment, 20/50 crossover age, 20-vs-60 range compression,
         prior-20-session-high proximity, volatility expansion, available volume
         participation and same-session F&O eligibility/state. Frozen rules
         separate `leading`, `improving`, `neutral`, `weakening` and `lagging`
         leadership and distinguish `approaching_trigger`, `triggered`,
         `extended`, `conflicted` and `unprimed` setups. A `Primed` or
         `approaching_trigger` result cannot bypass sector confirmation. Missing
         history or a gated sector remains explicitly unclassified. Build/query
         endpoints are `/api/research/stock-leadership/build` and
         `/api/research/stock-leadership`. The 20-session audit stores 500 stocks
         per date; on 13-Nov-2026, 477 classify and 23 are gated. No stock is
         top-down primed because no sector is then leading/improving versus NIFTY
         50; one completed-close trigger remains visible as descriptive evidence;
      6. **Condition frozen crossover baselines — complete.** Added the
         `crossover-state-conditioning-v1` contract for the unchanged 20/50 SMA
         and 20/50 EMA models. Each completed trade now carries its dated entry
         signal/execution, exit, gross/net return, holding-period MFE/MAE and
         move-capture ratio without changing next-session-open execution or the
         published cost schedule. Entry dates join only to admissible frozen
         market-regime, sector-state, stock-leadership and setup-state rows. The
         conditioning ledger publishes state observations, completed trades,
         gross/net expectancy, profit factor, worst per-stock conditioned
         drawdown and average move capture. A 15-trade gate labels a bucket
         `descriptive_sample_sufficient` or `insufficient_evidence`; neither is
         an OOS edge claim. The bounded state history was extended to 100 sessions
         and joins 416 of 2,624 completed baseline trades. Every sample-sufficient
         bucket has negative net expectancy; the only positive bucket—SMA in
         neutral stock leadership—has just 10 trades and remains insufficient.
         Build/query endpoints are `/api/research/crossover-conditioning/build`
         and `/api/research/crossover-conditioning`;
      7. **Align and measure F&O positioning — complete.** Extended the frozen
         crossover-conditioning contract with same-contract entry-date F&O
         states: long build-up, short covering, short build-up, long unwinding
         and neutral (`no_clear_signal`). It retains the existing front-month,
         prior-completed-session, maximum-seven-day-gap and positive-baseline
         safeguards; expiry/roll changes, stale gaps and missing baselines stay
         unavailable. Availability is explicit: 17,545 of 43,352 admissible
         stock-state observations and 179 of 416 joined completed trades have an
         F&O state. All F&O-conditioned buckets have negative net expectancy.
         The larger short-covering, neutral and long-unwinding samples clear the
         15-trade descriptive gate but do not show edge; long/short build-up
         remains insufficient for at least one baseline. The remaining 237
         joined trades are disclosed as F&O-unavailable and are never assigned a
         state or silently excluded from the simpler baseline;
      8. **Run the frozen ablation ladder — complete, model rejected.** Added the
         persistent `crossover-top-down-ablation-v1` contract and build/query APIs
         at `/api/research/crossover-ablation/build` and
         `/api/research/crossover-ablation`. The predeclared nested rules are:
         unchanged crossover; + bullish/transition market regime; +
         leading/improving sector; + leading/improving stock in a primed,
         approaching or triggered setup; + same-contract long build-up or short
         covering. Each stage discloses missing-state attrition separately from
         filter exclusions and reports retention, net expectancy, profit factor,
         worst per-stock drawdown and move capture. From 2,624 baseline trades,
         market conditioning retains 99 SMA and 88 EMA trades and worsens net
         expectancy from -1.02%/-1.50% to -3.03%/-5.12%. Sector conditioning
         retains 47/41 and worsens it again. Stock priming retains only seven
         trades for each model; F&O retains three SMA and two EMA trades. No
         admissible stage shows incremental improvement, and the complete model
         is rejected rather than scored;
      9. **Complete-model OOS admission — complete, both strategies rejected.**
         Added the persistent `top-down-oos-admission-v1` contract and build/query
         APIs at `/api/research/top-down-oos-admission/build` and
         `/api/research/top-down-oos-admission`. Its frozen pre-admission gates
         require every ablation step to improve, at least 15 final-stage trades,
         positive net expectancy, net profit factor above one and 95% dated-state
         coverage before walk-forward, bootstrap or FDR scoring may run. SMA has
         3 final-stage trades, -3.43% net expectancy, 0.22 profit factor and 17.0%
         state coverage; EMA has 2 trades, -4.31%, 0.00 and 14.5%. Both are
         rejected with null OOS scores. Walk-forward/bootstrap/within-strategy
         FDR are recorded `not_run_pre_admission_failed`; the invalidated cross-
         strategy family has zero current hypotheses and is explicitly
         incomplete, never treated as a pass. Existing next-session execution,
         costs and point-in-time joins remain enforced; and
      10. **Make the opportunity-board release decision — complete; board
          withheld.** Persisted the `opportunity-board-release-v1` contract and
          exposed build/query APIs. It authorises a board only when at least one
          strategy has passed the frozen OOS admission contract. The present
          ledger records zero admitted and two rejected strategies, so the UI
          publishes only a research-status explanation. Opportunity ranks,
          scores, trade labels, targets, position sizes and order controls remain
          prohibited. A future iteration must start as a separately specified
          research cycle with hypotheses frozen before results are inspected.

      The objective is tradable anticipation and ranking, not a causal explanation
      of prior moves. Composite weights and UI rankings remain deferred until each
      added layer demonstrates incremental out-of-sample value against the simpler
      frozen model.
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
