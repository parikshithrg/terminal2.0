# Workspace 2.0 — session handoff

Updated: 16 November 2026

## Product objective

Workspace 2.0 is a trader's completed-session research terminal. Its next major
workstream is to identify which sectors and stocks are **primed for an outsized
move**, then measure whether the observable setup has a repeatable, net,
out-of-sample edge. The engine is not intended to explain why a move happened.

The required top-down research path is:

**market regime → sector trend → stock leadership → entry trigger → F&O state →
out-of-sample evidence**

Long-term Portfolio analysis remains separate. The site does not place orders and
must not present a research score as a guaranteed forecast.

## Current state

- Independent Workspace-owned EOD storage and the dashboard, Market state, F&O
  positioning and Stock research foundations are operational.
- The F&O archive contains 825 validated same-contract sessions from June 2023
  through November 2026, with rollover baselines excluded.
- Ten frozen price strategies are available: five trend-following and five mean-
  reversion models.
- Signal execution, costs, data-quality gates, corporate actions, point-in-time
  NIFTY 500 membership, walk-forward validation, statistical evidence and the
  full-history OOS ranking gate are implemented.
- No stock currently passes every full-history OOS ranking gate; failed evidence
  remains visible as `Not ranked` with its reasons.
- Full all-strategy OOS computation is incremental and on demand. Ordinary page
  reads no longer launch all ten full-history families inside the request server.
- The refresh stall was fixed by serialising CPU-heavy local data contracts and
  adding bounded browser request deadlines. Manual refresh and browser reload
  were verified. The site is running on port 8520.
- The `forward-move-labels-v1` research contract now publishes 5/10/20/60-session
  absolute and relative returns, MFE, MAE, time-to-extreme and a frozen 1.5-sigma
  volatility-normalised large-move label. It excludes the feature candle from
  the future path and explicitly censors incomplete tails and missing references.
- The persistent `opportunity-feature-ledger-v1` contract now stores one dated
  completed-session feature row per investable point-in-time member. The first
  live sample for 13-Nov-2026 contains 500 rows: 495 priced members, 20 sectors
  and 217 same-contract F&O observations. It withholds future-dated
  classifications and contains no forward-outcome fields. Build and query APIs
  are operational.
- The persistent `market-regime-classification-v1` contract now freezes trend,
  range, volatility and breadth states with a 95% cross-sectional coverage gate.
  Its first bounded sample spans 20 completed sessions: 8 bearish-trend, 5
  transition and 7 bullish-trend. Strategy and forward fields are excluded;
  trade counts remain unjoined until conditioning. The latest classification is
  descriptive-only because the point-in-time universe review gate is incomplete.
- The persistent `sector-trend-classification-v1` contract now stores dated
  20/60/120-session relative strength versus NIFTY 50, sector ranks, breadth,
  MA alignment/slope, persistence, acceleration and volatility expansion. The
  bounded 20-session audit stores 20 sectors per date. On 13-Nov-2026, 19 sector
  states are available and Realty is explicitly gated because only 11 of 14
  members (78.6%) have the required evidence. All states remain descriptive-only
  until the universe review gate is complete.
- The persistent `stock-leadership-priming-v1` contract now ranks stocks within
  dated sectors and versus NIFTY 50 on 20/60/120-session relative strength,
  persistence and acceleration. It also records MA alignment/slope, 20/50
  crossover age, range compression, prior-high proximity, volatility expansion,
  available volume participation and F&O eligibility/state. The 13-Nov-2026
  sample classifies 477 of 500 stocks and gates 23. It reports one descriptive
  completed-close trigger, but correctly reports no top-down primed or
  approaching-trigger stocks because no sector qualifies as leading/improving.
- The `crossover-state-conditioning-v1` contract now joins completed frozen
  20/50 SMA and EMA trades at their entry signal date to market, sector, stock
  leadership and setup states. The dated state sample was extended to 100
  sessions, producing 416 joined completed trades from 2,624 baseline trades.
  Every bucket with at least 15 trades has negative net expectancy. SMA in the
  neutral stock-leadership bucket is positive in this sample, but has only 10
  trades and is correctly withheld as insufficient evidence. No parameter or MA
  window was optimised.
- Same-contract F&O conditioning is now included as a measured dimension. The
  frozen sample has F&O state for 179 of 416 joined completed trades and 17,545
  of 43,352 admissible stock-state observations. All five F&O states show
  negative net expectancy; larger short-covering, neutral and long-unwinding
  buckets clear only the descriptive sample gate, while build-up samples remain
  insufficient for at least one baseline. The 237 trades without comparable F&O
  evidence remain explicit and unclassified.
- The frozen `crossover-top-down-ablation-v1` ladder is complete and rejects the
  present model. Baseline net expectancy is -1.02% for SMA and -1.50% for EMA.
  Market-regime filtering retains only 99/88 trades and worsens expectancy to
  -3.03%/-5.12%; the sector layer worsens it again. Stock priming retains seven
  trades per model and F&O alignment just three SMA/two EMA trades. No admissible
  layer demonstrates incremental improvement, so no composite score or current
  opportunity rank is authorised.
- The persistent `top-down-oos-admission-v1` contract has formally rejected both
  complete models and publishes no OOS score. SMA fails with 3 final-stage trades,
  -3.43% net expectancy, 0.22 profit factor and 17.0% dated-state coverage; EMA
  fails with 2 trades, -4.31%, 0.00 and 14.5%. The frozen requirements are 15
  trades, positive expectancy, profit factor above one, 95% coverage and prior
  ablation improvement. Walk-forward, bootstrap and within-strategy FDR correctly
  stop at pre-admission rather than producing misleading evidence.
- Verification checkpoint: 91 automated tests pass, Python and JavaScript syntax
  checks pass, and `git diff --check` passes.

## Next workstream — top-down opportunity engine

Implement in this order. Do not skip directly to a composite score.

1. **Freeze the forward-move research labels — complete.**
   - Record 5-, 10-, 20- and 60-session forward absolute return, return versus
     sector, return versus NIFTY 50, maximum favourable excursion, maximum adverse
     excursion and time to maximum move.
   - Add an explicit `large_move` label using a frozen volatility-normalised rule
     and retain the continuous outcomes so results do not depend on one threshold.
   - Every label begins after the completed-session feature timestamp.

2. **Build the point-in-time feature ledger — complete.**
   - One row per completed session × stock with only information available at that
     close.
   - Join dated NIFTY 500/F&O eligibility, dated sector classification, market
     regime inputs, sector ranks, stock ranks, price/MA state, volatility,
     participation and same-contract F&O price/OI state.
   - Preserve missing values and source dates; never backfill a future
     classification or use the forward label as an input.

3. **Create the market-regime layer — complete.**
   - Classify trend direction/strength, range conditions, volatility state and
     breadth/participation state using frozen definitions.
   - Keep regime classification independent of the strategy result being tested.
   - Publish the number of sessions and trades available in every regime.

4. **Create the sector-trend layer — complete.**
   - Rank sectors on 20-, 60- and 120-session relative strength versus NIFTY 50,
     MA slope/alignment, breadth, persistence, acceleration and volatility
     expansion.
   - Separate `leading`, `improving`, `neutral`, `weakening` and `lagging` states.
   - Require adequate constituent coverage before publishing a sector state.

5. **Create the stock-leadership and priming layer — complete.**
   - Rank each stock inside its dated sector and against NIFTY 50.
   - Record relative-strength persistence, MA slope/alignment, crossover age,
     consolidation/compression, breakout proximity, volatility expansion, volume
     or delivery participation when available, and F&O eligibility.
   - Distinguish stocks already extended from stocks approaching a tested trigger.

6. **Condition the frozen crossover baselines — complete.**
   - Begin with the existing 20/50 SMA and 20/50 EMA models.
   - Measure their performance separately in each market regime, sector state and
     stock-leadership bucket.
   - Do not optimise the MA windows during this stage.

7. **Add F&O positioning as a measured factor — complete.**
   - Align long build-up, short covering, short build-up, long unwinding and
     neutral state to the signal date using same-contract observations.
   - Evaluate each state as a feature and as an optional entry filter; do not
     assume that any quadrant adds edge.
   - Keep expiry/roll exclusions and availability counts explicit.

8. **Run the ablation ladder — complete; current model rejected.**
   Compare, in order:
   1. crossover alone;
   2. crossover + market regime;
   3. + sector trend;
   4. + stock leadership/priming;
   5. + F&O state.

   Each layer must demonstrate incremental net expectancy, profit factor,
   drawdown control or move-capture improvement against the immediately simpler
   frozen model.

9. **Validate the complete model out of sample — complete; admission rejected.**
   - Use the existing point-in-time universe, next-session execution, cost model,
     expanding walk-forward folds, bootstrap evidence and FDR controls.
   - Report results by regime, sector and F&O state as well as in aggregate.
   - Reject combinations with inadequate sessions/trades or unstable fold results;
     do not fill missing evidence with an inferred score.

10. **Make the opportunity-board release decision — complete; board withheld.**
    - Rank current sectors first, then stocks within the leading sectors.
    - Show direction, setup/trigger, sector and stock rank, F&O state, expected
      horizon, historical move distribution, net OOS edge, drawdown and evidence
      status.
    - The persisted `opportunity-board-release-v1` contract records zero admitted
      and two rejected strategies. The Stock Research page therefore shows only
      the release status and next research action.
    - Opportunity ranks, scores, trade labels, target prices, position sizes and
      order controls are explicitly prohibited until a strategy is admitted.

## Exact restart point

The top-down opportunity-engine workstream is complete. Its formal release ledger
withholds the trading opportunity board because neither frozen strategy passed
ablation and OOS admission. The UI exposes only this research status; it does not
expose opportunity ranks, scores, trade labels or order controls.

No milestones remain in this workstream. A future model iteration must begin as a
separately specified research cycle with new hypotheses frozen before results are
inspected, or development can move to another project milestone at the user's
direction.
