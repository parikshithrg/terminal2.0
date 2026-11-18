(() => {
  'use strict';

  const $ = (id) => document.getElementById(id);
  const safeNumber = (value) => {
    if (value === null || value === undefined || value === '') return null;
    return Number.isFinite(Number(value)) ? Number(value) : null;
  };
  const displayDate = (value) => {
    if (!value) return '—';
    const date = new Date(`${String(value).slice(0, 10)}T00:00:00`);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' });
  };
  const signedPercent = (value) => {
    const number = safeNumber(value);
    return number === null ? '—' : `${number > 0 ? '+' : ''}${number.toFixed(2)}%`;
  };
  const text = (id, value) => { const element = $(id); if (element) element.textContent = value ?? '—'; };
  const escapeHtml = (value) => String(value ?? '').replace(/[&<>'"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' })[char]);
  const fnoStateLabels = {
    long_build_up: 'Long build-up', short_build_up: 'Short build-up',
    long_unwinding: 'Long unwinding', short_covering: 'Short covering',
    no_clear_signal: 'No clear signal',
  };
  let fnoRows = [];
  let fnoSort = { key: 'underlying', direction: 'asc' };
  let fnoHistorySessions = 0;
  let latestOverviewPayload = null;
  let latestDomesticPayload = null;
  let latestSnapshotPayload = null;
  let latestSeasonalityPayload = null;
  let latestNiftyYtdPayload = null;
  let latestFactorPayload = null;
  let latestScreenerPayload = null;
  let latestStockYtdPayload = null;
  let latestBacktestPayload = null;
  let latestCrossStrategyFdrPayload = null;
  let latestOpportunityReleasePayload = null;
  let crossStrategyPollTimer = null;
  let researchRows = [];
  let researchSort = { key: 'evidenceScore', direction: 'desc' };
  let selectedResearchSymbol = null;
  let researchBacktestPanelSymbol = null;
  const researchBacktestCache = new Map();
  const researchRankingPayloads = new Map();
  let researchRankingModel = 'sma';
  const researchBacktestModels = {
    sma: { endpoint: '/api/backtests/moving-average-crossover', label: '20/50-session SMA crossover', shortLabel: 'SMA' },
    donchian: { endpoint: '/api/backtests/donchian-breakout', label: '55/20-session Donchian breakout', shortLabel: 'Donchian' },
    ema: { endpoint: '/api/backtests/ema-crossover', label: '20/50-session EMA crossover', shortLabel: 'EMA' },
    momentum: { endpoint: '/api/backtests/time-series-momentum', label: '63-session absolute momentum', shortLabel: 'Momentum' },
    triple: { endpoint: '/api/backtests/triple-sma-alignment', label: '20/50/200 triple-SMA alignment', shortLabel: 'Triple-SMA' },
    rsiReversion: { endpoint: '/api/backtests/rsi-mean-reversion', label: 'RSI(2) oversold recovery', shortLabel: 'RSI(2)' },
    bollingerReversion: { endpoint: '/api/backtests/bollinger-mean-reversion', label: '20-session Bollinger re-entry', shortLabel: 'Bollinger' },
    zscoreReversion: { endpoint: '/api/backtests/zscore-mean-reversion', label: '10-session price z-score reversal', shortLabel: 'Z-score' },
    pullbackReversion: { endpoint: '/api/backtests/pullback-uptrend', label: '5/200 pullback in uptrend', shortLabel: '5/200 pullback' },
    selloffReversion: { endpoint: '/api/backtests/selloff-recovery', label: '3-day selloff recovery', shortLabel: '3-day recovery' },
  };
  let factorDialogRows = [];
  let factorDialogPayload = null;
  let factorDialogSort = { key: 'return_pct', direction: 'desc' };
  let kiteConnected = false;
  let portfolioLoaded = false;
  let historicalRegimesLoaded = false;

  function showToast(message) {
    const toast = $('toast');
    toast.textContent = message;
    toast.classList.add('visible');
    window.clearTimeout(showToast.timer);
    showToast.timer = window.setTimeout(() => toast.classList.remove('visible'), 3200);
  }

  function renderOpportunityRelease(payload) {
    latestOpportunityReleasePayload = payload;
    const authorised = payload?.board_authorised === true;
    const available = Boolean(payload && typeof payload === 'object');
    const card = $('opportunityReleaseCard');
    const badge = $('opportunityReleaseBadge');
    if (!card || !badge) return;
    card.classList.toggle('authorised', authorised);
    card.classList.toggle('withheld', !authorised);
    badge.className = `opportunity-release-badge ${authorised ? 'authorised' : 'withheld'}`;
    badge.textContent = authorised ? 'VALIDATED' : (available ? 'WITHHELD' : 'UNAVAILABLE');
    text('opportunityReleaseHeadline', payload?.headline || 'Opportunity release decision unavailable');
    text('opportunityReleaseExplanation', payload?.explanation || 'No opportunity rankings or trade controls are being published.');
    const admitted = Number(payload?.admitted_strategies || 0);
    const rejected = Number(payload?.rejected_strategies || 0);
    const counts = available ? `${admitted} admitted · ${rejected} rejected` : 'Release ledger unavailable';
    text('opportunityReleaseNextAction', `${counts}${payload?.next_action ? ` · ${payload.next_action}` : ''}`);
  }

  function setSourceState(state, label) {
    const node = $('sourceState');
    node.className = `source-state ${state}`;
    node.lastChild.textContent = label;
    text('engineState', label);
  }

  async function loadWorkspaceReleaseAudit() {
    try {
      const response = await fetchWithDeadline('/api/workspace/release-audit', { cache: 'no-store' }, 10000);
      const payload = await response.json();
      if (!response.ok || payload.ok !== true) throw new Error(payload.reason || `HTTP ${response.status}`);
      const ready = payload.status === 'ready';
      const node = $('releaseAuditState');
      node.textContent = ready ? 'Ready' : 'Withheld';
      node.className = ready ? 'positive' : 'negative';
      text('releaseAuditMeta', `${Number(payload.passed || 0)} of ${Number(payload.total || 0)} checks · ${payload.contract_version || 'release contract'}`);
      node.closest('strong').title = (payload.checks || []).filter((check) => !check.passed).map((check) => `${check.label}: ${check.detail}`).join(' · ') || 'All workspace release checks passed.';
    } catch (error) {
      text('releaseAuditState', 'Unavailable');
      text('releaseAuditMeta', String(error.message || 'Release contract unavailable'));
      $('releaseAuditState').className = 'negative';
    }
  }

  function setKiteState(connected) {
    kiteConnected = connected === true;
    const button = $('kiteLoginButton');
    button.classList.toggle('connected', kiteConnected);
    button.textContent = kiteConnected ? 'Kite connected' : 'Kite login';
    button.setAttribute('aria-label', kiteConnected ? 'Kite is connected; open connection details' : 'Log in to Kite for completed-session data');
    $('kiteDisconnectButton').hidden = !kiteConnected;
  }

  function setKitePanel(open) {
    $('kitePanel').hidden = !open;
    $('kiteLoginButton').setAttribute('aria-expanded', String(open));
  }

  async function fetchWithDeadline(url, options = {}, timeoutMs = 20000) {
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), timeoutMs);
    try {
      return await fetch(url, { ...options, signal: controller.signal });
    } catch (error) {
      if (error?.name === 'AbortError') {
        throw new Error(`Timed out after ${Math.round(timeoutMs / 1000)} seconds`);
      }
      throw error;
    } finally {
      window.clearTimeout(timer);
    }
  }

  async function loadKiteStatus() {
    try {
      const response = await fetchWithDeadline('/api/kite/status', { cache: 'no-store' });
      const payload = await response.json();
      setKiteState(response.ok && payload.connected === true);
    } catch (_error) {
      setKiteState(false);
    }
  }

  async function sourceKiteEod(onComplete = () => {}) {
    if (!kiteConnected) return;
    const endpoints = [
      '/api/kite/indices/eod-summary',
      '/api/kite/market-breadth',
      '/api/kite/futures/eod-summary',
      '/api/kite/removed-constituent-history',
    ];
    for (const endpoint of endpoints) {
      try {
        const response = await fetchWithDeadline(endpoint, { cache: 'no-store' }, 60000);
        const payload = await response.json();
        if (!response.ok || payload.ok === false) throw new Error(payload.reason || `HTTP ${response.status}`);
      } finally {
        onComplete();
      }
    }
  }

  async function sourceCorporateActions(onComplete = () => {}) {
    try {
      const response = await fetchWithDeadline('/api/workspace/corporate-actions/refresh', { cache: 'no-store' }, 60000);
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.detail || payload.reason || `HTTP ${response.status}`);
      return payload;
    } finally {
      onComplete();
    }
  }

  async function sourceOfficialSnapshots(onComplete = () => {}) {
    try {
      const response = await fetchWithDeadline('/api/workspace/snapshots/refresh', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, cache: 'no-store', body: '{}',
      }, 60000);
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || payload.reason || `HTTP ${response.status}`);
      if (payload.status === 'partial') {
        const failed = Array.isArray(payload.failed_sources) ? payload.failed_sources.join(', ') : 'unknown sources';
        showToast(`Official-source refresh was partial: ${failed}. Latest available observations remain visible.`);
      }
      return payload;
    } finally {
      onComplete();
    }
  }

  async function sourceGlobalRisk(onComplete = () => {}) {
    try {
      const response = await fetchWithDeadline('/api/workspace/global-risk/refresh', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, cache: 'no-store', body: '{}',
      }, 60000);
      const payload = await response.json();
      if (!response.ok || payload.ok !== true) {
        throw new Error(payload.global_risk?.reason || payload.detail || payload.reason || `HTTP ${response.status}`);
      }
      if (payload.status === 'partial') {
        const failed = (payload.series || []).filter((row) => row.status !== 'successful').map((row) => row.label).join(', ');
        showToast(`Global-risk refresh was partial${failed ? `: ${failed}` : ''}. Admitted core evidence remains visible.`);
      }
      return payload;
    } finally {
      onComplete();
    }
  }

  function researchNumber(value, digits = 2) {
    const number = safeNumber(value);
    return number === null ? '—' : Number(number).toLocaleString('en-IN', { minimumFractionDigits: digits, maximumFractionDigits: digits });
  }

  function researchPercent(value) {
    return signedPercent(value);
  }

  function researchPValue(value) {
    const number = safeNumber(value);
    if (number === null) return 'p —';
    if (number < 0.001) return 'p <0.001';
    return `p ${number.toFixed(3)}`;
  }

  function researchQValue(value) {
    const number = safeNumber(value);
    if (number === null) return 'q —';
    if (number < 0.001) return 'q <0.001';
    return `q ${number.toFixed(3)}`;
  }

  function researchTone(value) {
    const number = safeNumber(value);
    return number === null ? '' : number >= 0 ? 'positive-text' : 'negative-text';
  }

  function renderFactorDialog() {
    if (!factorDialogPayload) return;
    const query = $('factorDialogSearch').value.trim().toLowerCase();
    const rows = factorDialogRows.filter((row) => !query || `${row.symbol} ${row.company_name} ${row.industry}`.toLowerCase().includes(query));
    const { key, direction } = factorDialogSort;
    rows.sort((a, b) => {
      const av = a[key];
      const bv = b[key];
      let result;
      if (typeof av === 'string' || typeof bv === 'string') result = String(av ?? '').localeCompare(String(bv ?? ''));
      else {
        const an = safeNumber(av);
        const bn = safeNumber(bv);
        if (an === null && bn === null) result = 0;
        else if (an === null) return 1;
        else if (bn === null) return -1;
        else result = an - bn;
      }
      return direction === 'asc' ? result : -result;
    });
    text('factorDialogCount', `${rows.length.toLocaleString('en-IN')} of ${factorDialogRows.length.toLocaleString('en-IN')} stocks`);
    $('factorDialogBody').innerHTML = rows.length ? rows.map((row) => `<tr>
      <td class="factor-stock"><strong>${escapeHtml(row.symbol)}</strong><small title="${escapeHtml(row.company_name)}">${escapeHtml(row.company_name)}</small></td>
      <td title="${escapeHtml(row.industry)}">${escapeHtml(row.industry)}</td>
      <td class="numeric">₹${researchNumber(row.close_price)}</td>
      <td class="numeric ${researchTone(row.return_pct)}">${signedPercent(row.return_pct)}</td>
      <td class="numeric ${researchTone(row.value_score)}">${signedPercent(safeNumber(row.value_score) === null ? null : Number(row.value_score) * 100)}</td>
      <td class="numeric ${researchTone(row.growth_score)}">${signedPercent(safeNumber(row.growth_score) === null ? null : Number(row.growth_score) * 100)}</td>
      <td class="factor-evidence"><span>${escapeHtml(displayDate(row.current_period))} vs ${escapeHtml(displayDate(row.prior_period))}</span><small>Price evidence ${escapeHtml(displayDate(row.evidence_timestamp))}</small></td>
    </tr>`).join('') : '<tr><td colspan="7">No admitted stocks match this filter.</td></tr>';
  }

  async function openFactorDialog(size, style) {
    const dialog = $('factorDialog');
    factorDialogRows = [];
    factorDialogPayload = null;
    factorDialogSort = { key: 'return_pct', direction: 'desc' };
    $('factorDialogSearch').value = '';
    text('factorDialogTitle', `${size} cap · ${style}`);
    text('factorDialogMeta', 'Loading the latest admitted point-in-time cohort…');
    text('factorDialogCount', 'Loading stocks');
    $('factorDialogSummary').innerHTML = '';
    $('factorDialogBody').innerHTML = '<tr><td colspan="7">Loading admitted constituents…</td></tr>';
    if (!dialog.open) dialog.showModal();
    try {
      const response = await fetch(`/api/workspace/factor-constituents?size=${encodeURIComponent(size)}&style=${encodeURIComponent(style)}`, { cache: 'no-store' });
      const payload = await response.json();
      if (!response.ok || payload.ok !== true) throw new Error(payload.reason || `HTTP ${response.status}`);
      factorDialogPayload = payload;
      factorDialogRows = Array.isArray(payload.rows) ? payload.rows : [];
      const average = (field) => {
        const values = factorDialogRows.map((row) => safeNumber(row[field])).filter((value) => value !== null);
        return values.length ? values.reduce((total, value) => total + value, 0) / values.length : null;
      };
      const cell = (latestSnapshotPayload?.factor_returns || []).find((row) => row.size_bucket === size && row.style_bucket === style);
      text('factorDialogMeta', `Evidence ${displayDate(payload.snapshot_date)} · ${payload.methodology_version}`);
      $('factorDialogSummary').innerHTML = [
        ['Admitted stocks', factorDialogRows.length.toLocaleString('en-IN')],
        ['Cell return', signedPercent(cell?.return_pct)],
        ['Average earnings yield', signedPercent(safeNumber(average('value_score')) === null ? null : average('value_score') * 100)],
        ['Average growth score', signedPercent(safeNumber(average('growth_score')) === null ? null : average('growth_score') * 100)],
      ].map(([label, value]) => `<article><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></article>`).join('');
      renderFactorDialog();
    } catch (error) {
      text('factorDialogMeta', 'Cohort evidence unavailable');
      text('factorDialogCount', '0 stocks');
      $('factorDialogBody').innerHTML = `<tr><td colspan="7">Unable to load this factor cohort: ${escapeHtml(error.message)}</td></tr>`;
    }
  }

  function buildResearchRows() {
    const universe = $('researchUniverse').value;
    const backtestMap = new Map((latestBacktestPayload?.results || []).map((row) => [row.symbol, row]));
    if (universe === 'fno') {
      const screenMap = new Map((latestScreenerPayload?.rows || []).map((row) => [row.symbol, row]));
      researchRows = (latestFactorPayload?.rows || []).map((factor) => {
        const screen = screenMap.get(factor.symbol) || {};
        const backtest = backtestMap.get(factor.symbol) || {};
        const crossStrategy = latestCrossStrategyFdrPayload?.by_engine?.[latestBacktestPayload?.engine]?.[factor.symbol] || {};
        const evidenceRanking = backtest.evidence_ranking || {};
        const globalQValue = crossStrategy.cross_strategy_q_value;
        const globalFdrPassed = safeNumber(globalQValue) !== null && Number(globalQValue) <= 0.05;
        const rankingFailedChecks = [...new Set([...(evidenceRanking.failed_checks || []), ...(globalFdrPassed ? [] : ['all_strategy_fdr'])])];
        const rankingAdmitted = evidenceRanking.ranking_admissible === true && globalFdrPassed;
        return {
          symbol: factor.symbol,
          name: factor.symbol,
          sector: factor.sector || 'Unclassified',
          close: screen.close,
          primaryReturn: screen.return_20d_pct,
          secondaryReturn: screen.excess_20d_vs_nifty_pct,
          vs20: screen.vs_20dma_pct,
          vs50: screen.vs_50dma_pct,
          vs200: screen.vs_200dma_pct,
          return60: screen.return_60d_pct,
          momentum12: factor.momentum_12_1_pct,
          volatility: screen.realised_volatility_20d_pct,
          turnover: factor.median_daily_turnover_inr,
          status: factor.research_eligible ? 'eligible' : (screen.status === 'ready' ? 'gated' : 'partial'),
          ready: screen.status === 'ready',
          eligible: factor.research_eligible === true,
          reason: factor.research_eligible
            ? 'Price history, sector identity, adjustment provenance and liquidity evidence pass the current research gates.'
            : 'One or more research admission gates remain incomplete.',
          backtestReady: backtest.status === 'ready',
          backtestStatus: backtest.status,
          backtestQuality: backtest.data_quality || null,
          backtestSignal: backtest.current_signal,
          profitFactor: backtest.net_profit_factor,
          tradeEdge: backtest.net_trade_expectancy_pct,
          pValue: evidenceRanking.oos_net_trade_edge_p_value ?? backtest.net_trade_edge_p_value,
          qValue: evidenceRanking.oos_net_trade_edge_q_value ?? backtest.net_trade_edge_q_value,
          globalQValue,
          rankingAdmitted,
          evidenceScore: rankingAdmitted ? evidenceRanking.score : null,
          diagnosticScore: evidenceRanking.diagnostic_score,
          rankingFailedChecks,
          oosMetrics: evidenceRanking.oos_metrics || {},
        };
      });
    } else {
      researchRows = (latestStockYtdPayload?.rows || []).map((row) => {
        const backtest = backtestMap.get(row.symbol) || {};
        const crossStrategy = latestCrossStrategyFdrPayload?.by_engine?.[latestBacktestPayload?.engine]?.[row.symbol] || {};
        const evidenceRanking = backtest.evidence_ranking || {};
        const globalQValue = crossStrategy.cross_strategy_q_value;
        const globalFdrPassed = safeNumber(globalQValue) !== null && Number(globalQValue) <= 0.05;
        const rankingFailedChecks = [...new Set([...(evidenceRanking.failed_checks || []), ...(globalFdrPassed ? [] : ['all_strategy_fdr'])])];
        const rankingAdmitted = evidenceRanking.ranking_admissible === true && globalFdrPassed;
        return {
          symbol: row.symbol,
          name: row.name || row.symbol,
          sector: row.sector || 'Unclassified',
          close: row.ltp,
          primaryReturn: row.ytd_performance_pct,
          secondaryReturn: row.from_20dma_pct,
          vs20: row.from_20dma_pct,
          vs50: row.from_50dma_pct,
          vs200: row.from_200dma_pct,
          return60: null,
          momentum12: null,
          volatility: null,
          turnover: null,
          status: row.status === 'ready' ? 'ready' : 'partial',
          ready: row.status === 'ready',
          eligible: null,
          reason: row.status === 'ready'
            ? 'Completed-session price, YTD and moving-average evidence is available.'
            : 'The displayed evidence is limited by local price history.',
          backtestReady: backtest.status === 'ready',
          backtestStatus: backtest.status,
          backtestQuality: backtest.data_quality || null,
          backtestSignal: backtest.current_signal,
          profitFactor: backtest.net_profit_factor,
          tradeEdge: backtest.net_trade_expectancy_pct,
          pValue: evidenceRanking.oos_net_trade_edge_p_value ?? backtest.net_trade_edge_p_value,
          qValue: evidenceRanking.oos_net_trade_edge_q_value ?? backtest.net_trade_edge_q_value,
          globalQValue,
          rankingAdmitted,
          evidenceScore: rankingAdmitted ? evidenceRanking.score : null,
          diagnosticScore: evidenceRanking.diagnostic_score,
          rankingFailedChecks,
          oosMetrics: evidenceRanking.oos_metrics || {},
        };
      });
    }
    updateResearchSectors();
    renderResearchTable();
  }

  async function loadResearchRanking(modelKey) {
    const model = researchBacktestModels[modelKey] || researchBacktestModels.sma;
    researchRankingModel = modelKey in researchBacktestModels ? modelKey : 'sma';
    $('researchRankModel').value = researchRankingModel;
    $('researchRankModel').disabled = true;
    text('researchRankStatus', `Calculating ${model.label} across the stored universe…`);
    try {
      let payload = researchRankingPayloads.get(researchRankingModel);
      if (!payload) {
        const response = await fetch(model.endpoint, { cache: 'no-store' });
        payload = await response.json();
        if (!response.ok || payload.ok !== true) throw new Error(payload.reason || `HTTP ${response.status}`);
        researchRankingPayloads.set(researchRankingModel, payload);
      }
      latestBacktestPayload = payload;
      const ready = Number(payload.coverage?.ready || 0);
      const requested = Number(payload.coverage?.requested || 0);
      const gated = Number(payload.coverage?.data_quality_gated || 0);
      const commonDate = payload.data_quality_contract?.common_as_of_date;
      const tested = Number(payload.multiple_testing?.tested_hypotheses || 0);
      const discoveries = Number(payload.multiple_testing?.fdr_q_below_alpha || 0);
      const oosRanked = Number(payload.evidence_ranking?.ranking_admitted || 0);
      const globalDiscoveries = Number(latestCrossStrategyFdrPayload?.fdr_q_below_alpha || 0);
      const globalTested = Number(latestCrossStrategyFdrPayload?.tested_hypotheses || 0);
      text('researchRankStatus', `${model.label} · ${oosRanked} pass full-history OOS gates · ${ready.toLocaleString('en-IN')} comparable · ${discoveries} of ${tested} pass comparison-window FDR${globalTested ? ` · ${globalDiscoveries} of ${globalTested} pass all-strategy OOS FDR` : ' · all-strategy OOS FDR calculating'}${commonDate ? ` through ${displayDate(commonDate)}` : ''}`);
      text('researchBacktestColumnLabel', `${model.shortLabel} evidence score / gate`);
      researchSort = { key: 'evidenceScore', direction: $('researchProfitOrder').value };
      buildResearchRows();
    } catch (error) {
      latestBacktestPayload = null;
      text('researchRankStatus', `${model.label} unavailable`);
      text('researchBacktestColumnLabel', `${model.shortLabel} evidence score / gate`);
      buildResearchRows();
      showToast(`Unable to load ${model.label}: ${error.message}`);
    } finally {
      $('researchRankModel').disabled = false;
    }
  }

  async function loadCrossStrategyFdr() {
    try {
      const response = await fetchWithDeadline('/api/backtests/cross-strategy-fdr', { cache: 'no-store' });
      const payload = await response.json();
      if (!response.ok || payload.ok !== true) throw new Error(payload.reason || `HTTP ${response.status}`);
      latestCrossStrategyFdrPayload = payload;
      buildResearchRows();
      const model = researchBacktestModels[researchRankingModel] || researchBacktestModels.sma;
      const strategyTests = Number(latestBacktestPayload?.multiple_testing?.tested_hypotheses || 0);
      const strategyDiscoveries = Number(latestBacktestPayload?.multiple_testing?.fdr_q_below_alpha || 0);
      if (payload.coverage_complete) {
        text('researchRankStatus', `${model.label} · ${Number(latestBacktestPayload?.evidence_ranking?.ranking_admitted || 0)} pass full-history OOS gates · ${strategyDiscoveries} of ${strategyTests} pass comparison-window FDR · ${Number(payload.fdr_q_below_alpha || 0)} of ${Number(payload.tested_hypotheses || 0)} pass all-strategy OOS FDR · ${Number(payload.strategy_count || 0)} models through ${displayDate(payload.as_of_date)}`);
        if (crossStrategyPollTimer) clearTimeout(crossStrategyPollTimer);
        crossStrategyPollTimer = null;
      } else if (payload.status === 'building_oos_strategy_family') {
        const current = payload.background_progress?.current_engine;
        const progress = Number(payload.background_progress?.progress_pct || 0);
        text('researchRankStatus', `${model.label} · ${Number(latestBacktestPayload?.evidence_ranking?.ranking_admitted || 0)} pass full-history OOS gates · building all-strategy OOS family ${progress.toFixed(0)}% · ${Number(payload.strategy_count || 0)} / ${Number(payload.required_strategy_count || 10)}${current ? ` · ${current.replaceAll('_', ' ')}` : ''}`);
        if (crossStrategyPollTimer) clearTimeout(crossStrategyPollTimer);
        crossStrategyPollTimer = setTimeout(() => { void loadCrossStrategyFdr(); }, 5000);
      } else {
        text('researchRankStatus', `${model.label} · ${Number(latestBacktestPayload?.evidence_ranking?.ranking_admitted || 0)} pass full-history OOS gates · all-strategy OOS family cached ${Number(payload.strategy_count || 0)} / ${Number(payload.required_strategy_count || 10)}; final FDR withheld until each model is opened`);
        if (crossStrategyPollTimer) clearTimeout(crossStrategyPollTimer);
        crossStrategyPollTimer = null;
      }
    } catch (error) {
      text('researchRankStatus', `${researchBacktestModels[researchRankingModel]?.label || 'Backtest'} · all-strategy FDR unavailable; within-strategy correction remains active`);
    }
  }

  function updateResearchSectors() {
    const select = $('researchSector');
    const previous = select.value;
    const sectors = [...new Set(researchRows.map((row) => row.sector).filter(Boolean))].sort((a, b) => a.localeCompare(b));
    select.innerHTML = '<option value="all">All sectors</option>' + sectors.map((sector) => `<option value="${escapeHtml(sector)}">${escapeHtml(sector)}</option>`).join('');
    select.value = sectors.includes(previous) ? previous : 'all';
  }

  function researchFilteredRows() {
    const query = $('researchSearch').value.trim().toLowerCase();
    const sector = $('researchSector').value;
    const state = $('researchState').value;
    const filtered = researchRows.filter((row) => {
      if (query && !`${row.symbol} ${row.name}`.toLowerCase().includes(query)) return false;
      if (sector !== 'all' && row.sector !== sector) return false;
      if (state === 'ready' && !row.ready) return false;
      const vs200 = safeNumber(row.vs200);
      if (state === 'above_200' && !(vs200 !== null && vs200 >= 0)) return false;
      if (state === 'below_200' && !(vs200 !== null && vs200 < 0)) return false;
      if (state === 'positive_return' && !(safeNumber(row.primaryReturn) > 0)) return false;
      if (state === 'eligible' && row.eligible !== true) return false;
      return true;
    });
    const { key, direction } = researchSort;
    return filtered.sort((a, b) => {
      const av = a[key];
      const bv = b[key];
      let result;
      if (typeof av === 'string' || typeof bv === 'string') result = String(av ?? '').localeCompare(String(bv ?? ''));
      else {
        const an = safeNumber(av);
        const bn = safeNumber(bv);
        if (an === null && bn === null) result = 0;
        else if (an === null) return 1;
        else if (bn === null) return -1;
        else result = an - bn;
      }
      return direction === 'asc' ? result : -result;
    });
  }

  function renderResearchSummary() {
    const universe = $('researchUniverse').value;
    const ready = researchRows.filter((row) => row.ready).length;
    const above200 = researchRows.filter((row) => {
      const value = safeNumber(row.vs200);
      return value !== null && value >= 0;
    }).length;
    const positive = researchRows.filter((row) => safeNumber(row.primaryReturn) > 0).length;
    const eligible = researchRows.filter((row) => row.eligible === true).length;
    text('researchUniverseCount', researchRows.length.toLocaleString('en-IN'));
    text('researchUniverseName', universe === 'fno' ? 'Stored F&O equities' : 'Current official NIFTY 500');
    text('researchReadyCount', ready.toLocaleString('en-IN'));
    text('researchAbove200Count', above200.toLocaleString('en-IN'));
    text('researchAbove200Share', ready ? `${(100 * above200 / ready).toFixed(1)}% of ready stocks` : 'No ready stocks');
    text('researchPositiveCount', positive.toLocaleString('en-IN'));
    text('researchReturnWindow', universe === 'fno' ? 'Positive over 20 sessions' : 'Positive calendar YTD');
    text('researchEligibleCount', universe === 'fno' ? eligible.toLocaleString('en-IN') : '—');
    text('researchEligibleMeta', universe === 'fno' ? 'Liquidity and history gates' : 'Available in F&O research universe');
    const researchDate = universe === 'fno' ? latestFactorPayload?.as_of_date : latestStockYtdPayload?.as_of_date;
    text('researchAsOf', displayDate(researchDate));
    if (researchDate) text('sessionAsOf', `Completed through ${displayDate(researchDate)}`);
  }

  function renderResearchTable() {
    const universe = $('researchUniverse').value;
    const rankingModel = researchBacktestModels[researchRankingModel] || researchBacktestModels.sma;
    renderResearchSummary();
    text('researchTableTitle', universe === 'fno' ? 'F&O research universe' : 'NIFTY 500 research table');
    text('researchReturnLabel', universe === 'fno' ? '20-session return' : 'YTD return');
    text('researchSecondaryLabel', universe === 'fno' ? 'Excess vs Nifty 50' : 'Vs 20DMA');
    text('researchBacktestColumnLabel', `${rankingModel.shortLabel} evidence score / gate`);
    const rows = researchFilteredRows();
    text('researchResultCount', `${rows.length.toLocaleString('en-IN')} results`);
    const visibleRows = Math.min(12, Math.max(1, rows.length));
    $('researchTableWrap').style.height = `${39 + visibleRows * 37}px`;
    const body = $('researchTableBody');
    body.innerHTML = rows.length ? rows.map((row) => {
      const evidenceClass = row.status === 'eligible' ? 'eligible' : (row.status === 'ready' ? 'ready' : 'partial');
      const evidenceLabel = row.status === 'eligible' ? 'Eligible' : (row.status === 'ready' ? 'Ready' : row.status === 'gated' ? 'Gate pending' : 'Partial');
      const backtestTone = row.backtestReady
        ? (row.rankingAdmitted ? 'positive' : 'negative')
        : 'unavailable';
      const backtestPrimary = row.backtestReady
        ? (row.rankingAdmitted && safeNumber(row.evidenceScore) !== null ? `Score ${Number(row.evidenceScore).toFixed(1)}` : 'Not ranked')
        : row.backtestStatus === 'data_quality_gated' ? 'Quality gated' : 'Unavailable';
      const gateLabels = {
        stale_latest_observation: 'stale close',
        insufficient_common_history: 'short history',
        missing_reference_sessions: 'missing sessions',
        duplicate_sessions: 'duplicate sessions',
        invalid_ohlc: 'invalid OHLC',
        abnormal_price_jump: 'extreme jump',
        reference_window_unavailable: 'reference unavailable',
        adjustment_provenance_unverified: 'adjustment source unverified',
        corporate_action_ledger_incomplete: 'corporate-action coverage incomplete',
        corporate_action_manual_review: 'corporate action needs review',
      };
      const rankingGateLabels = {
        minimum_admitted_test_folds: 'admitted OOS folds',
        minimum_completed_trades: 'trade sample',
        positive_fold_ratio: 'positive-fold ratio',
        net_profit_factor: 'net PF',
        positive_net_trade_edge: 'net edge',
        within_strategy_fdr: 'strategy FDR',
        oos_within_strategy_fdr: 'OOS strategy FDR',
        all_strategy_fdr: 'all-strategy FDR',
        drawdown_limit: 'drawdown',
        positive_excess_return: 'excess return',
        out_of_sample_stability: 'OOS stability',
        data_quality: 'data quality',
        full_history_walk_forward_unavailable: 'full-history OOS unavailable',
        full_history_data_quality: 'full-history data quality',
        missing_reference_sessions: 'full-history gaps',
      };
      const qualityReasons = (row.backtestQuality?.blocking_gates || []).map((gate) => gateLabels[gate] || gate.replaceAll('_', ' '));
      const rankingReasons = (row.rankingFailedChecks || []).map((gate) => rankingGateLabels[gate] || gate.replaceAll('_', ' '));
      const backtestSecondary = row.backtestReady
        ? `${rankingReasons.length ? `Gates: ${rankingReasons.join(', ')} · ` : ''}Diagnostic ${safeNumber(row.diagnosticScore) === null ? '—' : Number(row.diagnosticScore).toFixed(1)} · PF ${safeNumber(row.profitFactor) === null ? '—' : `${Number(row.profitFactor).toFixed(2)}×`} · ${researchQValue(row.qValue)} · all ${researchQValue(row.globalQValue)}`
        : qualityReasons.length ? qualityReasons.join(' · ') : 'Insufficient stored history';
      return `<tr>
        <td><button class="research-symbol" type="button" data-research-symbol="${escapeHtml(row.symbol)}">${escapeHtml(row.symbol)}</button><small class="research-company">${escapeHtml(row.name)}</small></td>
        <td><span class="research-sector" title="${escapeHtml(row.sector)}">${escapeHtml(row.sector)}</span></td>
        <td class="numeric">${researchNumber(row.close)}</td>
        <td class="numeric ${researchTone(row.primaryReturn)}">${researchPercent(row.primaryReturn)}</td>
        <td class="numeric ${researchTone(row.secondaryReturn)}">${researchPercent(row.secondaryReturn)}</td>
        <td class="numeric ${researchTone(row.vs50)}">${researchPercent(row.vs50)}</td>
        <td class="numeric ${researchTone(row.vs200)}">${researchPercent(row.vs200)}</td>
        <td><span class="research-evidence ${evidenceClass}">${evidenceLabel}</span></td>
        <td><span class="research-backtest-evidence ${backtestTone}"><strong>${escapeHtml(backtestPrimary)}</strong><small>${escapeHtml(backtestSecondary)}</small></span></td>
      </tr>`;
    }).join('') : '<tr><td colspan="9">No stocks match the selected research filters.</td></tr>';
    const coverage = universe === 'fno' ? latestFactorPayload?.coverage : latestStockYtdPayload?.coverage;
    text('researchCoverageNote', universe === 'fno'
      ? `${Number(coverage?.research_eligible || 0)} research eligible · ${Number(coverage?.price_history_ready || 0)} price-history ready · no ranked recommendation is published.`
      : `${Number(coverage?.ready || 0)} ready · ${Number(coverage?.partial_history || 0)} partial history · failed evidence is not ranked · diagnostic scores remain visible · full-history OOS substitution is the next increment.`);
    body.querySelectorAll('[data-research-symbol]').forEach((button) => button.addEventListener('click', () => renderResearchDetail(button.dataset.researchSymbol)));
    if ($('researchDetailDialog').open && selectedResearchSymbol && researchRows.some((row) => row.symbol === selectedResearchSymbol)) renderResearchDetail(selectedResearchSymbol);
  }

  function renderResearchDetail(symbol) {
    const row = researchRows.find((item) => item.symbol === symbol);
    if (!row) return;
    const selectionChanged = selectedResearchSymbol !== symbol;
    selectedResearchSymbol = symbol;
    text('researchDetailTitle', `${row.symbol} · ${row.name}`);
    const state = $('researchDetailState');
    state.textContent = row.eligible === true ? 'Research eligible' : row.ready ? 'Evidence ready' : 'Partial evidence';
    state.className = `state-pill ${row.eligible === true || row.ready ? 'positive' : 'neutral'}`;
    const universe = $('researchUniverse').value;
    const tiles = universe === 'fno' ? [
      ['Price', `₹${researchNumber(row.close)}`, 'Latest completed close'],
      ['20-session return', researchPercent(row.primaryReturn), 'Observed close-to-close return'],
      ['Excess vs Nifty 50', researchPercent(row.secondaryReturn), 'Same 20-session window'],
      ['12–1 momentum', researchPercent(row.momentum12), 'Final 21 sessions excluded'],
      ['20-session volatility', researchPercent(row.volatility), 'Annualised realised volatility'],
      ['Research gate', row.eligible ? 'Passed' : 'Pending', row.reason],
    ] : [
      ['Price', `₹${researchNumber(row.close)}`, 'Latest completed close'],
      ['Calendar YTD', researchPercent(row.primaryReturn), 'Current year performance'],
      ['Vs 20DMA', researchPercent(row.vs20), 'Distance from moving average'],
      ['Vs 200DMA', researchPercent(row.vs200), 'Long-trend distance'],
      ['Sector', row.sector, 'Current NIFTY 500 classification'],
      ['Evidence', row.ready ? 'Ready' : 'Partial', row.reason],
    ];
    $('researchDetailGrid').innerHTML = tiles.map(([label, value, note]) => `<article><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(note)}</small></article>`).join('');
    const button = $('researchBacktestButton');
    button.disabled = false;
    if (selectionChanged) {
      researchBacktestPanelSymbol = null;
      $('researchBacktestPanel').hidden = true;
      button.setAttribute('aria-expanded', 'false');
      button.textContent = 'View backtest evidence';
    }
    const dialog = $('researchDetailDialog');
    if (!dialog.open) dialog.showModal();
  }

  function closeResearchDetail() {
    const dialog = $('researchDetailDialog');
    if (dialog.open) dialog.close();
    selectedResearchSymbol = null;
    researchBacktestPanelSymbol = null;
    $('researchBacktestPanel').hidden = true;
    $('researchBacktestButton').setAttribute('aria-expanded', 'false');
    $('researchBacktestButton').textContent = 'View backtest evidence';
  }

  function renderResearchBacktest(payload, symbol) {
    const result = Array.isArray(payload?.results) ? payload.results[0] : null;
    if (!result) throw new Error(payload?.reason || 'Backtest evidence unavailable');
    const strategy = payload.strategy || {};
    const engine = payload.engine;
    if (result.status !== 'ready') {
      const audit = result.data_quality || {};
      const gates = Array.isArray(audit.blocking_gates) ? audit.blocking_gates : [];
      text('researchBacktestTitle', `${symbol} · backtest comparability audit`);
      const state = $('researchBacktestState');
      state.textContent = result.status === 'data_quality_gated' ? 'Quality gated' : 'Unavailable';
      state.className = 'state-pill risk';
      text('researchBacktestMethod', 'This stock is retained in the research table but excluded from profit-factor ranking until its completed-session history is comparable with the common market window.');
      const tiles = [
        ['Common cutoff', displayDate(audit.common_as_of_date), 'Shared across the ranked universe'],
        ['Comparison window', audit.comparison_sessions ? `${Number(audit.comparison_sessions)} sessions` : '—', 'After strategy-specific warm-up'],
        ['Stored history', Number(audit.stored_observations || 0).toLocaleString('en-IN'), `Required ${Number(audit.required_total_observations || 0).toLocaleString('en-IN')}`],
        ['Missing sessions', Number(audit.missing_session_count || 0).toLocaleString('en-IN'), 'Against completed Nifty 50 sessions'],
        ['Duplicate sessions', Number(audit.duplicate_session_count || 0).toLocaleString('en-IN'), 'More than one stored candle for the same date'],
        ['Invalid OHLC rows', Number(audit.invalid_ohlc_count || 0).toLocaleString('en-IN'), 'Non-finite or inconsistent prices'],
        ['Extreme jumps', Number(audit.abnormal_jump_count || 0).toLocaleString('en-IN'), 'Absolute close jump above 50% requires review'],
        ['Adjustment provenance', audit.adjustment_status || 'Unknown', audit.adjustment_gate_enforced ? 'Hard gate enforced' : 'Reported now; hard gate follows in the next milestone'],
        ['Corporate actions', Number(audit.corporate_action_count || 0).toLocaleString('en-IN'), `${Number(audit.unresolved_corporate_action_count || 0)} unresolved · ordinary cash dividends excluded from price return`],
        ['Point-in-time universe', payload.data_quality_contract?.point_in_time_universe_status === 'complete' ? 'Complete' : 'Partial', payload.data_quality_contract?.point_in_time_universe_gate_enforced ? 'Hard ranking gate enforced' : 'Official notices and temporary lineage stored; survivorship warning retained'],
        ['Blocking checks', gates.length ? gates.length.toLocaleString('en-IN') : '—', gates.length ? gates.map((gate) => gate.replaceAll('_', ' ')).join(' · ') : (result.reason || 'Evidence unavailable')],
      ];
      $('researchBacktestGrid').innerHTML = tiles.map(([label, value, note]) => `<article><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(note)}</small></article>`).join('');
      text('researchBacktestCrossover', 'No strategy rank is published for this stock while a comparability gate is active.');
      text('researchBacktestLimitations', Array.isArray(payload.limitations) ? payload.limitations.join(' ') : 'Data-quality gate active.');
      return;
    }
    const currentState = result.current_signal === 'invested' ? 'Invested state' : 'Cash state';
    const titles = {
      moving_average_crossover: `${symbol} · ${strategy.fast_window || 20}/${strategy.slow_window || 50}-session SMA crossover`,
      donchian_breakout: `${symbol} · ${strategy.entry_window || 55}/${strategy.exit_window || 20}-session Donchian breakout`,
      ema_crossover: `${symbol} · ${strategy.fast_window || 20}/${strategy.slow_window || 50}-session EMA crossover`,
      time_series_momentum: `${symbol} · ${strategy.lookback || 63}-session absolute momentum`,
      triple_sma_alignment: `${symbol} · ${strategy.fast_window || 20}/${strategy.medium_window || 50}/${strategy.slow_window || 200} triple-SMA alignment`,
      rsi_mean_reversion: `${symbol} · RSI(${strategy.period || 2}) oversold recovery`,
      bollinger_mean_reversion: `${symbol} · ${strategy.window || 20}-session Bollinger re-entry`,
      zscore_mean_reversion: `${symbol} · ${strategy.window || 10}-session price z-score reversal`,
      pullback_uptrend: `${symbol} · ${strategy.pullback_window || 5}/${strategy.trend_window || 200} pullback in uptrend`,
      selloff_recovery: `${symbol} · ${strategy.down_sessions || 3}-day selloff recovery`,
    };
    text('researchBacktestTitle', titles[engine] || `${symbol} · strategy evidence`);
    const state = $('researchBacktestState');
    state.textContent = currentState;
    state.className = `state-pill ${result.current_signal === 'invested' ? 'positive' : 'neutral'}`;
    const evaluated = `${Number(result.evaluated_sessions || 0).toLocaleString('en-IN')} sessions through ${displayDate(result.as_of_date)}`;
    const fullHistory = strategy.history_mode === 'full';
    const studyLabel = fullHistory ? 'Full available history' : '252-session comparison';
    const methods = {
      moving_average_crossover: `Long-or-cash simple moving-average crossover · completed-close signal shifted one session · evaluated ${evaluated}.`,
      donchian_breakout: `Long-or-cash channel breakout · enter above the prior ${strategy.entry_window || 55}-session high and exit below the prior ${strategy.exit_window || 20}-session low · one-session signal shift · evaluated ${evaluated}.`,
      ema_crossover: `Long-or-cash exponential moving-average crossover · faster weighting responds sooner than the SMA model · one-session signal shift · evaluated ${evaluated}.`,
      time_series_momentum: `Long-or-cash absolute momentum · invested only while the completed close is above its level ${strategy.lookback || 63} sessions earlier · one-session signal shift · evaluated ${evaluated}.`,
      triple_sma_alignment: `Long-or-cash trend alignment · invested only while the ${strategy.fast_window || 20}-session SMA is above the ${strategy.medium_window || 50}-session SMA and both are above the ${strategy.slow_window || 200}-session SMA · one-session signal shift · evaluated ${evaluated}.`,
      rsi_mean_reversion: `Long-or-cash mean reversion · enter at RSI(${strategy.period || 2}) ≤ ${strategy.entry_rsi || 10} and exit at RSI ≥ ${strategy.exit_rsi || 70} · one-session signal shift · evaluated ${evaluated}.`,
      bollinger_mean_reversion: `Long-or-cash mean reversion · enter below the ${strategy.window || 20}-session lower band at ${strategy.deviations || 2} standard deviations and exit at the rolling mean · one-session signal shift · evaluated ${evaluated}.`,
      zscore_mean_reversion: `Long-or-cash mean reversion · enter at a ${strategy.window || 10}-session price z-score ≤ ${strategy.entry_z ?? -1.5} and exit at z-score ≥ ${strategy.exit_z ?? 0} · one-session signal shift · evaluated ${evaluated}.`,
      pullback_uptrend: `Long-or-cash conditional mean reversion · enter below the ${strategy.pullback_window || 5}-session SMA only while above the ${strategy.trend_window || 200}-session SMA; exit on recovery or trend failure · evaluated ${evaluated}.`,
      selloff_recovery: `Long-or-cash short-horizon mean reversion · enter after ${strategy.down_sessions || 3} consecutive lower closes below the ${strategy.exit_window || 5}-session SMA and exit on recovery to that average · evaluated ${evaluated}.`,
    };
    text('researchBacktestMethod', `${studyLabel} · ${methods[engine] || `Long-or-cash research model · evaluated ${evaluated}.`}`);
    let signalTile;
    if (engine === 'donchian_breakout') {
      signalTile = ['Channel position', result.current_signal === 'invested' ? researchPercent(result.distance_to_exit_pct) : researchPercent(result.distance_to_entry_pct), result.current_signal === 'invested' ? `Above ${researchNumber(result.current_exit_channel)} exit channel` : `Vs ${researchNumber(result.current_entry_channel)} entry channel`, result.current_signal === 'invested' ? 'positive' : ''];
    } else if (engine === 'time_series_momentum') {
      signalTile = ['Absolute momentum', researchPercent(result.current_lookback_return_pct), `${strategy.lookback || 63}-session completed-close return`, result.current_lookback_return_pct > 0 ? 'positive' : 'negative'];
    } else if (engine === 'triple_sma_alignment') {
      signalTile = ['Trend alignment', result.current_signal === 'invested' ? 'Aligned' : 'Not aligned', `${researchNumber(result.current_fast_sma)} / ${researchNumber(result.current_medium_sma)} / ${researchNumber(result.current_slow_sma)}`, result.current_signal === 'invested' ? 'positive' : 'neutral'];
    } else if (engine === 'rsi_mean_reversion') {
      signalTile = ['Current RSI', researchNumber(result.current_rsi), `Entry ≤ ${strategy.entry_rsi || 10} · exit ≥ ${strategy.exit_rsi || 70}`, result.current_rsi <= (strategy.entry_rsi || 10) ? 'positive' : ''];
    } else if (engine === 'bollinger_mean_reversion') {
      signalTile = ['Band position', researchPercent(result.distance_to_middle_pct), `${researchNumber(result.current_lower_band)} lower · ${researchNumber(result.current_middle_band)} mean`, result.current_signal === 'invested' ? 'positive' : ''];
    } else if (engine === 'zscore_mean_reversion') {
      signalTile = ['Current z-score', researchNumber(result.current_zscore), `Entry ≤ ${strategy.entry_z ?? -1.5} · exit ≥ ${strategy.exit_z ?? 0}`, result.current_zscore <= (strategy.entry_z ?? -1.5) ? 'positive' : ''];
    } else if (engine === 'pullback_uptrend') {
      signalTile = ['Pullback position', researchPercent(result.distance_to_pullback_sma_pct), `${researchNumber(result.current_pullback_sma)} pullback SMA · ${researchNumber(result.current_trend_sma)} trend SMA`, result.current_signal === 'invested' ? 'positive' : ''];
    } else if (engine === 'selloff_recovery') {
      signalTile = ['Recovery distance', researchPercent(result.distance_to_exit_sma_pct), `${researchNumber(result.current_exit_sma)} exit SMA`, result.current_signal === 'invested' ? 'positive' : ''];
    } else {
      const fast = engine === 'ema_crossover' ? result.current_fast_ema : result.current_fast_sma;
      const slow = engine === 'ema_crossover' ? result.current_slow_ema : result.current_slow_sma;
      signalTile = ['Current spread', researchPercent(result.current_spread_pct), `${researchNumber(fast)} fast vs ${researchNumber(slow)} slow`, result.current_spread_pct >= 0 ? 'positive' : 'negative'];
    }
    const walkForward = result.rolling_walk_forward || {};
    const admission = walkForward.ranking_admission || {};
    const admissionMetrics = admission.metrics || {};
    const admittedFolds = Number(admissionMetrics.admitted_test_folds || 0);
    const totalFolds = Number(walkForward.fold_count || 0);
    const failedChecks = Array.isArray(admission.failed_checks) ? admission.failed_checks : [];
    const failedLabels = {
      admitted_test_folds: 'minimum admitted folds',
      completed_trades: 'completed trades',
      positive_fold_ratio: 'positive-fold ratio',
      median_net_profit_factor: 'median net profit factor',
      median_net_trade_expectancy_pct: 'median net trade edge',
      worst_net_drawdown_pct: 'drawdown limit',
    };
    const statisticalEvidence = result.statistical_evidence || {};
    const bootstrapSample = statisticalEvidence.sample || {};
    const ordinaryBootstrap = statisticalEvidence.ordinary_bootstrap || {};
    const blockBootstrap = statisticalEvidence.moving_block_bootstrap || {};
    const formatInterval = (interval, suffix = '') => Array.isArray(interval) && interval.length === 2 && interval.every((value) => safeNumber(value) !== null)
      ? `${Number(interval[0]) >= 0 ? '+' : ''}${Number(interval[0]).toFixed(2)}${suffix} to ${Number(interval[1]) >= 0 ? '+' : ''}${Number(interval[1]).toFixed(2)}${suffix}`
      : 'Unavailable';
    const bootstrapReady = statisticalEvidence.status === 'ready';
    const rankingEvidence = researchRows.find((row) => row.symbol === symbol) || {};
    const rankingGateLabels = {
      minimum_admitted_test_folds: 'admitted OOS folds',
      minimum_completed_trades: 'trade sample',
      positive_fold_ratio: 'positive-fold ratio',
      net_profit_factor: 'net PF',
      positive_net_trade_edge: 'net edge',
      within_strategy_fdr: 'strategy FDR',
      oos_within_strategy_fdr: 'OOS strategy FDR',
      all_strategy_fdr: 'all-strategy FDR',
      drawdown_limit: 'drawdown',
      positive_excess_return: 'excess return',
      out_of_sample_stability: 'OOS stability',
      data_quality: 'data quality',
      full_history_walk_forward_unavailable: 'full-history OOS unavailable',
      full_history_data_quality: 'full-history data quality',
      missing_reference_sessions: 'full-history gaps',
    };
    const rankingFailures = (rankingEvidence.rankingFailedChecks || []).map((name) => rankingGateLabels[name] || name.replaceAll('_', ' '));
    const holdoutPeriods = result.chronological_validation?.periods || [];
    const trainingPeriod = holdoutPeriods.find((period) => period.period === 'training') || {};
    const untouchedPeriod = holdoutPeriods.find((period) => period.period === 'untouched_test') || {};
    const oosEvidence = walkForward.out_of_sample_statistical_evidence || {};
    const tiles = [
      ['Study window', studyLabel, `${displayDate(result.evaluation_start)} to ${displayDate(result.as_of_date)} · ${Number(result.evaluated_sessions || 0).toLocaleString('en-IN')} evaluated sessions`, 'neutral'],
      signalTile,
      ['Execution state', result.current_executed_position === 'invested' ? 'Invested' : 'Cash', result.pending_next_session_action ? `Latest close signal targets ${result.current_signal}; action awaits the next valid open` : 'Latest close signal is already reflected in the executed position', result.current_executed_position === 'invested' ? 'positive' : 'neutral'],
      ['Gross strategy return', researchPercent(result.gross_strategy_return_pct), `Before estimated charges since ${displayDate(result.first_execution_date || result.evaluation_start)}`, result.gross_strategy_return_pct >= 0 ? 'positive' : 'negative'],
      ['Net strategy return', researchPercent(result.net_strategy_return_pct), `Estimated cost drag ${researchPercent(result.estimated_cost_drag_pct)}`, result.net_strategy_return_pct >= 0 ? 'positive' : 'negative'],
      ['Buy & hold', researchPercent(result.buy_and_hold_return_pct), 'Same evaluation window', result.buy_and_hold_return_pct >= 0 ? 'positive' : 'negative'],
      ['Excess return', researchPercent(result.excess_return_pct), 'Strategy minus buy & hold', result.excess_return_pct >= 0 ? 'positive' : 'negative'],
      ['Maximum drawdown', researchPercent(result.maximum_drawdown_pct), 'Peak-to-trough strategy equity', result.maximum_drawdown_pct < 0 ? 'negative' : ''],
      ['Annualised return', researchPercent(result.annualised_return_pct), `Annualised volatility ${researchPercent(result.annualised_volatility_pct)}`, result.annualised_return_pct >= 0 ? 'positive' : 'negative'],
      ['Exposure', researchPercent(result.exposure_pct), `${Number(result.invested_sessions || 0).toLocaleString('en-IN')} invested sessions`, ''],
      ['Trade evidence', safeNumber(result.win_rate_pct) === null ? '—' : researchPercent(result.win_rate_pct), `${Number(result.winning_trades || 0)} winners of ${Number(result.completed_trades || 0)} completed trades`, ''],
      ['Net profit factor', safeNumber(result.net_profit_factor) === null ? '—' : `${Number(result.net_profit_factor).toFixed(2)}×`, `Gross PF ${safeNumber(result.profit_factor) === null ? '—' : `${Number(result.profit_factor).toFixed(2)}×`} · estimated charges ₹${researchNumber(result.estimated_total_cost_inr)}`, safeNumber(result.net_profit_factor) >= 1 ? 'positive' : 'negative'],
      ['Net trade edge', safeNumber(result.net_trade_expectancy_pct) === null ? '—' : researchPercent(result.net_trade_expectancy_pct), `Gross edge ${researchPercent(result.trade_expectancy_pct)} · ${Number(result.completed_trades || 0)} completed trades`, safeNumber(result.net_trade_expectancy_pct) >= 0 ? 'positive' : 'negative'],
      ['Net statistical significance', researchPValue(result.net_trade_edge_p_value), `${Number(result.completed_trades || 0)} cost-adjusted trades · exploratory two-sided t-test`, safeNumber(result.net_trade_edge_p_value) !== null && result.net_trade_edge_p_value <= 0.05 ? (safeNumber(result.net_trade_expectancy_pct) >= 0 ? 'positive' : 'negative') : ''],
      ['OOS universe-adjusted significance', researchQValue(rankingEvidence.qValue), `${researchPValue(rankingEvidence.pValue)} across full-history, point-in-time-admitted ${researchBacktestModels[researchRankingModel]?.shortLabel || 'strategy'} test folds`, safeNumber(rankingEvidence.qValue) !== null && rankingEvidence.qValue <= 0.05 ? (safeNumber(rankingEvidence.tradeEdge) >= 0 ? 'positive' : 'negative') : 'neutral'],
      ['All-strategy OOS significance', researchQValue(rankingEvidence.globalQValue), latestCrossStrategyFdrPayload ? `${Number(latestCrossStrategyFdrPayload.tested_hypotheses || 0).toLocaleString('en-IN')} full-history OOS stock × strategy hypotheses across ${Number(latestCrossStrategyFdrPayload.strategy_count || 0)} models` : 'Combined OOS correction is still calculating', safeNumber(rankingEvidence.globalQValue) !== null && rankingEvidence.globalQValue <= 0.05 ? (safeNumber(rankingEvidence.tradeEdge) >= 0 ? 'positive' : 'negative') : 'neutral'],
      ['Evidence ranking gate', rankingEvidence.rankingAdmitted ? 'Admitted' : 'Not ranked', rankingFailures.length ? `Failed: ${rankingFailures.join(' · ')}` : 'Every ranking gate passes', rankingEvidence.rankingAdmitted ? 'positive' : 'negative'],
      ['Composite diagnostic score', safeNumber(rankingEvidence.diagnosticScore) === null ? '—' : `${Number(rankingEvidence.diagnosticScore).toFixed(1)} / 100`, 'Diagnostic only until every evidence gate passes; it is not a recommendation.', 'neutral'],
      ['Frozen training significance', researchPValue(trainingPeriod.net_trade_edge_p_value), `${Number(trainingPeriod.completed_trades || 0)} completed net trades · in-sample 60% holdout segment`, 'neutral'],
      ['Untouched-test significance', researchPValue(untouchedPeriod.net_trade_edge_p_value), `${Number(untouchedPeriod.completed_trades || 0)} completed net trades · frozen final 20% holdout`, safeNumber(untouchedPeriod.net_trade_edge_p_value) !== null && untouchedPeriod.net_trade_edge_p_value <= 0.05 ? (safeNumber(untouchedPeriod.net_trade_expectancy_pct) >= 0 ? 'positive' : 'negative') : 'neutral'],
      ['Admitted-fold OOS significance', researchPValue(oosEvidence.net_trade_edge_p_value), `${Number(oosEvidence.completed_net_trades || 0)} net trades across ${Number(oosEvidence.admitted_test_folds || 0)} point-in-time-admitted test folds`, safeNumber(oosEvidence.net_trade_edge_p_value) !== null && oosEvidence.net_trade_edge_p_value <= 0.05 ? 'positive' : 'neutral'],
      ['Bootstrap edge interval', bootstrapReady ? formatInterval(ordinaryBootstrap.net_expectancy_ci_95_pct, '%') : 'Insufficient sample', bootstrapReady ? `95% ordinary-bootstrap interval · ${Number(ordinaryBootstrap.resamples || 0).toLocaleString('en-IN')} resamples` : `${Number(bootstrapSample.completed_net_trades || 0)} / ${Number(bootstrapSample.minimum_required || 15)} completed net trades`, bootstrapReady && safeNumber(ordinaryBootstrap.net_expectancy_ci_95_pct?.[0]) > 0 ? 'positive' : bootstrapReady && safeNumber(ordinaryBootstrap.net_expectancy_ci_95_pct?.[1]) < 0 ? 'negative' : 'neutral'],
      ['Block-bootstrap edge interval', bootstrapReady ? formatInterval(blockBootstrap.net_expectancy_ci_95_pct, '%') : 'Insufficient sample', bootstrapReady ? `95% interval · ${Number(blockBootstrap.block_length_trades || 0)}-trade circular blocks` : 'Serial-dependence check activates at the minimum sample', bootstrapReady && safeNumber(blockBootstrap.net_expectancy_ci_95_pct?.[0]) > 0 ? 'positive' : bootstrapReady && safeNumber(blockBootstrap.net_expectancy_ci_95_pct?.[1]) < 0 ? 'negative' : 'neutral'],
      ['Net PF uncertainty', bootstrapReady ? formatInterval(blockBootstrap.net_profit_factor_ci_95, '×') : 'Insufficient sample', bootstrapReady ? `${Number(blockBootstrap.valid_profit_factor_resamples || 0).toLocaleString('en-IN')} valid block-bootstrap resamples` : 'Profit-factor interval activates with adequate completed trades', bootstrapReady && safeNumber(blockBootstrap.net_profit_factor_ci_95?.[0]) > 1 ? 'positive' : bootstrapReady && safeNumber(blockBootstrap.net_profit_factor_ci_95?.[1]) < 1 ? 'negative' : 'neutral'],
      ['Walk-forward coverage', totalFolds ? `${admittedFolds} / ${totalFolds} folds admitted` : 'Unavailable', `Point-in-time reviewed window starts ${displayDate(walkForward.point_in_time_coverage?.reviewed_from)}`, admittedFolds >= 3 ? 'positive' : 'neutral'],
      ['OOS stability gate', totalFolds ? (admission.ranking_admissible ? 'Admitted' : 'Gated') : 'Unavailable', totalFolds ? (failedChecks.length ? `Pending: ${failedChecks.map((name) => failedLabels[name] || name.replaceAll('_', ' ')).join(' · ')}` : 'Every out-of-sample stability check passes') : 'Stored history does not yet support the fixed walk-forward fold design', admission.ranking_admissible ? 'positive' : 'negative'],
      ['OOS median net PF', safeNumber(admissionMetrics.median_net_profit_factor) === null ? '—' : `${Number(admissionMetrics.median_net_profit_factor).toFixed(2)}×`, `${Number(admissionMetrics.completed_trades || 0)} completed trades across admitted test folds`, safeNumber(admissionMetrics.median_net_profit_factor) >= 1.1 ? 'positive' : 'negative'],
      ['OOS positive folds', safeNumber(admissionMetrics.positive_fold_ratio) === null ? '—' : researchPercent(Number(admissionMetrics.positive_fold_ratio) * 100), `Median net edge ${researchPercent(admissionMetrics.median_net_trade_expectancy_pct)} · worst drawdown ${researchPercent(admissionMetrics.worst_net_drawdown_pct)}`, safeNumber(admissionMetrics.positive_fold_ratio) >= 0.6 ? 'positive' : 'negative'],
      ['Point-in-time universe', payload.data_quality_contract?.point_in_time_universe_status === 'complete_for_reviewed_window' ? 'Reviewed window complete' : 'Partial', payload.data_quality_contract?.walk_forward_fold_universe_gate_enforced ? 'Applied independently to every walk-forward test fold; current-window ranking remains exploratory' : 'Historical membership gate not yet applied', 'neutral'],
    ];
    $('researchBacktestGrid').innerHTML = tiles.map(([label, value, note, tone]) => `<article class="${tone}"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(note)}</small></article>`).join('');
    const event = engine === 'moving_average_crossover' ? result.last_crossover : result.last_event;
    const eventName = engine === 'donchian_breakout' ? 'channel event'
      : engine === 'time_series_momentum' ? 'momentum change'
      : engine === 'triple_sma_alignment' ? 'alignment change'
      : ['rsi_mean_reversion', 'bollinger_mean_reversion', 'zscore_mean_reversion', 'pullback_uptrend', 'selloff_recovery'].includes(engine) ? 'mean-reversion event'
      : 'crossover';
    text('researchBacktestCrossover', event
      ? `Latest ${eventName}: ${String(event.type || '').replaceAll('_', ' ')} on ${displayDate(event.signal_date)} · ${event.execution_status === 'executed' ? `executed at ${researchNumber(event.execution_price)} on ${displayDate(event.execution_date)}` : event.execution_status === 'pending_next_session' ? 'pending the next valid session open' : 'rejected because the next opening price was unavailable'}.`
      : `No ${eventName} was observed in the available evaluation window.`);
    text('researchBacktestLimitations', Array.isArray(payload.limitations) ? payload.limitations.join(' ') : 'Gross historical calculation; no trade recommendation or order instruction.');
  }

  async function toggleResearchBacktest() {
    const symbol = selectedResearchSymbol;
    if (!symbol) return;
    const strategy = $('researchBacktestStrategy').value;
    const model = researchBacktestModels[strategy] || researchBacktestModels.sma;
    const panelKey = `full:${strategy}:${symbol}`;
    const panel = $('researchBacktestPanel');
    const button = $('researchBacktestButton');
    if (!panel.hidden && researchBacktestPanelSymbol === panelKey) {
      panel.hidden = true;
      button.setAttribute('aria-expanded', 'false');
      button.textContent = 'View backtest evidence';
      return;
    }
    researchBacktestPanelSymbol = panelKey;
    panel.hidden = false;
    button.setAttribute('aria-expanded', 'true');
    button.textContent = 'Hide backtest evidence';
    text('researchBacktestTitle', `${symbol} · loading ${model.label} evidence`);
    $('researchBacktestState').textContent = 'Loading';
    $('researchBacktestState').className = 'state-pill neutral';
    $('researchBacktestGrid').innerHTML = `<div class="empty-state">Running the background ${escapeHtml(model.label)} calculation.</div>`;
    try {
      let payload = researchBacktestCache.get(panelKey);
      if (!payload) {
        const response = await fetch(`${model.endpoint}?symbol=${encodeURIComponent(symbol)}&history=full`, { cache: 'no-store' });
        payload = await response.json();
        if (!response.ok || payload.ok !== true) throw new Error(payload.reason || `HTTP ${response.status}`);
        researchBacktestCache.set(panelKey, payload);
      }
      if (researchBacktestPanelSymbol !== panelKey) return;
      renderResearchBacktest(payload, symbol);
    } catch (error) {
      if (researchBacktestPanelSymbol !== panelKey) return;
      $('researchBacktestState').textContent = 'Unavailable';
      $('researchBacktestState').className = 'state-pill risk';
      $('researchBacktestGrid').innerHTML = `<div class="empty-state">Unable to load backtest evidence: ${escapeHtml(error.message)}</div>`;
      text('researchBacktestCrossover', 'Latest crossover —');
      text('researchBacktestLimitations', 'The stock may not have sufficient stored history for this calculation.');
    }
  }

  function renderSignals(story) {
    const signals = Array.isArray(story?.signals) ? story.signals.slice(0, 4) : [];
    $('signalGrid').innerHTML = signals.length ? signals.map((signal) => {
      const meter = Math.max(0, Math.min(100, Number(signal.meter_pct || 0)));
      return `<article class="signal-tile ${escapeHtml(signal.state || '')}" style="--meter:${meter / 100}">
        <span>${escapeHtml(signal.label || 'Signal')}</span>
        <strong>${escapeHtml(signal.value ?? '—')}</strong>
        <p>${escapeHtml(signal.detail || '')}</p>
      </article>`;
    }).join('') : '<div class="empty-state">Validated signal tiles are not available.</div>';
  }

  function renderLeadership(story) {
    const relative = story?.market_context?.relative_strength || {};
    const leaders = Array.isArray(relative.indices?.leaders) ? relative.indices.leaders : [relative.indices?.strongest].filter(Boolean);
    const laggards = Array.isArray(relative.indices?.laggards) ? relative.indices.laggards : [relative.indices?.weakest].filter(Boolean);
    const benchmark = relative.benchmark || {};
    const windowSessions = Number(relative.window_sessions || 0);
    const windowLabel = windowSessions > 0 ? `${windowSessions}-session relative return` : 'Relative-return window unavailable';
    const benchmarkWindow = safeNumber(benchmark.return_pct) === null ? '—' : signedPercent(benchmark.return_pct);
    const benchmarkYtd = safeNumber(benchmark.ytd_return_pct) === null ? '—' : signedPercent(benchmark.ytd_return_pct);
    const throughDate = displayDate(relative.as_of_date || benchmark.as_of_date);
    const pullDate = displayDate(relative.retrieved_at || benchmark.retrieved_at);
    text('relativeBenchmark', `${windowLabel} · Nifty 50 ${benchmarkWindow} · YTD ${benchmarkYtd} · data through ${throughDate} · pulled ${pullDate}`);
    const renderRows = (rows, label, weak) => rows.slice(0, 3).map((row, index) => {
      const excess = safeNumber(row.excess_vs_nifty50_pct) || 0;
      const ytd = safeNumber(row.ytd_return_pct);
      const width = Math.min(100, Math.max(6, Math.abs(excess) * 10));
      return `<article class="leader-row ${weak ? 'weak' : ''} ${excess < 0 ? 'underperform' : ''}">
        <header><span>${escapeHtml(label)} ${index + 1}</span><b>${signedPercent(excess)}</b></header>
        <strong>${escapeHtml(row.name || '—')}</strong>
        <div class="leader-ytd"><span>YTD return</span><em class="${(ytd || 0) < 0 ? 'negative' : 'positive'}">${signedPercent(ytd)}</em></div>
        <div class="leader-bar"><i style="width:${width}%"></i></div>
      </article>`;
    }).join('');
    $('leadershipGrid').innerHTML = leaders.length || laggards.length
      ? `<section class="leadership-column leadership-strongest" aria-label="Strongest indices">
          ${renderRows(leaders, 'Strongest', false)}
        </section>
        <section class="leadership-column leadership-weakest" aria-label="Weakest indices">
          ${renderRows(laggards, 'Weakest', true)}
        </section>`
      : '<div class="empty-state">Connect Kite and refresh Workspace EOD data to populate leadership.</div>';
  }

  function renderEvents(story, eventsSummary) {
    const events = Array.isArray(story?.catalysts) ? story.catalysts.slice(0, 3) : [];
    $('eventList').innerHTML = events.length ? events.map((event) => `<div class="event-row">
      <time>${escapeHtml(displayDate(event.start_at))}</time>
      <div><strong>${escapeHtml(event.title || 'Scheduled event')}</strong><span>${escapeHtml(event.region || '—')} · ${escapeHtml(String(event.category || '').replaceAll('_', ' '))}</span></div>
      <b>${Number(event.days_until || 0)}D</b>
    </div>`).join('') : '<div class="empty-state">No reviewed future catalyst is available.</div>';
    const seasonality = story?.seasonality || {};
    text('seasonalityNote', safeNumber(seasonality.average_return_pct) === null
      ? (eventsSummary?.next_event?.title ? `Next event: ${eventsSummary.next_event.title}` : 'Seasonality context is unavailable.')
      : `${seasonality.month || 'Current month'} average ${signedPercent(seasonality.average_return_pct)} across ${Number(seasonality.count || 0)} completed years.`);
  }

  function evidenceManifest(evidence = {}) {
    const clusters = Array.isArray(evidence.clusters) ? evidence.clusters : [];
    const available = clusters.filter((cluster) => cluster.available === true);
    const missing = clusters.filter((cluster) => cluster.available !== true);
    const availableCount = clusters.length ? available.length : Number(evidence.available_clusters || 0);
    const totalCount = clusters.length ? clusters.length : Number(evidence.total_clusters || 6);
    const availableLabels = clusters.length
      ? available.map((cluster) => cluster.label || cluster.key)
      : (Array.isArray(evidence.available_labels) ? evidence.available_labels : []);
    const missingLabels = clusters.length
      ? missing.map((cluster) => cluster.label || cluster.key)
      : (Array.isArray(evidence.missing_labels) ? evidence.missing_labels : []);
    return {
      clusters,
      availableCount,
      totalCount,
      availableLabels,
      missingLabels,
      count: `${availableCount}/${totalCount}`,
      missingText: missingLabels.length ? `Missing: ${missingLabels.join(', ')}` : 'Missing: none',
      availableText: availableLabels.length ? `Available: ${availableLabels.join(', ')}` : 'Available: none',
    };
  }

  function applyEvidenceDisclosure(id, evidence, options = {}) {
    const element = $(id);
    if (!element) return evidenceManifest(evidence);
    const manifest = evidenceManifest(evidence);
    const suffix = options.suffix || '';
    element.textContent = `${manifest.count}${suffix}`;
    element.title = `${manifest.availableText}. ${manifest.missingText}.`;
    element.setAttribute('aria-label', `${manifest.count} evidence clusters. ${manifest.availableText}. ${manifest.missingText}.`);
    return manifest;
  }

  function renderReadiness(payload) {
    const sentiment = payload.sentiment || {};
    const fno = payload.fno || {};
    const screen = payload.screener || {};
    const counts = sentiment.decision_counts || {};
    const evidence = sentiment.evidence || {};
    text('sentimentState', String(sentiment.status || 'Unavailable').replaceAll('_', ' '));
    text('sentimentObservation', `${Number(counts.long || 0)} long · ${Number(counts.confirmed_short || 0)} confirmed short · ${Number(counts.watch || 0)} watch`);
    const sentimentManifest = applyEvidenceDisclosure('sentimentCoverage', evidence);
    text('sentimentObservation', `${Number(counts.long || 0)} long · ${Number(counts.confirmed_short || 0)} confirmed short · ${Number(counts.watch || 0)} watch · ${sentimentManifest.missingText}`);

    const fnoCounts = fno.positioning || fno.counts || fno.positioning_counts || {};
    text('fnoState', String(fno.status || 'Unavailable').replaceAll('_', ' '));
    text('fnoObservation', `${Number(fnoCounts.bullish || fnoCounts.long_buildup || 0)} bullish · ${Number(fnoCounts.bearish || fnoCounts.short_buildup || 0)} bearish · ${Number(fnoCounts.neutral || fnoCounts.no_clear_signal || 0)} neutral · ${Number(fnoCounts.unavailable || 0)} unavailable`);
    const fnoCoverage = fno.coverage || {};
    text('fnoCoverage', fno.coverage_label || (safeNumber(fnoCoverage.latest) === null ? '—' : `${fnoCoverage.latest}/${fnoCoverage.expected}`));

    const screenCounts = screen.observations || screen.counts || {};
    const screenCoverage = screen.coverage || {};
    text('screenState', String(screen.status || 'Unavailable').replaceAll('_', ' '));
    text('screenObservation', `${Number(screenCounts.positive_20d || screenCounts.positive_20_session || screen.positive_20_session || 0)} positive over 20 sessions`);
    text('screenCoverage', screen.calculation_ready ?? screenCounts.calculation_ready ?? screenCoverage.ready ?? '—');
  }

  function renderCrossAssetContext(payload, domestic = latestDomesticPayload, snapshots = latestSnapshotPayload) {
    if (!payload || !$('stateContextStack')) return;
    const story = payload.market_story || {};
    const fno = payload.fno || {};
    const fnoPositioning = fno.positioning || {};
    const relative = story.market_context?.relative_strength || {};
    const curves = Array.isArray(snapshots?.yields) ? snapshots.yields : [];
    const curve = (market) => curves.find((row) => row.market === market);
    const yieldValue = (market, tenor) => safeNumber(curve(market)?.tenors?.[String(tenor)]);
    const yieldContext = (market, tenor) => curve(market)?.tenor_context?.[String(tenor)] || {};
    const india10 = yieldValue('India', 10);
    const us10 = yieldValue('United States', 10);
    const india10Context = yieldContext('India', 10);
    const us10Context = yieldContext('United States', 10);
    const brent = domestic?.global_risk?.metrics?.brent_crude || {};
    const brentLevel = safeNumber(brent.level);
    const brentChange = safeNumber(brent.twenty_session_change_pct);
    const indiaVix = domestic?.global_risk?.metrics?.india_vix || {};
    const usVix = domestic?.global_risk?.metrics?.us_vix || {};
    const sp500 = domestic?.global_risk?.metrics?.sp500 || {};
    const usdInr = domestic?.global_risk?.metrics?.usd_inr || {};
    const riskCoverage = domestic?.global_risk?.coverage || {};
    const indiaVixLevel = safeNumber(indiaVix.level);
    const usVixLevel = safeNumber(usVix.level);
    const sp500Change = safeNumber(sp500.twenty_session_change_pct);
    const usdInrChange = safeNumber(usdInr.twenty_session_change_pct);
    const globalRiskValue = domestic?.global_risk?.available
      ? `India VIX ${indiaVixLevel === null ? '—' : indiaVixLevel.toFixed(2)} · U.S. VIX ${usVixLevel === null ? '—' : usVixLevel.toFixed(2)}`
      : 'Awaiting admitted core EOD';
    const globalRiskDetail = domestic?.global_risk?.available
      ? `S&P 500 20-session ${sp500Change === null ? '—' : signedPercent(sp500Change)} · USD/INR 20-session ${usdInrChange === null ? '—' : signedPercent(usdInrChange)} · ${Number(riskCoverage.core_available || 0)}/${Number(riskCoverage.core_required || 4)} core series`
      : (domestic?.global_risk?.reason || 'No admitted global-risk observation.');
    const contextRows = [
      ['F&O positioning', `${Number(fnoPositioning.bullish || 0)} bullish / ${Number(fnoPositioning.bearish || 0)} bearish / ${Number(fnoPositioning.neutral || 0)} neutral`, `${Number(fnoPositioning.unavailable || 0)} unavailable of ${Number(fnoPositioning.total || fno.coverage?.expected || 0)} F&O stocks; ${Number(fno.stored_history_sessions || 0)} stored sessions.`],
      ['India government yield', india10 === null ? 'Awaiting stored EOD' : `10Y ${india10.toFixed(3)}%`, india10 === null ? 'No admitted sovereign-curve observation.' : `${String(india10Context.state || 'limited')} range · ${displayDate(curve('India')?.observation_date)}`],
      ['U.S. government yield', us10 === null ? 'Awaiting stored EOD' : `10Y ${us10.toFixed(3)}%`, us10 === null ? 'No admitted sovereign-curve observation.' : `${String(us10Context.state || 'limited')} range · ${displayDate(curve('United States')?.observation_date)}`],
      ['Commodity — Brent crude', brentLevel === null ? 'Awaiting stored EOD' : `$${brentLevel.toFixed(2)} / bbl`, brentLevel === null ? (domestic?.global_risk?.reason || 'No admitted commodity observation.') : `${brentChange === null ? '20-session change unavailable' : `20-session ${signedPercent(brentChange)}`} · ${displayDate(brent.as_of_date)}`],
      ['Global risk pulse', globalRiskValue, globalRiskDetail],
      ['Index leadership', relative.indices?.strongest ? `${relative.indices.strongest.name} ${signedPercent(relative.indices.strongest.excess_vs_nifty50_pct)} vs Nifty 50` : 'Unavailable', `${Number(relative.indices?.evaluated || 0)} indices evaluated over ${Number(relative.window_sessions || 0)} sessions.`],
      ['Sector leadership', relative.sectors?.strongest ? `${relative.sectors.strongest.name} ${signedPercent(relative.sectors.strongest.excess_vs_nifty50_pct)} vs Nifty 50` : 'Unavailable', `${Number(relative.sectors?.evaluated || 0)} sectors evaluated on equal-weight stock returns.`],
    ];
    $('stateContextStack').innerHTML = contextRows.map(([label, value, detail]) => `<article><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong><p>${escapeHtml(detail)}</p></article>`).join('');
  }

  function renderMarketSeasonality(payload) {
    latestSeasonalityPayload = payload;
    const current = payload?.current_month || {};
    const monthNumber = Number(current.number || new Date().getMonth() + 1);
    const monthName = new Date(2000, Math.max(0, monthNumber - 1), 1).toLocaleDateString('en-IN', { month: 'long' });
    const nifty50 = current.nifty50 || {};
    const leader = current.leading_index || {};
    const lagger = current.lagging_index || {};
    const evaluated = Number(current.indices_evaluated || 0);
    text('stateSeasonalityTitle', `${monthName} historical seasonality`);
    text('stateSeasonalityAsOf', evaluated ? `${evaluated} indices evaluated` : 'Stored index history');
    text('stateSeasonalityAverage', safeNumber(nifty50.average_return_pct) === null ? 'Unavailable' : signedPercent(nifty50.average_return_pct));
    text('stateSeasonalityAverageMeta', safeNumber(nifty50.average_return_pct) === null ? 'Nifty 50 month history is unavailable.' : `${Number(nifty50.count || 0)} completed ${monthName} observations · Nifty 50`);
    text('stateSeasonalityLeader', leader.instrument || 'Unavailable');
    text('stateSeasonalityLeaderMeta', safeNumber(leader.average_return_pct) === null ? 'Comparable index history is unavailable.' : `${signedPercent(leader.average_return_pct)} average · ${Number(leader.count || 0)} completed years`);
    text('stateSeasonalityLagger', lagger.instrument || 'Unavailable');
    text('stateSeasonalityLaggerMeta', safeNumber(lagger.average_return_pct) === null ? 'Comparable index history is unavailable.' : `${signedPercent(lagger.average_return_pct)} average · ${Number(lagger.count || 0)} completed years`);
    text('stateSeasonalityNote', current.return_definition || 'Historical calendar-month averages are descriptive and exclude the current incomplete month.');
  }

  function renderMarketState(payload) {
    const story = payload.market_story || {};
    const regime = story.regime || {};
    const confidence = story.confidence || {};
    const breadth = story.market_context?.breadth || {};
    const analogue = story.historical_analogue || {};
    const signals = Array.isArray(story.signals) ? story.signals : [];
    const score = safeNumber(regime.score);
    const asOf = story.as_of_date || payload.as_of_date;

    text('stateAsOf', displayDate(asOf));
    text('stateRegime', regime.label || 'Unclassified');
    text('stateRegimeScore', score === null ? 'Score —' : `Score ${score > 0 ? '+' : ''}${score.toFixed(1)}`);
    text('stateAbove50', safeNumber(breadth.above_50dma_pct) === null ? '—' : `${Number(breadth.above_50dma_pct).toFixed(1)}%`);
    text('stateAbove50Count', `${Number(breadth.evaluated_50dma || 0)} of ${Number(breadth.official_count || 0)} current NIFTY 500 stocks evaluated`);
    text('stateAbove200', safeNumber(breadth.above_200dma_pct) === null ? '—' : `${Number(breadth.above_200dma_pct).toFixed(1)}%`);
    text('stateAbove200Count', `${Number(breadth.evaluated_200dma || 0)} of ${Number(breadth.official_count || 0)} current NIFTY 500 stocks evaluated`);
    text('stateTape', safeNumber(breadth.positive_20d_pct) === null ? '—' : `${Number(breadth.positive_20d_pct).toFixed(1)}%`);
    text('stateTapeCount', `${Number(breadth.positive_20d || 0)} of ${Number(breadth.evaluated_20d || 0)} stocks positive`);
    const volatility = signals.find((signal) => signal.key === 'volatility');
    text('stateVolatility', volatility?.value || '—');
    const stateEvidence = applyEvidenceDisclosure('stateCoverage', confidence);
    text('stateCoverageMeta', stateEvidence.missingText);
    text('stateHeadline', story.headline || 'Current market state unavailable');
    text('stateNarrative', story.narrative || 'No connected market-state narrative was returned by the local evidence engine.');
    text('stateConfidence', `${confidence.label || 'Limited'} · ${stateEvidence.count} · ${stateEvidence.missingText}`);
    $('stateConfidence').title = `${stateEvidence.availableText}. ${stateEvidence.missingText}.`;
    $('stateConfidence').className = `state-pill ${String(regime.tone || '').includes('risk') ? 'risk' : 'ready'}`;

    $('stateEvidenceGrid').innerHTML = signals.length ? signals.map((signal) => `<article class="evidence-cluster ${escapeHtml(signal.state || '')}">
      <span>${escapeHtml(signal.label || signal.key || 'Evidence')}</span>
      <strong>${escapeHtml(signal.value || '—')}</strong>
      <p>${escapeHtml(signal.detail || '')}</p>
    </article>`).join('') : '<div class="empty-state">Validated evidence clusters are unavailable.</div>';

    text('stateAnalogueTitle', `${analogue.label || regime.label || 'Comparable'} regime outcomes`);
    text('stateAnalogueSample', `${Number(analogue.sessions_observed || 0)} observed sessions`);
    const horizons = Array.isArray(analogue.horizons) ? analogue.horizons : [];
    $('stateAnalogueGrid').innerHTML = horizons.length ? horizons.map((row) => `<article>
      <span>NEXT ${Number(row.sessions || 0)} SESSIONS</span>
      <strong>${signedPercent(row.mean_return_pct)}</strong>
      <dl><div><dt>Median</dt><dd>${signedPercent(row.median_return_pct)}</dd></div><div><dt>Positive</dt><dd>${Number(row.positive_rate_pct || 0).toFixed(1)}%</dd></div><div><dt>Worst</dt><dd>${signedPercent(row.worst_return_pct)}</dd></div><div><dt>Mean drawdown</dt><dd>${signedPercent(row.mean_max_drawdown_pct)}</dd></div></dl>
    </article>`).join('') : '<div class="empty-state">Historical analogue evidence is unavailable.</div>';
    text('stateAnalogueMethod', analogue.method || 'Trailing-only regime classifications; descriptive, not a forecast.');

    const fno = payload.fno || {};
    renderCrossAssetContext(payload);

    const auditRows = [
      ['Regime trend', regime.label || 'Unavailable', score === null ? 'No validated score' : `Validated score ${score.toFixed(1)}`, asOf],
      ['Breadth / strength', `${Number(breadth.evaluated_50dma || 0)} 50DMA / ${Number(breadth.evaluated_200dma || 0)} 200DMA`, `${Number(breadth.official_count || 0)} current members · ${safeNumber(breadth.coverage_pct) === null ? '—' : `${Number(breadth.coverage_pct).toFixed(1)}%`} 200DMA coverage`, story.market_context?.as_of_date],
      ['Volatility', volatility?.value || 'Unavailable', volatility?.detail || 'No observation', asOf],
      ['Futures positioning', String(fno.status || 'Unavailable').replaceAll('_', ' '), `${Number(fno.coverage?.latest || 0)}/${Number(fno.coverage?.expected || 0)} latest coverage`, fno.as_of_date],
      ['Historical analogue', `${Number(analogue.sessions_observed || 0)} sessions`, horizons.length ? `${horizons.length} forward windows` : 'No forward windows', asOf],
      ['Evidence clusters', stateEvidence.count, `${stateEvidence.availableText}; ${stateEvidence.missingText}`, asOf],
      ['Decision boundary', 'Descriptive evidence only', 'No forecasts, orders or automatic trade instructions', asOf],
    ];
    $('stateAuditTable').innerHTML = '<div class="state-audit-row header"><span>Evidence</span><span>Status</span><span>Coverage / limitation</span><span>As of</span></div>' + auditRows.map((row) => `<div class="state-audit-row"><strong>${escapeHtml(row[0])}</strong><span>${escapeHtml(row[1])}</span><span>${escapeHtml(row[2])}</span><time>${escapeHtml(displayDate(row[3]))}</time></div>`).join('');
  }

  function renderFnoTable() {
    const filter = $('fnoStateFilter')?.value || 'all';
    const rows = fnoRows.filter((row) => filter === 'all' || row.state === filter);
    const direction = fnoSort.direction === 'asc' ? 1 : -1;
    rows.sort((a, b) => {
      const left = a[fnoSort.key];
      const right = b[fnoSort.key];
      if (typeof left === 'number' && typeof right === 'number') return direction * (left - right);
      return direction * String(left ?? '').localeCompare(String(right ?? ''));
    });
    document.querySelectorAll('[data-fno-sort]').forEach((button) => {
      const active = button.dataset.fnoSort === fnoSort.key;
      button.parentElement.setAttribute('aria-sort', active ? (fnoSort.direction === 'asc' ? 'ascending' : 'descending') : 'none');
      button.querySelector('span').textContent = active ? (fnoSort.direction === 'asc' ? '▲' : '▼') : '';
    });
    $('fnoTableBody').innerHTML = rows.length ? rows.map((row) => `<tr>
      <td><strong>${escapeHtml(row.underlying || '—')}</strong><small>${escapeHtml(row.tradingsymbol || '')}</small></td>
      <td><span class="fno-state ${escapeHtml(String(row.state || '').replaceAll('_', '-'))}">${escapeHtml(fnoStateLabels[row.state] || row.state || '—')}</span></td>
      <td class="numeric ${Number(row.price_change_pct) >= 0 ? 'positive-text' : 'negative-text'}">${signedPercent(row.price_change_pct)}</td>
      <td class="numeric ${Number(row.oi_change_pct) >= 0 ? 'positive-text' : 'negative-text'}">${signedPercent(row.oi_change_pct)}</td>
      <td class="numeric">${Number(row.volume || 0).toLocaleString('en-IN')}</td>
      <td class="numeric">${Number(row.open_interest || 0).toLocaleString('en-IN')}</td>
      <td class="numeric">${escapeHtml(displayDate(row.expiry))}</td>
    </tr>`).join('') : '<tr><td colspan="7">No observations match this classification.</td></tr>';
  }

  function renderFnoPage(domestic) {
    const futures = domestic?.futures_oi || {};
    const counts = futures.state_counts || {};
    const expected = Number(futures.expected_universe || 0);
    const latest = Number(futures.latest_coverage || 0);
    const comparable = Number(futures.comparable_count || 0);
    fnoRows = Array.isArray(futures.states) ? [...futures.states] : [];
    text('fnoPageAsOf', displayDate(futures.as_of_date || domestic?.as_of_date));
    text('fnoPageCoverage', `${latest} / ${expected || '—'}`);
    text('fnoPageCoveragePct', `${safeNumber(futures.latest_coverage_pct) === null ? '—' : `${Number(futures.latest_coverage_pct).toFixed(1)}%`} of universe`);
    text('fnoPageComparable', comparable.toLocaleString('en-IN'));
    text('fnoPageHistory', `${fnoHistorySessions} / 252`);
    text('fnoPageHistoryGate', fnoHistorySessions >= 252 ? 'History gate passed' : '252 sessions required');
    text('fnoPageRollovers', Number(futures.rollover_baseline_count || 0).toLocaleString('en-IN'));
    text('fnoPageClassStatus', String(futures.classification_status || 'unavailable').replaceAll('_', ' '));
    text('fnoStaleGaps', Number(futures.stale_gap_count || 0).toLocaleString('en-IN'));
    text('fnoLiquidityExcluded', Number(futures.liquidity_excluded_count || 0).toLocaleString('en-IN'));
    text('fnoLatestMissing', Number(futures.latest_missing_count || 0).toLocaleString('en-IN'));

    const quadrantDefs = [
      ['long_build_up', 'PRICE ↑ · OI ↑', 'Constructive positioning', 'positive'],
      ['short_build_up', 'PRICE ↓ · OI ↑', 'Bearish positioning', 'negative'],
      ['long_unwinding', 'PRICE ↓ · OI ↓', 'Position reduction', 'caution'],
      ['short_covering', 'PRICE ↑ · OI ↓', 'Bearish positions closing', 'recovery'],
    ];
    const classifiedTotal = quadrantDefs.reduce((total, row) => total + Number(counts[row[0]] || 0), 0);
    const neutralTotal = Number(counts.no_clear_signal || 0);
    const leader = quadrantDefs.map((row) => [row[0], Number(counts[row[0]] || 0)]).sort((a, b) => b[1] - a[1])[0];
    text('fnoPageRead', leader ? `${fnoStateLabels[leader[0]]} leads` : 'Awaiting comparison');
    $('fnoPageRead').className = `state-pill ${leader?.[0] === 'short_build_up' || leader?.[0] === 'long_unwinding' ? 'risk' : 'ready'}`;
    text('fnoPageContext', comparable
      ? `${classifiedTotal} directional and ${neutralTotal} neutral contracts account for ${comparable} current observations. ${leader ? fnoStateLabels[leader[0]] : 'No quadrant'} is the largest directional state.`
      : 'Two completed observations of the same contract are required before positioning can be classified.');
    $('fnoQuadrants').innerHTML = quadrantDefs.map(([key, axes, detail, tone]) => {
      const count = Number(counts[key] || 0);
      const width = comparable ? Math.max(3, 100 * count / comparable) : 0;
      return `<article class="fno-quadrant ${tone}"><span>${axes}</span><strong>${count.toLocaleString('en-IN')}</strong><p>${detail}</p><div><i style="width:${width}%"></i></div></article>`;
    }).join('');

    const attribution = futures.movement_attribution || {};
    const rising = attribution.rising || {};
    const falling = attribution.falling || {};
    const movementRows = [
      ['long_build_up', 'Long build-up', 'Price up · OI up', 'New long positioning', 'positive'],
      ['short_covering', 'Short covering', 'Price up · OI down', 'Short positions closing', 'recovery'],
      ['short_build_up', 'Short build-up', 'Price down · OI up', 'New short positioning', 'negative'],
      ['long_unwinding', 'Long unwinding', 'Price down · OI down', 'Long positions closing', 'caution'],
    ];
    const directionalTotal = Number(attribution.directional_total ?? classifiedTotal ?? 0);
    const addition = Number(attribution.position_addition || 0);
    const reduction = Number(attribution.position_reduction || 0);
    const additionShare = directionalTotal ? 100 * addition / directionalTotal : null;
    const dominantMechanism = addition > reduction ? 'Position build-up leads' : (reduction > addition ? 'Position closure leads' : 'Build-up and closure are balanced');
    text('fnoMovementRead', directionalTotal ? dominantMechanism : 'Awaiting comparison');
    $('fnoMovementRead').className = `state-pill ${directionalTotal ? 'ready' : 'neutral'}`;
    text('fnoMovementContext', directionalTotal
      ? `${addition} of ${directionalTotal} directional moves reflect new position build-up; ${reduction} reflect short covering or long unwinding.`
      : 'No directional price and open-interest comparisons passed the current safeguards.');
    $('fnoMovementAttribution').innerHTML = `<section class="fno-movement-comparison" aria-label="Movement authenticity summary">
      <article class="rise"><span>RISING STOCKS</span><strong>${Number(rising.total || 0).toLocaleString('en-IN')}</strong><p><b>${Number(rising.build_up || 0)}</b> long build-up · <b>${Number(rising.position_closure || 0)}</b> short covering</p><div><i style="width:${safeNumber(rising.build_up_share_pct) === null ? 0 : Math.max(2, Number(rising.build_up_share_pct))}%"></i></div><small>${safeNumber(rising.build_up_share_pct) === null ? 'No classified rising moves' : `${Number(rising.build_up_share_pct).toFixed(1)}% backed by added long OI`}</small></article>
      <article class="fall"><span>FALLING STOCKS</span><strong>${Number(falling.total || 0).toLocaleString('en-IN')}</strong><p><b>${Number(falling.build_up || 0)}</b> short build-up · <b>${Number(falling.position_closure || 0)}</b> long unwinding</p><div><i style="width:${safeNumber(falling.build_up_share_pct) === null ? 0 : Math.max(2, Number(falling.build_up_share_pct))}%"></i></div><small>${safeNumber(falling.build_up_share_pct) === null ? 'No classified falling moves' : `${Number(falling.build_up_share_pct).toFixed(1)}% backed by added short OI`}</small></article>
    </section><section class="fno-movement-counts" aria-label="Four movement classifications">${movementRows.map(([key, label, axes, detail, tone]) => `<article class="${tone}"><span>${escapeHtml(label)}</span><strong>${Number(attribution[key] ?? counts[key] ?? 0).toLocaleString('en-IN')}</strong><small>${escapeHtml(axes)} · ${escapeHtml(detail)}</small></article>`).join('')}</section>`;
    text('fnoMovementNote', `${directionalTotal} directional · ${Number(attribution.neutral ?? neutralTotal)} neutral · ${Number(attribution.unavailable ?? futures.latest_missing_count ?? 0)} unavailable. ${attribution.method || 'Same-contract completed-session comparison after noise safeguards.'}`);
    const safeguards = futures.safeguards || {};
    text('fnoNoiseNote', `Noise floor: |price| ≥ ${Number(safeguards.minimum_absolute_price_change_pct || 0).toFixed(2)}%, |OI| ≥ ${Number(safeguards.minimum_absolute_oi_change_pct || 0).toFixed(2)}% · maximum gap ${Number(safeguards.maximum_calendar_gap_days || 0)} days · regime scoring off.`);

    const progress = Math.min(100, 100 * fnoHistorySessions / 252);
    const archive = domestic?.fno_archive_backfill || {};
    const archiveRange = archive.first_imported && archive.last_imported
      ? ` Official NSE archive coverage: ${displayDate(archive.first_imported)} to ${displayDate(archive.last_imported)}.`
      : '';
    text('fnoHistoryProgress', `${fnoHistorySessions} / 252`);
    $('fnoHistoryBar').style.width = `${progress}%`;
    text('fnoHistoryReason', fnoHistorySessions >= 252
      ? `History gate passed with rollover-safe same-contract comparisons.${archiveRange}`
      : `Current quadrants are visible, but ${252 - fnoHistorySessions} more completed sessions are required before the history gate can pass.`);

    const renderLeaders = (id, key) => {
      const ranked = [...fnoRows].sort((a, b) => Math.abs(Number(b[key] || 0)) - Math.abs(Number(a[key] || 0))).slice(0, 6);
      const maximum = Math.max(1, ...ranked.map((row) => Math.abs(Number(row[key] || 0))));
      $(id).innerHTML = ranked.length ? ranked.map((row, index) => {
        const value = Number(row[key] || 0);
        return `<article><span class="rank">${index + 1}</span><div class="name"><strong>${escapeHtml(row.underlying)}</strong><small>${escapeHtml(fnoStateLabels[row.state] || row.state)}</small></div><div class="track"><i style="width:${100 * Math.abs(value) / maximum}%"></i></div><b class="${value >= 0 ? 'positive-text' : 'negative-text'}">${signedPercent(value)}</b></article>`;
      }).join('') : '<div class="empty-state">Comparable contracts are unavailable.</div>';
    };
    renderLeaders('fnoPriceLeaders', 'price_change_pct');
    renderLeaders('fnoOiLeaders', 'oi_change_pct');
    text('fnoTableNote', `${latest}/${expected || '—'} contracts · ${neutralTotal} neutral · ${Number(futures.latest_missing_count || 0)} unavailable · ${Number(futures.stale_gap_count || 0)} stale gaps. Descriptive EOD evidence only.`);
    renderFnoTable();
  }

  function renderOverview(payload) {
    latestOverviewPayload = payload;
    const story = payload.market_story || {};
    const regime = story.regime || {};
    const confidence = story.confidence || {};
    const sentimentEvidence = payload.sentiment?.evidence || {};
    // The event calendar carries today's date, while the market story carries
    // the latest completed trading session.  The EOD workspace must display
    // the latter so a 3 November calendar does not mislabel 2 November data.
    const asOf = story.as_of_date || payload.as_of_date;
    const score = safeNumber(regime.score);
    fnoHistorySessions = Number(payload.fno?.stored_history_sessions || 0);

    text('dashboardAsOf', displayDate(asOf));
    text('sessionAsOf', `Completed through ${displayDate(asOf)}`);
    text('tickerRegime', regime.label || 'Unclassified');
    text('tickerRegimeScore', score === null ? 'Score —' : `Score ${score > 0 ? '+' : ''}${score.toFixed(1)}`);
    text('tickerClose', safeNumber(regime.close) === null ? '—' : Number(regime.close).toLocaleString('en-IN', { maximumFractionDigits: 2 }));
    text('tickerCloseAsOf', latestNiftyYtdPayload?.ok === true
      ? `YTD ${signedPercent(latestNiftyYtdPayload.ytd_return_pct)} · ${displayDate(latestNiftyYtdPayload.as_of_date)}`
      : `YTD — · ${displayDate(asOf)}`);
    const breadth = story.market_context?.breadth || {};
    const breadthUniverse = Number(breadth.official_count || breadth.universe_count || 500);
    const breadth50Evaluated = Number(breadth.evaluated_50dma || 0);
    const breadth200Evaluated = Number(breadth.evaluated_200dma || 0);
    const above50 = safeNumber(breadth.above_50dma_pct);
    const above200 = safeNumber(breadth.above_200dma_pct);
    text('tickerBreadth', `${above50 === null ? '—' : `${above50.toFixed(1)}%`} / ${above200 === null ? '—' : `${above200.toFixed(1)}%`}`);
    text('tickerBreadthMeta', `Above 50DMA / 200DMA · ${breadth50Evaluated || '—'} / ${breadth200Evaluated || '—'} of ${breadthUniverse} current constituents`);
    const fnoCounts = payload.fno?.positioning || payload.fno?.counts || payload.fno?.positioning_counts || {};
    text('tickerFno', `${Number(fnoCounts.bullish || fnoCounts.long_buildup || 0)} / ${Number(fnoCounts.bearish || fnoCounts.short_buildup || 0)} / ${Number(fnoCounts.neutral || fnoCounts.no_clear_signal || 0)}`);
    const fnoCoverage = payload.fno?.coverage || {};
    const fnoAvailable = Number(fnoCoverage.latest || 0);
    const fnoExpected = Number(fnoCoverage.expected || 0);
    text('tickerFnoMeta', `Bullish / bearish / neutral · ${Number(fnoCounts.unavailable || Math.max(0, fnoExpected - fnoAvailable))} unavailable · ${fnoExpected || '—'} total F&O stocks`);
    const dashboardEvidence = Object.keys(confidence).length ? confidence : sentimentEvidence;
    const dashboardManifest = applyEvidenceDisclosure('tickerEvidence', dashboardEvidence);
    text('tickerEvidenceMeta', dashboardManifest.missingText);
    $('tickerEvidenceMeta').title = `${dashboardManifest.availableText}. ${dashboardManifest.missingText}.`;

    text('storyHeadline', story.headline || 'Current market story unavailable');
    text('storyNarrative', story.narrative || 'The local data engine did not return a connected market narrative.');
    text('storyConfidence', `${confidence.label || 'Limited'} · ${dashboardManifest.count} · ${dashboardManifest.missingText}`);
    $('storyConfidence').title = `${dashboardManifest.availableText}. ${dashboardManifest.missingText}.`;
    $('storyConfidence').className = `state-pill ${String(regime.tone || '').includes('risk') ? 'risk' : 'ready'}`;
    renderSignals(story);

    text('regimeScore', score === null ? '—' : `${score > 0 ? '+' : ''}${score.toFixed(1)}`);
    text('regimeLabel', regime.label || 'Unclassified');
    text('regimeAsOf', displayDate(asOf));
    text('regimeEvidence', `${dashboardManifest.count} clusters · ${dashboardManifest.missingText}`);
    $('regimeEvidence').title = `${dashboardManifest.availableText}. ${dashboardManifest.missingText}.`;
    $('riskMarker').style.left = `${score === null ? 50 : Math.max(0, Math.min(100, (score + 100) / 2))}%`;

    renderLeadership(story);
    renderEvents(story, payload.events);
    renderReadiness(payload);
    text('contractVersion', payload.contract_version || 'dashboard-overview-v1');
    text('latestSession', displayDate(asOf));
    renderMarketState(payload);
    if ($('fnoPageHistory')) {
      text('fnoPageHistory', `${fnoHistorySessions} / 252`);
      text('fnoHistoryProgress', `${fnoHistorySessions} / 252`);
      $('fnoHistoryBar').style.width = `${Math.min(100, 100 * fnoHistorySessions / 252)}%`;
      text('fnoHistoryReason', fnoHistorySessions >= 252
        ? 'The required history is stored; directional use still needs a separately validated model.'
        : `Current quadrants are visible, but ${Math.max(0, 252 - fnoHistorySessions)} more completed sessions are required before the history gate can pass.`);
    }
  }

  function renderSupplement(domestic, factorResearch) {
    const macro = domestic?.macro_context || {};
    const indiaYield = macro?.metrics?.india_10y_gsec_yield;
    if (macro.available === true && safeNumber(indiaYield?.level) !== null) {
      text('indiaYield10y', `${Number(indiaYield.level).toFixed(3)}%`);
      text('yieldPanelAsOf', displayDate(indiaYield.as_of_date || macro.as_of_date));
      const changes = [];
      if (safeNumber(indiaYield.five_session_change) !== null) changes.push(`5-session ${Number(indiaYield.five_session_change) > 0 ? '+' : ''}${Number(indiaYield.five_session_change).toFixed(1)} bp`);
      if (safeNumber(indiaYield.twenty_session_change) !== null) changes.push(`20-session ${Number(indiaYield.twenty_session_change) > 0 ? '+' : ''}${Number(indiaYield.twenty_session_change).toFixed(1)} bp`);
      text('yieldPanelNote', `Official India 10-year G-Sec${changes.length ? ` · ${changes.join(' · ')}` : ''}. Other sovereign curves await approved EOD inputs.`);
    } else {
      text('indiaYield10y', '—');
      text('yieldPanelAsOf', 'Data pending');
      text('yieldPanelNote', macro.reason || 'Official India and global sovereign-curve observations are not available.');
    }

    const factorAsOf = factorResearch?.as_of_date;
    const momentum = factorResearch?.sector_neutral_momentum || {};
    const coverage = momentum.coverage || {};
    text('factorPanelAsOf', factorAsOf ? `Evidence ${displayDate(factorAsOf)}` : 'Contract pending');
    text('factorPanelNote', factorResearch?.ok === true
      ? `${Number(coverage.factor_input_admitted || 0)} locally admitted equities have momentum inputs. Calculations are restricted to dated NIFTY 500 membership; point-in-time size and value/core/growth buckets are not yet approved, so cells remain blank.`
      : 'Waiting for dated NIFTY 500 membership plus point-in-time market-cap and value/core/growth classification evidence.');
  }

  function renderStoredSnapshots(payload) {
    const membership = payload?.membership;
    const factors = Array.isArray(payload?.factor_returns) ? payload.factor_returns : [];
    const sizeBuckets = Array.isArray(payload?.size_classification?.buckets) ? payload.size_classification.buckets : [];
    const sizeCoverage = sizeBuckets.reduce((total, row) => total + Number(row.constituents || 0), 0);
    const filingSymbols = Number(payload?.filing_intake?.symbols || 0);
    const filingCount = Number(payload?.filing_intake?.filings || 0);
    const factSymbols = Number(payload?.fundamental_facts?.symbols || 0);
    const factCount = Number(payload?.fundamental_facts?.facts || 0);
    if (membership) {
      text('factorPanelAsOf', factors.length
        ? `Evidence ${displayDate(payload?.factor_as_of)} · ${payload?.factor_methodology || 'validated methodology'}`
        : `Membership stored ${displayDate(membership.snapshot_date)}`);
      text('factorPanelNote', factors.length
        ? `${membership.constituents} dated NIFTY 500 constituents are stored locally. Cells show equal-weight completed-session returns; hover a cell for admitted constituent count.`
        : `${membership.constituents} NIFTY 500 constituents stored · ${sizeCoverage || 0} officially size-classified · ${filingCount} NSE filings across ${filingSymbols} constituents · ${factCount} XBRL facts normalized across ${factSymbols} constituents. Cells remain blank until comparable prior-period facts and completed-session prices pass the style coverage gate.`);
    }
    const factorIds = {
      'Large|Value': 'factorLargeValue', 'Large|Core': 'factorLargeCore', 'Large|Growth': 'factorLargeGrowth',
      'Mid|Value': 'factorMidValue', 'Mid|Core': 'factorMidCore', 'Mid|Growth': 'factorMidGrowth',
      'Small|Value': 'factorSmallValue', 'Small|Core': 'factorSmallCore', 'Small|Growth': 'factorSmallGrowth',
    };
    document.querySelectorAll('[data-factor-size][data-factor-style]').forEach((element) => {
      element.disabled = true;
      element.classList.remove('positive', 'negative');
      element.textContent = '—';
      element.removeAttribute('title');
    });
    factors.forEach((row) => {
      const element = $(factorIds[`${row.size_bucket}|${row.style_bucket}`]);
      if (!element) return;
      const value = safeNumber(row.return_pct);
      element.textContent = signedPercent(value);
      element.classList.toggle('positive', value !== null && value >= 0);
      element.classList.toggle('negative', value !== null && value < 0);
      element.disabled = false;
      element.title = `Open ${Number(row.constituents_count || 0)} admitted ${row.size_bucket} cap ${row.style_bucket.toLowerCase()} stocks`;
      element.setAttribute('aria-label', `${row.size_bucket} cap ${row.style_bucket}; return ${signedPercent(value)}; open ${Number(row.constituents_count || 0)} stocks`);
    });

    const curves = Array.isArray(payload?.yields) ? payload.yields : [];
    let stored = 0;
    const dates = [];
    curves.forEach((curve) => {
      const row = document.querySelector(`.yield-row[data-market="${curve.market}"]`);
      if (!row || !curve.observation_date) return;
      dates.push(curve.observation_date);
      const observationDate = row.querySelector('[data-observation-date]');
      if (observationDate) observationDate.textContent = `As of ${displayDate(curve.observation_date)}`;
      [2, 5, 10, 30].forEach((tenor) => {
        const value = safeNumber(curve.tenors?.[String(tenor)]);
        const cell = row.querySelector(`[data-tenor="${tenor}"]`);
        if (cell && value !== null) {
          cell.textContent = `${value.toFixed(3)}%`;
          const context = curve.tenor_context?.[String(tenor)] || {};
          const state = ['normal', 'elevated', 'high'].includes(context.state) ? context.state : 'limited';
          cell.classList.remove('yield-normal', 'yield-elevated', 'yield-high', 'yield-limited');
          cell.classList.add(`yield-${state}`);
          const percentile = context.percentile === null || context.percentile === undefined ? null : safeNumber(context.percentile);
          const level = state === 'high' ? 'High' : (state === 'elevated' ? 'Elevated' : (state === 'normal' ? 'Normal' : 'Limited history'));
          cell.title = percentile === null
            ? `${curve.market} ${tenor}Y · ${level} · ${Number(context.observations || 0)} stored observations`
            : `${curve.market} ${tenor}Y · ${level} · percentile ${percentile.toFixed(1)} from ${Number(context.observations || 0)} observations over five years`;
          stored += 1;
        }
      });
      row.title = `${curve.curve_type || 'Official curve'} · ${displayDate(curve.observation_date)}`;
    });
    if (dates.length) {
      const latest = dates.sort().at(-1);
      text('yieldPanelAsOf', `Latest available source · ${displayDate(latest)}`);
      text('yieldPanelNote', `${stored}/20 latest available official observations · Each market shows its own source date. Historical range: green normal (<65th percentile), amber elevated (65th–85th), red high (≥85th).`);
    }
  }

  function setRefreshProgress(percent) {
    const button = $('refreshButton');
    const bounded = Math.max(0, Math.min(100, Math.round(percent)));
    button.classList.add('refreshing');
    button.style.setProperty('--progress', `${bounded}%`);
    button.textContent = `Refreshing ${bounded}%`;
    button.setAttribute('aria-busy', 'true');
    button.setAttribute('aria-valuemin', '0');
    button.setAttribute('aria-valuemax', '100');
    button.setAttribute('aria-valuenow', String(bounded));
    button.setAttribute('aria-label', `Refreshing workspace data, ${bounded}% complete`);
  }

  function resetRefreshButton() {
    const button = $('refreshButton');
    button.classList.remove('refreshing');
    button.style.removeProperty('--progress');
    button.textContent = 'Refresh data';
    button.removeAttribute('aria-busy');
    button.removeAttribute('aria-valuemin');
    button.removeAttribute('aria-valuemax');
    button.removeAttribute('aria-valuenow');
    button.setAttribute('aria-label', 'Refresh workspace data');
  }

  async function loadNiftyYtd(onComplete = () => {}) {
    try {
      const response = await fetchWithDeadline('/api/workspace/nifty50-ytd', { cache: 'no-store' });
      const payload = await response.json();
      if (!response.ok || payload.ok !== true) throw new Error(payload.reason || `HTTP ${response.status}`);
      latestNiftyYtdPayload = payload;
      text('tickerCloseAsOf', `YTD ${signedPercent(payload.ytd_return_pct)} · ${displayDate(payload.as_of_date)}`);
    } catch (_error) {
      const current = $('tickerCloseAsOf')?.textContent?.split(' · ').pop() || 'Local history';
      text('tickerCloseAsOf', `YTD unavailable · ${current}`);
    } finally {
      onComplete();
    }
  }

  async function loadSupplement(onComplete = () => {}) {
    const fetchOptional = async (url) => {
      try {
        const response = await fetchWithDeadline(url, { cache: 'no-store' });
        return response.ok ? await response.json() : null;
      } finally {
        onComplete();
      }
    };
    try {
      // These contracts are CPU-heavy Python/SQLite reads. Running all eight
      // simultaneously makes them contend for the interpreter and turns a
      // roughly 14-second sequential load into a minute-long apparent hang.
      const supplementUrls = [
        '/api/market-sentiment/domestic-core',
        '/api/factor-research',
        '/api/workspace/snapshots',
        '/api/dashboard/seasonality-summary',
        '/api/screener/local',
        '/api/stocks/ytd',
        '/api/backtests/moving-average-crossover',
        '/api/research/opportunity-board-release',
      ];
      const supplementValues = [];
      for (const url of supplementUrls) supplementValues.push(await fetchOptional(url));
      const [domestic, factorResearch, snapshots, seasonality, screener, stockYtd, backtests, opportunityRelease] = supplementValues;
      latestDomesticPayload = domestic;
      latestSnapshotPayload = snapshots;
      latestSeasonalityPayload = seasonality;
      latestFactorPayload = factorResearch;
      latestScreenerPayload = screener;
      latestStockYtdPayload = stockYtd;
      latestBacktestPayload = backtests;
      renderOpportunityRelease(opportunityRelease);
      if (backtests?.ok === true) {
        researchRankingPayloads.set('sma', backtests);
        const ready = Number(backtests.coverage?.ready || 0);
        const gated = Number(backtests.coverage?.data_quality_gated || 0);
        const commonDate = backtests.data_quality_contract?.common_as_of_date;
        const tested = Number(backtests.multiple_testing?.tested_hypotheses || 0);
        const discoveries = Number(backtests.multiple_testing?.fdr_q_below_alpha || 0);
        const oosRanked = Number(backtests.evidence_ranking?.ranking_admitted || 0);
        text('researchRankStatus', `${researchBacktestModels.sma.label} · ${oosRanked} pass full-history OOS gates · ${ready.toLocaleString('en-IN')} comparable · ${gated.toLocaleString('en-IN')} quality-gated · ${discoveries} of ${tested} pass comparison-window FDR${commonDate ? ` through ${displayDate(commonDate)}` : ''}`);
        void loadCrossStrategyFdr();
      }
      if (!stockYtd?.rows?.length && factorResearch?.rows?.length && $('researchUniverse').value === 'nifty500') {
        $('researchUniverse').value = 'fno';
        showToast('NIFTY 500 refresh is still running; showing the completed F&O research universe.');
      }
      renderSupplement(domestic, factorResearch);
      renderStoredSnapshots(snapshots);
      renderFnoPage(domestic);
      renderCrossAssetContext(latestOverviewPayload, domestic, snapshots);
      renderMarketSeasonality(seasonality);
      buildResearchRows();
    } catch (_error) {
      renderSupplement(null, null);
      renderOpportunityRelease(null);
    }
  }

  async function loadDashboard(showMessage = false, sourceEod = false) {
    const button = $('refreshButton');
    const kiteStages = sourceEod && kiteConnected ? 4 : 0;
    const corporateStages = showMessage ? 1 : 0;
    const snapshotStages = showMessage ? 2 : 0;
    const totalStages = 9 + kiteStages + corporateStages + snapshotStages;
    let completedStages = 0;
    const markComplete = () => {
      completedStages += 1;
      setRefreshProgress((completedStages / totalStages) * 100);
    };
    button.disabled = true;
    setRefreshProgress(0);
    setSourceState('', kiteStages ? 'Sourcing Kite EOD' : 'Reading Workspace data');
    if (kiteStages) {
      try {
        await sourceKiteEod(markComplete);
      } catch (error) {
        showToast(`Kite EOD refresh was incomplete: ${error.message}`);
      }
      setSourceState('', 'Reading refreshed EOD');
    }
    if (showMessage) {
      setSourceState('', 'Refreshing official EOD snapshots');
      try {
        await sourceOfficialSnapshots(markComplete);
      } catch (error) {
        showToast(`Official-source refresh was incomplete: ${error.message}`);
      }
      setSourceState('', 'Refreshing global-risk EOD observations');
      try {
        await sourceGlobalRisk(markComplete);
      } catch (error) {
        showToast(`Global-risk refresh was incomplete: ${error.message}`);
      }
      setSourceState('', 'Reading refreshed EOD');
      try {
        await sourceCorporateActions(markComplete);
      } catch (error) {
        showToast(`NSE corporate-action refresh was incomplete: ${error.message}`);
      }
    }
    try {
      let response;
      let payload;
      try {
        response = await fetchWithDeadline('/api/dashboard/overview', { cache: 'no-store' });
        payload = await response.json();
      } finally {
        markComplete();
      }
      if (!response.ok || payload.ok !== true) throw new Error(payload.reason || `HTTP ${response.status}`);
      renderOverview(payload);
      setSourceState('ready', 'Workspace data ready');
      if (showMessage) {
        const label = !$('fnoPositioningView').hidden ? 'F&O positioning' : (!$('marketStateView').hidden ? 'Market state' : 'Dashboard');
        showToast(`${label} refreshed through ${displayDate(payload.market_story?.as_of_date || payload.as_of_date)}.`);
      }
    } catch (error) {
      setSourceState('error', 'Workspace data unavailable');
      text('storyHeadline', 'Workspace market data is unavailable');
      text('storyNarrative', 'The independent Workspace market store could not be read. Check the local server, then refresh this dashboard.');
      text('storyConfidence', 'No data');
      $('storyConfidence').className = 'state-pill risk';
      if (showMessage) showToast(`Refresh failed: ${error.message}`);
    } finally {
      // Keep the server-facing work sequential. The local engine is fast per
      // contract but concurrent full-universe calculations compete for the GIL.
      await loadNiftyYtd(markComplete);
      await loadSupplement(markComplete);
      if (!$('stockResearchView').hidden && (latestStockYtdPayload?.rows?.length || latestFactorPayload?.rows?.length)) {
        setSourceState('ready', 'Workspace data ready');
      }
      setRefreshProgress(100);
      window.setTimeout(() => {
        resetRefreshButton();
        button.disabled = false;
      }, 350);
    }
  }

  function setTheme(theme) {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem('workspace-2-theme', theme);
    $('themeToggle').textContent = theme === 'dark' ? '☼' : '☾';
    $('themeToggle').setAttribute('aria-label', `Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`);
  }

  function portfolioMoney(value) {
    const number = safeNumber(value);
    return number === null ? '₹—' : `₹${number.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  }

  function renderPortfolio(payload) {
    const summary = payload?.summary || {};
    const holdings = Array.isArray(payload?.holdings) ? payload.holdings : [];
    const transactions = Array.isArray(payload?.recent_transactions) ? payload.recent_transactions : [];
    text('portfolioAsOf', displayDate(payload?.holdings?.map((row) => row.price_as_of_date).filter(Boolean).sort().at(-1)));
    text('portfolioMarketValue', portfolioMoney(summary.market_value_inr));
    text('portfolioCostBasis', portfolioMoney(summary.cost_basis_inr));
    text('portfolioUnrealised', portfolioMoney(summary.unrealised_pnl_inr));
    text('portfolioUnrealisedPct', `Return ${signedPercent(summary.unrealised_return_pct)}`);
    text('portfolioRealised', portfolioMoney(summary.realised_pnl_inr));
    text('portfolioPositionCount', holdings.length.toLocaleString('en-IN'));
    text('portfolioPriceCoverage', `${holdings.filter((row) => safeNumber(row.market_price_inr) !== null).length} of ${holdings.length} priced`);
    text('portfolioCashBalance', portfolioMoney(summary.cash_balance_inr));
    text('portfolioCashMeta', `${Number(summary.cash_flow_count || 0).toLocaleString('en-IN')} cash flows · ${Number(summary.transaction_count || 0).toLocaleString('en-IN')} trades`);
    text('portfolioHoldingCount', `${holdings.length.toLocaleString('en-IN')} holding${holdings.length === 1 ? '' : 's'}`);
    const unrealisedNode = $('portfolioUnrealised');
    unrealisedNode.className = researchTone(summary.unrealised_pnl_inr);
    const realisedNode = $('portfolioRealised');
    realisedNode.className = researchTone(summary.realised_pnl_inr);
    $('portfolioHoldingsBody').innerHTML = holdings.length ? holdings.map((row) => `<tr>
      <td><strong>${escapeHtml(row.symbol)}</strong><small>${escapeHtml(row.display_name)}</small></td>
      <td class="numeric">${researchNumber(row.quantity, 4)}</td>
      <td class="numeric">${portfolioMoney(row.average_cost_inr)}</td>
      <td class="numeric">${portfolioMoney(row.market_price_inr)}<small>${escapeHtml(displayDate(row.price_as_of_date))}</small></td>
      <td class="numeric">${portfolioMoney(row.market_value_inr)}</td>
      <td class="numeric ${researchTone(row.unrealised_pnl_inr)}"><strong>${portfolioMoney(row.unrealised_pnl_inr)}</strong><small>${signedPercent(row.unrealised_return_pct)}</small></td>
      <td class="numeric">${safeNumber(row.allocation_pct) === null ? '—' : `${Number(row.allocation_pct).toFixed(2)}%`}</td>
    </tr>`).join('') : '<tr><td colspan="7" class="portfolio-empty">No open holdings. Add a completed transaction to start the local ledger.</td></tr>';
    $('portfolioTransactionsBody').innerHTML = transactions.length ? transactions.map((row) => `<tr>
      <td>${escapeHtml(displayDate(row.trade_date))}</td><td><strong>${escapeHtml(row.symbol)}</strong></td>
      <td><span class="portfolio-side ${String(row.side).toLowerCase()}">${escapeHtml(row.side)}</span></td>
      <td class="numeric">${researchNumber(row.quantity, 4)}</td><td class="numeric">${portfolioMoney(row.price_inr)}</td>
      <td class="numeric">${portfolioMoney(row.fees_inr)}</td><td>${escapeHtml(row.notes || '—')}</td>
    </tr>`).join('') : '<tr><td colspan="7" class="portfolio-empty">No transactions recorded.</td></tr>';
    const cashFlows = Array.isArray(payload?.recent_cash_flows) ? payload.recent_cash_flows : [];
    const imports = Array.isArray(payload?.recent_imports) ? payload.recent_imports : [];
    const reconciliation = payload?.reconciliation || {};
    text('portfolioCashFlowCount', `${cashFlows.length.toLocaleString('en-IN')} record${cashFlows.length === 1 ? '' : 's'}`);
    $('portfolioCashFlowList').innerHTML = cashFlows.length ? cashFlows.slice(0, 4).map((row) => `<div><span>${escapeHtml(row.flow_type.replaceAll('_', ' '))}<small>${escapeHtml(displayDate(row.flow_date))}</small></span><strong>${portfolioMoney(row.amount_inr)}</strong></div>`).join('') : '<p>No cash flows recorded.</p>';
    $('portfolioImportList').innerHTML = imports.length ? imports.slice(0, 4).map((row) => `<div><span>${escapeHtml(row.file_name)}<small>${escapeHtml(displayDate(row.imported_at))}</small></span><strong>${Number(row.records_imported || 0).toLocaleString('en-IN')} rows</strong></div>`).join('') : '<p>No broker files imported.</p>';
    const reconciliationLabels = { empty: 'EMPTY', reconciled: 'RECONCILED', review_required: 'REVIEW' };
    const status = reconciliation.status || 'empty';
    text('portfolioReconcileStatus', reconciliationLabels[status] || status.replaceAll('_', ' ').toUpperCase());
    $('portfolioReconcileStatus').className = `state-pill ${status === 'reconciled' ? 'ready' : status === 'review_required' ? 'risk' : 'neutral'}`;
    text('portfolioReconcileCash', portfolioMoney(reconciliation.cash_balance_inr));
    text('portfolioContributions', portfolioMoney(reconciliation.contributions_inr));
    text('portfolioWithdrawals', portfolioMoney(reconciliation.withdrawals_inr));
    text('portfolioIncome', portfolioMoney(reconciliation.income_inr));
    const issueLabels = {
      cash_flow_history_missing: 'Record contributions and withdrawals to reconcile cash.',
      holding_price_unavailable: 'One or more open holdings lack a stored EOD close.',
      negative_reconciled_cash: 'Recorded cash is negative; review missing contributions or fees.',
    };
    const issues = Array.isArray(reconciliation.issues) ? reconciliation.issues : [];
    text('portfolioReconcileIssues', issues.length ? issues.map((issue) => issueLabels[issue] || issue.replaceAll('_', ' ')).join(' ') : (status === 'reconciled' ? 'Trade, cash-flow and EOD price records reconcile.' : 'Add transactions and cash flows to begin reconciliation.'));
    portfolioLoaded = true;
  }

  function renderPortfolioPerformance(payload) {
    const ready = payload?.status === 'ready';
    const portfolio = payload?.portfolio || {};
    const benchmark = payload?.benchmark || {};
    const comparison = payload?.comparison || {};
    const percent = (value) => safeNumber(value) === null ? '—' : signedPercent(value);
    text('portfolioTwr', percent(portfolio.time_weighted_return_pct));
    text('portfolioAnnualisedTwr', percent(portfolio.annualised_twr_pct));
    text('portfolioMwr', percent(portfolio.money_weighted_return_pct));
    text('portfolioBenchmarkTwr', percent(benchmark.time_weighted_return_pct));
    text('portfolioExcessTwr', percent(comparison.excess_twr_pct));
    text('portfolioPerformanceObservations', ready ? Number(payload.observations || 0).toLocaleString('en-IN') : '—');
    text('portfolioPerformanceDates', ready ? `${displayDate(payload.from_date)} to ${displayDate(payload.through_date)}` : 'No measured period');
    const status = $('portfolioPerformanceStatus');
    status.textContent = ready ? 'READY' : 'GATED';
    status.className = `state-pill ${ready ? 'ready' : 'neutral'}`;
    ['portfolioTwr', 'portfolioAnnualisedTwr', 'portfolioMwr', 'portfolioBenchmarkTwr', 'portfolioExcessTwr'].forEach((id) => {
      const node = $(id);
      node.className = researchTone(safeNumber(node.textContent.replace('%', '')));
    });
    const gateLabels = {
      transaction_history_missing: 'Add completed transactions.',
      external_cash_flow_history_missing: 'Record contributions and withdrawals.',
      cash_flow_history_missing: 'Cash-flow history is required.',
      benchmark_history_missing: 'Stored Nifty 50 history is unavailable.',
      events_after_latest_benchmark_session: 'A ledger event is newer than the latest benchmark session.',
      positive_initial_contribution_missing: 'A positive initial contribution is required.',
      non_positive_prior_valuation: 'The portfolio value crossed zero; review cash-flow timing.',
      negative_terminal_portfolio_value: 'Terminal portfolio value is negative; reconcile the ledger.',
    };
    const gates = Array.isArray(payload?.gates) ? payload.gates : [];
    text('portfolioPerformanceNote', ready
      ? 'TWR excludes contributions and withdrawals. XIRR uses dated investor flows and terminal value. Nifty 50 receives the same external flows at stored EOD closes.'
      : (gates.map((gate) => gateLabels[gate] || gate.replaceAll('_', ' ')).join(' ') || 'Performance remains gated until the local ledger reconciles.'));
  }

  function renderPortfolioRisk(payload) {
    const ready = payload?.status === 'ready';
    const concentration = payload?.concentration || {};
    const drawdown = payload?.drawdown || {};
    const allocations = Array.isArray(payload?.allocation) ? payload.allocation : [];
    const contributions = Array.isArray(payload?.contributions) ? payload.contributions : [];
    text('portfolioLargestWeight', safeNumber(concentration.largest_position_pct) === null ? '—' : `${Number(concentration.largest_position_pct).toFixed(2)}%`);
    text('portfolioTopThree', safeNumber(concentration.top_three_pct) === null ? '—' : `${Number(concentration.top_three_pct).toFixed(2)}%`);
    text('portfolioEffectivePositions', safeNumber(concentration.effective_positions) === null ? '—' : Number(concentration.effective_positions).toFixed(2));
    text('portfolioMaxDrawdown', safeNumber(drawdown.maximum_drawdown_pct) === null ? '—' : signedPercent(drawdown.maximum_drawdown_pct));
    text('portfolioDrawdownDates', drawdown.peak_date && drawdown.trough_date
      ? `${displayDate(drawdown.peak_date)} to ${displayDate(drawdown.trough_date)}`
      : 'No measured drawdown');
    const status = $('portfolioRiskStatus');
    status.textContent = ready ? String(concentration.state || 'ready').toUpperCase() : 'GATED';
    status.className = `state-pill ${ready ? (concentration.state === 'concentrated' ? 'risk' : 'ready') : 'neutral'}`;
    $('portfolioAllocationList').innerHTML = allocations.length ? allocations.map((row) => {
      const weight = Math.max(0, Math.min(100, Number(row.allocation_pct || 0)));
      return `<div><span><b>${escapeHtml(row.symbol)}</b><small>${portfolioMoney(row.market_value_inr)}</small></span><strong>${weight.toFixed(2)}%</strong><i><b style="width:${weight}%"></b></i></div>`;
    }).join('') : '<p>No allocation evidence available.</p>';
    $('portfolioContributionList').innerHTML = contributions.length ? contributions.slice().sort((left, right) => Math.abs(Number(right.total_pnl_inr || 0)) - Math.abs(Number(left.total_pnl_inr || 0))).slice(0, 8).map((row) => {
      const contribution = safeNumber(row.return_contribution_pct);
      return `<div><span><b>${escapeHtml(row.symbol)}</b><small>${escapeHtml(row.display_name)}</small></span><strong class="${researchTone(row.total_pnl_inr)}">${portfolioMoney(row.total_pnl_inr)}<small>${contribution === null ? 'Contribution —' : `${signedPercent(contribution)} of net capital`}</small></strong></div>`;
    }).join('') : '<p>No contribution evidence available.</p>';
    const gates = Array.isArray(payload?.gates) ? payload.gates : [];
    text('portfolioRiskNote', ready
      ? `HHI ${Number(concentration.hhi || 0).toFixed(4)} · ${Number(concentration.position_count || 0)} positions · drawdown uses the cash-flow-neutral TWR path. Contribution is realised plus unrealised P&L over net external capital.`
      : (gates.map((gate) => gate.replaceAll('_', ' ')).join(' · ') || 'Risk analysis remains gated until performance evidence is admissible.'));
  }

  async function loadPortfolio(force = false) {
    if (portfolioLoaded && !force) return;
    try {
      const response = await fetchWithDeadline('/api/portfolio/overview', { cache: 'no-store' });
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.detail || payload.reason || `HTTP ${response.status}`);
      renderPortfolio(payload);
      const performanceResponse = await fetchWithDeadline('/api/portfolio/performance', { cache: 'no-store' });
      const performancePayload = await performanceResponse.json();
      if (!performanceResponse.ok || performancePayload.ok === false) throw new Error(performancePayload.detail || performancePayload.reason || `HTTP ${performanceResponse.status}`);
      renderPortfolioPerformance(performancePayload);
      const riskResponse = await fetchWithDeadline('/api/portfolio/risk-analysis', { cache: 'no-store' });
      const riskPayload = await riskResponse.json();
      if (!riskResponse.ok || riskPayload.ok === false) throw new Error(riskPayload.detail || riskPayload.reason || `HTTP ${riskResponse.status}`);
      renderPortfolioRisk(riskPayload);
    } catch (error) {
      $('portfolioHoldingsBody').innerHTML = `<tr><td colspan="7" class="portfolio-empty">Portfolio unavailable: ${escapeHtml(error.message)}</td></tr>`;
      showToast(`Portfolio unavailable: ${error.message}`);
    }
  }

  const regimeFamilyLabels = {
    bullish_trend: 'Bullish trend',
    bearish_trend: 'Bearish trend',
    transition: 'Transition',
    range: 'Range',
  };

  function regimeFamilyLabel(value) {
    return regimeFamilyLabels[value] || String(value || 'Unavailable').replaceAll('_', ' ');
  }

  function renderHistoricalRegimes(payload) {
    const coverage = payload?.coverage || {};
    const current = payload?.current || {};
    const distribution = Array.isArray(payload?.distribution) ? payload.distribution : [];
    const segments = Array.isArray(payload?.segments) ? payload.segments : [];
    const evidence = Array.isArray(payload?.recent_evidence) ? payload.recent_evidence : [];
    const policy = payload?.policy_cycle || {};
    const crossAsset = payload?.cross_asset_context || {};
    const analogues = payload?.analogues || {};
    const statusLabels = coverage.classification_statuses || {};
    text('regimeHistoryAsOf', displayDate(current.session_date || coverage.through_date));
    text('regimeHistoryCurrent', regimeFamilyLabel(current.regime_family));
    text('regimeHistoryCurrentMeta', `${String(current.trend_direction || '—').replaceAll('_', ' ')} trend · ${String(current.trend_strength || '—').replaceAll('_', ' ')} strength`);
    text('regimeHistorySessions', Number(coverage.sessions || 0).toLocaleString('en-IN'));
    text('regimeHistoryWindow', `${displayDate(coverage.from_date)} to ${displayDate(coverage.through_date)}`);
    text('regimeHistoryWindowMeta', `${Number(coverage.segments || 0).toLocaleString('en-IN')} contiguous runs`);
    text('regimeHistoryTransitions', Number(coverage.transitions || 0).toLocaleString('en-IN'));
    text('regimeHistorySegments', `${Number(coverage.segments || 0).toLocaleString('en-IN')} segments`);
    text('regimeHistoryStatus', `${Number(coverage.ranking_admissible_sessions || 0).toLocaleString('en-IN')} ranking-admissible · ${Number(statusLabels.descriptive_only || 0).toLocaleString('en-IN')} descriptive`);
    const policyCurrent = policy?.current || {};
    const policyChange = safeNumber(policyCurrent.change_bps);
    text('regimePolicyStatus', policy.status === 'ready' && safeNumber(policyCurrent.repo_rate_pct) !== null ? `${Number(policyCurrent.repo_rate_pct).toFixed(2)}%` : 'GATED');
    text('regimePolicyMeta', policy.status === 'ready'
      ? `${String(policyCurrent.cycle_phase || 'hold').replaceAll('_', ' ')} · ${policyChange === null ? 'change unavailable' : `${policyChange > 0 ? '+' : ''}${policyChange.toFixed(0)} bps`} on ${displayDate(policyCurrent.observation_date)}`
      : String(policy.required_source || 'Policy-cycle history is unavailable.').replaceAll('_', ' '));
    text('regimeTimelineCoverage', `${displayDate(coverage.from_date)} to ${displayDate(coverage.through_date)} · ${Number(coverage.sessions || 0).toLocaleString('en-IN')} sessions`);
    $('regimeTimeline').innerHTML = segments.length ? segments.map((segment) => {
      const family = String(segment.regime_family || 'transition');
      const label = regimeFamilyLabel(family);
      const title = `${label}: ${displayDate(segment.from_date)} to ${displayDate(segment.through_date)}, ${segment.sessions} sessions`;
      return `<button type="button" class="regime-timeline-segment ${escapeHtml(family)}" style="flex-grow:${Math.max(1, Number(segment.sessions || 1))}" aria-label="${escapeHtml(title)}" title="${escapeHtml(title)}">${Number(segment.sessions || 0) >= 4 ? `<span>${escapeHtml(label)}</span>` : ''}</button>`;
    }).join('') : '<p class="regime-gate-message">No completed-session regime segments are stored.</p>';
    text('regimeDistributionStatus', `${distribution.length.toLocaleString('en-IN')} families`);
    text('regimeEvidenceCount', `${evidence.length.toLocaleString('en-IN')} rows`);
    $('regimeDistribution').innerHTML = distribution.length ? distribution.map((row) => {
      const family = String(row.regime_family || 'transition');
      const share = Math.max(0, Math.min(100, Number(row.share_pct || 0)));
      return `<article class="regime-distribution-row ${escapeHtml(family)}">
        <header><strong>${escapeHtml(regimeFamilyLabel(family))}</strong><b>${Number(row.sessions || 0).toLocaleString('en-IN')} sessions · ${share.toFixed(1)}%</b></header>
        <div class="regime-distribution-bar"><i style="width:${share}%"></i></div>
        <footer><span>20-session return <b>${signedPercent(row.average_return_20d_pct)}</b></span><span>Volatility <b>${safeNumber(row.average_volatility_20d_pct) === null ? '—' : `${Number(row.average_volatility_20d_pct).toFixed(1)}%`}</b></span><span>50DMA breadth <b>${safeNumber(row.average_breadth_50dma_pct) === null ? '—' : `${Number(row.average_breadth_50dma_pct).toFixed(1)}%`}</b></span></footer>
      </article>`;
    }).join('') : '<p class="regime-gate-message">Distribution evidence is unavailable.</p>';
    $('regimeRunList').innerHTML = segments.length ? segments.slice().reverse().slice(0, 8).map((segment) => `<article class="regime-run ${escapeHtml(segment.regime_family || 'transition')}">
      <span><b>${escapeHtml(regimeFamilyLabel(segment.regime_family))}</b><small>${escapeHtml(displayDate(segment.from_date))} to ${escapeHtml(displayDate(segment.through_date))}</small></span>
      <strong>${Number(segment.sessions || 0).toLocaleString('en-IN')}<small>sessions</small></strong>
      <span><small>Avg 20-session return</small><b>${signedPercent(segment.average_return_20d_pct)}</b></span>
      <span><small>Avg 50DMA breadth</small><b>${safeNumber(segment.average_breadth_50dma_pct) === null ? '—' : `${Number(segment.average_breadth_50dma_pct).toFixed(1)}%`}</b></span>
    </article>`).join('') : '<p class="regime-gate-message">No recent regime runs are available.</p>';
    const policyPeriods = Array.isArray(policy?.periods) ? policy.periods : [];
    const policyCoverage = policy?.coverage || {};
    text('regimePolicyCoverage', policy.status === 'ready' ? `${Number(policyCoverage.validated_events || 0).toLocaleString('en-IN')} EVENTS` : 'GATED');
    $('regimePolicyDecisionList').innerHTML = policyPeriods.length ? policyPeriods.map((row) => {
      const change = safeNumber(row.change_bps);
      const changeLabel = change === null ? 'Change —' : `${change > 0 ? '+' : ''}${change.toFixed(0)} bps`;
      return `<article class="regime-policy-decision ${escapeHtml(String(row.cycle_phase || 'hold'))}">
        <time>${escapeHtml(displayDate(row.observation_date))}</time>
        <span><b>${Number(row.repo_rate_pct).toFixed(2)}%</b><small>${escapeHtml(changeLabel)} · ${escapeHtml(String(row.decision || 'hold').replaceAll('_', ' '))}</small></span>
        <span><b>${escapeHtml(String(row.cycle_phase || 'hold').replaceAll('_', ' '))}</b><small>${Number(row.classified_sessions || 0).toLocaleString('en-IN')} classified sessions</small></span>
        <span><b>${escapeHtml(regimeFamilyLabel(row.dominant_regime))}</b><small>Dominant stored regime · avg tape ${signedPercent(row.average_return_20d_pct)}</small></span>
        <a href="${escapeHtml(row.source_url)}" target="_blank" rel="noopener noreferrer" aria-label="Open official RBI source for ${escapeHtml(displayDate(row.observation_date))}">RBI source ↗</a>
      </article>`;
    }).join('') : `<div class="regime-gate-message"><strong>Validated RBI history unavailable</strong><p>${escapeHtml(String(policy.required_source || policy.reason || 'No policy series').replaceAll('_', ' '))}</p></div>`;
    text('regimePolicyMethod', policy.methodology || 'Policy observations are context only and do not alter market-regime classification.');
    const crossRows = Array.isArray(crossAsset?.observations) ? crossAsset.observations : [];
    text('regimeCrossAssetAsOf', crossAsset.through_date ? `Through ${displayDate(crossAsset.through_date)}` : 'GATED');
    $('regimeCrossAssetList').innerHTML = crossRows.length ? crossRows.map((row) => {
      const value = safeNumber(row.value);
      let valueLabel = '—';
      if (value !== null) {
        if (row.unit === 'basis_points') valueLabel = `${value.toFixed(0)} bps`;
        else if (row.unit === 'percent' || row.unit === 'percent_change') valueLabel = `${value > 0 && row.unit === 'percent_change' ? '+' : ''}${value.toFixed(2)}%`;
        else if (row.unit === 'USD/bbl') valueLabel = `$${value.toFixed(2)}`;
        else valueLabel = value.toFixed(2);
      }
      const change = safeNumber(row.change_20d_pct);
      return `<article><span>${escapeHtml(row.label)}</span><strong class="${row.unit === 'percent_change' ? researchTone(value) : ''}">${escapeHtml(valueLabel)}</strong><small>${change === null ? `As of ${escapeHtml(displayDate(row.observation_date))}` : `20-session ${signedPercent(change)} · ${escapeHtml(displayDate(row.observation_date))}`}</small></article>`;
    }).join('') : '<div class="regime-gate-message"><strong>Cross-asset context unavailable</strong><p>No validated observations precede the classified session.</p></div>';
    const analogueCurrent = analogues?.current || {};
    const analogueWindows = analogueCurrent?.windows || {};
    const analogueAudit = analogues?.release_audit || {};
    const analogueChecks = Array.isArray(analogueAudit?.checks) ? analogueAudit.checks : [];
    const analogueCatalogue = Array.isArray(analogues?.catalogue) ? analogues.catalogue : [];
    const analogueReady = analogues.status === 'ready';
    const analoguePill = $('regimeAnalogueStatus');
    analoguePill.textContent = analogueReady ? 'DESCRIPTIVE READY' : 'WITHHELD';
    analoguePill.className = `state-pill ${analogueReady ? 'ready' : 'neutral'}`;
    text('regimeAnalogueCurrent', `${regimeFamilyLabel(analogues.current_regime)} · ${Number(analogueCurrent.episode_anchors || 0).toLocaleString('en-IN')} independent episode${Number(analogueCurrent.episode_anchors || 0) === 1 ? '' : 's'}`);
    text('regimeAnalogueDetail', analogues.detail || 'No analogue conclusion is published without passing every evidence gate.');
    $('regimeAnalogueWindows').innerHTML = [5, 20, 60].map((horizon) => {
      const window = analogueWindows[String(horizon)] || {};
      const sample = Number(window.sample_size || 0);
      const ready = window.status === 'ready';
      return `<article class="${ready ? 'ready' : 'gated'}">
        <header><span>NEXT ${horizon} SESSIONS</span><b>${ready ? 'READY' : `${sample}/${Number(window.minimum_sample || 0)} SAMPLES`}</b></header>
        <strong class="${researchTone(window.median_return_pct)}">${safeNumber(window.median_return_pct) === null ? '—' : signedPercent(window.median_return_pct)}</strong>
        <small>Median return</small>
        <footer><span>Positive <b>${safeNumber(window.positive_rate_pct) === null ? '—' : `${Number(window.positive_rate_pct).toFixed(1)}%`}</b></span><span>Worst <b>${safeNumber(window.worst_return_pct) === null ? '—' : signedPercent(window.worst_return_pct)}</b></span></footer>
      </article>`;
    }).join('');
    text('regimeAnalogueAuditTitle', `${Number(analogueAudit.passed || 0)} of ${Number(analogueAudit.total || 0)} gates passed`);
    $('regimeAnalogueAudit').innerHTML = analogueChecks.length ? analogueChecks.map((check) => `<article class="${check.passed ? 'passed' : 'failed'}">
      <i aria-hidden="true">${check.passed ? '✓' : '!'}</i><span><b>${escapeHtml(check.label)}</b><small>${escapeHtml(check.detail)}</small></span>
    </article>`).join('') : '<div class="regime-gate-message"><strong>Release audit unavailable</strong><p>The regime ledger is not available.</p></div>';
    $('regimeAnalogueBody').innerHTML = analogueCatalogue.length ? analogueCatalogue.map((row) => {
      const windowCell = (horizon) => {
        const window = row?.windows?.[String(horizon)] || {};
        const value = safeNumber(window.median_return_pct);
        return `<span class="analogue-table-value ${researchTone(value)}">${value === null ? '—' : signedPercent(value)}<small>${Number(window.sample_size || 0)} mature · ${window.status === 'ready' ? 'ready' : 'gated'}</small></span>`;
      };
      return `<tr class="${row.regime_family === analogues.current_regime ? 'current' : ''}">
        <td><strong>${escapeHtml(regimeFamilyLabel(row.regime_family))}</strong>${row.regime_family === analogues.current_regime ? '<small>Current family</small>' : ''}</td>
        <td class="numeric">${Number(row.episode_anchors || 0).toLocaleString('en-IN')}</td>
        <td>${windowCell(5)}</td><td>${windowCell(20)}</td><td>${windowCell(60)}</td>
      </tr>`;
    }).join('') : '<tr><td colspan="5" class="portfolio-empty">No analogue catalogue is available.</td></tr>';
    const rules = analogues?.rules || {};
    text('regimeAnalogueMethod', rules.anchor
      ? `${rules.anchor}. ${rules.match}. Forward windows: ${(rules.forward_windows_sessions || []).join(', ')} sessions. Minimum ${Number(rules.minimum_independent_episodes || 0)} independent episodes per window and ${Number(rules.minimum_ledger_sessions || 0).toLocaleString('en-IN')} classified sessions. Forward outcomes never alter the classifier.`
      : 'One anchor per contiguous regime episode; exact-family matching; trailing-only completed-session returns.');
    $('regimeEvidenceBody').innerHTML = evidence.length ? evidence.map((row) => `<tr>
      <td>${escapeHtml(displayDate(row.session_date))}</td><td><strong>${escapeHtml(regimeFamilyLabel(row.regime_family))}</strong><small>${escapeHtml(String(row.trend_direction || '—').replaceAll('_', ' '))}</small></td>
      <td class="numeric ${researchTone(row.market_return_20d_pct)}">${signedPercent(row.market_return_20d_pct)}</td>
      <td class="numeric ${researchTone(row.market_distance_sma200_pct)}">${signedPercent(row.market_distance_sma200_pct)}</td>
      <td class="numeric">${safeNumber(row.market_volatility_20d_pct) === null ? '—' : `${Number(row.market_volatility_20d_pct).toFixed(2)}%`}</td>
      <td class="numeric">${safeNumber(row.market_breadth_50dma_pct) === null ? '—' : `${Number(row.market_breadth_50dma_pct).toFixed(1)}%`}</td>
      <td class="numeric">${safeNumber(row.market_breadth_200dma_pct) === null ? '—' : `${Number(row.market_breadth_200dma_pct).toFixed(1)}%`}</td>
      <td>${escapeHtml(String(row.classification_status || '—').replaceAll('_', ' '))}</td>
    </tr>`).join('') : '<tr><td colspan="8" class="portfolio-empty">No completed-session regime evidence is stored.</td></tr>';
    text('regimeEvidenceCount', `${evidence.length.toLocaleString('en-IN')} rows`);
    text('regimeHistoryBoundary', payload?.methodology?.decision_boundary || 'Descriptive completed-session classifications only.');
    historicalRegimesLoaded = true;
  }

  async function loadHistoricalRegimes(force = false) {
    if (historicalRegimesLoaded && !force) return;
    try {
      const response = await fetchWithDeadline('/api/historical-regimes/overview?limit=500', { cache: 'no-store' });
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.detail || payload.reason || `HTTP ${response.status}`);
      renderHistoricalRegimes(payload);
    } catch (error) {
      $('regimeTimeline').innerHTML = `<p class="regime-gate-message">Historical regime evidence unavailable: ${escapeHtml(error.message)}</p>`;
      $('regimeEvidenceBody').innerHTML = `<tr><td colspan="8" class="portfolio-empty">Historical regime evidence unavailable: ${escapeHtml(error.message)}</td></tr>`;
      showToast(`Historical regimes unavailable: ${error.message}`);
    }
  }

  function showView(view) {
    const marketState = view === 'market-state';
    const fnoPositioning = view === 'fno-positioning';
    const stockResearch = view === 'stock-research';
    const portfolio = view === 'portfolio';
    const historicalRegimes = view === 'historical-regimes';
    const dashboard = !marketState && !fnoPositioning && !stockResearch && !portfolio && !historicalRegimes;
    $('dashboardView').hidden = !dashboard;
    $('marketStateView').hidden = !marketState;
    $('fnoPositioningView').hidden = !fnoPositioning;
    $('stockResearchView').hidden = !stockResearch;
    $('portfolioView').hidden = !portfolio;
    $('historicalRegimesView').hidden = !historicalRegimes;
    $('dashboardView').classList.toggle('active', dashboard);
    $('marketStateView').classList.toggle('active', marketState);
    $('fnoPositioningView').classList.toggle('active', fnoPositioning);
    $('stockResearchView').classList.toggle('active', stockResearch);
    $('portfolioView').classList.toggle('active', portfolio);
    $('historicalRegimesView').classList.toggle('active', historicalRegimes);
    $('dashboardNav').classList.toggle('active', dashboard);
    $('marketStateNav').classList.toggle('active', marketState);
    $('fnoNav').classList.toggle('active', fnoPositioning);
    $('stockResearchNav').classList.toggle('active', stockResearch);
    $('portfolioNav').classList.toggle('active', portfolio);
    $('historicalRegimesNav').classList.toggle('active', historicalRegimes);
    document.title = historicalRegimes ? 'Historical Regimes · Workspace 2.0' : (portfolio ? 'Portfolio · Workspace 2.0' : (stockResearch ? 'Stock Research · Workspace 2.0' : (fnoPositioning ? 'F&O Positioning · Workspace 2.0' : (marketState ? 'Market State · Workspace 2.0' : 'Workspace 2.0'))));
    const url = new URL(window.location.href);
    if (!dashboard) url.searchParams.set('view', view);
    else url.searchParams.delete('view');
    window.history.replaceState({}, '', url);
    window.scrollTo({ top: 0, behavior: 'instant' });
    if (portfolio) void loadPortfolio();
    if (historicalRegimes) void loadHistoricalRegimes();
  }

  $('themeToggle').addEventListener('click', () => setTheme(document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'));
  $('refreshButton').addEventListener('click', async () => {
    await loadDashboard(true, kiteConnected);
    await loadWorkspaceReleaseAudit();
    if (!$('historicalRegimesView').hidden) {
      historicalRegimesLoaded = false;
      await loadHistoricalRegimes(true);
    }
  });
  $('kiteLoginButton').addEventListener('click', () => setKitePanel($('kitePanel').hidden));
  $('kiteCloseButton').addEventListener('click', () => setKitePanel(false));
  $('openKiteLoginButton').addEventListener('click', () => {
    const apiKey = $('kiteApiKey').value.trim();
    if (!apiKey) {
      showToast('Enter the Kite API key first.');
      $('kiteApiKey').focus();
      return;
    }
    if (!$('kiteApiSecret').value) {
      showToast('Enter the API secret before opening Kite login.');
      $('kiteApiSecret').focus();
      return;
    }
    const loginUrl = new URL('https://kite.zerodha.com/connect/login');
    loginUrl.searchParams.set('v', '3');
    loginUrl.searchParams.set('api_key', apiKey);
    $('kiteRequestToken').value = '';
    window.open(loginUrl.toString(), '_blank', 'noopener,noreferrer');
    showToast('Official Kite login opened. Complete it now, then paste the new redirect URL. Request tokens are single-use.');
  });
  $('kiteCredentialForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const button = $('kiteConnectButton');
    button.disabled = true;
    button.textContent = 'Connecting…';
    try {
      const response = await fetch('/api/kite/session', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, cache: 'no-store',
        body: JSON.stringify({
          api_key: $('kiteApiKey').value.trim(),
          api_secret: $('kiteApiSecret').value,
          request_token: $('kiteRequestToken').value.trim(),
        }),
      });
      const payload = await response.json();
      if (!response.ok || payload.ok !== true) throw new Error(payload.reason || `HTTP ${response.status}`);
      $('kiteCredentialForm').reset();
      setKiteState(true);
      setKitePanel(false);
      showToast('Kite connected. Refresh data will now source completed-session EOD inputs.');
      await loadDashboard(true, true);
    } catch (error) {
      setKiteState(false);
      const message = String(error.message).replaceAll('_', ' ');
      const expired = /expired|invalid.*token|token.*invalid/i.test(message);
      showToast(expired
        ? 'Kite request token expired or was already used. Open official Kite login again and paste the newly returned redirect URL immediately.'
        : `Kite connection failed: ${message}`);
    } finally {
      button.disabled = false;
      button.textContent = 'Connect to Kite';
    }
  });
  $('kiteDisconnectButton').addEventListener('click', async () => {
    try {
      await fetch('/api/kite/disconnect', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
    } finally {
      $('kiteCredentialForm').reset();
      setKiteState(false);
      setKitePanel(false);
      showToast('Kite session removed from local memory.');
    }
  });
  $('dashboardNav').addEventListener('click', () => showView('dashboard'));
  $('marketStateNav').addEventListener('click', () => showView('market-state'));
  $('fnoNav').addEventListener('click', () => showView('fno-positioning'));
  $('stockResearchNav').addEventListener('click', () => showView('stock-research'));
  $('portfolioNav').addEventListener('click', () => showView('portfolio'));
  $('historicalRegimesNav').addEventListener('click', () => showView('historical-regimes'));
  $('portfolioTransactionForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const button = $('portfolioSaveButton');
    button.disabled = true;
    button.textContent = 'Saving…';
    try {
      const response = await fetch('/api/portfolio/transactions', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, cache: 'no-store',
        body: JSON.stringify({
          trade_date: $('portfolioTradeDate').value,
          symbol: $('portfolioSymbol').value.trim(),
          side: $('portfolioSide').value,
          quantity: $('portfolioQuantity').value,
          price_inr: $('portfolioPrice').value,
          fees_inr: $('portfolioFees').value || 0,
          notes: $('portfolioNotes').value.trim(),
        }),
      });
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.reason || payload.detail || `HTTP ${response.status}`);
      $('portfolioTransactionForm').reset();
      $('portfolioTradeDate').value = new Date().toISOString().slice(0, 10);
      $('portfolioFees').value = '0';
      portfolioLoaded = false;
      await loadPortfolio(true);
      showToast(`${payload.side} ${payload.quantity} ${payload.symbol} added to the local ledger.`);
    } catch (error) {
      showToast(`Transaction not saved: ${String(error.message).replaceAll('_', ' ')}`);
    } finally {
      button.disabled = false;
      button.textContent = 'Add to ledger';
    }
  });
  $('portfolioCashFlowForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const button = $('portfolioCashFlowButton');
    button.disabled = true;
    button.textContent = 'Saving…';
    try {
      const response = await fetch('/api/portfolio/cash-flows', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, cache: 'no-store',
        body: JSON.stringify({
          flow_date: $('portfolioCashFlowDate').value,
          flow_type: $('portfolioCashFlowType').value,
          amount_inr: $('portfolioCashFlowAmount').value,
          notes: $('portfolioCashFlowNotes').value.trim(),
        }),
      });
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.reason || payload.detail || `HTTP ${response.status}`);
      $('portfolioCashFlowForm').reset();
      $('portfolioCashFlowDate').value = new Date().toISOString().slice(0, 10);
      portfolioLoaded = false;
      await loadPortfolio(true);
      showToast(`${payload.flow_type.replaceAll('_', ' ')} added to the local cash ledger.`);
    } catch (error) {
      showToast(`Cash flow not saved: ${String(error.message).replaceAll('_', ' ')}`);
    } finally {
      button.disabled = false;
      button.textContent = 'Add cash flow';
    }
  });
  $('portfolioImportForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    const file = $('portfolioImportFile').files?.[0];
    if (!file) return;
    const button = $('portfolioImportButton');
    button.disabled = true;
    button.textContent = 'Validating…';
    try {
      if (file.size > 1500000) throw new Error('portfolio import too large');
      const response = await fetch('/api/portfolio/import', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, cache: 'no-store',
        body: JSON.stringify({ file_name: file.name, csv_text: await file.text() }),
      });
      const payload = await response.json();
      if (!response.ok || payload.ok === false) throw new Error(payload.reason || payload.detail || `HTTP ${response.status}`);
      $('portfolioImportForm').reset();
      portfolioLoaded = false;
      await loadPortfolio(true);
      showToast(payload.duplicate ? 'This tradebook was already imported.' : `${payload.records_imported} tradebook rows imported.`);
    } catch (error) {
      showToast(`Import failed: ${String(error.message).replaceAll('_', ' ')}`);
    } finally {
      button.disabled = false;
      button.textContent = 'Validate and import';
    }
  });
  document.querySelectorAll('[data-factor-size][data-factor-style]').forEach((button) => button.addEventListener('click', () => openFactorDialog(button.dataset.factorSize, button.dataset.factorStyle)));
  $('factorDialogClose').addEventListener('click', () => $('factorDialog').close());
  $('factorDialog').addEventListener('click', (event) => {
    if (event.target === $('factorDialog')) $('factorDialog').close();
  });
  $('factorDialogSearch').addEventListener('input', renderFactorDialog);
  document.querySelectorAll('[data-factor-sort]').forEach((button) => button.addEventListener('click', () => {
    const key = button.dataset.factorSort;
    factorDialogSort = factorDialogSort.key === key
      ? { key, direction: factorDialogSort.direction === 'asc' ? 'desc' : 'asc' }
      : { key, direction: ['symbol', 'industry'].includes(key) ? 'asc' : 'desc' };
    renderFactorDialog();
  }));
  $('fnoStateFilter').addEventListener('change', renderFnoTable);
  document.querySelectorAll('[data-fno-sort]').forEach((button) => button.addEventListener('click', () => {
    const key = button.dataset.fnoSort;
    fnoSort = fnoSort.key === key ? { key, direction: fnoSort.direction === 'asc' ? 'desc' : 'asc' } : { key, direction: 'asc' };
    renderFnoTable();
  }));
  $('researchUniverse').addEventListener('change', () => {
    researchSort = { key: 'evidenceScore', direction: $('researchProfitOrder').value };
    closeResearchDetail();
    buildResearchRows();
  });
  $('researchSearch').addEventListener('input', renderResearchTable);
  $('researchSector').addEventListener('change', renderResearchTable);
  $('researchState').addEventListener('change', renderResearchTable);
  $('researchRankModel').addEventListener('change', () => loadResearchRanking($('researchRankModel').value));
  $('researchProfitOrder').addEventListener('change', () => {
    researchSort = { key: 'evidenceScore', direction: $('researchProfitOrder').value };
    renderResearchTable();
  });
  $('researchReset').addEventListener('click', () => {
    $('researchSearch').value = '';
    $('researchSector').value = 'all';
    $('researchState').value = 'all';
    renderResearchTable();
  });
  $('researchBacktestButton').addEventListener('click', toggleResearchBacktest);
  $('researchDetailClose').addEventListener('click', closeResearchDetail);
  $('researchDetailDialog').addEventListener('click', (event) => {
    if (event.target === $('researchDetailDialog')) closeResearchDetail();
  });
  $('researchDetailDialog').addEventListener('cancel', (event) => {
    event.preventDefault();
    closeResearchDetail();
  });
  $('researchBacktestStrategy').addEventListener('change', () => {
    if (!selectedResearchSymbol) return;
    researchBacktestPanelSymbol = null;
    $('researchBacktestPanel').hidden = true;
    toggleResearchBacktest();
  });
  document.querySelectorAll('[data-research-sort]').forEach((button) => button.addEventListener('click', () => {
    const key = button.dataset.researchSort;
    researchSort = researchSort.key === key
      ? { key, direction: researchSort.direction === 'asc' ? 'desc' : 'asc' }
      : { key, direction: ['symbol', 'sector', 'status'].includes(key) ? 'asc' : 'desc' };
    if (key === 'evidenceScore') $('researchProfitOrder').value = researchSort.direction;
    renderResearchTable();
  }));
  setTheme(localStorage.getItem('workspace-2-theme') || 'dark');
  $('portfolioTradeDate').value = new Date().toISOString().slice(0, 10);
  $('portfolioCashFlowDate').value = new Date().toISOString().slice(0, 10);
  const requestedView = new URLSearchParams(window.location.search).get('view');
  showView(['market-state', 'fno-positioning', 'stock-research', 'portfolio', 'historical-regimes'].includes(requestedView) ? requestedView : 'dashboard');
  loadKiteStatus();
  loadWorkspaceReleaseAudit();
  loadDashboard(false);
})();
