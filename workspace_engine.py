#!/usr/bin/env python3
"""Independent Workspace 2.0 EOD data engine.

The module owns the Workspace market database, reads the official-source
snapshot database, calculates the UI contracts, and talks to Kite directly.
It deliberately contains no imports, HTTP calls, or filesystem references to
PG-terminal.
"""

from __future__ import annotations

import csv
import calendar
import hashlib
import io
import json
import math
import re
import sqlite3
import threading
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

import requests

from workspace_analytics.cache import (
    load_versioned_result,
    load_versioned_results_by_prefix,
    semantic_source_version,
    store_versioned_result,
)
from workspace_analytics.data_quality import quality_checked_backtest_series
from workspace_analytics.execution import (
    DEFAULT_BACKTEST_COST_CONFIG,
    EQUITY_DELIVERY_COST_SCHEDULE,
    equity_delivery_cost_calculator,
    equity_delivery_side_charges as _equity_delivery_side_charges,
    normalise_cost_config as _normalise_cost_config,
)
from workspace_analytics.jobs import (
    finish_job,
    job_status,
    list_jobs,
    start_job,
    update_job,
)
from workspace_analytics.historical_regimes import historical_regimes_overview
from workspace_analytics.metrics import maximum_drawdown_pct as _maximum_drawdown_pct
from workspace_analytics.mean_reversion_strategies import (
    bollinger_mean_reversion_series as _bollinger_mean_reversion_component,
    pullback_uptrend_series as _pullback_uptrend_component,
    rsi_mean_reversion_series as _rsi_mean_reversion_component,
    selloff_recovery_series as _selloff_recovery_component,
    zscore_mean_reversion_series as _zscore_mean_reversion_component,
)
from workspace_analytics.portfolio import (
    import_portfolio_tradebook,
    portfolio_overview,
    portfolio_performance,
    portfolio_risk_analysis,
    record_portfolio_cash_flow,
    record_portfolio_transaction,
)
from workspace_analytics.regimes import (
    market_regime_classification as _market_regime_classification_component,
    market_regime_sample_summary as _market_regime_sample_summary_component,
    sector_trend_classification as _sector_trend_classification_component,
    stock_leadership_classification as _stock_leadership_classification_component,
)
from workspace_analytics.ranking import (
    attach_comparison_evidence_rankings,
    attach_oos_evidence_rankings,
    cross_strategy_fdr_from_payloads,
)
from workspace_analytics.statistics import (
    attach_benjamini_hochberg_fdr as _attach_benjamini_hochberg_fdr,
    bootstrap_trade_evidence as _bootstrap_trade_evidence,
    trade_edge_p_value as _trade_edge_p_value,
)
from workspace_analytics.sqlite_store import connect_database
from workspace_analytics.trend_strategies import (
    donchian_breakout_series as _donchian_breakout_component,
    ema_crossover_series as _ema_crossover_component,
    moving_average_crossover_series as _moving_average_crossover_component,
    time_series_momentum_series as _time_series_momentum_component,
    triple_sma_alignment_series as _triple_sma_alignment_component,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_MARKET_DB = ROOT / "data" / "workspace_market.sqlite3"
DEFAULT_SNAPSHOT_DB = ROOT / "data" / "workspace_snapshots.sqlite3"
KITE_API = "https://api.kite.trade"
KITE_VERSION = "3"
NSE_CORPORATE_ACTIONS_API = "https://www.nseindia.com/api/corporates-corporateActions"
KITE_ADJUSTMENT_POLICY_URL = "https://kite.trade/forum/discussion/15358/documentation-of-adjustments-in-historical-data"
NSE_CORPORATE_ACTIONS_URL = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
NIFTY500_INDEX_URL = "https://www.niftyindices.com/indices/equity/broad-based-indices/nifty-500"
NIFTY500_RECONSTITUTION_URL = "https://www.niftyindices.com/resources/index-rebalancing-schedule"
YAHOO_CHART_API = "https://query1.finance.yahoo.com/v8/finance/chart"
NSE_FO_UDIFF_TEMPLATE = (
    "https://nsearchives.nseindia.com/content/fo/"
    "BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"
)
NSE_FO_LEGACY_TEMPLATE = (
    "https://archives.nseindia.com/content/historical/DERIVATIVES/"
    "{year}/{month}/fo{day}{month}{year}bhav.csv.zip"
)
NSE_FO_UDIFF_START = date(2024, 7, 8)
NSE_FO_ARCHIVE_DIR = ROOT / "data" / "nse_fo_archives"
NSE_FO_USER_AGENT = "Workspace-2.0-EOD-Archive/1.0"
NSE_CM_FULL_TEMPLATE = (
    "https://nsearchives.nseindia.com/products/content/"
    "sec_bhavdata_full_{date}.csv"
)
NSE_CM_ARCHIVE_DIR = ROOT / "data" / "nse_cm_archives"
NSE_CM_USER_AGENT = "Workspace-2.0-Cash-Archive/1.0"
NIFTY500_CONSTITUENT_ARCHIVE_DIR = ROOT / "data" / "nifty500_constituent_archives"

# Official semi-annual size-index changes.  Nifty 100, Nifty Midcap 150 and
# Nifty Smallcap 250 form the mutually exclusive size buckets used by the
# NIFTY 500 factor views.  Reversing these ledgers from the latest validated
# snapshot produces the bucket that was actually in force on an earlier date.
NIFTY_SIZE_RECONSTITUTION_NOTICES = (
    {
        "effective_date": "2025-09-30",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs22082025.pdf",
        "buckets": {
            "Large": {
                "excluded": ("DABUR", "HEROMOTOCO", "ICICIPRULI", "INDUSINDBK", "SWIGGY"),
                "included": ("HINDZINC", "MAXHEALTH", "MAZDOCK", "ENRIN", "SOLARINDS"),
            },
            "Mid": {
                "excluded": ("ABFRL", "BANDHANBNK", "EMAMILTD", "GLAND", "HINDZINC", "MRPL", "MAXHEALTH", "MAZDOCK", "MSUMI", "OLAELEC", "SOLARINDS", "STARHEALTH", "SUNTV"),
                "included": ("DABUR", "FACT", "GODFRYPHLP", "HEROMOTOCO", "HEXT", "ICICIPRULI", "IDBI", "IOB", "INDUSINDBK", "ITCHOTELS", "PGHH", "SWIGGY", "UCOBANK"),
            },
            "Small": {
                "excluded": ("ALIVUS", "FACT", "GODFRYPHLP", "GNFC", "GPPL", "IDBI", "IOB", "JSWHL", "JUSTDIAL", "KANSAINER", "KNRCON", "MASTEK", "NETWORK18", "PNCINFRA", "RTNINDIA", "RAYMONDLSL", "RAYMOND", "ROUTE", "RENUKA", "SWSOLAR", "TANLA", "UCOBANK", "WESTLIFE"),
                "included": ("ABFRL", "ABLBL", "AEGISVOPAK", "AKZOINDIA", "ATHERENERG", "BANDHANBNK", "BLUEJET", "CHOICEIN", "AGARWALEYE", "EMAMILTD", "FORCEMOT", "GLAND", "KSB", "MAHSCOOTER", "MRPL", "MSUMI", "NUVOCO", "OLAELEC", "ONESOURCE", "THELEELA", "STARHEALTH", "SUNTV", "VENTIVE"),
            },
        },
    },
    {
        "effective_date": "2026-03-30",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs23022026.pdf",
        "buckets": {
            "Large": {
                "excluded": ("BAJAJHFL", "HAVELLS", "ICICIGI", "NAUKRI", "JSWENERGY", "LICI"),
                "included": ("CUMMINSIND", "HDFCAMC", "MUTHOOTFIN", "TATACAP", "TMCV", "UNIONBANK"),
            },
            "Mid": {
                "excluded": ("CUMMINSIND", "DEEPAKNTR", "FACT", "GUJGASLTD", "HDFCAMC", "IDBI", "IOB", "IGL", "IRB", "MUTHOOTFIN", "PGHH", "SONACOMS", "SYNGENE", "TATATECH", "UCOBANK", "UNIONBANK"),
                "included": ("ANTHEM", "AIIL", "BAJAJHFL", "GROWW", "HAVELLS", "HDBFS", "ICICIGI", "ICICIAMC", "NAUKRI", "JSWENERGY", "LAURUSLABS", "LENSKART", "LGEINDIA", "LICI", "MCX", "RADICO"),
            },
            "Small": {
                "excluded": ("AKUMS", "APLLTD", "ALKYLAMINE", "ALOKINDS", "ASTRAZEN", "AIIL", "BASF", "CAMPUS", "CENTURYPLY", "CERA", "AGARWALEYE", "FINPIPE", "GODREJAGRO", "HAPPSTMNDS", "INOXINDIA", "JYOTHYLAB", "KIRLOSBROS", "KSB", "LAURUSLABS", "MAHSCOOTER", "MAHSEAMLES", "METROPOLIS", "MCX", "PRAJIND", "RADICO", "RCF", "RELINFRA", "SUNDRMFAST", "TRIVENI", "DBREALTY", "MANYAVAR", "VENTIVE", "VGUARD"),
                "included": ("ACUTAAS", "CPPLUS", "ABDL", "ANURAS", "BELRISE", "CANHLIFE", "CARTRADE", "CEMPRO", "DEEPAKNTR", "EMMVEE", "FACT", "GABRIEL", "GALLANTT", "IDBI", "IOB", "IGL", "IRB", "JAINREC", "MEESHO", "PARADEEP", "PWL", "PINELABS", "PIRAMALFIN", "SONACOMS", "SPLPETRO", "SYNGENE", "TATATECH", "TEGA", "TENNIND", "TRAVELFOOD", "UCOBANK", "URBANCO", "ZYDUSWELL"),
            },
        },
    },
    {
        "effective_date": "2026-09-30",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs10082026.pdf",
        "buckets": {
            "Large": {
                "excluded": ("INDHOTEL", "LODHA", "RECLTD", "SHREECEM", "UNITDSPR"),
                "included": ("BSE", "POWERINDIA", "POLYCAB", "VAML", "IDEA"),
            },
            "Mid": {
                "excluded": ("3MINDIA", "ACC", "APOLLOTYRE", "AWL", "BSE", "POWERINDIA", "HONAUT", "KPITTECH", "POLYCAB", "SJVN", "TATAELXSI", "NIACL", "IDEA"),
                "included": ("ASTERDM", "EMBASSY", "FACT", "HINDCOPPER", "INDHOTEL", "IOB", "LODHA", "MEESHO", "NH", "PIRAMALFIN", "RECLTD", "SHREECEM", "UNITDSPR"),
            },
            "Small": {
                "excluded": ("ABFRL", "ABLBL", "ASTERDM", "BAYERCROP", "BIKAJI", "BLUEDART", "BLUEJET", "BBTC", "MAPMYINDIA", "CHALET", "DCMSHRIRAM", "ERIS", "FACT", "GODIGIT", "HINDCOPPER", "INDGN", "IOB", "JSWDULUX", "JUBLPHARMA", "LATENTVIEW", "THELEELA", "MEESHO", "NH", "NEWGEN", "NIVABUPA", "PFIZER", "PIRAMALFIN", "SAPPHIRE", "SAREGAMA", "SBFC", "SONATSOFTW", "SPLPETRO", "TRAVELFOOD"),
                "included": ("ACC", "AETHER", "APOLLOTYRE", "AVANTIFEED", "AWL", "AZAD", "BAGMANE", "BHARATCOAL", "BBOX", "BIRET", "CMPDI", "CLEANMAX", "CUPID", "FRACTAL", "HONAUT", "INOXINDIA", "KIRLOSBROS", "KPITTECH", "KSB", "MTARTECH", "PRIVISCL", "RUBICON", "SANSERA", "SJVN", "SHRIPISTON", "STLTECH", "TATAELXSI", "TDPOWERSYS", "THANGAMAYL", "NIACL", "VISL", "VOGL", "VEDPOWER"),
            },
        },
    },
)

# Ad-hoc NIFTY 500 replacements outside the semi-annual review remain in the
# small-cap bucket unless an official size-index notice says otherwise.
NIFTY_SIZE_AD_HOC_REPLACEMENTS = (
    {"effective_date": "2025-09-23", "bucket": "Small", "excluded": ("PEL",), "included": ("RELINFRA",)},
    {"effective_date": "2025-11-28", "bucket": "Small", "excluded": ("ADVENTHTL",), "included": ()},
    {"effective_date": "2025-12-15", "bucket": "Small", "excluded": ("SKFINDUS",), "included": ()},
    {"effective_date": "2025-12-31", "bucket": "Small", "excluded": ("SKFINDIA",), "included": ("JSWCEMENT",)},
    {"effective_date": "2026-02-24", "bucket": "Large", "excluded": ("KWIL",), "included": ()},
    {"effective_date": "2026-05-12", "bucket": "Small", "excluded": ("GSPL",), "included": ("CIEINDIA",)},
    {"effective_date": "2026-07-17", "bucket": "Small", "excluded": ("JBCHEPHARM",), "included": ("PFOCUS",)},
)

NIFTY500_LINEAGE_SIZE_BUCKETS = {
    "ADVENTHTL": "Small",
    "SKFINDUS": "Small",
    "KWIL": "Large",
    "VAML": "Large",
    "VEDPOWER": "Large",
    "VOGL": "Large",
    "VISL": "Large",
    "HEGAM": "Small",
}

NIFTY500_SYMBOL_CHANGES = (
    {
        "old_symbol": "HEG",
        "new_symbol": "HEGAM",
        "effective_date": "2026-09-07",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs03092026.pdf",
    },
    {
        "old_symbol": "AKZOINDIA",
        "new_symbol": "JSWDULUX",
        "effective_date": "2026-04-15",
        "source_url": "https://nsearchives.nseindia.com/content/circulars/CML73652.pdf",
    },
)

# Industry labels for constituents admitted after the last pre-review archived
# constituent file.  The membership effective date comes from the official
# change notice; the sector is confirmed by the first subsequent official
# constituent CSV.  This avoids applying a present-day sector blindly while
# keeping the later confirmation explicit in provenance.
NIFTY500_SECTOR_CLASSIFICATION_EVENTS = (
    {
        "effective_date": "2025-07-18", "symbol": "ADVENTHTL",
        "sector": "Consumer Services",
        "event_url": "https://www.niftyindices.com/Press_Release/ind_prs15072025.pdf",
        "confirmation_date": "2025-11-13",
    },
    {
        "effective_date": "2025-09-23", "symbol": "RELINFRA", "sector": "Power",
        "event_url": "https://www.niftyindices.com/Press_Release/ind_prs15092025_1.pdf",
        "confirmation_date": "2026-01-07",
    },
    *(
        {
            "effective_date": "2025-09-30", "symbol": symbol, "sector": sector,
            "event_url": "https://niftyindices.com/Press_Release/ind_prs22082025.pdf",
            "confirmation_date": confirmation_date,
        }
        for symbol, sector, confirmation_date in (
            ("ABLBL", "Consumer Services", "2026-01-07"),
            ("AGARWALEYE", "Healthcare", "2026-01-07"),
            ("BLUEJET", "Healthcare", "2026-01-07"),
            ("AKZOINDIA", "Consumer Durables", "2026-01-07"),
            ("KSB", "Capital Goods", "2026-01-07"),
            ("MAHSCOOTER", "Financial Services", "2026-01-07"),
            ("PGHH", "Fast Moving Consumer Goods", "2026-01-07"),
            ("THELEELA", "Consumer Services", "2026-01-07"),
            ("VENTIVE", "Consumer Services", "2026-01-07"),
        )
    ),
    {
        "effective_date": "2025-12-05", "symbol": "KWIL",
        "sector": "Fast Moving Consumer Goods",
        "event_url": "https://www.niftyindices.com/Press_Release/ind_prs28112025.pdf",
        "confirmation_date": "2025-12-05",
    },
    {
        "effective_date": "2025-10-15", "symbol": "SKFINDUS",
        "sector": "Capital Goods",
        "event_url": "https://www.niftyindices.com/Press_Release/ind_prs10102025.pdf",
        "confirmation_date": "2025-12-05",
    },
    {
        "effective_date": "2026-03-30", "symbol": "SPLPETRO", "sector": "Chemicals",
        "event_url": "https://www.niftyindices.com/Press_Release/ind_prs23022026.pdf",
        "confirmation_date": "2026-05-02",
    },
    {
        "effective_date": "2026-03-30", "symbol": "TRAVELFOOD", "sector": "Consumer Services",
        "event_url": "https://www.niftyindices.com/Press_Release/ind_prs23022026.pdf",
        "confirmation_date": "2026-05-02",
    },
    {
        "effective_date": "2026-04-15", "symbol": "JSWDULUX", "sector": "Consumer Durables",
        "event_url": "https://nsearchives.nseindia.com/content/circulars/CML73652.pdf",
        "confirmation_date": "2026-05-02",
    },
    {
        "effective_date": "2026-04-30", "symbol": "VAML", "sector": "Metals & Mining",
        "event_url": "https://www.niftyindices.com/Press_Release/ind_prs23042026.pdf",
        "confirmation_date": "2026-11-02",
    },
    {
        "effective_date": "2026-04-30", "symbol": "VEDPOWER", "sector": "Power",
        "event_url": "https://www.niftyindices.com/Press_Release/ind_prs23042026.pdf",
        "confirmation_date": "2026-11-02",
    },
    {
        "effective_date": "2026-04-30", "symbol": "VOGL", "sector": "Oil Gas & Consumable Fuels",
        "event_url": "https://www.niftyindices.com/Press_Release/ind_prs23042026.pdf",
        "confirmation_date": "2026-11-02",
    },
    {
        "effective_date": "2026-04-30", "symbol": "VISL", "sector": "Metals & Mining",
        "event_url": "https://www.niftyindices.com/Press_Release/ind_prs23042026.pdf",
        "confirmation_date": "2026-11-02",
    },
)

# Workspace-owned completed-session global-risk inputs. Yahoo is used as the
# consolidated EOD transport; each stored row keeps the upstream series and URL
# so the observation remains auditable and can later be replaced independently.
GLOBAL_RISK_SERIES = (
    {"key": "brent_crude", "symbol": "BZ=F", "label": "Brent crude", "unit": "USD/bbl", "core": True},
    {"key": "india_vix", "symbol": "^INDIAVIX", "label": "India VIX", "unit": "index", "core": True},
    {"key": "usd_inr", "symbol": "INR=X", "label": "USD/INR", "unit": "INR per USD", "core": True},
    {"key": "sp500", "symbol": "^GSPC", "label": "S&P 500", "unit": "index", "core": True},
    {"key": "us_vix", "symbol": "^VIX", "label": "U.S. VIX", "unit": "index", "core": False},
    {"key": "dxy", "symbol": "DX-Y.NYB", "label": "U.S. Dollar Index", "unit": "index", "core": False},
)

# Official broad-market notices reviewed for the active research period.  These
# events are deliberately stored as a ledger rather than silently rewriting the
# latest constituent file.  The ledger is not yet declared complete: ad-hoc
# suspensions, schemes of arrangement and delistings must also be reviewed before
# it may be used as a hard backtest-admission gate.
NIFTY500_MEMBERSHIP_NOTICES = (
    {
        "effective_date": "2025-09-23",
        "published_date": "2025-09-15",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs15092025_1.pdf",
        "excluded": ("PEL",),
        "included": ("RELINFRA",),
    },
    {
        "effective_date": "2025-09-30",
        "published_date": "2025-08-22",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs22082025.pdf",
        "excluded": (
            "ALIVUS", "GNFC", "GPPL", "JSWHL", "JUSTDIAL", "KANSAINER",
            "KNRCON", "MASTEK", "NETWORK18", "PNCINFRA", "RTNINDIA",
            "RAYMONDLSL", "RAYMOND", "ROUTE", "RENUKA", "SWSOLAR", "TANLA",
            "WESTLIFE",
        ),
        "included": (
            "ABLBL", "AEGISVOPAK", "AKZOINDIA", "ATHERENERG", "BLUEJET",
            "CHOICEIN", "AGARWALEYE", "FORCEMOT", "HEXT", "ITCHOTELS", "KSB",
            "MAHSCOOTER", "NUVOCO", "ONESOURCE", "PGHH", "THELEELA", "ENRIN",
            "VENTIVE",
        ),
    },
    {
        "effective_date": "2025-11-28",
        "published_date": "2025-11-26",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs26112025.pdf",
        "excluded": ("ADVENTHTL",),
        "included": (),
    },
    {
        "effective_date": "2025-12-15",
        "published_date": "2025-12-11",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs11122025_1.pdf",
        "excluded": ("SKFINDUS",),
        "included": (),
    },
    {
        "effective_date": "2025-12-31",
        "published_date": "2025-12-11",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs11122025.pdf",
        "excluded": ("SKFINDIA",),
        "included": ("JSWCEMENT",),
    },
    {
        "effective_date": "2026-02-24",
        "published_date": "2026-02-20",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs20022026_1.pdf",
        "excluded": ("KWIL",),
        "included": (),
    },
    {
        "effective_date": "2026-03-30",
        "published_date": "2026-02-23",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs23022026.pdf",
        "excluded": (
            "AKUMS", "APLLTD", "ALKYLAMINE", "ALOKINDS", "ASTRAZEN", "BASF",
            "CAMPUS", "CENTURYPLY", "CERA", "AGARWALEYE", "FINPIPE", "GODREJAGRO",
            "GUJGASLTD", "HAPPSTMNDS", "INOXINDIA", "JYOTHYLAB", "KIRLOSBROS",
            "KSB", "MAHSCOOTER", "MAHSEAMLES", "METROPOLIS", "PRAJIND", "PGHH",
            "RCF", "RELINFRA", "SUNDRMFAST", "TRIVENI", "DBREALTY", "MANYAVAR",
            "VENTIVE", "VGUARD",
        ),
        "included": (
            "ACUTAAS", "CPPLUS", "ABDL", "ANTHEM", "ANURAS", "BELRISE", "GROWW",
            "CANHLIFE", "CARTRADE", "CEMPRO", "EMMVEE", "GABRIEL", "GALLANTT",
            "HDBFS", "ICICIAMC", "JAINREC", "LENSKART", "LGEINDIA", "MEESHO",
            "PARADEEP", "PWL", "PINELABS", "PIRAMALFIN", "SPLPETRO", "TATACAP",
            "TMCV", "TEGA", "TENNIND", "TRAVELFOOD", "URBANCO", "ZYDUSWELL",
        ),
    },
    {
        "effective_date": "2026-05-12",
        "published_date": "2026-05-04",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs04052026.pdf",
        "excluded": ("GSPL",),
        "included": ("CIEINDIA",),
    },
    {
        "effective_date": "2026-07-17",
        "published_date": "2026-07-13",
        "source_url": "https://niftyindices.com/Press_Release/ind_prs13072026_1.pdf",
        "excluded": ("JBCHEPHARM",),
        "included": ("PFOCUS",),
    },
    {
        "effective_date": "2026-09-30",
        "published_date": "2026-08-10",
        "source_url": "https://www.niftyindices.com/Press_Release/ind_prs10082026.pdf",
        "excluded": (
            "3MINDIA", "ABFRL", "ABLBL", "BAYERCROP", "BIKAJI", "BLUEDART",
            "BLUEJET", "BBTC", "MAPMYINDIA", "CHALET", "DCMSHRIRAM", "ERIS",
            "GODIGIT", "INDGN", "JSWDULUX", "JUBLPHARMA", "LATENTVIEW", "THELEELA",
            "NEWGEN", "NIVABUPA", "PFIZER", "SAPPHIRE", "SAREGAMA", "SBFC",
            "SONATSOFTW", "SPLPETRO", "TRAVELFOOD",
        ),
        "included": (
            "AETHER", "AVANTIFEED", "AZAD", "BAGMANE", "BHARATCOAL", "BBOX",
            "BIRET", "CMPDI", "CLEANMAX", "CUPID", "EMBASSY", "FRACTAL",
            "INOXINDIA", "KIRLOSBROS", "KSB", "MTARTECH", "PRIVISCL", "RUBICON",
            "SANSERA", "SHRIPISTON", "STLTECH", "TDPOWERSYS", "THANGAMAYL",
            "VAML", "VISL", "VOGL", "VEDPOWER",
        ),
    },
)

NIFTY500_NOTICE_REVIEW_WINDOWS = (
    {
        "reviewed_from": "2025-07-15",
        "reviewed_through": "2026-11-12",
        "catalog_url": "https://niftyindices.com/press-release?date=2025",
        "methodology_url": "https://niftyindices.com/Methodology/Method_NIFTY_Equity_Indices.pdf",
        "review_status": "complete_for_nifty500_membership_changes",
        "notes": (
            "Reviewed the NSE Indices press-release catalog for scheduled and ad-hoc "
            "NIFTY 500 changes caused by screening, suspension, delisting, schemes, "
            "amalgamation and demerger treatment through the anchor date."
        ),
    },
)

# A classification snapshot is never silently backdated. This bridge permits
# the first later official constituent snapshot to classify an explicitly
# reviewed interval only when the dated notice ledger proves that membership
# remained stable throughout that interval. Provenance continues to disclose
# the later confirmation date.
NIFTY500_STABLE_CLASSIFICATION_INTERVALS = (
    {
        "valid_from": "2025-07-18",
        "valid_through": "2025-08-14",
        "confirmation_snapshot_date": "2025-08-15",
        "event_source_url": "https://www.niftyindices.com/Press_Release/ind_prs15072025.pdf",
        "source_status": "later_official_snapshot_confirmed_stable_interval",
    },
)

# Corporate-action constituents can temporarily take an index above its stated
# stock count.  They are modelled separately from normal reconstitution events
# because a zero-price dummy is not an investable security and later changes
# identity when the resulting company lists.
NIFTY500_LINEAGE_INTERVALS = (
    {
        "parent_symbol": "HEG",
        "entity_name": "HEG Graphite Ltd.",
        "dummy_symbol": "DUMMYHEG",
        "listed_symbol": "HEGGRAPH",
        "effective_date": "2026-09-07",
        "listing_date": "9999-12-31",
        "exclusion_date": "9999-12-31",
        "inclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs03092026.pdf",
        "exclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs03092026.pdf",
    },
    {
        "parent_symbol": "DBREALTY",
        "entity_name": "Advent Hotels International Ltd.",
        "dummy_symbol": "DUMMYDBRLT",
        "listed_symbol": "ADVENTHTL",
        "effective_date": "2025-07-18",
        "listing_date": "2025-11-13",
        "exclusion_date": "2025-11-28",
        "inclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs15072025.pdf",
        "exclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs26112025.pdf",
    },
    {
        "parent_symbol": "SKFINDIA",
        "entity_name": "SKF India (Industrial) Ltd.",
        "dummy_symbol": "DUMMYSKFIN",
        "listed_symbol": "SKFINDUS",
        "effective_date": "2025-10-15",
        "listing_date": "2025-12-05",
        "exclusion_date": "2025-12-15",
        "inclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs10102025.pdf",
        "exclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs11122025_1.pdf",
    },
    {
        "parent_symbol": "HINDUNILVR",
        "entity_name": "Kwality Wall's (India) Ltd.",
        "dummy_symbol": "DUMMYHDLVR",
        "listed_symbol": "KWIL",
        "effective_date": "2025-12-05",
        "listing_date": "2026-02-16",
        "exclusion_date": "2026-02-24",
        "inclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs28112025.pdf",
        "exclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs20022026_1.pdf",
    },
    {
        "parent_symbol": "VEDL",
        "entity_name": "Vedanta Aluminium Metal Ltd.",
        "dummy_symbol": "DUMMYVEDL1",
        "listed_symbol": "VAML",
        "effective_date": "2026-04-30",
        "listing_date": "2026-06-15",
        "exclusion_date": "2026-06-23",
        "inclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs23042026.pdf",
        "exclusion_source_url": "https://niftyindices.com/Press_Release/ind_prs19062026.pdf",
    },
    {
        "parent_symbol": "VEDL",
        "entity_name": "Vedanta Power Ltd.",
        "dummy_symbol": "DUMMYVEDL2",
        "listed_symbol": "VEDPOWER",
        "effective_date": "2026-04-30",
        "listing_date": "2026-06-15",
        "exclusion_date": "2026-06-19",
        "inclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs23042026.pdf",
        "exclusion_source_url": "https://niftyindices.com/Press_Release/ind_prs17062026.pdf",
    },
    {
        "parent_symbol": "VEDL",
        "entity_name": "Vedanta Oil and Gas Ltd.",
        "dummy_symbol": "DUMMYVEDL3",
        "listed_symbol": "VOGL",
        "effective_date": "2026-04-30",
        "listing_date": "2026-06-15",
        "exclusion_date": "2026-06-24",
        "inclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs23042026.pdf",
        "exclusion_source_url": "https://niftyindices.com/Press_Release/ind_prs22062026.pdf",
    },
    {
        "parent_symbol": "VEDL",
        "entity_name": "Vedanta Iron and Steel Ltd.",
        "dummy_symbol": "DUMMYVEDL4",
        "listed_symbol": "VISL",
        "effective_date": "2026-04-30",
        "listing_date": "2026-06-15",
        "exclusion_date": "2026-06-19",
        "inclusion_source_url": "https://www.niftyindices.com/Press_Release/ind_prs23042026.pdf",
        "exclusion_source_url": "https://niftyindices.com/Press_Release/ind_prs17062026.pdf",
    },
)

# Reviewed official-calendar snapshot owned by Workspace 2.0.  Dates are kept
# source-linked and descriptive; they are not scraped from news feeds.
WORKSPACE_MACRO_EVENTS = (
    {"key": "us_cpi_2026_11", "title": "U.S. CPI release — October 2026", "region": "United States", "category": "inflation", "start_at": "2026-11-10T08:30:00-05:00", "source_label": "U.S. BLS CPI schedule", "source_url": "https://www.bls.gov/schedule/news_release/cpi.htm"},
    {"key": "india_cpi_2026_11", "title": "India CPI release", "region": "India", "category": "inflation", "start_at": "2026-11-12", "source_label": "MoSPI advance release calendar", "source_url": "https://www.mospi.gov.in/"},
    {"key": "fomc_minutes_2026_11", "title": "Federal Reserve FOMC minutes — October meeting", "region": "United States", "category": "central_bank", "start_at": "2026-11-18T14:00:00-05:00", "source_label": "Federal Reserve FOMC calendar", "source_url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"},
    {"key": "india_iip_2026_11", "title": "India IIP release", "region": "India", "category": "growth", "start_at": "2026-11-28", "source_label": "MoSPI advance release calendar", "source_url": "https://www.mospi.gov.in/"},
    {"key": "india_gdp_2026_q2", "title": "India GDP release — Q2 FY 2026-27", "region": "India", "category": "growth", "start_at": "2026-11-30", "source_label": "MoSPI advance release calendar", "source_url": "https://www.mospi.gov.in/"},
    {"key": "rbi_mpc_2026_12", "title": "RBI Monetary Policy Committee meeting", "region": "India", "category": "central_bank", "start_at": "2026-12-02", "end_at": "2026-12-04", "source_label": "RBI MPC meeting schedule", "source_url": "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx?prid=62422"},
    {"key": "fomc_2026_12", "title": "Federal Reserve FOMC meeting", "region": "United States", "category": "central_bank", "start_at": "2026-12-08", "end_at": "2026-12-09", "source_label": "Federal Reserve FOMC calendar", "source_url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"},
    {"key": "us_cpi_2026_12", "title": "U.S. CPI release — November 2026", "region": "United States", "category": "inflation", "start_at": "2026-12-10T08:30:00-05:00", "source_label": "U.S. BLS CPI schedule", "source_url": "https://www.bls.gov/schedule/news_release/cpi.htm"},
    {"key": "india_cpi_2026_12", "title": "India CPI release", "region": "India", "category": "inflation", "start_at": "2026-12-12", "source_label": "MoSPI advance release calendar", "source_url": "https://www.mospi.gov.in/"},
)


def _upcoming_macro_events(as_of: date) -> list[dict[str, Any]]:
    rows = []
    for source in WORKSPACE_MACRO_EVENTS:
        start_date = date.fromisoformat(str(source["start_at"])[:10])
        end_date = date.fromisoformat(str(source.get("end_at") or source["start_at"])[:10])
        if end_date < as_of:
            continue
        row = dict(source)
        row["days_until"] = max(0, (start_date - as_of).days)
        row["ongoing"] = start_date < as_of <= end_date
        row["source_status"] = "reviewed_official_snapshot"
        rows.append(row)
    return sorted(rows, key=lambda row: (str(row["start_at"]), str(row["title"])))


def _connect(path: Path) -> sqlite3.Connection:
    return connect_database(path)


def _ensure_workspace_query_indexes(connection: sqlite3.Connection) -> None:
    """Install indexes justified by repeated point-in-time research lookups."""
    tables = {
        str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    definitions = (
        (
            "instruments", "instruments_kind_id_idx",
            "CREATE INDEX IF NOT EXISTS instruments_kind_id_idx "
            "ON instruments(kind, instrument_id)",
        ),
        (
            "universe_classification_snapshots",
            "universe_classification_symbol_date_idx",
            "CREATE INDEX IF NOT EXISTS universe_classification_symbol_date_idx "
            "ON universe_classification_snapshots(index_name, symbol, snapshot_date DESC)",
        ),
        (
            "universe_membership_events", "universe_membership_symbol_date_idx",
            "CREATE INDEX IF NOT EXISTS universe_membership_symbol_date_idx "
            "ON universe_membership_events(index_name, symbol, effective_date, action)",
        ),
        (
            "symbol_history", "symbol_history_instrument_date_idx",
            "CREATE INDEX IF NOT EXISTS symbol_history_instrument_date_idx "
            "ON symbol_history(instrument_id, effective_date)",
        ),
        (
            "symbol_history", "symbol_history_effective_date_idx",
            "CREATE INDEX IF NOT EXISTS symbol_history_effective_date_idx "
            "ON symbol_history(effective_date)",
        ),
    )
    for table, _name, statement in definitions:
        if table in tables:
            connection.execute(statement)


def _pct(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return (current / previous - 1.0) * 100.0


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None or not math.isfinite(value) else round(value, digits)


def _latest_snapshot(connection: sqlite3.Connection, key: str) -> dict[str, Any] | None:
    row = connection.execute(
        """SELECT payload_json FROM workspace_snapshots
           WHERE snapshot_key = ? ORDER BY as_of_date DESC, created_at DESC LIMIT 1""",
        (key,),
    ).fetchone()
    return None if row is None else json.loads(row[0])


class KiteSession:
    """Memory-only Kite credentials and direct EOD collection."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.api_key = ""
        self.access_token = ""
        self.last_error = ""

    @property
    def connected(self) -> bool:
        with self._lock:
            return bool(self.api_key and self.access_token)

    def connect(self, api_key: str, api_secret: str, request_token: str) -> dict[str, Any]:
        request_token = self._extract_request_token(request_token)
        if not all((api_key, api_secret, request_token)):
            raise ValueError("missing_kite_credentials")
        checksum = hashlib.sha256(f"{api_key}{request_token}{api_secret}".encode()).hexdigest()
        response = requests.post(
            f"{KITE_API}/session/token",
            data={"api_key": api_key, "request_token": request_token, "checksum": checksum},
            headers={"X-Kite-Version": KITE_VERSION, "User-Agent": "Workspace-2.0/2.0"},
            timeout=30,
        )
        payload = response.json()
        if not response.ok or payload.get("status") != "success":
            reason = payload.get("message") or f"kite_http_{response.status_code}"
            with self._lock:
                self.last_error = reason
            raise ValueError(reason)
        with self._lock:
            self.api_key = api_key
            self.access_token = payload["data"]["access_token"]
            self.last_error = ""
        return {"ok": True, "connected": True, "user_name": payload["data"].get("user_name")}

    @staticmethod
    def _extract_request_token(value: str) -> str:
        """Accept either Kite's raw request token or its complete redirect URL."""
        candidate = (value or "").strip()
        if not candidate:
            return ""
        if "request_token=" not in candidate:
            return candidate
        parsed = urlsplit(candidate)
        query = parsed.query if parsed.query else candidate.lstrip("?")
        return (parse_qs(query).get("request_token") or [""])[0].strip()

    def disconnect(self) -> None:
        with self._lock:
            self.api_key = ""
            self.access_token = ""
            self.last_error = ""

    def _headers(self) -> dict[str, str]:
        with self._lock:
            if not self.connected:
                raise ValueError("kite_not_connected")
            return {
                "Authorization": f"token {self.api_key}:{self.access_token}",
                "X-Kite-Version": KITE_VERSION,
                "User-Agent": "Workspace-2.0/2.0",
            }

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        response = requests.get(f"{KITE_API}{path}", params=params, headers=self._headers(), timeout=60)
        payload = response.json()
        if not response.ok or payload.get("status") != "success":
            raise ValueError(payload.get("message") or f"kite_http_{response.status_code}")
        return payload.get("data")

    def instruments(self, exchange: str) -> list[dict[str, str]]:
        response = requests.get(f"{KITE_API}/instruments/{exchange}", headers=self._headers(), timeout=90)
        if not response.ok:
            raise ValueError(f"kite_instruments_http_{response.status_code}")
        return list(csv.DictReader(io.StringIO(response.text)))

    def candles(self, token: str, start: str, end: str, oi: bool = False) -> list[list[Any]]:
        return self.get_json(
            f"/instruments/historical/{token}/day",
            {"from": start, "to": end, "continuous": 0, "oi": 1 if oi else 0},
        )["candles"]


KITE = KiteSession()


def ensure_workspace_schema(path: Path = DEFAULT_MARKET_DB) -> None:
    connection = _connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS workspace_source_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_key TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                status TEXT NOT NULL,
                records_written INTEGER NOT NULL DEFAULT 0,
                detail TEXT
            );
            CREATE TABLE IF NOT EXISTS workspace_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS global_risk_observations (
                observation_date TEXT NOT NULL,
                metric_key TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                source_series TEXT NOT NULL,
                source TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 2,
                PRIMARY KEY(observation_date, metric_key)
            );
            CREATE INDEX IF NOT EXISTS global_risk_metric_date_idx
                ON global_risk_observations(metric_key, observation_date);
            CREATE TABLE IF NOT EXISTS corporate_actions (
                event_key TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                action_type TEXT NOT NULL,
                purpose TEXT NOT NULL,
                ex_date TEXT NOT NULL,
                record_date TEXT,
                face_value REAL,
                adjustment_requirement TEXT NOT NULL,
                review_status TEXT NOT NULL,
                source_url TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS corporate_actions_instrument_date_idx
                ON corporate_actions(instrument_id, ex_date);
            CREATE TABLE IF NOT EXISTS symbol_history (
                lineage_key TEXT PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                old_symbol TEXT NOT NULL,
                new_symbol TEXT NOT NULL,
                effective_date TEXT NOT NULL,
                review_status TEXT NOT NULL,
                source_url TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS universe_membership_events (
                index_name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL CHECK(action IN ('included', 'excluded')),
                effective_date TEXT NOT NULL,
                published_date TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_status TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(index_name, symbol, action, effective_date)
            );
            CREATE INDEX IF NOT EXISTS universe_membership_events_date_idx
                ON universe_membership_events(index_name, effective_date);
            CREATE TABLE IF NOT EXISTS universe_notice_review_windows (
                index_name TEXT NOT NULL,
                reviewed_from TEXT NOT NULL,
                reviewed_through TEXT NOT NULL,
                catalog_url TEXT NOT NULL,
                methodology_url TEXT NOT NULL,
                review_status TEXT NOT NULL,
                notes TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(index_name, reviewed_from, reviewed_through)
            );
            CREATE TABLE IF NOT EXISTS universe_classification_snapshots (
                index_name TEXT NOT NULL,
                snapshot_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                sector TEXT,
                size_bucket TEXT,
                classification_source_url TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                source_status TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(index_name, snapshot_date, symbol)
            );
            CREATE TABLE IF NOT EXISTS universe_lineage_intervals (
                index_name TEXT NOT NULL,
                parent_symbol TEXT NOT NULL,
                entity_name TEXT NOT NULL,
                dummy_symbol TEXT NOT NULL,
                listed_symbol TEXT NOT NULL,
                effective_date TEXT NOT NULL,
                listing_date TEXT NOT NULL,
                exclusion_date TEXT NOT NULL,
                inclusion_source_url TEXT NOT NULL,
                exclusion_source_url TEXT NOT NULL,
                review_status TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(index_name, dummy_symbol, effective_date)
            );
            CREATE TABLE IF NOT EXISTS cash_archive_symbol_checks (
                session_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                present INTEGER NOT NULL CHECK(present IN (0, 1)),
                source_url TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(session_date, symbol)
            );
            CREATE INDEX IF NOT EXISTS cash_archive_symbol_checks_symbol_idx
                ON cash_archive_symbol_checks(symbol, session_date);
            CREATE TABLE IF NOT EXISTS futures_contracts (
                contract_key TEXT PRIMARY KEY,
                underlying TEXT NOT NULL,
                tradingsymbol TEXT NOT NULL,
                exchange TEXT NOT NULL CHECK(exchange = 'NFO'),
                provider_token TEXT NOT NULL,
                expiry_date TEXT NOT NULL,
                lot_size INTEGER NOT NULL,
                source TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS futures_contract_underlying_idx
                ON futures_contracts(underlying, expiry_date);
            CREATE TABLE IF NOT EXISTS futures_eod_snapshots (
                contract_key TEXT NOT NULL,
                session_date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                open_interest INTEGER NOT NULL,
                source TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                PRIMARY KEY(contract_key, session_date),
                FOREIGN KEY(contract_key) REFERENCES futures_contracts(contract_key)
            );
            CREATE INDEX IF NOT EXISTS futures_eod_date_idx
                ON futures_eod_snapshots(session_date);
            CREATE TABLE IF NOT EXISTS fno_archive_files (
                session_date TEXT PRIMARY KEY,
                format TEXT NOT NULL CHECK(format IN ('legacy', 'udiff')),
                source_url TEXT NOT NULL,
                cache_path TEXT,
                sha256 TEXT,
                status TEXT NOT NULL CHECK(status IN ('imported', 'unavailable', 'failed')),
                records_written INTEGER NOT NULL DEFAULT 0,
                detail TEXT,
                retrieved_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS fno_archive_status_date_idx
                ON fno_archive_files(status, session_date);
            CREATE INDEX IF NOT EXISTS futures_eod_contract_date_idx
                ON futures_eod_snapshots(contract_key, session_date);
            CREATE TABLE IF NOT EXISTS opportunity_feature_ledger (
                contract_version TEXT NOT NULL,
                session_date TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                membership_status TEXT NOT NULL,
                ranking_admissible INTEGER NOT NULL CHECK(ranking_admissible IN (0, 1)),
                membership_role TEXT,
                nifty500_member INTEGER NOT NULL CHECK(nifty500_member IN (0, 1)),
                sector TEXT,
                sector_source_date TEXT,
                size_bucket TEXT,
                size_source_date TEXT,
                fno_eligible INTEGER CHECK(fno_eligible IN (0, 1)),
                close REAL,
                return_5d_pct REAL,
                return_20d_pct REAL,
                return_60d_pct REAL,
                return_120d_pct REAL,
                distance_sma20_pct REAL,
                distance_sma50_pct REAL,
                distance_sma200_pct REAL,
                sma20_slope_5d_pct REAL,
                sma20_50_spread_pct REAL,
                sma20_50_crossover_age_sessions INTEGER,
                realised_volatility_20d_pct REAL,
                realised_volatility_60d_pct REAL,
                volatility_expansion_ratio_20v60 REAL,
                range_compression_ratio_20v60 REAL,
                breakout_proximity_20d_pct REAL,
                volume_ratio_20d REAL,
                market_return_20d_pct REAL,
                market_return_60d_pct REAL,
                market_return_120d_pct REAL,
                market_distance_sma200_pct REAL,
                market_volatility_20d_pct REAL,
                market_breadth_50dma_pct REAL,
                market_breadth_200dma_pct REAL,
                sector_return_20d_pct REAL,
                sector_return_60d_pct REAL,
                sector_return_120d_pct REAL,
                sector_breadth_50dma_pct REAL,
                sector_rank_20d INTEGER,
                stock_sector_rank_20d INTEGER,
                stock_universe_rank_20d INTEGER,
                fno_state TEXT,
                fno_price_change_pct REAL,
                fno_oi_change_pct REAL,
                fno_contract_key TEXT,
                source_dates_json TEXT NOT NULL,
                missing_features_json TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(contract_version, session_date, instrument_id)
            );
            CREATE INDEX IF NOT EXISTS opportunity_feature_ledger_session_idx
                ON opportunity_feature_ledger(session_date, sector, symbol);
            CREATE INDEX IF NOT EXISTS opportunity_feature_ledger_symbol_idx
                ON opportunity_feature_ledger(symbol, session_date);
            CREATE TABLE IF NOT EXISTS market_regime_ledger (
                contract_version TEXT NOT NULL,
                session_date TEXT NOT NULL,
                classification_status TEXT NOT NULL,
                ranking_admissible INTEGER NOT NULL CHECK(ranking_admissible IN (0, 1)),
                regime_family TEXT,
                trend_direction TEXT,
                trend_strength TEXT,
                range_state TEXT,
                volatility_state TEXT,
                breadth_state TEXT,
                regime_key TEXT,
                market_return_20d_pct REAL,
                market_distance_sma200_pct REAL,
                market_volatility_20d_pct REAL,
                market_breadth_50dma_pct REAL,
                market_breadth_200dma_pct REAL,
                universe_rows INTEGER NOT NULL,
                priced_rows INTEGER NOT NULL,
                breadth_50dma_denominator INTEGER NOT NULL,
                breadth_200dma_denominator INTEGER NOT NULL,
                priced_coverage_ratio REAL,
                breadth_50dma_coverage_ratio REAL,
                breadth_200dma_coverage_ratio REAL,
                source_dates_json TEXT NOT NULL,
                missing_inputs_json TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(contract_version, session_date)
            );
            CREATE INDEX IF NOT EXISTS market_regime_ledger_state_idx
                ON market_regime_ledger(regime_family, session_date);
            CREATE TABLE IF NOT EXISTS sector_trend_ledger (
                contract_version TEXT NOT NULL,
                session_date TEXT NOT NULL,
                sector TEXT NOT NULL,
                classification_status TEXT NOT NULL,
                ranking_admissible INTEGER NOT NULL CHECK(ranking_admissible IN (0, 1)),
                sector_state TEXT,
                member_count INTEGER NOT NULL,
                priced_count INTEGER NOT NULL,
                constituent_coverage_ratio REAL NOT NULL,
                return_20d_coverage_ratio REAL NOT NULL,
                return_60d_coverage_ratio REAL NOT NULL,
                return_120d_coverage_ratio REAL NOT NULL,
                sector_return_20d_pct REAL,
                sector_return_60d_pct REAL,
                sector_return_120d_pct REAL,
                market_return_20d_pct REAL,
                market_return_60d_pct REAL,
                market_return_120d_pct REAL,
                relative_strength_20d_pct REAL,
                relative_strength_60d_pct REAL,
                relative_strength_120d_pct REAL,
                rank_20d INTEGER,
                rank_60d INTEGER,
                rank_120d INTEGER,
                breadth_50dma_pct REAL,
                ma_alignment_pct REAL,
                average_sma20_slope_5d_pct REAL,
                realised_volatility_20d_pct REAL,
                persistence_ratio REAL,
                acceleration_5_sessions_pct REAL,
                volatility_expansion_ratio REAL,
                source_dates_json TEXT NOT NULL,
                missing_inputs_json TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(contract_version, session_date, sector)
            );
            CREATE INDEX IF NOT EXISTS sector_trend_ledger_state_idx
                ON sector_trend_ledger(sector_state, session_date, sector);
            CREATE INDEX IF NOT EXISTS sector_trend_ledger_sector_idx
                ON sector_trend_ledger(sector, session_date);
            CREATE TABLE IF NOT EXISTS stock_leadership_ledger (
                contract_version TEXT NOT NULL,
                session_date TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                sector TEXT,
                classification_status TEXT NOT NULL,
                ranking_admissible INTEGER NOT NULL CHECK(ranking_admissible IN (0, 1)),
                leadership_state TEXT,
                setup_state TEXT,
                sector_state TEXT,
                return_20d_pct REAL,
                return_60d_pct REAL,
                return_120d_pct REAL,
                relative_nifty_20d_pct REAL,
                relative_nifty_60d_pct REAL,
                relative_nifty_120d_pct REAL,
                relative_sector_20d_pct REAL,
                relative_sector_60d_pct REAL,
                relative_sector_120d_pct REAL,
                sector_rank_20d INTEGER,
                stock_sector_rank_20d INTEGER,
                stock_sector_rank_60d INTEGER,
                stock_sector_rank_120d INTEGER,
                stock_universe_rank_20d INTEGER,
                stock_universe_rank_60d INTEGER,
                stock_universe_rank_120d INTEGER,
                relative_strength_persistence_ratio REAL,
                relative_strength_acceleration_5_sessions_pct REAL,
                ma_alignment TEXT,
                sma20_slope_5d_pct REAL,
                sma20_50_spread_pct REAL,
                sma20_50_crossover_age_sessions INTEGER,
                range_compression_ratio_20v60 REAL,
                breakout_proximity_20d_pct REAL,
                volatility_expansion_ratio_20v60 REAL,
                volume_ratio_20d REAL,
                participation_status TEXT,
                fno_eligible INTEGER NOT NULL CHECK(fno_eligible IN (0, 1)),
                fno_state TEXT,
                source_dates_json TEXT NOT NULL,
                missing_inputs_json TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(contract_version, session_date, instrument_id)
            );
            CREATE INDEX IF NOT EXISTS stock_leadership_ledger_state_idx
                ON stock_leadership_ledger(leadership_state, setup_state, session_date);
            CREATE INDEX IF NOT EXISTS stock_leadership_ledger_sector_idx
                ON stock_leadership_ledger(sector, session_date, stock_sector_rank_20d);
            CREATE INDEX IF NOT EXISTS stock_leadership_ledger_symbol_idx
                ON stock_leadership_ledger(symbol, session_date);
            CREATE TABLE IF NOT EXISTS crossover_conditioning_ledger (
                contract_version TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                strategy TEXT NOT NULL,
                dimension TEXT NOT NULL,
                state TEXT NOT NULL,
                state_observations INTEGER NOT NULL,
                completed_trades INTEGER NOT NULL,
                gross_trade_expectancy_pct REAL,
                net_trade_expectancy_pct REAL,
                gross_profit_factor REAL,
                net_profit_factor REAL,
                net_maximum_drawdown_pct REAL,
                average_move_capture_ratio REAL,
                evidence_status TEXT NOT NULL,
                missing_evidence_json TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(contract_version, strategy, dimension, state)
            );
            CREATE INDEX IF NOT EXISTS crossover_conditioning_ledger_dimension_idx
                ON crossover_conditioning_ledger(dimension, state, strategy);
            CREATE TABLE IF NOT EXISTS crossover_ablation_ledger (
                contract_version TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                strategy TEXT NOT NULL,
                stage_order INTEGER NOT NULL,
                stage_key TEXT NOT NULL,
                stage_label TEXT NOT NULL,
                prior_stage_trades INTEGER,
                state_eligible_trades INTEGER NOT NULL,
                unavailable_state_trades INTEGER NOT NULL,
                filter_excluded_trades INTEGER NOT NULL,
                completed_trades INTEGER NOT NULL,
                baseline_retention_ratio REAL NOT NULL,
                gross_trade_expectancy_pct REAL,
                net_trade_expectancy_pct REAL,
                gross_profit_factor REAL,
                net_profit_factor REAL,
                net_maximum_drawdown_pct REAL,
                average_move_capture_ratio REAL,
                delta_net_expectancy_pct REAL,
                delta_net_profit_factor REAL,
                delta_net_drawdown_pct REAL,
                delta_move_capture_ratio REAL,
                sample_status TEXT NOT NULL,
                incremental_status TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(contract_version, strategy, stage_key)
            );
            CREATE INDEX IF NOT EXISTS crossover_ablation_ledger_stage_idx
                ON crossover_ablation_ledger(stage_order, strategy);
            CREATE TABLE IF NOT EXISTS top_down_oos_admission_ledger (
                contract_version TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                strategy TEXT NOT NULL,
                admission_decision TEXT NOT NULL,
                ablation_gate_passed INTEGER NOT NULL CHECK(ablation_gate_passed IN (0, 1)),
                minimum_trade_gate_passed INTEGER NOT NULL CHECK(minimum_trade_gate_passed IN (0, 1)),
                positive_net_expectancy_gate_passed INTEGER NOT NULL CHECK(positive_net_expectancy_gate_passed IN (0, 1)),
                profit_factor_gate_passed INTEGER NOT NULL CHECK(profit_factor_gate_passed IN (0, 1)),
                state_coverage_gate_passed INTEGER NOT NULL CHECK(state_coverage_gate_passed IN (0, 1)),
                final_stage_trades INTEGER NOT NULL,
                final_stage_net_expectancy_pct REAL,
                final_stage_net_profit_factor REAL,
                state_coverage_ratio REAL NOT NULL,
                walk_forward_status TEXT NOT NULL,
                bootstrap_status TEXT NOT NULL,
                within_strategy_fdr_status TEXT NOT NULL,
                cross_strategy_fdr_status TEXT NOT NULL,
                oos_score REAL,
                failure_reasons_json TEXT NOT NULL,
                audit_json TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(contract_version, strategy)
            );
            CREATE TABLE IF NOT EXISTS opportunity_board_release_ledger (
                contract_version TEXT PRIMARY KEY,
                generated_at TEXT NOT NULL,
                release_decision TEXT NOT NULL,
                board_authorised INTEGER NOT NULL CHECK(board_authorised IN (0, 1)),
                ui_mode TEXT NOT NULL,
                admitted_strategies INTEGER NOT NULL,
                rejected_strategies INTEGER NOT NULL,
                headline TEXT NOT NULL,
                explanation TEXT NOT NULL,
                failure_reasons_json TEXT NOT NULL,
                next_action TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1
            );
            """
        )
        feature_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(opportunity_feature_ledger)")
        }
        feature_migrations = {
            "market_return_60d_pct": "REAL",
            "market_return_120d_pct": "REAL",
            "sma20_50_spread_pct": "REAL",
            "sma20_50_crossover_age_sessions": "INTEGER",
            "realised_volatility_60d_pct": "REAL",
            "volatility_expansion_ratio_20v60": "REAL",
            "range_compression_ratio_20v60": "REAL",
            "breakout_proximity_20d_pct": "REAL",
        }
        for column, data_type in feature_migrations.items():
            if column not in feature_columns:
                connection.execute(
                    f"ALTER TABLE opportunity_feature_ledger ADD COLUMN {column} {data_type}"
                )
        # Older Workspace databases inherited a closed metric-name CHECK from
        # the PG-terminal seed. Workspace 2.0 owns this table now, so migrate it
        # to an extensible contract while preserving every stored observation.
        global_risk_schema = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='global_risk_observations'"
        ).fetchone()
        if global_risk_schema and "CHECK" in str(global_risk_schema[0]).upper():
            connection.executescript(
                """
                ALTER TABLE global_risk_observations RENAME TO global_risk_observations_legacy;
                CREATE TABLE global_risk_observations (
                    observation_date TEXT NOT NULL,
                    metric_key TEXT NOT NULL,
                    value REAL NOT NULL,
                    unit TEXT NOT NULL,
                    source_series TEXT NOT NULL,
                    source TEXT NOT NULL,
                    retrieved_at TEXT NOT NULL,
                    schema_version INTEGER NOT NULL DEFAULT 2,
                    PRIMARY KEY(observation_date, metric_key)
                );
                INSERT OR IGNORE INTO global_risk_observations(
                    observation_date,metric_key,value,unit,source_series,source,retrieved_at,schema_version)
                SELECT observation_date,metric_key,value,unit,source_series,source,retrieved_at,schema_version
                FROM global_risk_observations_legacy;
                DROP TABLE global_risk_observations_legacy;
                CREATE INDEX IF NOT EXISTS global_risk_metric_date_idx
                    ON global_risk_observations(metric_key, observation_date);
                """
            )
        retrieved_at = datetime.now(timezone.utc).isoformat()
        for notice in NIFTY500_MEMBERSHIP_NOTICES:
            for action in ("excluded", "included"):
                connection.executemany(
                    """INSERT OR REPLACE INTO universe_membership_events(
                           index_name,symbol,action,effective_date,published_date,source_url,
                           source_status,retrieved_at,schema_version)
                       VALUES ('NIFTY 500',?,?,?,?,?,?,?,1)""",
                    [
                        (
                            symbol, action, notice["effective_date"], notice["published_date"],
                            notice["source_url"], "reviewed_official_notice", retrieved_at,
                        )
                        for symbol in notice[action]
                    ],
                )
        connection.executemany(
            """INSERT OR REPLACE INTO universe_notice_review_windows(
                   index_name,reviewed_from,reviewed_through,catalog_url,methodology_url,
                   review_status,notes,retrieved_at,schema_version)
               VALUES ('NIFTY 500',?,?,?,?,?,?,?,1)""",
            [
                (
                    row["reviewed_from"], row["reviewed_through"], row["catalog_url"],
                    row["methodology_url"], row["review_status"], row["notes"], retrieved_at,
                )
                for row in NIFTY500_NOTICE_REVIEW_WINDOWS
            ],
        )
        connection.executemany(
            """INSERT OR REPLACE INTO universe_lineage_intervals(
                   index_name,parent_symbol,entity_name,dummy_symbol,listed_symbol,
                   effective_date,listing_date,exclusion_date,inclusion_source_url,
                   exclusion_source_url,review_status,retrieved_at,schema_version)
               VALUES ('NIFTY 500',?,?,?,?,?,?,?,?,?,?,?,1)""",
            [
                (
                    row["parent_symbol"], row["entity_name"], row["dummy_symbol"],
                    row["listed_symbol"], row["effective_date"], row["listing_date"],
                    row["exclusion_date"], row["inclusion_source_url"],
                    row["exclusion_source_url"], "reviewed_official_lineage", retrieved_at,
                )
                for row in NIFTY500_LINEAGE_INTERVALS
            ],
        )
        connection.executemany(
            """INSERT OR REPLACE INTO symbol_history(
                   lineage_key,instrument_id,old_symbol,new_symbol,effective_date,
                   review_status,source_url,retrieved_at,schema_version)
               VALUES (?,?,?,?,?,?,?,?,1)""",
            [
                (
                    hashlib.sha256(
                        f"{row['old_symbol']}|{row['new_symbol']}|{row['effective_date']}".encode()
                    ).hexdigest(),
                    f"NSE:{row['new_symbol']}", row["old_symbol"], row["new_symbol"],
                    row["effective_date"], "reviewed_official_symbol_change",
                    row["source_url"], retrieved_at,
                )
                for row in NIFTY500_SYMBOL_CHANGES
            ],
        )
        connection.executemany(
            """INSERT OR REPLACE INTO universe_classification_snapshots(
                   index_name,snapshot_date,symbol,sector,size_bucket,
                   classification_source_url,retrieved_at,source_status,schema_version)
               VALUES ('NIFTY 500',?,?,?,?,?,?,?,1)""",
            [
                (
                    row["effective_date"], row["symbol"], row["sector"], None,
                    (
                        f"{row['event_url']} | "
                        f"https://web.archive.org/web/{row['confirmation_date'].replace('-', '')}000000id_/"
                        "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
                    ),
                    retrieved_at, "official_event_with_subsequent_sector_confirmation",
                )
                for row in NIFTY500_SECTOR_CLASSIFICATION_EVENTS
            ],
        )
        connection.execute(
            """UPDATE instruments SET adjustment_status='provider_reported_adjusted',
               adjustment_source_url=?
               WHERE kind='stock' AND source LIKE 'Kite%' AND adjustment_status='provider_unspecified'""",
            (KITE_ADJUSTMENT_POLICY_URL,),
        )
        connection.execute(
            """INSERT INTO workspace_metadata(key, value, updated_at) VALUES ('engine_owner', 'Workspace 2.0', ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
            (datetime.now(timezone.utc).isoformat(),),
        )
        _ensure_workspace_query_indexes(connection)
        connection.commit()
    finally:
        connection.close()


def _corporate_action_type(purpose: str) -> str:
    text = purpose.lower()
    if "split" in text or "sub-division" in text or "sub division" in text:
        return "split"
    if "bonus" in text:
        return "bonus"
    if "right" in text:
        return "rights"
    if "demerger" in text or "spin-off" in text or "spinoff" in text:
        return "demerger"
    if "merger" in text or "amalgamation" in text:
        return "merger"
    if "special dividend" in text or "extraordinary dividend" in text:
        return "special_dividend"
    if "dividend" in text:
        return "ordinary_dividend"
    if "buy back" in text or "buyback" in text:
        return "buyback"
    if "general meeting" in text or text.endswith(" meeting"):
        return "corporate_meeting"
    if "symbol" in text or "name change" in text:
        return "symbol_change"
    if "delist" in text:
        return "delisting"
    return "other"


def _action_contract(action_type: str) -> tuple[str, str]:
    if action_type in {"split", "bonus", "rights", "demerger", "merger", "special_dividend"}:
        return "price_history_adjustment_required", "provider_reported_adjusted"
    if action_type == "ordinary_dividend":
        return "cash_distribution_not_in_price_return", "no_price_adjustment_expected"
    if action_type == "buyback":
        return "capital_structure_event_no_price_adjustment", "no_price_adjustment_expected"
    if action_type == "corporate_meeting":
        return "governance_event_no_price_adjustment", "no_price_adjustment_expected"
    if action_type == "symbol_change":
        return "symbol_lineage_required", "manual_lineage_review"
    if action_type == "delisting":
        return "terminal_value_and_membership_review_required", "manual_review"
    return "manual_classification_required", "manual_review"


def _nse_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text == "-":
        return None
    for pattern in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, pattern).date().isoformat()
        except ValueError:
            continue
    return None


def refresh_nse_corporate_actions(
    market_db: Path = DEFAULT_MARKET_DB,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    """Store NSE corporate-action observations without rewriting price history."""
    end_date = end_date or date.today()
    start_date = start_date or (end_date - timedelta(days=366))
    if start_date > end_date or (end_date - start_date).days > 370:
        raise ValueError("corporate_action_range_must_not_exceed_370_days")
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/130 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": NSE_CORPORATE_ACTIONS_URL,
    }
    session.get("https://www.nseindia.com/", headers=headers, timeout=30)
    response = session.get(
        NSE_CORPORATE_ACTIONS_API,
        params={"index": "equities", "from_date": start_date.strftime("%d-%m-%Y"), "to_date": end_date.strftime("%d-%m-%Y")},
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    records = payload if isinstance(payload, list) else payload.get("data", [])
    retrieved_at = datetime.now(timezone.utc).isoformat()
    connection = _connect(market_db)
    written = ignored = 0
    try:
        instruments = {row[0] for row in connection.execute("SELECT instrument_id FROM instruments WHERE kind='stock'")}
        for raw in records:
            symbol = str(raw.get("symbol") or raw.get("SYMBOL") or "").strip().upper()
            purpose = str(raw.get("subject") or raw.get("purpose") or raw.get("PURPOSE") or "").strip()
            ex_date = _nse_date(raw.get("exDate") or raw.get("ex_date") or raw.get("EX-DATE"))
            instrument_id = f"stock:{symbol}"
            if not symbol or not purpose or not ex_date or instrument_id not in instruments:
                ignored += 1
                continue
            action_type = _corporate_action_type(purpose)
            requirement, review_status = _action_contract(action_type)
            event_key = hashlib.sha256(f"{symbol}|{ex_date}|{purpose}".encode()).hexdigest()
            face_value_raw = raw.get("faceVal") or raw.get("face_value") or raw.get("FACE VALUE")
            try:
                face_value = float(face_value_raw) if face_value_raw not in (None, "", "-") else None
            except (TypeError, ValueError):
                face_value = None
            connection.execute(
                """INSERT OR REPLACE INTO corporate_actions
                   (event_key,instrument_id,symbol,action_type,purpose,ex_date,record_date,face_value,
                    adjustment_requirement,review_status,source_url,retrieved_at,raw_json,schema_version)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
                (event_key, instrument_id, symbol, action_type, purpose, ex_date,
                 _nse_date(raw.get("recordDate") or raw.get("record_date") or raw.get("RECORD DATE")),
                 face_value, requirement, review_status, NSE_CORPORATE_ACTIONS_URL, retrieved_at,
                 json.dumps(raw, sort_keys=True, default=str)),
            )
            written += 1
        existing_coverage = connection.execute(
            "SELECT value FROM workspace_metadata WHERE key='corporate_action_coverage'"
        ).fetchone()
        coverage_from, coverage_through = start_date, end_date
        if existing_coverage:
            try:
                stored = json.loads(existing_coverage["value"])
                coverage_from = min(coverage_from, date.fromisoformat(stored["from"]))
                coverage_through = max(coverage_through, date.fromisoformat(stored["through"]))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                pass
        connection.execute(
            """INSERT INTO workspace_metadata(key,value,updated_at) VALUES ('corporate_action_coverage',?,?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
            (json.dumps({"from": coverage_from.isoformat(), "through": coverage_through.isoformat(), "source": NSE_CORPORATE_ACTIONS_URL}), retrieved_at),
        )
        connection.commit()
    finally:
        connection.close()
    return {"ok": True, "records_received": len(records), "records_written": written, "records_ignored": ignored,
            "from_date": start_date.isoformat(), "through_date": end_date.isoformat(), "source": NSE_CORPORATE_ACTIONS_URL}


def corporate_action_contract(market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None) -> dict[str, Any]:
    connection = _connect(market_db)
    try:
        parameters: tuple[Any, ...] = ()
        where = ""
        if symbol:
            where = " WHERE UPPER(symbol)=?"
            parameters = (symbol.strip().upper(),)
        events = [dict(row) for row in connection.execute(
            f"""SELECT symbol,action_type,purpose,ex_date,record_date,face_value,
                       adjustment_requirement,review_status,source_url,retrieved_at
                FROM corporate_actions{where} ORDER BY ex_date DESC,symbol""", parameters
        )]
        coverage_row = connection.execute("SELECT value,updated_at FROM workspace_metadata WHERE key='corporate_action_coverage'").fetchone()
        status_rows = connection.execute(
            "SELECT adjustment_status,COUNT(*) AS instruments FROM instruments WHERE kind='stock' GROUP BY adjustment_status"
        ).fetchall()
    finally:
        connection.close()
    return {
        "ok": True,
        "contract_version": "corporate-action-audit-v1",
        "provider_policy": {
            "price_adjusted_actions": ["split", "bonus", "rights", "demerger", "merger", "special_dividend"],
            "ordinary_dividends": "cash distribution excluded from price-return backtests",
            "source_url": KITE_ADJUSTMENT_POLICY_URL,
        },
        "event_source": NSE_CORPORATE_ACTIONS_URL,
        "coverage": json.loads(coverage_row["value"]) if coverage_row else None,
        "coverage_updated_at": coverage_row["updated_at"] if coverage_row else None,
        "instrument_adjustment_status": [dict(row) for row in status_rows],
        "events": events,
    }


def sync_universe_classification_snapshots(
    market_db: Path = DEFAULT_MARKET_DB,
    snapshot_db: Path = DEFAULT_SNAPSHOT_DB,
) -> dict[str, Any]:
    """Persist dated official sector and current size observations locally."""
    ensure_workspace_schema(market_db)
    market = _connect(market_db)
    snapshots = _connect(snapshot_db)
    imported_files = 0
    imported_rows = 0
    try:
        now = datetime.now(timezone.utc).isoformat()
        for archive_path in sorted(NIFTY500_CONSTITUENT_ARCHIVE_DIR.glob("nifty500_*.csv")):
            match = re.search(r"(\d{4}-\d{2}-\d{2})", archive_path.name)
            if not match:
                continue
            snapshot_date = match.group(1)
            source_url = (
                f"https://web.archive.org/web/{snapshot_date.replace('-', '')}000000id_/"
                "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
            )
            with archive_path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            valid_rows = 0
            for row in rows:
                symbol = str(row.get("Symbol") or "").strip().upper()
                sector = str(row.get("Industry") or "").strip() or None
                if not symbol:
                    continue
                market.execute(
                    """INSERT OR REPLACE INTO universe_classification_snapshots(
                           index_name,snapshot_date,symbol,sector,size_bucket,
                           classification_source_url,retrieved_at,source_status,schema_version
                       ) VALUES ('NIFTY 500',?,?,?,?,?,?,?,1)""",
                    (snapshot_date, symbol, sector, None, source_url, now, "archived_official_constituent_csv"),
                )
                valid_rows += 1
            if valid_rows:
                imported_files += 1
                imported_rows += valid_rows

        snapshot_tables = {row[0] for row in snapshots.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        snapshot_dates = [row[0] for row in snapshots.execute(
            "SELECT DISTINCT snapshot_date FROM index_membership_snapshots WHERE index_name='NIFTY 500'"
        )] if "index_membership_snapshots" in snapshot_tables else []
        for snapshot_date in snapshot_dates:
            membership_rows = snapshots.execute(
                """SELECT symbol,industry,source_url,retrieved_at
                   FROM index_membership_snapshots
                   WHERE index_name='NIFTY 500' AND snapshot_date=?""",
                (snapshot_date,),
            ).fetchall()
            size_rows = snapshots.execute(
                """SELECT symbol,size_bucket,source_url
                   FROM equity_size_classifications WHERE snapshot_date=?""",
                (snapshot_date,),
            ).fetchall() if "equity_size_classifications" in snapshot_tables else []
            sizes = {row["symbol"]: (row["size_bucket"], row["source_url"]) for row in size_rows}
            for row in membership_rows:
                size_bucket, size_url = sizes.get(row["symbol"], (None, None))
                source_url = row["source_url"] if not size_url else f"{row['source_url']} | {size_url}"
                market.execute(
                    """INSERT OR REPLACE INTO universe_classification_snapshots(
                           index_name,snapshot_date,symbol,sector,size_bucket,
                           classification_source_url,retrieved_at,source_status,schema_version
                       ) VALUES ('NIFTY 500',?,?,?,?,?,?,?,1)""",
                    (
                        snapshot_date, row["symbol"], row["industry"], size_bucket,
                        source_url, row["retrieved_at"], "validated_official_current_snapshot",
                    ),
                )
        market.commit()
        stored = market.execute(
            """SELECT COUNT(*) AS rows,COUNT(DISTINCT snapshot_date) AS dates,
                      MIN(snapshot_date) AS first_date,MAX(snapshot_date) AS last_date
               FROM universe_classification_snapshots WHERE index_name='NIFTY 500'"""
        ).fetchone()
        return {
            "ok": True,
            "contract_version": "universe-classification-snapshot-sync-v1",
            "archived_files_imported": imported_files,
            "archived_rows_imported": imported_rows,
            "stored": dict(stored),
        }
    finally:
        snapshots.close()
        market.close()


def _historical_size_map(snapshot_db: Path, requested_date: date) -> tuple[dict[str, str], dict[str, Any]]:
    snapshots = _connect(snapshot_db)
    try:
        if not snapshots.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='equity_size_classifications'"
        ).fetchone():
            return {}, {"status": "missing_size_anchor", "source_date": None, "notices_reversed": []}
        anchor = snapshots.execute("SELECT MAX(snapshot_date) FROM equity_size_classifications").fetchone()[0]
        if not anchor:
            return {}, {"status": "missing_size_anchor", "source_date": None, "notices_reversed": []}
        mapping = {
            row["symbol"]: row["size_bucket"]
            for row in snapshots.execute(
                "SELECT symbol,size_bucket FROM equity_size_classifications WHERE snapshot_date=?",
                (anchor,),
            )
        }
    finally:
        snapshots.close()

    reversed_notices = []
    for notice in sorted(NIFTY_SIZE_RECONSTITUTION_NOTICES, key=lambda row: row["effective_date"], reverse=True):
        if date.fromisoformat(notice["effective_date"]) <= requested_date:
            continue
        for changes in notice["buckets"].values():
            for symbol in changes["included"]:
                mapping.pop(symbol, None)
        for bucket, changes in notice["buckets"].items():
            for symbol in changes["excluded"]:
                mapping[symbol] = bucket
        reversed_notices.append({"effective_date": notice["effective_date"], "source_url": notice["source_url"]})
    for notice in sorted(NIFTY_SIZE_AD_HOC_REPLACEMENTS, key=lambda row: row["effective_date"], reverse=True):
        if date.fromisoformat(notice["effective_date"]) <= requested_date:
            continue
        for symbol in notice["included"]:
            mapping.pop(symbol, None)
        for symbol in notice["excluded"]:
            mapping[symbol] = notice["bucket"]
        reversed_notices.append({
            "effective_date": notice["effective_date"],
            "source_url": "official_nifty500_ad_hoc_replacement_ledger",
        })
    for change in sorted(NIFTY500_SYMBOL_CHANGES, key=lambda row: row["effective_date"], reverse=True):
        if date.fromisoformat(change["effective_date"]) <= requested_date:
            continue
        bucket = mapping.pop(change["new_symbol"], None)
        if bucket:
            mapping[change["old_symbol"]] = bucket
    return mapping, {
        "status": "official_reconstitution_reconstruction",
        "source_date": anchor,
        "notices_reversed": reversed_notices,
    }


def _classification_for_members(
    market: sqlite3.Connection,
    snapshot_db: Path,
    members: dict[str, dict[str, Any]],
    requested_date: date,
) -> dict[str, Any]:
    size_map, size_provenance = _historical_size_map(snapshot_db, requested_date)
    missing_sector: list[str] = []
    missing_size: list[str] = []
    sector_sources: set[str] = set()
    retrospective_bridge = next(
        (
            row for row in NIFTY500_STABLE_CLASSIFICATION_INTERVALS
            if date.fromisoformat(row["valid_from"]) <= requested_date
            <= date.fromisoformat(row["valid_through"])
        ),
        None,
    )
    bridged_symbols: list[str] = []
    for symbol, member in members.items():
        sector_row = market.execute(
            """SELECT snapshot_date,sector,classification_source_url,source_status
               FROM universe_classification_snapshots
               WHERE index_name='NIFTY 500' AND symbol=? AND snapshot_date<=?
                     AND sector IS NOT NULL AND TRIM(sector)<>''
               ORDER BY snapshot_date DESC LIMIT 1""",
            (symbol, requested_date.isoformat()),
        ).fetchone()
        if not sector_row and retrospective_bridge:
            sector_row = market.execute(
                """SELECT snapshot_date,sector,classification_source_url,source_status
                   FROM universe_classification_snapshots
                   WHERE index_name='NIFTY 500' AND symbol=? AND snapshot_date=?
                         AND sector IS NOT NULL AND TRIM(sector)<>''
                   LIMIT 1""",
                (symbol, retrospective_bridge["confirmation_snapshot_date"]),
            ).fetchone()
            if sector_row:
                bridged_symbols.append(symbol)
        member["industry"] = sector_row["sector"] if sector_row else member.get("industry")
        member["sector_source_date"] = sector_row["snapshot_date"] if sector_row else None
        member["size_bucket"] = size_map.get(symbol) or NIFTY500_LINEAGE_SIZE_BUCKETS.get(symbol)
        if sector_row:
            sector_sources.add(sector_row["classification_source_url"])
        if member.get("is_investable", True):
            if not member.get("industry"):
                missing_sector.append(symbol)
            if not member.get("size_bucket"):
                missing_size.append(symbol)
    complete = not missing_sector and not missing_size
    return {
        "complete": complete,
        "classified_members": len(members) - len(set(missing_sector) | set(missing_size)),
        "member_count": len(members),
        "missing_sector_count": len(missing_sector),
        "missing_size_count": len(missing_size),
        "missing_sector_symbols": sorted(missing_sector),
        "missing_size_symbols": sorted(missing_size),
        "sector_sources": sorted(sector_sources),
        "retrospective_stable_interval": (
            {
                **retrospective_bridge,
                "applied_symbols": len(bridged_symbols),
                "provenance_note": (
                    "Sector labels come from the later official snapshot; the snapshot date "
                    "is preserved and was applied backward only across this reviewed stable interval."
                ),
            }
            if retrospective_bridge else None
        ),
        "size_provenance": size_provenance,
    }


def point_in_time_universe_contract(
    market_db: Path = DEFAULT_MARKET_DB,
    snapshot_db: Path = DEFAULT_SNAPSHOT_DB,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    """Return an evidence-labelled NIFTY 500 membership reconstruction.

    The latest official constituent snapshot is the anchor. Reviewed official
    replacement notices are reversed to reconstruct earlier membership. Until
    every ad-hoc notice is reviewed, reconstructed dates remain explicitly
    partial and are not allowed to remove the survivorship-bias warning.
    """
    ensure_workspace_schema(market_db)
    classification_sync = sync_universe_classification_snapshots(market_db, snapshot_db)
    snapshots = _connect(snapshot_db)
    market = _connect(market_db)
    try:
        anchor_date = snapshots.execute(
            "SELECT MAX(snapshot_date) FROM index_membership_snapshots WHERE index_name='NIFTY 500'"
        ).fetchone()[0]
        if not anchor_date:
            return {"ok": False, "reason": "nifty500_anchor_snapshot_unavailable"}
        anchor_rows = snapshots.execute(
            """SELECT symbol,company_name,industry,source_url,retrieved_at
               FROM index_membership_snapshots
               WHERE index_name='NIFTY 500' AND snapshot_date=? ORDER BY symbol""",
            (anchor_date,),
        ).fetchall()
        requested = as_of_date or anchor_date
        try:
            requested_date = date.fromisoformat(requested)
            anchor_day = date.fromisoformat(anchor_date)
        except ValueError as error:
            raise ValueError("invalid_universe_as_of_date") from error
        if requested_date > anchor_day:
            raise ValueError("universe_as_of_after_anchor")

        members = {
            row["symbol"]: {
                "symbol": row["symbol"], "company_name": row["company_name"],
                "industry": row["industry"], "membership_role": "ordinary_constituent",
                "is_investable": True,
            }
            for row in anchor_rows
        }
        events = market.execute(
            """SELECT symbol,action,effective_date,published_date,source_url,source_status
               FROM universe_membership_events
               WHERE index_name='NIFTY 500' AND effective_date>? AND effective_date<=?
               ORDER BY effective_date DESC, action, symbol""",
            (requested_date.isoformat(), anchor_date),
        ).fetchall()
        reversed_events = []
        for event in events:
            symbol = event["symbol"]
            if event["action"] == "included":
                members.pop(symbol, None)
                reverse_action = "removed_from_reconstruction"
            else:
                members.setdefault(symbol, {
                    "symbol": symbol, "company_name": None, "industry": None,
                    "membership_role": "ordinary_constituent", "is_investable": True,
                })
                reverse_action = "restored_to_reconstruction"
            reversed_events.append({**dict(event), "reverse_action": reverse_action})

        lineage_rows = market.execute(
            """SELECT parent_symbol,entity_name,dummy_symbol,listed_symbol,effective_date,
                      listing_date,exclusion_date,inclusion_source_url,exclusion_source_url,
                      review_status
               FROM universe_lineage_intervals WHERE index_name='NIFTY 500'
               ORDER BY effective_date,dummy_symbol"""
        ).fetchall()
        active_lineages = []
        for lineage in lineage_rows:
            effective_day = date.fromisoformat(lineage["effective_date"])
            listing_day = date.fromisoformat(lineage["listing_date"])
            exclusion_day = date.fromisoformat(lineage["exclusion_date"])
            reentered = market.execute(
                """SELECT 1 FROM universe_membership_events
                   WHERE index_name='NIFTY 500' AND symbol=? AND action='included'
                         AND effective_date>=? AND effective_date<=? LIMIT 1""",
                (lineage["listed_symbol"], lineage["exclusion_date"], requested_date.isoformat()),
            ).fetchone()
            if reentered:
                continue
            # Ordinary event reversal may restore the later listed ticker. A
            # corporate-action lineage owns both identities and must remove them
            # outside its actual effective interval.
            members.pop(lineage["dummy_symbol"], None)
            members.pop(lineage["listed_symbol"], None)
            if not (effective_day <= requested_date < exclusion_day):
                continue
            dummy_phase = requested_date < listing_day
            active_symbol = lineage["dummy_symbol"] if dummy_phase else lineage["listed_symbol"]
            members[active_symbol] = {
                "symbol": active_symbol,
                "company_name": lineage["entity_name"],
                "industry": None,
                "membership_role": "zero_price_corporate_action_dummy" if dummy_phase else "listed_resulting_company",
                "is_investable": not dummy_phase,
                "parent_symbol": lineage["parent_symbol"],
                "lineage_symbol": lineage["listed_symbol"],
            }
            active_lineages.append({
                **dict(lineage),
                "active_symbol": active_symbol,
                "phase": "zero_price_dummy" if dummy_phase else "listed_pending_index_exit",
                "is_investable": not dummy_phase,
            })

        symbol_change_rows = market.execute(
            """SELECT old_symbol,new_symbol,effective_date,review_status,source_url
               FROM symbol_history WHERE effective_date>? AND effective_date<=?
               ORDER BY effective_date DESC""",
            (requested_date.isoformat(), anchor_date),
        ).fetchall()
        reversed_symbol_changes = []
        for change in symbol_change_rows:
            current_member = members.pop(change["new_symbol"], None)
            if not current_member:
                continue
            introduced_after_request = market.execute(
                """SELECT 1 FROM universe_membership_events
                   WHERE index_name='NIFTY 500' AND symbol=? AND action='included'
                         AND effective_date>? AND effective_date<=? LIMIT 1""",
                (change["old_symbol"], requested_date.isoformat(), anchor_date),
            ).fetchone()
            if introduced_after_request:
                reversed_symbol_changes.append({
                    **dict(change),
                    "reverse_action": "removed_pre_inclusion_symbol_lineage",
                })
                continue
            current_member["symbol"] = change["old_symbol"]
            current_member["lineage_symbol"] = change["new_symbol"]
            current_member["symbol_effective_through"] = change["effective_date"]
            members[change["old_symbol"]] = current_member
            reversed_symbol_changes.append(dict(change))

        classification_audit = _classification_for_members(market, snapshot_db, members, requested_date)

        notice_rows = market.execute(
            """SELECT effective_date,published_date,source_url,COUNT(*) AS event_count
               FROM universe_membership_events WHERE index_name='NIFTY 500'
               GROUP BY effective_date,published_date,source_url ORDER BY effective_date"""
        ).fetchall()
        notice_review = market.execute(
            """SELECT reviewed_from,reviewed_through,catalog_url,methodology_url,
                      review_status,notes,retrieved_at
               FROM universe_notice_review_windows
               WHERE index_name='NIFTY 500' AND reviewed_from<=? AND reviewed_through>=?
               ORDER BY reviewed_through DESC LIMIT 1""",
            (requested, anchor_date),
        ).fetchone()
        history_audit = removed_constituent_history_audit(market_db)
        exact_snapshot = requested == anchor_date
        ad_hoc_complete = bool(
            notice_review
            and notice_review["review_status"] == "complete_for_nifty500_membership_changes"
        )
        classification_complete = classification_audit["complete"]
        survivorship_gate_complete = bool(
            ad_hoc_complete
            and history_audit["coverage"]["complete"]
            and classification_complete
        )
        return {
            "ok": True,
            "contract_version": "nifty500-point-in-time-universe-v2",
            "index_name": "NIFTY 500",
            "requested_as_of_date": requested,
            "anchor_snapshot_date": anchor_date,
            "anchor_member_count": len(anchor_rows),
            "member_count": len(members),
            "membership_status": (
                "validated_snapshot" if exact_snapshot
                else "official_notice_reconstruction" if survivorship_gate_complete
                else "partial_official_reconstruction"
            ),
            "ranking_admissible": survivorship_gate_complete,
            "survivorship_bias_gate_complete": survivorship_gate_complete,
            "classification_status": "historical_complete" if classification_complete else "historical_partial",
            "coverage": {
                "reviewed_notice_from": notice_rows[0]["effective_date"] if notice_rows else None,
                "reviewed_notice_through": notice_rows[-1]["effective_date"] if notice_rows else None,
                "reviewed_notices": len(notice_rows),
                "reviewed_events": sum(int(row["event_count"]) for row in notice_rows),
                "reviewed_temporary_lineages": len(lineage_rows),
                "temporary_lineage_review_complete": bool(lineage_rows),
                "ad_hoc_notice_review_complete": ad_hoc_complete,
                "removed_and_delisted_history_complete": history_audit["coverage"]["complete"],
                "historical_sector_and_size_complete": classification_complete,
                "removed_history_audit": history_audit["coverage"],
                "classification_audit": classification_audit,
            },
            "sources": {
                "anchor": anchor_rows[0]["source_url"] if anchor_rows else NIFTY500_INDEX_URL,
                "reconstitution_policy": NIFTY500_RECONSTITUTION_URL,
                "notices": [dict(row) for row in notice_rows],
                "notice_review_window": dict(notice_review) if notice_review else None,
                "temporary_lineages": [dict(row) for row in lineage_rows],
                "symbol_changes": [dict(row) for row in symbol_change_rows],
                "classification_sync": classification_sync,
            },
            "limitations": [
                "A reconstructed date is ranking-admissible only when notice, removed-history, sector and size coverage all pass.",
                "The dated press-release catalog review covers NIFTY 500 membership changes only through its recorded review boundary; later releases require a new review window.",
                "Sector labels come from official dated constituent snapshots; constituents first admitted between archived snapshots use an explicit official-event ledger whose sector is confirmed by the next official constituent file and labelled as subsequent confirmation.",
                "Size buckets are reversed from the latest validated official snapshot using the March and September official reconstitution ledgers.",
                "The backtest engine retains its survivorship-bias warning whenever any coverage flag is incomplete.",
            ],
            "reversed_events": reversed_events,
            "active_lineages": active_lineages,
            "reversed_symbol_changes": reversed_symbol_changes,
            "members": sorted(members.values(), key=lambda row: row["symbol"]),
        }
    finally:
        snapshots.close()
        market.close()


def _instrument_series(connection: sqlite3.Connection, instrument_id: str) -> list[sqlite3.Row]:
    return connection.execute(
        "SELECT session_date, open, high, low, close, volume FROM candles WHERE instrument_id=? ORDER BY session_date",
        (instrument_id,),
    ).fetchall()


FORWARD_MOVE_HORIZONS = (5, 10, 20, 60)
FORWARD_MOVE_VOLATILITY_LOOKBACK = 20
FORWARD_MOVE_LARGE_MOVE_SIGMA = 1.5


def _price_row_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def _normalise_forward_price_rows(rows: list[Any], series_name: str) -> list[dict[str, Any]]:
    normalised: list[dict[str, Any]] = []
    seen_sessions: set[str] = set()
    for row in rows:
        session = str(_price_row_value(row, "session_date", "")).strip()
        try:
            date.fromisoformat(session)
        except ValueError as error:
            raise ValueError(f"invalid_{series_name}_session_date") from error
        if session in seen_sessions:
            raise ValueError(f"duplicate_{series_name}_session")
        seen_sessions.add(session)
        try:
            close = float(_price_row_value(row, "close"))
            high = float(_price_row_value(row, "high", close))
            low = float(_price_row_value(row, "low", close))
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid_{series_name}_price") from error
        if not all(math.isfinite(value) and value > 0 for value in (close, high, low)):
            raise ValueError(f"invalid_{series_name}_price")
        if high < close or low > close or high < low:
            raise ValueError(f"invalid_{series_name}_ohlc")
        normalised.append({"session_date": session, "close": close, "high": high, "low": low})
    normalised.sort(key=lambda row: row["session_date"])
    return normalised


def _trailing_daily_volatility(rows: list[dict[str, Any]], feature_index: int, lookback: int) -> float | None:
    if feature_index < lookback:
        return None
    closes = [row["close"] for row in rows[feature_index - lookback:feature_index + 1]]
    returns = [closes[index] / closes[index - 1] - 1.0 for index in range(1, len(closes))]
    if len(returns) < 2:
        return None
    average = sum(returns) / len(returns)
    variance = sum((value - average) ** 2 for value in returns) / (len(returns) - 1)
    return math.sqrt(max(0.0, variance))


def _reference_forward_return(
    close_by_session: dict[str, float], feature_session: str, end_session: str,
) -> float | None:
    feature_close = close_by_session.get(feature_session)
    end_close = close_by_session.get(end_session)
    if feature_close is None or end_close is None or feature_close <= 0:
        return None
    return end_close / feature_close - 1.0


def forward_move_label_contract(
    stock_rows: list[Any],
    sector_rows: list[Any] | None = None,
    benchmark_rows: list[Any] | None = None,
    *,
    horizons: tuple[int, ...] = FORWARD_MOVE_HORIZONS,
    volatility_lookback: int = FORWARD_MOVE_VOLATILITY_LOOKBACK,
    large_move_sigma: float = FORWARD_MOVE_LARGE_MOVE_SIGMA,
    feature_from: str | None = None,
    feature_through: str | None = None,
) -> dict[str, Any]:
    """Create strictly forward completed-session move labels.

    A feature is observed at a completed close. The outcome window starts on the
    following stored session; the feature candle can never contribute to MFE,
    MAE or the endpoint. Returns use the known feature close as their baseline.
    Ex-ante volatility uses only closes at or before the feature timestamp.
    """
    cleaned_horizons = tuple(sorted(set(int(value) for value in horizons)))
    if not cleaned_horizons or cleaned_horizons[0] <= 0 or cleaned_horizons[-1] > 252:
        raise ValueError("invalid_forward_move_horizons")
    if volatility_lookback < 2:
        raise ValueError("invalid_forward_move_volatility_lookback")
    if not math.isfinite(large_move_sigma) or large_move_sigma <= 0:
        raise ValueError("invalid_forward_move_sigma")
    if feature_from:
        date.fromisoformat(feature_from)
    if feature_through:
        date.fromisoformat(feature_through)
    if feature_from and feature_through and feature_from > feature_through:
        raise ValueError("invalid_forward_move_feature_window")

    stock = _normalise_forward_price_rows(stock_rows, "stock")
    sector = _normalise_forward_price_rows(sector_rows or [], "sector")
    benchmark = _normalise_forward_price_rows(benchmark_rows or [], "benchmark")
    sector_closes = {row["session_date"]: row["close"] for row in sector}
    benchmark_closes = {row["session_date"]: row["close"] for row in benchmark}
    horizon_counts = {str(horizon): 0 for horizon in cleaned_horizons}
    tail_censored = {str(horizon): 0 for horizon in cleaned_horizons}
    missing_sector = {str(horizon): 0 for horizon in cleaned_horizons}
    missing_benchmark = {str(horizon): 0 for horizon in cleaned_horizons}
    labels: list[dict[str, Any]] = []
    features_considered = 0
    volatility_ready = 0

    for feature_index, feature in enumerate(stock):
        feature_session = feature["session_date"]
        if feature_from and feature_session < feature_from:
            continue
        if feature_through and feature_session > feature_through:
            continue
        features_considered += 1
        trailing_volatility = _trailing_daily_volatility(stock, feature_index, volatility_lookback)
        outcomes: dict[str, dict[str, Any]] = {}
        for horizon in cleaned_horizons:
            key = str(horizon)
            end_index = feature_index + horizon
            if end_index >= len(stock):
                tail_censored[key] += 1
                continue
            future_rows = stock[feature_index + 1:end_index + 1]
            endpoint = stock[end_index]
            stock_return = endpoint["close"] / feature["close"] - 1.0
            sector_return = _reference_forward_return(
                sector_closes, feature_session, endpoint["session_date"]
            )
            benchmark_return = _reference_forward_return(
                benchmark_closes, feature_session, endpoint["session_date"]
            )
            if sector_return is None:
                missing_sector[key] += 1
            if benchmark_return is None:
                missing_benchmark[key] += 1
            maximum_high_index = max(range(len(future_rows)), key=lambda index: future_rows[index]["high"])
            maximum_high = future_rows[maximum_high_index]["high"]
            minimum_low_index = min(range(len(future_rows)), key=lambda index: future_rows[index]["low"])
            minimum_low = future_rows[minimum_low_index]["low"]
            expected_volatility = (
                trailing_volatility * math.sqrt(horizon)
                if trailing_volatility is not None else None
            )
            volatility_normalised_move = (
                abs(stock_return) / expected_volatility
                if expected_volatility is not None and expected_volatility > 0 else None
            )
            if trailing_volatility is None:
                large_move_reason = "insufficient_ex_ante_volatility_history"
            elif expected_volatility == 0:
                large_move_reason = "zero_ex_ante_volatility"
            else:
                large_move_reason = "evaluated"
            outcomes[key] = {
                "horizon_sessions": horizon,
                "label_start_session": future_rows[0]["session_date"],
                "label_end_session": endpoint["session_date"],
                "forward_return_pct": _round(stock_return * 100, 6),
                "sector_forward_return_pct": (
                    _round(sector_return * 100, 6) if sector_return is not None else None
                ),
                "relative_to_sector_pct": (
                    _round((stock_return - sector_return) * 100, 6)
                    if sector_return is not None else None
                ),
                "nifty50_forward_return_pct": (
                    _round(benchmark_return * 100, 6) if benchmark_return is not None else None
                ),
                "relative_to_nifty50_pct": (
                    _round((stock_return - benchmark_return) * 100, 6)
                    if benchmark_return is not None else None
                ),
                "maximum_favourable_excursion_pct": _round(
                    (maximum_high / feature["close"] - 1.0) * 100, 6
                ),
                "maximum_adverse_excursion_pct": _round(
                    (minimum_low / feature["close"] - 1.0) * 100, 6
                ),
                "time_to_maximum_move_sessions": maximum_high_index + 1,
                "time_to_minimum_move_sessions": minimum_low_index + 1,
                "ex_ante_daily_volatility_pct": (
                    _round(trailing_volatility * 100, 6)
                    if trailing_volatility is not None else None
                ),
                "horizon_expected_volatility_pct": (
                    _round(expected_volatility * 100, 6)
                    if expected_volatility is not None else None
                ),
                "volatility_normalised_move": (
                    _round(volatility_normalised_move, 6)
                    if volatility_normalised_move is not None else None
                ),
                "large_move": (
                    volatility_normalised_move >= large_move_sigma
                    if volatility_normalised_move is not None else None
                ),
                "large_move_direction": (
                    "up" if stock_return > 0 else "down" if stock_return < 0 else "flat"
                ),
                "large_move_reason": large_move_reason,
            }
            horizon_counts[key] += 1
        if outcomes:
            if trailing_volatility is not None:
                volatility_ready += 1
            labels.append({
                "feature_session": feature_session,
                "feature_close": _round(feature["close"], 6),
                "outcomes": outcomes,
            })

    return {
        "ok": bool(stock),
        "contract_version": "forward-move-labels-v1",
        "definition": {
            "feature_timestamp": "completed_session_close",
            "outcome_window": "subsequent_completed_sessions_only",
            "return_baseline": "feature_session_close",
            "endpoint": "future_horizon_session_close",
            "excursion_prices": "future_session_highs_and_lows_excluding_feature_candle",
            "relative_return_method": "stock_return_minus_reference_return_on_exact_aligned_sessions",
            "volatility_estimator": "sample_standard_deviation_of_trailing_close_to_close_returns",
            "volatility_lookback_sessions": volatility_lookback,
            "large_move_rule": "absolute_forward_return_at_least_sigma_times_expected_horizon_volatility",
            "large_move_sigma": large_move_sigma,
            "horizons_sessions": list(cleaned_horizons),
            "no_look_ahead_enforced": True,
        },
        "coverage": {
            "stored_stock_sessions": len(stock),
            "feature_rows_considered": features_considered,
            "feature_rows_with_any_label": len(labels),
            "feature_rows_with_ex_ante_volatility": volatility_ready,
            "labels_by_horizon": horizon_counts,
            "tail_censored_by_horizon": tail_censored,
            "missing_sector_reference_by_horizon": missing_sector,
            "missing_nifty50_reference_by_horizon": missing_benchmark,
            "feature_from": feature_from,
            "feature_through": feature_through,
        },
        "labels": labels,
        "limitations": [
            "Labels are research outcomes, not forecasts or order instructions.",
            "Tail observations without the complete future horizon are censored rather than inferred.",
            "Sector and NIFTY 50 relative outcomes require exact feature and endpoint sessions.",
            "The binary large-move threshold is frozen; continuous returns and excursions remain primary evidence.",
        ],
    }


def forward_move_labels(
    market_db: Path = DEFAULT_MARKET_DB,
    symbol: str | None = None,
    sector_index: str | None = None,
    feature_from: str | None = None,
    feature_through: str | None = None,
) -> dict[str, Any]:
    if not symbol or not str(symbol).strip():
        raise ValueError("symbol_required")
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        stock_instrument_id = (
            str(symbol).strip() if str(symbol).startswith("stock:")
            else f"stock:{str(symbol).strip().upper()}"
        )
        instrument = connection.execute(
            "SELECT instrument_id,display_name,source,updated_at FROM instruments WHERE instrument_id=? AND kind='stock'",
            (stock_instrument_id,),
        ).fetchone()
        if not instrument:
            return {"ok": False, "reason": "stock_instrument_not_found", "symbol": str(symbol).strip().upper()}
        benchmark_instrument_id = "index:Nifty 50"
        benchmark_rows = _instrument_series(connection, benchmark_instrument_id)
        sector_instrument_id = None
        sector_rows: list[sqlite3.Row] = []
        if sector_index:
            sector_instrument_id = (
                str(sector_index).strip() if str(sector_index).startswith("index:")
                else f"index:{str(sector_index).strip()}"
            )
            sector_rows = _instrument_series(connection, sector_instrument_id)
        payload = forward_move_label_contract(
            _instrument_series(connection, stock_instrument_id),
            sector_rows,
            benchmark_rows,
            feature_from=feature_from,
            feature_through=feature_through,
        )
        payload["instrument"] = {
            "instrument_id": instrument["instrument_id"],
            "symbol": instrument["display_name"],
            "source": instrument["source"],
            "updated_at": instrument["updated_at"],
        }
        payload["references"] = {
            "nifty50_instrument_id": benchmark_instrument_id,
            "nifty50_status": "ready" if benchmark_rows else "unavailable",
            "sector_instrument_id": sector_instrument_id,
            "sector_status": (
                "ready" if sector_rows else "not_requested" if not sector_index else "unavailable"
            ),
        }
        return payload
    finally:
        connection.close()


OPPORTUNITY_FEATURE_CONTRACT = "opportunity-feature-ledger-v1"
OPPORTUNITY_FEATURE_HISTORY_SESSIONS = 205


def _instrument_series_through(
    connection: sqlite3.Connection, instrument_id: str, as_of_date: str,
    limit: int = OPPORTUNITY_FEATURE_HISTORY_SESSIONS,
) -> list[sqlite3.Row]:
    return connection.execute(
        """SELECT session_date,open,high,low,close,volume,source,retrieved_at
           FROM (
               SELECT session_date,open,high,low,close,volume,source,retrieved_at
               FROM candles WHERE instrument_id=? AND session_date<=?
               ORDER BY session_date DESC LIMIT ?
           ) ORDER BY session_date""",
        (instrument_id, as_of_date, limit),
    ).fetchall()


def _completed_session_price_features(rows: list[Any], as_of_date: str) -> dict[str, Any]:
    try:
        date.fromisoformat(as_of_date)
    except ValueError as error:
        raise ValueError("invalid_feature_session_date") from error
    eligible_rows = [
        row for row in rows
        if str(_price_row_value(row, "session_date", "")) <= as_of_date
    ]
    prices = _normalise_forward_price_rows(eligible_rows, "feature")
    missing: list[str] = []
    if not prices or prices[-1]["session_date"] != as_of_date:
        return {
            "status": "missing_session",
            "source_session": prices[-1]["session_date"] if prices else None,
            "close": None,
            "missing_features": ["completed_session_candle"],
        }
    closes = [row["close"] for row in prices]

    def trailing_return(lookback: int) -> float | None:
        return closes[-1] / closes[-1 - lookback] - 1.0 if len(closes) > lookback else None

    def moving_average(window: int, end_offset: int = 0) -> float | None:
        end = len(closes) - end_offset
        start = end - window
        return sum(closes[start:end]) / window if start >= 0 and end > start else None

    returns = {lookback: trailing_return(lookback) for lookback in (5, 20, 60, 120)}
    averages = {window: moving_average(window) for window in (20, 50, 200)}
    previous_sma20 = moving_average(20, 5)
    sma20_slope = (
        averages[20] / previous_sma20 - 1.0
        if averages[20] is not None and previous_sma20 not in (None, 0) else None
    )
    daily_volatility = _trailing_daily_volatility(prices, len(prices) - 1, 20)
    daily_volatility_60 = _trailing_daily_volatility(prices, len(prices) - 1, 60)
    sma20_50_spread = (
        averages[20] / averages[50] - 1.0
        if averages[20] not in (None, 0) and averages[50] not in (None, 0) else None
    )
    crossover_age = None
    if sma20_50_spread is not None:
        current_sign = 1 if sma20_50_spread >= 0 else -1
        for offset in range(1, min(121, len(closes) - 49)):
            prior20 = moving_average(20, offset)
            prior50 = moving_average(50, offset)
            if prior20 in (None, 0) or prior50 in (None, 0):
                break
            prior_sign = 1 if prior20 / prior50 - 1.0 >= 0 else -1
            if prior_sign != current_sign:
                crossover_age = offset
                break
    volatility_expansion = (
        daily_volatility / daily_volatility_60
        if daily_volatility is not None and daily_volatility_60 not in (None, 0) else None
    )
    range20 = None
    range60 = None
    if len(prices) >= 20:
        highs20 = [float(row["high"]) for row in prices[-20:]]
        lows20 = [float(row["low"]) for row in prices[-20:]]
        if min(lows20) > 0:
            range20 = max(highs20) / min(lows20) - 1.0
    if len(prices) >= 60:
        highs60 = [float(row["high"]) for row in prices[-60:]]
        lows60 = [float(row["low"]) for row in prices[-60:]]
        if min(lows60) > 0:
            range60 = max(highs60) / min(lows60) - 1.0
    range_compression = range20 / range60 if range20 is not None and range60 not in (None, 0) else None
    prior_high20 = max(float(row["high"]) for row in prices[-21:-1]) if len(prices) >= 21 else None
    breakout_proximity = closes[-1] / prior_high20 - 1.0 if prior_high20 not in (None, 0) else None
    volumes: list[float] = []
    for row in eligible_rows[-20:]:
        value = _price_row_value(row, "volume")
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            volumes = []
            break
        if not math.isfinite(numeric) or numeric < 0:
            volumes = []
            break
        volumes.append(numeric)
    average_volume = sum(volumes) / len(volumes) if len(volumes) == 20 else None
    volume_ratio = (
        volumes[-1] / average_volume
        if average_volume not in (None, 0) and volumes else None
    )
    output = {
        "status": "ready",
        "source_session": as_of_date,
        "close": _round(closes[-1], 6),
        "return_5d_pct": _round(returns[5] * 100, 6) if returns[5] is not None else None,
        "return_20d_pct": _round(returns[20] * 100, 6) if returns[20] is not None else None,
        "return_60d_pct": _round(returns[60] * 100, 6) if returns[60] is not None else None,
        "return_120d_pct": _round(returns[120] * 100, 6) if returns[120] is not None else None,
        "distance_sma20_pct": (
            _round((closes[-1] / averages[20] - 1.0) * 100, 6) if averages[20] else None
        ),
        "distance_sma50_pct": (
            _round((closes[-1] / averages[50] - 1.0) * 100, 6) if averages[50] else None
        ),
        "distance_sma200_pct": (
            _round((closes[-1] / averages[200] - 1.0) * 100, 6) if averages[200] else None
        ),
        "sma20_slope_5d_pct": _round(sma20_slope * 100, 6) if sma20_slope is not None else None,
        "sma20_50_spread_pct": _round(sma20_50_spread * 100, 6) if sma20_50_spread is not None else None,
        "sma20_50_crossover_age_sessions": crossover_age,
        "realised_volatility_20d_pct": (
            _round(daily_volatility * math.sqrt(252) * 100, 6)
            if daily_volatility is not None else None
        ),
        "realised_volatility_60d_pct": (
            _round(daily_volatility_60 * math.sqrt(252) * 100, 6)
            if daily_volatility_60 is not None else None
        ),
        "volatility_expansion_ratio_20v60": _round(volatility_expansion, 6),
        "range_compression_ratio_20v60": _round(range_compression, 6),
        "breakout_proximity_20d_pct": _round(breakout_proximity * 100, 6) if breakout_proximity is not None else None,
        "volume_ratio_20d": _round(volume_ratio, 6) if volume_ratio is not None else None,
        "missing_features": missing,
    }
    for key, value in output.items():
        if key.endswith("_pct") or key in (
            "volume_ratio_20d", "volatility_expansion_ratio_20v60",
            "range_compression_ratio_20v60", "sma20_50_crossover_age_sessions",
        ):
            if value is None:
                missing.append(key)
    return output


def _same_contract_fno_feature(
    connection: sqlite3.Connection, symbol: str, as_of_date: str,
) -> dict[str, Any]:
    current = connection.execute(
        """SELECT f.contract_key,f.tradingsymbol,f.expiry_date,s.session_date,
                  s.close,s.open_interest,s.source,s.retrieved_at
           FROM futures_contracts f JOIN futures_eod_snapshots s USING(contract_key)
           WHERE f.underlying=? AND s.session_date=? AND f.expiry_date>=?
           ORDER BY f.expiry_date LIMIT 1""",
        (symbol, as_of_date, as_of_date),
    ).fetchone()
    if not current:
        return {
            "fno_eligible": False,
            "state": None,
            "missing_reason": "no_front_month_futures_observation",
            "current_session": None,
            "previous_session": None,
        }
    previous = connection.execute(
        """SELECT session_date,close,open_interest,source,retrieved_at
           FROM futures_eod_snapshots WHERE contract_key=? AND session_date<?
           ORDER BY session_date DESC LIMIT 1""",
        (current["contract_key"], as_of_date),
    ).fetchone()
    if not previous:
        return {
            "fno_eligible": True,
            "contract_key": current["contract_key"],
            "state": None,
            "missing_reason": "same_contract_baseline_unavailable",
            "current_session": current["session_date"],
            "previous_session": None,
        }
    gap = (date.fromisoformat(current["session_date"]) - date.fromisoformat(previous["session_date"])).days
    if gap < 1 or gap > 7 or float(previous["close"]) <= 0 or float(previous["open_interest"]) <= 0:
        return {
            "fno_eligible": True,
            "contract_key": current["contract_key"],
            "state": None,
            "missing_reason": "same_contract_baseline_not_comparable",
            "current_session": current["session_date"],
            "previous_session": previous["session_date"],
        }
    price_change = 100 * (float(current["close"]) / float(previous["close"]) - 1.0)
    oi_change = 100 * (float(current["open_interest"]) / float(previous["open_interest"]) - 1.0)
    if abs(price_change) < 0.15 or abs(oi_change) < 0.5:
        state = "no_clear_signal"
    elif price_change >= 0 and oi_change >= 0:
        state = "long_build_up"
    elif price_change < 0 and oi_change >= 0:
        state = "short_build_up"
    elif price_change < 0 and oi_change < 0:
        state = "long_unwinding"
    else:
        state = "short_covering"
    return {
        "fno_eligible": True,
        "contract_key": current["contract_key"],
        "tradingsymbol": current["tradingsymbol"],
        "expiry_date": current["expiry_date"],
        "state": state,
        "price_change_pct": _round(price_change, 6),
        "oi_change_pct": _round(oi_change, 6),
        "missing_reason": None,
        "current_session": current["session_date"],
        "previous_session": previous["session_date"],
        "source": current["source"],
        "retrieved_at": current["retrieved_at"],
    }


def _assign_descending_rank(rows: list[dict[str, Any]], value_key: str, rank_key: str) -> None:
    ranked = sorted(
        (row for row in rows if row.get(value_key) is not None),
        key=lambda row: (-float(row[value_key]), str(row.get("symbol") or row.get("sector") or "")),
    )
    for position, row in enumerate(ranked, start=1):
        row[rank_key] = position


def _build_opportunity_feature_rows(
    connection: sqlite3.Connection,
    as_of_date: str,
    universe: dict[str, Any],
) -> dict[str, Any]:
    try:
        date.fromisoformat(as_of_date)
    except ValueError as error:
        raise ValueError("invalid_feature_session_date") from error
    if not universe.get("ok"):
        return {"ok": False, "reason": universe.get("reason", "point_in_time_universe_unavailable")}
    members = [row for row in universe.get("members", []) if row.get("is_investable", True)]
    excluded_non_investable = len(universe.get("members", [])) - len(members)
    market_features = _completed_session_price_features(
        _instrument_series_through(connection, "index:Nifty 50", as_of_date), as_of_date
    )
    classification_audit = universe.get("coverage", {}).get("classification_audit", {})
    size_source_date = classification_audit.get("size_provenance", {}).get("source_date")
    generated_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for member in members:
        symbol = str(member["symbol"])
        instrument_id = f"stock:{symbol}"
        price_rows = _instrument_series_through(connection, instrument_id, as_of_date)
        price_features = _completed_session_price_features(price_rows, as_of_date)
        latest_price_row = price_rows[-1] if price_rows and str(price_rows[-1]["session_date"]) == as_of_date else None
        sector_source_date = member.get("sector_source_date")
        sector_known = bool(sector_source_date and str(sector_source_date) <= as_of_date)
        size_known = bool(size_source_date and str(size_source_date) <= as_of_date)
        sector = member.get("industry") if sector_known else None
        size_bucket = member.get("size_bucket") if size_known else None
        fno = _same_contract_fno_feature(connection, symbol, as_of_date)
        missing = list(price_features.get("missing_features", []))
        if not sector_known:
            missing.append("sector_classification_not_known_at_feature_time")
        if not size_known:
            missing.append("size_classification_not_known_at_feature_time")
        if fno.get("fno_eligible") and not fno.get("state"):
            missing.append(str(fno.get("missing_reason") or "fno_state_unavailable"))
        row = {
            "contract_version": OPPORTUNITY_FEATURE_CONTRACT,
            "session_date": as_of_date,
            "instrument_id": instrument_id,
            "symbol": symbol,
            "membership_status": universe.get("membership_status", "unknown"),
            "ranking_admissible": bool(universe.get("ranking_admissible")),
            "membership_role": member.get("membership_role"),
            "nifty500_member": True,
            "sector": sector,
            "sector_source_date": sector_source_date,
            "size_bucket": size_bucket,
            "size_source_date": size_source_date,
            "fno_eligible": bool(fno.get("fno_eligible")),
            "close": price_features.get("close"),
            "return_5d_pct": price_features.get("return_5d_pct"),
            "return_20d_pct": price_features.get("return_20d_pct"),
            "return_60d_pct": price_features.get("return_60d_pct"),
            "return_120d_pct": price_features.get("return_120d_pct"),
            "distance_sma20_pct": price_features.get("distance_sma20_pct"),
            "distance_sma50_pct": price_features.get("distance_sma50_pct"),
            "distance_sma200_pct": price_features.get("distance_sma200_pct"),
            "sma20_slope_5d_pct": price_features.get("sma20_slope_5d_pct"),
            "sma20_50_spread_pct": price_features.get("sma20_50_spread_pct"),
            "sma20_50_crossover_age_sessions": price_features.get("sma20_50_crossover_age_sessions"),
            "realised_volatility_20d_pct": price_features.get("realised_volatility_20d_pct"),
            "realised_volatility_60d_pct": price_features.get("realised_volatility_60d_pct"),
            "volatility_expansion_ratio_20v60": price_features.get("volatility_expansion_ratio_20v60"),
            "range_compression_ratio_20v60": price_features.get("range_compression_ratio_20v60"),
            "breakout_proximity_20d_pct": price_features.get("breakout_proximity_20d_pct"),
            "volume_ratio_20d": price_features.get("volume_ratio_20d"),
            "market_return_20d_pct": market_features.get("return_20d_pct"),
            "market_return_60d_pct": market_features.get("return_60d_pct"),
            "market_return_120d_pct": market_features.get("return_120d_pct"),
            "market_distance_sma200_pct": market_features.get("distance_sma200_pct"),
            "market_volatility_20d_pct": market_features.get("realised_volatility_20d_pct"),
            "market_breadth_50dma_pct": None,
            "market_breadth_200dma_pct": None,
            "sector_return_20d_pct": None,
            "sector_return_60d_pct": None,
            "sector_return_120d_pct": None,
            "sector_breadth_50dma_pct": None,
            "sector_rank_20d": None,
            "stock_sector_rank_20d": None,
            "stock_universe_rank_20d": None,
            "fno_state": fno.get("state"),
            "fno_price_change_pct": fno.get("price_change_pct"),
            "fno_oi_change_pct": fno.get("oi_change_pct"),
            "fno_contract_key": fno.get("contract_key"),
            "source_dates": {
                "feature_session": as_of_date,
                "membership_as_of": universe.get("requested_as_of_date"),
                "membership_anchor": universe.get("anchor_snapshot_date"),
                "sector_source_date": sector_source_date,
                "size_source_date": size_source_date,
                "price_source_session": price_features.get("source_session"),
                "price_retrieved_at": latest_price_row["retrieved_at"] if latest_price_row else None,
                "market_source_session": market_features.get("source_session"),
                "fno_current_session": fno.get("current_session"),
                "fno_previous_session": fno.get("previous_session"),
                "fno_retrieved_at": fno.get("retrieved_at"),
            },
            "missing_features": missing,
            "generated_at": generated_at,
        }
        rows.append(row)

    ready_rows = [row for row in rows if row.get("close") is not None]
    breadth50_values = [row for row in ready_rows if row.get("distance_sma50_pct") is not None]
    breadth200_values = [row for row in ready_rows if row.get("distance_sma200_pct") is not None]
    breadth50 = (
        100 * sum(float(row["distance_sma50_pct"]) > 0 for row in breadth50_values) / len(breadth50_values)
        if breadth50_values else None
    )
    breadth200 = (
        100 * sum(float(row["distance_sma200_pct"]) > 0 for row in breadth200_values) / len(breadth200_values)
        if breadth200_values else None
    )
    for row in rows:
        row["market_breadth_50dma_pct"] = _round(breadth50, 6)
        row["market_breadth_200dma_pct"] = _round(breadth200, 6)

    sector_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ready_rows:
        if row.get("sector"):
            sector_groups[str(row["sector"])].append(row)
    sector_summaries: list[dict[str, Any]] = []
    for sector, sector_rows in sector_groups.items():
        summary: dict[str, Any] = {"sector": sector}
        for lookback in (20, 60, 120):
            key = f"return_{lookback}d_pct"
            values = [float(row[key]) for row in sector_rows if row.get(key) is not None]
            summary[key] = sum(values) / len(values) if values else None
        breadth_rows = [row for row in sector_rows if row.get("distance_sma50_pct") is not None]
        summary["breadth_50dma_pct"] = (
            100 * sum(float(row["distance_sma50_pct"]) > 0 for row in breadth_rows) / len(breadth_rows)
            if breadth_rows else None
        )
        sector_summaries.append(summary)
        _assign_descending_rank(sector_rows, "return_20d_pct", "stock_sector_rank_20d")
    _assign_descending_rank(sector_summaries, "return_20d_pct", "sector_rank_20d")
    sector_by_name = {row["sector"]: row for row in sector_summaries}
    for row in rows:
        summary = sector_by_name.get(row.get("sector"))
        if summary:
            row["sector_return_20d_pct"] = _round(summary.get("return_20d_pct"), 6)
            row["sector_return_60d_pct"] = _round(summary.get("return_60d_pct"), 6)
            row["sector_return_120d_pct"] = _round(summary.get("return_120d_pct"), 6)
            row["sector_breadth_50dma_pct"] = _round(summary.get("breadth_50dma_pct"), 6)
            row["sector_rank_20d"] = summary.get("sector_rank_20d")
        for required_key in (
            "market_breadth_50dma_pct", "market_breadth_200dma_pct",
            "sector_return_20d_pct", "sector_breadth_50dma_pct",
        ):
            if row.get(required_key) is None:
                row["missing_features"].append(required_key)
    _assign_descending_rank(ready_rows, "return_20d_pct", "stock_universe_rank_20d")
    for row in rows:
        if row.get("return_20d_pct") is not None and row.get("stock_universe_rank_20d") is None:
            row["missing_features"].append("stock_universe_rank_20d")
        row["missing_features"] = sorted(set(row["missing_features"]))

    return {
        "ok": True,
        "contract_version": OPPORTUNITY_FEATURE_CONTRACT,
        "session_date": as_of_date,
        "rows": rows,
        "coverage": {
            "point_in_time_members": len(universe.get("members", [])),
            "investable_members": len(members),
            "excluded_non_investable_members": excluded_non_investable,
            "rows_built": len(rows),
            "rows_with_completed_session_price": len(ready_rows),
            "rows_with_sector": sum(bool(row.get("sector")) for row in rows),
            "rows_with_fno_observation": sum(bool(row.get("fno_eligible")) for row in rows),
            "market_breadth_50dma_denominator": len(breadth50_values),
            "market_breadth_200dma_denominator": len(breadth200_values),
            "sectors_evaluated": len(sector_summaries),
            "universe_ranking_admissible": bool(universe.get("ranking_admissible")),
        },
        "no_look_ahead": {
            "candle_query_boundary": f"session_date <= {as_of_date}",
            "classification_source_boundary": f"source_date <= {as_of_date}",
            "fno_comparison": "same_contract_current_and_prior_completed_session_only",
            "forward_label_fields_present": False,
        },
    }


_OPPORTUNITY_LEDGER_COLUMNS = (
    "contract_version", "session_date", "instrument_id", "symbol",
    "membership_status", "ranking_admissible", "membership_role", "nifty500_member",
    "sector", "sector_source_date", "size_bucket", "size_source_date", "fno_eligible",
    "close", "return_5d_pct", "return_20d_pct", "return_60d_pct", "return_120d_pct",
    "distance_sma20_pct", "distance_sma50_pct", "distance_sma200_pct", "sma20_slope_5d_pct",
    "sma20_50_spread_pct", "sma20_50_crossover_age_sessions", "realised_volatility_20d_pct",
    "realised_volatility_60d_pct", "volatility_expansion_ratio_20v60",
    "range_compression_ratio_20v60", "breakout_proximity_20d_pct", "volume_ratio_20d",
    "market_return_20d_pct",
    "market_return_60d_pct", "market_return_120d_pct",
    "market_distance_sma200_pct", "market_volatility_20d_pct", "market_breadth_50dma_pct",
    "market_breadth_200dma_pct", "sector_return_20d_pct", "sector_return_60d_pct",
    "sector_return_120d_pct", "sector_breadth_50dma_pct", "sector_rank_20d",
    "stock_sector_rank_20d", "stock_universe_rank_20d", "fno_state",
    "fno_price_change_pct", "fno_oi_change_pct", "fno_contract_key",
    "source_dates_json", "missing_features_json", "generated_at", "schema_version",
)


def _persist_opportunity_feature_rows(
    connection: sqlite3.Connection, rows: list[dict[str, Any]],
) -> int:
    placeholders = ",".join("?" for _ in _OPPORTUNITY_LEDGER_COLUMNS)
    columns = ",".join(_OPPORTUNITY_LEDGER_COLUMNS)
    values = []
    for row in rows:
        serialised = dict(row)
        serialised["ranking_admissible"] = int(bool(row.get("ranking_admissible")))
        serialised["nifty500_member"] = int(bool(row.get("nifty500_member")))
        serialised["fno_eligible"] = int(bool(row.get("fno_eligible")))
        serialised["source_dates_json"] = json.dumps(row.get("source_dates", {}), sort_keys=True)
        serialised["missing_features_json"] = json.dumps(row.get("missing_features", []), sort_keys=True)
        serialised["schema_version"] = 1
        values.append(tuple(serialised.get(column) for column in _OPPORTUNITY_LEDGER_COLUMNS))
    if values:
        connection.executemany(
            f"INSERT OR REPLACE INTO opportunity_feature_ledger({columns}) VALUES ({placeholders})",
            values,
        )
    return len(values)


def build_opportunity_feature_ledger(
    market_db: Path = DEFAULT_MARKET_DB,
    snapshot_db: Path = DEFAULT_SNAPSHOT_DB,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        completed_session = as_of_date or connection.execute(
            "SELECT MAX(session_date) FROM candles WHERE instrument_id='index:Nifty 50'"
        ).fetchone()[0]
    finally:
        connection.close()
    if not completed_session:
        return {"ok": False, "reason": "completed_reference_session_unavailable"}
    universe = point_in_time_universe_contract(market_db, snapshot_db, str(completed_session))
    connection = _connect(market_db)
    try:
        contract = _build_opportunity_feature_rows(connection, str(completed_session), universe)
        if not contract.get("ok"):
            return contract
        persisted = _persist_opportunity_feature_rows(connection, contract["rows"])
        connection.commit()
        return {
            "ok": True,
            "contract_version": OPPORTUNITY_FEATURE_CONTRACT,
            "session_date": str(completed_session),
            "persisted_rows": persisted,
            "coverage": contract["coverage"],
            "no_look_ahead": contract["no_look_ahead"],
            "sample": contract["rows"][:5],
        }
    finally:
        connection.close()


def opportunity_feature_ledger(
    market_db: Path = DEFAULT_MARKET_DB,
    as_of_date: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    if limit < 1 or limit > 1000:
        raise ValueError("invalid_feature_ledger_limit")
    connection = _connect(market_db)
    try:
        session = as_of_date or connection.execute(
            "SELECT MAX(session_date) FROM opportunity_feature_ledger WHERE contract_version=?",
            (OPPORTUNITY_FEATURE_CONTRACT,),
        ).fetchone()[0]
        if not session:
            return {"ok": False, "reason": "feature_ledger_empty"}
        filters = ["contract_version=?", "session_date=?"]
        parameters: list[Any] = [OPPORTUNITY_FEATURE_CONTRACT, session]
        if symbol:
            filters.append("symbol=?")
            parameters.append(str(symbol).strip().upper())
        total = connection.execute(
            f"SELECT COUNT(*) FROM opportunity_feature_ledger WHERE {' AND '.join(filters)}",
            parameters,
        ).fetchone()[0]
        stored_rows = connection.execute(
            f"""SELECT * FROM opportunity_feature_ledger
                WHERE {' AND '.join(filters)}
                ORDER BY sector_rank_20d,stock_sector_rank_20d,symbol LIMIT ?""",
            (*parameters, limit),
        ).fetchall()
        rows = []
        for stored in stored_rows:
            row = dict(stored)
            row["ranking_admissible"] = bool(row["ranking_admissible"])
            row["nifty500_member"] = bool(row["nifty500_member"])
            row["fno_eligible"] = bool(row["fno_eligible"])
            row["source_dates"] = json.loads(row.pop("source_dates_json"))
            row["missing_features"] = json.loads(row.pop("missing_features_json"))
            rows.append(row)
        return {
            "ok": True,
            "contract_version": OPPORTUNITY_FEATURE_CONTRACT,
            "session_date": session,
            "total_rows": int(total),
            "returned_rows": len(rows),
            "rows": rows,
            "limitations": [
                "The ledger contains completed-session features only; forward outcomes are stored separately.",
                "Missing features remain null and carry an explicit reason.",
                "Rows are research evidence and are not forecasts or order instructions.",
            ],
        }
    finally:
        connection.close()


MARKET_REGIME_CONTRACT = "market-regime-classification-v1"
MARKET_REGIME_MINIMUM_COVERAGE_RATIO = 0.95


def market_regime_classification(
    feature_rows: list[Any], session_date: str | None = None,
) -> dict[str, Any]:
    rows = [dict(row) for row in feature_rows]
    sessions = sorted({str(row.get("session_date") or "") for row in rows if row.get("session_date")})
    if session_date:
        try:
            date.fromisoformat(session_date)
        except ValueError as error:
            raise ValueError("invalid_market_regime_session_date") from error
    if len(sessions) > 1 or (session_date and sessions and sessions[0] != session_date):
        raise ValueError("market_regime_requires_single_feature_session")
    resolved_session = session_date or (sessions[0] if sessions else None)
    if not rows or not resolved_session:
        return {"ok": False, "reason": "feature_ledger_session_unavailable"}

    input_keys = (
        "market_return_20d_pct", "market_distance_sma200_pct",
        "market_volatility_20d_pct", "market_breadth_50dma_pct",
        "market_breadth_200dma_pct",
    )
    inputs: dict[str, float | None] = {}
    missing_inputs: list[str] = []
    inconsistent_inputs: list[str] = []
    for key in input_keys:
        values = sorted({round(float(row[key]), 8) for row in rows if row.get(key) is not None})
        if not values:
            inputs[key] = None
            missing_inputs.append(key)
        elif len(values) > 1:
            inputs[key] = None
            inconsistent_inputs.append(key)
        else:
            inputs[key] = values[0]

    universe_rows = len(rows)
    priced_rows = sum(row.get("close") is not None for row in rows)
    breadth50_denominator = sum(row.get("distance_sma50_pct") is not None for row in rows)
    breadth200_denominator = sum(row.get("distance_sma200_pct") is not None for row in rows)
    priced_coverage = priced_rows / universe_rows if universe_rows else 0.0
    breadth50_coverage = breadth50_denominator / universe_rows if universe_rows else 0.0
    breadth200_coverage = breadth200_denominator / universe_rows if universe_rows else 0.0
    coverage_failures = []
    if priced_coverage < MARKET_REGIME_MINIMUM_COVERAGE_RATIO:
        coverage_failures.append("priced_coverage_below_95_pct")
    if breadth50_coverage < MARKET_REGIME_MINIMUM_COVERAGE_RATIO:
        coverage_failures.append("breadth_50dma_coverage_below_95_pct")
    if breadth200_coverage < MARKET_REGIME_MINIMUM_COVERAGE_RATIO:
        coverage_failures.append("breadth_200dma_coverage_below_95_pct")
    missing_inputs.extend(f"inconsistent_{key}" for key in inconsistent_inputs)
    missing_inputs.extend(coverage_failures)
    universe_admissible = all(bool(row.get("ranking_admissible")) for row in rows)

    trend_direction = None
    trend_strength = None
    range_state = None
    volatility_state = None
    breadth_state = None
    regime_family = None
    regime_key = None
    if not missing_inputs:
        return20 = float(inputs["market_return_20d_pct"])
        distance200 = float(inputs["market_distance_sma200_pct"])
        volatility20 = float(inputs["market_volatility_20d_pct"])
        breadth50 = float(inputs["market_breadth_50dma_pct"])
        breadth200 = float(inputs["market_breadth_200dma_pct"])

        if distance200 >= 3 and return20 >= 2 and breadth50 >= 60 and breadth200 >= 55:
            trend_direction, trend_strength = "up", "strong"
        elif distance200 > 0 and return20 > 0:
            trend_direction = "up"
            trend_strength = "moderate" if breadth50 >= 50 else "weak"
        elif distance200 <= -3 and return20 <= -2 and breadth50 <= 40 and breadth200 <= 45:
            trend_direction, trend_strength = "down", "strong"
        elif distance200 < 0 and return20 < 0:
            trend_direction = "down"
            trend_strength = "moderate" if breadth50 < 50 else "weak"
        else:
            trend_direction, trend_strength = "neutral", "conflicted"

        if abs(distance200) <= 2 and abs(return20) <= 3 and 40 <= breadth50 <= 60:
            range_state = "range_bound"
        elif trend_strength == "strong":
            range_state = "trend_expansion"
        else:
            range_state = "transition"

        if volatility20 < 12:
            volatility_state = "low"
        elif volatility20 < 20:
            volatility_state = "normal"
        elif volatility20 < 30:
            volatility_state = "elevated"
        else:
            volatility_state = "high"

        if breadth50 >= 60 and breadth200 >= 55:
            breadth_state = "broad_positive"
        elif breadth50 <= 40 and breadth200 <= 45:
            breadth_state = "broad_negative"
        elif breadth50 >= 50 and breadth200 >= 45:
            breadth_state = "positive"
        elif breadth50 < 50 and breadth200 < 55:
            breadth_state = "negative"
        else:
            breadth_state = "mixed"

        if range_state == "range_bound":
            regime_family = "range"
        elif trend_direction == "up":
            regime_family = "bullish_trend"
        elif trend_direction == "down":
            regime_family = "bearish_trend"
        else:
            regime_family = "transition"
        regime_key = "/".join((
            regime_family, trend_strength, range_state, volatility_state, breadth_state,
        ))

    source_dates = {}
    for row in rows:
        raw = row.get("source_dates")
        if raw is None and row.get("source_dates_json"):
            try:
                raw = json.loads(row["source_dates_json"])
            except (TypeError, json.JSONDecodeError):
                raw = None
        if raw:
            source_dates = dict(raw)
            break
    classification_status = (
        "gated" if missing_inputs else "ready" if universe_admissible else "descriptive_only"
    )
    generated_at = datetime.now(timezone.utc).isoformat()
    return {
        "ok": True,
        "contract_version": MARKET_REGIME_CONTRACT,
        "session_date": resolved_session,
        "classification_status": classification_status,
        "ranking_admissible": bool(not missing_inputs and universe_admissible),
        "regime_family": regime_family,
        "trend_direction": trend_direction,
        "trend_strength": trend_strength,
        "range_state": range_state,
        "volatility_state": volatility_state,
        "breadth_state": breadth_state,
        "regime_key": regime_key,
        **inputs,
        "universe_rows": universe_rows,
        "priced_rows": priced_rows,
        "breadth_50dma_denominator": breadth50_denominator,
        "breadth_200dma_denominator": breadth200_denominator,
        "priced_coverage_ratio": _round(priced_coverage, 6),
        "breadth_50dma_coverage_ratio": _round(breadth50_coverage, 6),
        "breadth_200dma_coverage_ratio": _round(breadth200_coverage, 6),
        "source_dates": source_dates,
        "missing_inputs": sorted(set(missing_inputs)),
        "generated_at": generated_at,
        "definitions": {
            "trend": {
                "strong_up": "distance_vs_200dma >= 3%, 20-session return >= 2%, breadth50 >= 60%, breadth200 >= 55%",
                "moderate_up": "distance_vs_200dma > 0, 20-session return > 0, breadth50 >= 50%",
                "strong_down": "distance_vs_200dma <= -3%, 20-session return <= -2%, breadth50 <= 40%, breadth200 <= 45%",
                "moderate_down": "distance_vs_200dma < 0, 20-session return < 0, breadth50 < 50%",
            },
            "range_bound": "absolute 200dma distance <= 2%, absolute 20-session return <= 3%, breadth50 between 40% and 60%",
            "volatility": "annualised 20-session realised volatility: low <12%, normal <20%, elevated <30%, high >=30%",
            "breadth": "frozen 50dma/200dma participation thresholds",
            "minimum_cross_sectional_coverage": MARKET_REGIME_MINIMUM_COVERAGE_RATIO,
            "strategy_results_used": False,
            "forward_labels_used": False,
        },
    }


def _market_regime_sample_summary(
    regimes: list[dict[str, Any]], trade_sessions: list[str] | None = None,
) -> dict[str, Any]:
    trade_counts = defaultdict(int)
    if trade_sessions is not None:
        for session in trade_sessions:
            trade_counts[str(session)] += 1
    family_counts: dict[str, dict[str, Any]] = {}
    dimension_counts: dict[str, dict[str, int]] = {
        "trend_direction": defaultdict(int),
        "trend_strength": defaultdict(int),
        "range_state": defaultdict(int),
        "volatility_state": defaultdict(int),
        "breadth_state": defaultdict(int),
    }
    for row in regimes:
        family = str(row.get("regime_family") or "gated")
        aggregate = family_counts.setdefault(family, {"sessions": 0, "trades": 0})
        aggregate["sessions"] += 1
        aggregate["trades"] += trade_counts.get(str(row.get("session_date")), 0)
        for dimension in dimension_counts:
            dimension_counts[dimension][str(row.get(dimension) or "gated")] += 1
    if trade_sessions is None:
        for aggregate in family_counts.values():
            aggregate["trades"] = None
    return {
        "contract_version": "market-regime-sample-summary-v1",
        "sessions": len(regimes),
        "trade_sample_status": "available" if trade_sessions is not None else "not_joined_until_strategy_conditioning",
        "by_regime_family": family_counts,
        "by_dimension": {key: dict(value) for key, value in dimension_counts.items()},
    }


# Compatibility bindings keep the published engine surface stable while the
# frozen calculation now lives in the focused analytics component.
market_regime_classification = _market_regime_classification_component
_market_regime_sample_summary = _market_regime_sample_summary_component


def _persist_market_regime(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
    connection.execute(
        """INSERT OR REPLACE INTO market_regime_ledger(
               contract_version,session_date,classification_status,ranking_admissible,
               regime_family,trend_direction,trend_strength,range_state,volatility_state,
               breadth_state,regime_key,market_return_20d_pct,market_distance_sma200_pct,
               market_volatility_20d_pct,market_breadth_50dma_pct,market_breadth_200dma_pct,
               universe_rows,priced_rows,breadth_50dma_denominator,breadth_200dma_denominator,
               priced_coverage_ratio,breadth_50dma_coverage_ratio,breadth_200dma_coverage_ratio,
               source_dates_json,missing_inputs_json,generated_at,schema_version)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
        (
            row["contract_version"], row["session_date"], row["classification_status"],
            int(bool(row["ranking_admissible"])), row["regime_family"], row["trend_direction"],
            row["trend_strength"], row["range_state"], row["volatility_state"],
            row["breadth_state"], row["regime_key"], row["market_return_20d_pct"],
            row["market_distance_sma200_pct"], row["market_volatility_20d_pct"],
            row["market_breadth_50dma_pct"], row["market_breadth_200dma_pct"],
            row["universe_rows"], row["priced_rows"], row["breadth_50dma_denominator"],
            row["breadth_200dma_denominator"], row["priced_coverage_ratio"],
            row["breadth_50dma_coverage_ratio"], row["breadth_200dma_coverage_ratio"],
            json.dumps(row["source_dates"], sort_keys=True),
            json.dumps(row["missing_inputs"], sort_keys=True), row["generated_at"],
        ),
    )


def build_market_regime_ledger(
    market_db: Path = DEFAULT_MARKET_DB, as_of_date: str | None = None,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        session = as_of_date or connection.execute(
            "SELECT MAX(session_date) FROM opportunity_feature_ledger WHERE contract_version=?",
            (OPPORTUNITY_FEATURE_CONTRACT,),
        ).fetchone()[0]
        if not session:
            return {"ok": False, "reason": "feature_ledger_empty"}
        feature_rows = connection.execute(
            """SELECT * FROM opportunity_feature_ledger
               WHERE contract_version=? AND session_date=? ORDER BY symbol""",
            (OPPORTUNITY_FEATURE_CONTRACT, session),
        ).fetchall()
        result = market_regime_classification(feature_rows, str(session))
        if result.get("ok"):
            _persist_market_regime(connection, result)
            connection.commit()
        return result
    finally:
        connection.close()


def market_regime_history(
    market_db: Path = DEFAULT_MARKET_DB,
    from_date: str | None = None,
    through_date: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    if limit < 1 or limit > 2000:
        raise ValueError("invalid_market_regime_limit")
    if from_date:
        date.fromisoformat(from_date)
    if through_date:
        date.fromisoformat(through_date)
    if from_date and through_date and from_date > through_date:
        raise ValueError("invalid_market_regime_window")
    connection = _connect(market_db)
    try:
        filters = ["contract_version=?"]
        parameters: list[Any] = [MARKET_REGIME_CONTRACT]
        if from_date:
            filters.append("session_date>=?")
            parameters.append(from_date)
        if through_date:
            filters.append("session_date<=?")
            parameters.append(through_date)
        stored = connection.execute(
            f"""SELECT * FROM market_regime_ledger WHERE {' AND '.join(filters)}
                ORDER BY session_date DESC LIMIT ?""",
            (*parameters, limit),
        ).fetchall()
        rows = []
        for stored_row in reversed(stored):
            row = dict(stored_row)
            row["ranking_admissible"] = bool(row["ranking_admissible"])
            row["source_dates"] = json.loads(row.pop("source_dates_json"))
            row["missing_inputs"] = json.loads(row.pop("missing_inputs_json"))
            rows.append(row)
        return {
            "ok": bool(rows),
            "reason": None if rows else "market_regime_ledger_empty",
            "contract_version": MARKET_REGIME_CONTRACT,
            "rows": rows,
            "sample_summary": _market_regime_sample_summary(rows),
            "decision_boundary": "Strategy and forward-outcome fields are excluded from classification; trade samples are joined only during later conditioning.",
        }
    finally:
        connection.close()


def backfill_market_regime_research(
    market_db: Path = DEFAULT_MARKET_DB,
    snapshot_db: Path = DEFAULT_SNAPSHOT_DB,
    from_date: str | None = None,
    through_date: str | None = None,
    max_sessions: int = 20,
) -> dict[str, Any]:
    if not from_date or not through_date:
        raise ValueError("market_regime_backfill_window_required")
    date.fromisoformat(from_date)
    date.fromisoformat(through_date)
    if from_date > through_date:
        raise ValueError("invalid_market_regime_backfill_window")
    if max_sessions < 1 or max_sessions > 252:
        raise ValueError("invalid_market_regime_backfill_limit")
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        sessions = [str(row[0]) for row in connection.execute(
            """SELECT session_date FROM candles
               WHERE instrument_id='index:Nifty 50' AND session_date BETWEEN ? AND ?
               ORDER BY session_date""",
            (from_date, through_date),
        )]
    finally:
        connection.close()
    if len(sessions) > max_sessions:
        raise ValueError("market_regime_backfill_session_limit_exceeded")
    completed = []
    failures = []
    for session in sessions:
        features = build_opportunity_feature_ledger(market_db, snapshot_db, session)
        if not features.get("ok"):
            failures.append({"session_date": session, "stage": "feature_ledger", "reason": features.get("reason")})
            continue
        regime = build_market_regime_ledger(market_db, session)
        if not regime.get("ok"):
            failures.append({"session_date": session, "stage": "market_regime", "reason": regime.get("reason")})
            continue
        completed.append({
            "session_date": session,
            "feature_rows": features.get("persisted_rows"),
            "classification_status": regime.get("classification_status"),
            "regime_family": regime.get("regime_family"),
        })
    return {
        "ok": not failures and bool(completed),
        "contract_version": "market-regime-research-backfill-v1",
        "requested_from": from_date,
        "requested_through": through_date,
        "requested_sessions": len(sessions),
        "completed_sessions": len(completed),
        "failed_sessions": len(failures),
        "completed": completed,
        "failures": failures,
        "execution_boundary": "bounded maintenance operation; not invoked by ordinary page reads",
    }


SECTOR_TREND_CONTRACT = "sector-trend-classification-v1"
SECTOR_TREND_MINIMUM_COVERAGE_RATIO = 0.80
SECTOR_TREND_PERSISTENCE_SESSIONS = 5
SECTOR_TREND_VOLATILITY_BASELINE_SESSIONS = 10


def _mean_present(rows: list[dict[str, Any]], key: str) -> tuple[float | None, int]:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return (sum(values) / len(values), len(values)) if values else (None, 0)


def sector_trend_classification(
    feature_rows: list[Any], historical_sector_rows: list[Any] | None = None,
    session_date: str | None = None,
) -> dict[str, Any]:
    rows = [dict(row) for row in feature_rows]
    sessions = sorted({str(row.get("session_date") or "") for row in rows if row.get("session_date")})
    if session_date:
        date.fromisoformat(session_date)
    if len(sessions) > 1 or (session_date and sessions and sessions[0] != session_date):
        raise ValueError("sector_trend_requires_single_feature_session")
    resolved_session = session_date or (sessions[0] if sessions else None)
    if not rows or not resolved_session:
        return {"ok": False, "reason": "feature_ledger_session_unavailable"}

    history_by_sector: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for historical in historical_sector_rows or []:
        item = dict(historical)
        if item.get("sector") and str(item.get("session_date") or "") < resolved_session:
            history_by_sector[str(item["sector"])].append(item)
    for sector_history in history_by_sector.values():
        sector_history.sort(key=lambda item: str(item.get("session_date") or ""))

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("sector"):
            grouped[str(row["sector"])].append(row)
    generated_at = datetime.now(timezone.utc).isoformat()
    summaries: list[dict[str, Any]] = []
    for sector, members in sorted(grouped.items()):
        member_count = len(members)
        priced_count = sum(row.get("close") is not None for row in members)
        summary: dict[str, Any] = {
            "contract_version": SECTOR_TREND_CONTRACT,
            "session_date": resolved_session,
            "sector": sector,
            "member_count": member_count,
            "priced_count": priced_count,
            "constituent_coverage_ratio": priced_count / member_count if member_count else 0.0,
            "ranking_admissible": False,
            "sector_state": None,
            "generated_at": generated_at,
        }
        missing: list[str] = []
        for lookback in (20, 60, 120):
            value, count = _mean_present(members, f"return_{lookback}d_pct")
            coverage = count / member_count if member_count else 0.0
            summary[f"sector_return_{lookback}d_pct"] = _round(value, 6)
            summary[f"return_{lookback}d_coverage_ratio"] = _round(coverage, 6)
            market_values = {
                round(float(row[f"market_return_{lookback}d_pct"]), 8)
                for row in members if row.get(f"market_return_{lookback}d_pct") is not None
            }
            market_value = next(iter(market_values)) if len(market_values) == 1 else None
            summary[f"market_return_{lookback}d_pct"] = market_value
            summary[f"relative_strength_{lookback}d_pct"] = (
                _round(value - market_value, 6) if value is not None and market_value is not None else None
            )
            if coverage < SECTOR_TREND_MINIMUM_COVERAGE_RATIO:
                missing.append(f"return_{lookback}d_coverage_below_80_pct")
            if market_value is None:
                missing.append(f"market_return_{lookback}d_pct")

        breadth_rows = [row for row in members if row.get("distance_sma50_pct") is not None]
        alignment_rows = [
            row for row in members
            if all(row.get(key) is not None for key in (
                "distance_sma20_pct", "distance_sma50_pct", "distance_sma200_pct",
            ))
        ]
        slope, slope_count = _mean_present(members, "sma20_slope_5d_pct")
        volatility, volatility_count = _mean_present(members, "realised_volatility_20d_pct")
        summary["breadth_50dma_pct"] = _round(
            100 * sum(float(row["distance_sma50_pct"]) > 0 for row in breadth_rows) / len(breadth_rows), 6
        ) if breadth_rows else None
        summary["ma_alignment_pct"] = _round(
            100 * sum(
                float(row["distance_sma20_pct"]) > 0
                and float(row["distance_sma50_pct"]) > 0
                and float(row["distance_sma200_pct"]) > 0
                for row in alignment_rows
            ) / len(alignment_rows), 6
        ) if alignment_rows else None
        summary["average_sma20_slope_5d_pct"] = _round(slope, 6)
        summary["realised_volatility_20d_pct"] = _round(volatility, 6)
        for name, count in (
            ("breadth_50dma", len(breadth_rows)), ("ma_alignment", len(alignment_rows)),
            ("sma20_slope", slope_count), ("realised_volatility_20d", volatility_count),
        ):
            if not member_count or count / member_count < SECTOR_TREND_MINIMUM_COVERAGE_RATIO:
                missing.append(f"{name}_coverage_below_80_pct")
        if summary["constituent_coverage_ratio"] < SECTOR_TREND_MINIMUM_COVERAGE_RATIO:
            missing.append("constituent_coverage_below_80_pct")
        summary["missing_inputs"] = missing
        summary["source_dates"] = {
            "feature_session": resolved_session,
            "sector_source_dates": sorted({
                str(row["sector_source_date"]) for row in members if row.get("sector_source_date")
            }),
        }
        summaries.append(summary)

    for lookback in (20, 60, 120):
        _assign_descending_rank(
            summaries, f"relative_strength_{lookback}d_pct", f"rank_{lookback}d"
        )
    sector_count = len(summaries)
    top_rank_cutoff = max(1, math.ceil(sector_count * 0.30))
    for summary in summaries:
        history = history_by_sector.get(summary["sector"], [])
        previous_five = history[-SECTOR_TREND_PERSISTENCE_SESSIONS:]
        previous_ten = history[-SECTOR_TREND_VOLATILITY_BASELINE_SESSIONS:]
        if len(previous_five) < SECTOR_TREND_PERSISTENCE_SESSIONS:
            summary["missing_inputs"].append("five_session_sector_history")
            summary["persistence_ratio"] = None
            summary["acceleration_5_sessions_pct"] = None
        else:
            persistence_window = previous_five[-4:] + [summary]
            persistent = sum(
                float(row.get("relative_strength_20d_pct") or 0) > 0
                and int(row.get("rank_20d") or sector_count + 1) <= math.ceil(sector_count / 2)
                for row in persistence_window
            )
            summary["persistence_ratio"] = _round(persistent / len(persistence_window), 6)
            baseline = previous_five[0].get("relative_strength_20d_pct")
            current = summary.get("relative_strength_20d_pct")
            summary["acceleration_5_sessions_pct"] = (
                _round(float(current) - float(baseline), 6)
                if current is not None and baseline is not None else None
            )
            if summary["acceleration_5_sessions_pct"] is None:
                summary["missing_inputs"].append("acceleration_5_sessions_pct")
        prior_volatility = [
            float(row["realised_volatility_20d_pct"])
            for row in previous_ten if row.get("realised_volatility_20d_pct") is not None
        ]
        if len(previous_ten) < SECTOR_TREND_VOLATILITY_BASELINE_SESSIONS or len(prior_volatility) < 8:
            summary["volatility_expansion_ratio"] = None
            summary["missing_inputs"].append("ten_session_volatility_history")
        else:
            baseline_volatility = median(prior_volatility)
            current_volatility = summary.get("realised_volatility_20d_pct")
            summary["volatility_expansion_ratio"] = (
                _round(float(current_volatility) / baseline_volatility, 6)
                if current_volatility is not None and baseline_volatility > 0 else None
            )
        summary["missing_inputs"] = sorted(set(summary["missing_inputs"]))
        universe_admissible = all(bool(row.get("ranking_admissible")) for row in grouped[summary["sector"]])
        if summary["missing_inputs"]:
            summary["classification_status"] = "gated"
        else:
            summary["classification_status"] = "ready" if universe_admissible else "descriptive_only"
            rel20 = float(summary["relative_strength_20d_pct"])
            rel60 = float(summary["relative_strength_60d_pct"])
            rel120 = float(summary["relative_strength_120d_pct"])
            breadth = float(summary["breadth_50dma_pct"])
            alignment = float(summary["ma_alignment_pct"])
            acceleration = float(summary["acceleration_5_sessions_pct"])
            persistence = float(summary["persistence_ratio"])
            if all(value > 0 for value in (rel20, rel60, rel120)) and int(summary["rank_20d"]) <= top_rank_cutoff and breadth >= 60 and alignment >= 50 and persistence >= 0.60:
                summary["sector_state"] = "leading"
            elif rel20 > 0 and acceleration > 0 and breadth >= 50:
                summary["sector_state"] = "improving"
            elif all(value < 0 for value in (rel20, rel60, rel120)) and breadth <= 40:
                summary["sector_state"] = "lagging"
            elif (rel20 < 0 or acceleration < 0) and breadth < 50:
                summary["sector_state"] = "weakening"
            else:
                summary["sector_state"] = "neutral"
        summary["ranking_admissible"] = bool(
            summary["classification_status"] == "ready" and universe_admissible
        )
        for ratio_key in (
            "constituent_coverage_ratio", "return_20d_coverage_ratio",
            "return_60d_coverage_ratio", "return_120d_coverage_ratio",
        ):
            summary[ratio_key] = _round(summary[ratio_key], 6)

    return {
        "ok": True,
        "contract_version": SECTOR_TREND_CONTRACT,
        "session_date": resolved_session,
        "rows": summaries,
        "coverage": {
            "feature_rows": len(rows),
            "classified_sector_rows": sum(row["sector_state"] is not None for row in summaries),
            "sectors": len(summaries),
        },
        "definitions": {
            "minimum_constituent_coverage": SECTOR_TREND_MINIMUM_COVERAGE_RATIO,
            "persistence_sessions": SECTOR_TREND_PERSISTENCE_SESSIONS,
            "volatility_baseline_sessions": SECTOR_TREND_VOLATILITY_BASELINE_SESSIONS,
            "forward_labels_used": False,
            "strategy_results_used": False,
        },
    }


# Preserve the public engine import while executing the focused component.
sector_trend_classification = _sector_trend_classification_component


_SECTOR_TREND_COLUMNS = (
    "contract_version", "session_date", "sector", "classification_status",
    "ranking_admissible", "sector_state", "member_count", "priced_count",
    "constituent_coverage_ratio", "return_20d_coverage_ratio", "return_60d_coverage_ratio",
    "return_120d_coverage_ratio", "sector_return_20d_pct", "sector_return_60d_pct",
    "sector_return_120d_pct", "market_return_20d_pct", "market_return_60d_pct",
    "market_return_120d_pct", "relative_strength_20d_pct", "relative_strength_60d_pct",
    "relative_strength_120d_pct", "rank_20d", "rank_60d", "rank_120d",
    "breadth_50dma_pct", "ma_alignment_pct", "average_sma20_slope_5d_pct",
    "realised_volatility_20d_pct", "persistence_ratio", "acceleration_5_sessions_pct",
    "volatility_expansion_ratio", "source_dates_json", "missing_inputs_json",
    "generated_at", "schema_version",
)


def _persist_sector_trends(connection: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    placeholders = ",".join("?" for _ in _SECTOR_TREND_COLUMNS)
    values = []
    for row in rows:
        stored = dict(row)
        stored["ranking_admissible"] = int(bool(row.get("ranking_admissible")))
        stored["source_dates_json"] = json.dumps(row.get("source_dates", {}), sort_keys=True)
        stored["missing_inputs_json"] = json.dumps(row.get("missing_inputs", []), sort_keys=True)
        stored["schema_version"] = 1
        values.append(tuple(stored.get(column) for column in _SECTOR_TREND_COLUMNS))
    if values:
        connection.executemany(
            f"INSERT OR REPLACE INTO sector_trend_ledger({','.join(_SECTOR_TREND_COLUMNS)}) VALUES ({placeholders})",
            values,
        )
    return len(values)


def build_sector_trend_ledger(
    market_db: Path = DEFAULT_MARKET_DB, as_of_date: str | None = None,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        session = as_of_date or connection.execute(
            "SELECT MAX(session_date) FROM opportunity_feature_ledger WHERE contract_version=?",
            (OPPORTUNITY_FEATURE_CONTRACT,),
        ).fetchone()[0]
        if not session:
            return {"ok": False, "reason": "feature_ledger_empty"}
        feature_rows = connection.execute(
            "SELECT * FROM opportunity_feature_ledger WHERE contract_version=? AND session_date=? ORDER BY sector,symbol",
            (OPPORTUNITY_FEATURE_CONTRACT, session),
        ).fetchall()
        historical = connection.execute(
            "SELECT * FROM sector_trend_ledger WHERE contract_version=? AND session_date<? ORDER BY session_date",
            (SECTOR_TREND_CONTRACT, session),
        ).fetchall()
        result = sector_trend_classification(feature_rows, historical, str(session))
        if result.get("ok"):
            result["persisted_rows"] = _persist_sector_trends(connection, result["rows"])
            connection.commit()
        return result
    finally:
        connection.close()


def sector_trend_history(
    market_db: Path = DEFAULT_MARKET_DB, from_date: str | None = None,
    through_date: str | None = None, sector: str | None = None, limit: int = 500,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    if limit < 1 or limit > 2000:
        raise ValueError("invalid_sector_trend_limit")
    if from_date:
        date.fromisoformat(from_date)
    if through_date:
        date.fromisoformat(through_date)
    if from_date and through_date and from_date > through_date:
        raise ValueError("invalid_sector_trend_window")
    connection = _connect(market_db)
    try:
        filters = ["contract_version=?"]
        parameters: list[Any] = [SECTOR_TREND_CONTRACT]
        for value, expression in ((from_date, "session_date>=?"), (through_date, "session_date<=?")):
            if value:
                filters.append(expression)
                parameters.append(value)
        if sector:
            filters.append("sector=?")
            parameters.append(sector)
        stored = connection.execute(
            f"SELECT * FROM sector_trend_ledger WHERE {' AND '.join(filters)} ORDER BY session_date DESC,rank_20d,sector LIMIT ?",
            (*parameters, limit),
        ).fetchall()
        rows = []
        for stored_row in stored:
            row = dict(stored_row)
            row["ranking_admissible"] = bool(row["ranking_admissible"])
            row["source_dates"] = json.loads(row.pop("source_dates_json"))
            row["missing_inputs"] = json.loads(row.pop("missing_inputs_json"))
            rows.append(row)
        return {
            "ok": bool(rows),
            "reason": None if rows else "sector_trend_ledger_empty",
            "contract_version": SECTOR_TREND_CONTRACT,
            "rows": rows,
            "decision_boundary": "Dated constituent features only; forward outcomes and strategy results are excluded.",
        }
    finally:
        connection.close()


def backfill_sector_trend_research(
    market_db: Path = DEFAULT_MARKET_DB, from_date: str | None = None,
    through_date: str | None = None, max_sessions: int = 252,
) -> dict[str, Any]:
    if not from_date or not through_date:
        raise ValueError("sector_trend_backfill_window_required")
    date.fromisoformat(from_date)
    date.fromisoformat(through_date)
    if from_date > through_date:
        raise ValueError("invalid_sector_trend_backfill_window")
    if max_sessions < 1 or max_sessions > 252:
        raise ValueError("invalid_sector_trend_backfill_limit")
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        sessions = [str(row[0]) for row in connection.execute(
            "SELECT DISTINCT session_date FROM opportunity_feature_ledger WHERE contract_version=? AND session_date BETWEEN ? AND ? ORDER BY session_date",
            (OPPORTUNITY_FEATURE_CONTRACT, from_date, through_date),
        )]
    finally:
        connection.close()
    if len(sessions) > max_sessions:
        raise ValueError("sector_trend_backfill_session_limit_exceeded")
    completed = []
    for session in sessions:
        result = build_sector_trend_ledger(market_db, session)
        completed.append({
            "session_date": session,
            "sector_rows": result.get("persisted_rows", 0),
            "classified_sector_rows": result.get("coverage", {}).get("classified_sector_rows", 0),
        })
    return {
        "ok": bool(completed),
        "contract_version": "sector-trend-research-backfill-v1",
        "requested_sessions": len(sessions),
        "completed": completed,
        "execution_boundary": "bounded maintenance operation; not invoked by ordinary page reads",
    }


STOCK_LEADERSHIP_CONTRACT = "stock-leadership-priming-v1"
STOCK_LEADERSHIP_HISTORY_SESSIONS = 5


def stock_leadership_classification(
    feature_rows: list[Any], sector_trend_rows: list[Any],
    historical_feature_rows: list[Any] | None = None, session_date: str | None = None,
) -> dict[str, Any]:
    rows = [dict(row) for row in feature_rows]
    sessions = sorted({str(row.get("session_date") or "") for row in rows if row.get("session_date")})
    if session_date:
        date.fromisoformat(session_date)
    if len(sessions) > 1 or (session_date and sessions and sessions[0] != session_date):
        raise ValueError("stock_leadership_requires_single_feature_session")
    resolved_session = session_date or (sessions[0] if sessions else None)
    if not rows or not resolved_session:
        return {"ok": False, "reason": "feature_ledger_session_unavailable"}

    sector_by_name = {str(dict(row).get("sector")): dict(row) for row in sector_trend_rows}
    history_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for historical in historical_feature_rows or []:
        item = dict(historical)
        if item.get("symbol") and str(item.get("session_date") or "") < resolved_session:
            history_by_symbol[str(item["symbol"])].append(item)
    for history in history_by_symbol.values():
        history.sort(key=lambda item: str(item.get("session_date") or ""))

    generated_at = datetime.now(timezone.utc).isoformat()
    output: list[dict[str, Any]] = []
    for feature in rows:
        sector = str(feature.get("sector") or "")
        sector_row = sector_by_name.get(sector, {})
        missing: list[str] = []
        gating: list[str] = []

        def difference(left: str, right: str, name: str, required: bool = True) -> float | None:
            left_value = feature.get(left)
            right_value = feature.get(right)
            if left_value is None or right_value is None:
                missing.append(name)
                if required:
                    gating.append(name)
                return None
            return _round(float(left_value) - float(right_value), 6)

        relative_nifty = {
            lookback: difference(
                f"return_{lookback}d_pct", f"market_return_{lookback}d_pct",
                f"relative_nifty_{lookback}d_pct",
            ) for lookback in (20, 60, 120)
        }
        relative_sector = {
            lookback: difference(
                f"return_{lookback}d_pct", f"sector_return_{lookback}d_pct",
                f"relative_sector_{lookback}d_pct",
            ) for lookback in (20, 60, 120)
        }
        if not sector:
            missing.append("sector_classification")
            gating.append("sector_classification")
        sector_state = sector_row.get("sector_state")
        if not sector_state:
            missing.append("published_sector_state")
            gating.append("published_sector_state")

        history = history_by_symbol.get(str(feature.get("symbol") or ""), [])
        previous = history[-STOCK_LEADERSHIP_HISTORY_SESSIONS:]
        persistence = None
        acceleration = None
        if len(previous) < STOCK_LEADERSHIP_HISTORY_SESSIONS:
            missing.append("five_session_stock_history")
            gating.append("five_session_stock_history")
        else:
            historical_relative = []
            for item in previous[-4:]:
                if item.get("return_20d_pct") is not None and item.get("sector_return_20d_pct") is not None:
                    historical_relative.append(
                        float(item["return_20d_pct"]) - float(item["sector_return_20d_pct"])
                    )
            if relative_sector[20] is not None and len(historical_relative) == 4:
                persistence = _round(
                    sum(value > 0 for value in historical_relative + [float(relative_sector[20])]) / 5, 6
                )
            else:
                missing.append("relative_strength_persistence_ratio")
                gating.append("relative_strength_persistence_ratio")
            baseline = previous[0]
            if (
                relative_sector[20] is not None
                and baseline.get("return_20d_pct") is not None
                and baseline.get("sector_return_20d_pct") is not None
            ):
                baseline_relative = float(baseline["return_20d_pct"]) - float(baseline["sector_return_20d_pct"])
                acceleration = _round(float(relative_sector[20]) - baseline_relative, 6)
            else:
                missing.append("relative_strength_acceleration_5_sessions_pct")
                gating.append("relative_strength_acceleration_5_sessions_pct")

        distance20 = feature.get("distance_sma20_pct")
        distance50 = feature.get("distance_sma50_pct")
        distance200 = feature.get("distance_sma200_pct")
        if all(value is not None for value in (distance20, distance50, distance200)):
            if all(float(value) > 0 for value in (distance20, distance50, distance200)):
                ma_alignment = "bullish"
            elif all(float(value) < 0 for value in (distance20, distance50, distance200)):
                ma_alignment = "bearish"
            else:
                ma_alignment = "mixed"
        else:
            ma_alignment = None
            missing.append("ma_alignment")
            gating.append("ma_alignment")

        optional_fields = (
            "sma20_50_crossover_age_sessions", "volume_ratio_20d", "fno_state",
        )
        for key in optional_fields:
            if feature.get(key) is None:
                missing.append(key)
        required_fields = (
            "sma20_slope_5d_pct", "sma20_50_spread_pct", "range_compression_ratio_20v60",
            "breakout_proximity_20d_pct", "volatility_expansion_ratio_20v60",
        )
        for key in required_fields:
            if feature.get(key) is None:
                missing.append(key)
                gating.append(key)

        row = {
            "contract_version": STOCK_LEADERSHIP_CONTRACT,
            "session_date": resolved_session,
            "instrument_id": feature.get("instrument_id"),
            "symbol": feature.get("symbol"),
            "sector": feature.get("sector"),
            "classification_status": None,
            "ranking_admissible": False,
            "leadership_state": None,
            "setup_state": None,
            "sector_state": sector_state,
            "return_20d_pct": feature.get("return_20d_pct"),
            "return_60d_pct": feature.get("return_60d_pct"),
            "return_120d_pct": feature.get("return_120d_pct"),
            "relative_nifty_20d_pct": relative_nifty[20],
            "relative_nifty_60d_pct": relative_nifty[60],
            "relative_nifty_120d_pct": relative_nifty[120],
            "relative_sector_20d_pct": relative_sector[20],
            "relative_sector_60d_pct": relative_sector[60],
            "relative_sector_120d_pct": relative_sector[120],
            "sector_rank_20d": sector_row.get("rank_20d"),
            "stock_sector_rank_20d": None,
            "stock_sector_rank_60d": None,
            "stock_sector_rank_120d": None,
            "stock_universe_rank_20d": None,
            "stock_universe_rank_60d": None,
            "stock_universe_rank_120d": None,
            "relative_strength_persistence_ratio": persistence,
            "relative_strength_acceleration_5_sessions_pct": acceleration,
            "ma_alignment": ma_alignment,
            "sma20_slope_5d_pct": feature.get("sma20_slope_5d_pct"),
            "sma20_50_spread_pct": feature.get("sma20_50_spread_pct"),
            "sma20_50_crossover_age_sessions": feature.get("sma20_50_crossover_age_sessions"),
            "range_compression_ratio_20v60": feature.get("range_compression_ratio_20v60"),
            "breakout_proximity_20d_pct": feature.get("breakout_proximity_20d_pct"),
            "volatility_expansion_ratio_20v60": feature.get("volatility_expansion_ratio_20v60"),
            "volume_ratio_20d": feature.get("volume_ratio_20d"),
            "participation_status": "volume_available" if feature.get("volume_ratio_20d") is not None else "delivery_and_volume_unavailable",
            "fno_eligible": bool(feature.get("fno_eligible")),
            "fno_state": feature.get("fno_state"),
            "source_dates": {
                "feature_session": resolved_session,
                "sector_source_date": feature.get("sector_source_date"),
                "sector_trend_session": sector_row.get("session_date"),
                "fno_current_session": (
                    json.loads(feature["source_dates_json"]).get("fno_current_session")
                    if feature.get("source_dates_json") else None
                ),
            },
            "missing_inputs": sorted(set(missing)),
            "_gating_inputs": sorted(set(gating)),
            "_feature_ranking_admissible": bool(feature.get("ranking_admissible")),
            "_sector_ranking_admissible": bool(sector_row.get("ranking_admissible")),
            "generated_at": generated_at,
        }
        output.append(row)

    sector_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in output:
        if row.get("sector"):
            sector_groups[str(row["sector"])].append(row)
    for sector_rows in sector_groups.values():
        for lookback in (20, 60, 120):
            _assign_descending_rank(
                sector_rows, f"relative_sector_{lookback}d_pct", f"stock_sector_rank_{lookback}d"
            )
    for lookback in (20, 60, 120):
        _assign_descending_rank(output, f"relative_nifty_{lookback}d_pct", f"stock_universe_rank_{lookback}d")

    for row in output:
        gating = row.pop("_gating_inputs")
        feature_admissible = row.pop("_feature_ranking_admissible")
        sector_admissible = row.pop("_sector_ranking_admissible")
        if gating:
            row["classification_status"] = "gated"
            continue
        row["classification_status"] = "ready" if feature_admissible and sector_admissible else "descriptive_only"
        sector_size = len(sector_groups[str(row["sector"])])
        top_cutoff = max(1, math.ceil(sector_size * 0.30))
        rel20 = float(row["relative_sector_20d_pct"])
        rel60 = float(row["relative_sector_60d_pct"])
        persistence = float(row["relative_strength_persistence_ratio"])
        acceleration = float(row["relative_strength_acceleration_5_sessions_pct"])
        slope = float(row["sma20_slope_5d_pct"])
        if rel20 > 0 and rel60 > 0 and int(row["stock_sector_rank_20d"]) <= top_cutoff and persistence >= 0.60 and row["ma_alignment"] == "bullish":
            row["leadership_state"] = "leading"
        elif rel20 > 0 and acceleration > 0 and slope > 0:
            row["leadership_state"] = "improving"
        elif rel20 < 0 and rel60 < 0 and row["ma_alignment"] == "bearish":
            row["leadership_state"] = "lagging"
        elif rel20 < 0 or acceleration < 0:
            row["leadership_state"] = "weakening"
        else:
            row["leadership_state"] = "neutral"

        breakout = float(row["breakout_proximity_20d_pct"])
        compression = float(row["range_compression_ratio_20v60"])
        distance20 = float(next(
            feature["distance_sma20_pct"] for feature in rows
            if feature.get("instrument_id") == row["instrument_id"]
        ))
        if distance20 >= 8 or breakout >= 3:
            row["setup_state"] = "extended"
        elif breakout >= 0 and row["ma_alignment"] == "bullish":
            row["setup_state"] = "triggered"
        elif -3 <= breakout < 0 and compression <= 0.80 and row["ma_alignment"] == "bullish" and row["leadership_state"] in ("leading", "improving") and row["sector_state"] in ("leading", "improving"):
            row["setup_state"] = "approaching_trigger"
        elif breakout >= -6 and slope > 0 and row["ma_alignment"] == "bullish" and row["leadership_state"] in ("leading", "improving") and row["sector_state"] in ("leading", "improving"):
            row["setup_state"] = "primed"
        elif row["sector_state"] in ("weakening", "lagging") or row["leadership_state"] in ("weakening", "lagging"):
            row["setup_state"] = "conflicted"
        else:
            row["setup_state"] = "unprimed"
        row["ranking_admissible"] = bool(
            row["classification_status"] == "ready" and feature_admissible and sector_admissible
        )

    return {
        "ok": True,
        "contract_version": STOCK_LEADERSHIP_CONTRACT,
        "session_date": resolved_session,
        "rows": output,
        "coverage": {
            "feature_rows": len(rows),
            "classified_rows": sum(row["leadership_state"] is not None for row in output),
            "gated_rows": sum(row["classification_status"] == "gated" for row in output),
            "sectors": len(sector_groups),
        },
        "definitions": {
            "trigger": "completed close at or above the prior 20-session high with bullish MA alignment",
            "approaching_trigger": "within 3% below the prior 20-session high, compressed 20/60 range, bullish alignment, and leading/improving stock plus sector",
            "extended": "at least 8% above the 20DMA or at least 3% above the prior 20-session high",
            "delivery_participation": "not stored; volume participation is used only when available",
            "forward_labels_used": False,
            "strategy_results_used": False,
        },
    }


# Preserve the public engine import while executing the focused component.
stock_leadership_classification = _stock_leadership_classification_component


_STOCK_LEADERSHIP_COLUMNS = (
    "contract_version", "session_date", "instrument_id", "symbol", "sector",
    "classification_status", "ranking_admissible", "leadership_state", "setup_state",
    "sector_state", "return_20d_pct", "return_60d_pct", "return_120d_pct",
    "relative_nifty_20d_pct", "relative_nifty_60d_pct", "relative_nifty_120d_pct",
    "relative_sector_20d_pct", "relative_sector_60d_pct", "relative_sector_120d_pct",
    "sector_rank_20d", "stock_sector_rank_20d", "stock_sector_rank_60d",
    "stock_sector_rank_120d", "stock_universe_rank_20d", "stock_universe_rank_60d",
    "stock_universe_rank_120d", "relative_strength_persistence_ratio",
    "relative_strength_acceleration_5_sessions_pct", "ma_alignment", "sma20_slope_5d_pct",
    "sma20_50_spread_pct", "sma20_50_crossover_age_sessions",
    "range_compression_ratio_20v60", "breakout_proximity_20d_pct",
    "volatility_expansion_ratio_20v60", "volume_ratio_20d", "participation_status",
    "fno_eligible", "fno_state", "source_dates_json", "missing_inputs_json",
    "generated_at", "schema_version",
)


def _persist_stock_leadership(connection: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    placeholders = ",".join("?" for _ in _STOCK_LEADERSHIP_COLUMNS)
    values = []
    for row in rows:
        stored = dict(row)
        stored["ranking_admissible"] = int(bool(row.get("ranking_admissible")))
        stored["fno_eligible"] = int(bool(row.get("fno_eligible")))
        stored["source_dates_json"] = json.dumps(row.get("source_dates", {}), sort_keys=True)
        stored["missing_inputs_json"] = json.dumps(row.get("missing_inputs", []), sort_keys=True)
        stored["schema_version"] = 1
        values.append(tuple(stored.get(column) for column in _STOCK_LEADERSHIP_COLUMNS))
    if values:
        connection.executemany(
            f"INSERT OR REPLACE INTO stock_leadership_ledger({','.join(_STOCK_LEADERSHIP_COLUMNS)}) VALUES ({placeholders})",
            values,
        )
    return len(values)


def build_stock_leadership_ledger(
    market_db: Path = DEFAULT_MARKET_DB, as_of_date: str | None = None,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        session = as_of_date or connection.execute(
            "SELECT MAX(session_date) FROM opportunity_feature_ledger WHERE contract_version=?",
            (OPPORTUNITY_FEATURE_CONTRACT,),
        ).fetchone()[0]
        if not session:
            return {"ok": False, "reason": "feature_ledger_empty"}
        features = connection.execute(
            "SELECT * FROM opportunity_feature_ledger WHERE contract_version=? AND session_date=? ORDER BY symbol",
            (OPPORTUNITY_FEATURE_CONTRACT, session),
        ).fetchall()
        sectors = connection.execute(
            "SELECT * FROM sector_trend_ledger WHERE contract_version=? AND session_date=? ORDER BY sector",
            (SECTOR_TREND_CONTRACT, session),
        ).fetchall()
        if not sectors:
            return {"ok": False, "reason": "sector_trend_session_unavailable"}
        historical = connection.execute(
            """SELECT * FROM opportunity_feature_ledger
               WHERE contract_version=? AND session_date<?
               ORDER BY session_date""",
            (OPPORTUNITY_FEATURE_CONTRACT, session),
        ).fetchall()
        result = stock_leadership_classification(features, sectors, historical, str(session))
        if result.get("ok"):
            result["persisted_rows"] = _persist_stock_leadership(connection, result["rows"])
            connection.commit()
        return result
    finally:
        connection.close()


def stock_leadership_history(
    market_db: Path = DEFAULT_MARKET_DB, as_of_date: str | None = None,
    symbol: str | None = None, sector: str | None = None, setup_state: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    if limit < 1 or limit > 2000:
        raise ValueError("invalid_stock_leadership_limit")
    if as_of_date:
        date.fromisoformat(as_of_date)
    connection = _connect(market_db)
    try:
        session = as_of_date or connection.execute(
            "SELECT MAX(session_date) FROM stock_leadership_ledger WHERE contract_version=?",
            (STOCK_LEADERSHIP_CONTRACT,),
        ).fetchone()[0]
        if not session:
            return {"ok": False, "reason": "stock_leadership_ledger_empty"}
        filters = ["contract_version=?", "session_date=?"]
        parameters: list[Any] = [STOCK_LEADERSHIP_CONTRACT, session]
        for value, expression in (
            (symbol.strip().upper() if symbol else None, "symbol=?"),
            (sector, "sector=?"), (setup_state, "setup_state=?"),
        ):
            if value:
                filters.append(expression)
                parameters.append(value)
        stored = connection.execute(
            f"SELECT * FROM stock_leadership_ledger WHERE {' AND '.join(filters)} ORDER BY stock_universe_rank_20d,symbol LIMIT ?",
            (*parameters, limit),
        ).fetchall()
        rows = []
        for stored_row in stored:
            row = dict(stored_row)
            row["ranking_admissible"] = bool(row["ranking_admissible"])
            row["fno_eligible"] = bool(row["fno_eligible"])
            row["source_dates"] = json.loads(row.pop("source_dates_json"))
            row["missing_inputs"] = json.loads(row.pop("missing_inputs_json"))
            rows.append(row)
        return {
            "ok": bool(rows),
            "reason": None if rows else "stock_leadership_rows_unavailable",
            "contract_version": STOCK_LEADERSHIP_CONTRACT,
            "session_date": session,
            "rows": rows,
            "decision_boundary": "Completed-session descriptive evidence only; no forward labels, strategy results or trade instructions.",
        }
    finally:
        connection.close()


def backfill_stock_leadership_research(
    market_db: Path = DEFAULT_MARKET_DB, from_date: str | None = None,
    through_date: str | None = None, max_sessions: int = 252,
) -> dict[str, Any]:
    if not from_date or not through_date:
        raise ValueError("stock_leadership_backfill_window_required")
    date.fromisoformat(from_date)
    date.fromisoformat(through_date)
    if from_date > through_date:
        raise ValueError("invalid_stock_leadership_backfill_window")
    if max_sessions < 1 or max_sessions > 252:
        raise ValueError("invalid_stock_leadership_backfill_limit")
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        sessions = [str(row[0]) for row in connection.execute(
            "SELECT DISTINCT session_date FROM sector_trend_ledger WHERE contract_version=? AND session_date BETWEEN ? AND ? ORDER BY session_date",
            (SECTOR_TREND_CONTRACT, from_date, through_date),
        )]
    finally:
        connection.close()
    if len(sessions) > max_sessions:
        raise ValueError("stock_leadership_backfill_session_limit_exceeded")
    completed = []
    for session in sessions:
        result = build_stock_leadership_ledger(market_db, session)
        completed.append({
            "session_date": session,
            "stock_rows": result.get("persisted_rows", 0),
            "classified_rows": result.get("coverage", {}).get("classified_rows", 0),
            "gated_rows": result.get("coverage", {}).get("gated_rows", 0),
        })
    return {
        "ok": bool(completed),
        "contract_version": "stock-leadership-research-backfill-v1",
        "requested_sessions": len(sessions),
        "completed": completed,
        "execution_boundary": "bounded maintenance operation; not invoked by ordinary page reads",
    }


CROSSOVER_CONDITIONING_CONTRACT = "crossover-state-conditioning-v1"
CROSSOVER_CONDITIONING_MINIMUM_TRADES = 15


def _conditioned_trade_metrics(trades: list[dict[str, Any]]) -> dict[str, Any]:
    gross = [float(row["gross_return_pct"]) for row in trades if row.get("gross_return_pct") is not None]
    net = [float(row["net_return_pct"]) for row in trades if row.get("net_return_pct") is not None]

    def profit_factor(values: list[float]) -> float | None:
        profits = sum(value for value in values if value > 0)
        losses = abs(sum(value for value in values if value < 0))
        return profits / losses if losses > 0 else None

    trades_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trades:
        trades_by_symbol[str(row.get("symbol") or "single_series")].append(row)
    symbol_drawdowns = []
    for symbol_trades in trades_by_symbol.values():
        symbol_trades.sort(key=lambda row: str(row.get("exit_execution_date") or ""))
        equity = [1.0]
        for row in symbol_trades:
            if row.get("net_return_pct") is not None:
                equity.append(equity[-1] * (1.0 + float(row["net_return_pct"]) / 100.0))
        if len(equity) > 1:
            symbol_drawdowns.append(_maximum_drawdown_pct(equity))
    captures = [float(row["move_capture_ratio"]) for row in trades if row.get("move_capture_ratio") is not None]
    return {
        "completed_trades": len(net),
        "gross_trade_expectancy_pct": _round(mean(gross), 6) if gross else None,
        "net_trade_expectancy_pct": _round(mean(net), 6) if net else None,
        "gross_profit_factor": _round(profit_factor(gross), 6),
        "net_profit_factor": _round(profit_factor(net), 6),
        "net_maximum_drawdown_pct": min(symbol_drawdowns) if symbol_drawdowns else None,
        "average_move_capture_ratio": _round(mean(captures), 6) if captures else None,
    }


def crossover_state_conditioning_contract(
    joined_trades: list[dict[str, Any]],
    state_observations: dict[tuple[str, str], int],
) -> dict[str, Any]:
    dimensions = (
        "market_regime", "sector_state", "stock_leadership", "setup_state", "fno_state",
    )
    strategies = ("sma_20_50", "ema_20_50")
    rows = []
    for strategy in strategies:
        strategy_trades = [row for row in joined_trades if row.get("strategy") == strategy]
        for dimension in dimensions:
            states = sorted({
                state for (observed_dimension, state), count in state_observations.items()
                if observed_dimension == dimension and count > 0
            })
            for state in states:
                bucket = [row for row in strategy_trades if row.get(dimension) == state]
                metrics = _conditioned_trade_metrics(bucket)
                missing = []
                if metrics["completed_trades"] < CROSSOVER_CONDITIONING_MINIMUM_TRADES:
                    missing.append("completed_trades_below_15")
                if metrics["net_profit_factor"] is None:
                    missing.append("net_profit_factor_unavailable")
                if metrics["average_move_capture_ratio"] is None:
                    missing.append("move_capture_unavailable")
                rows.append({
                    "contract_version": CROSSOVER_CONDITIONING_CONTRACT,
                    "strategy": strategy,
                    "dimension": dimension,
                    "state": state,
                    "state_observations": int(state_observations[(dimension, state)]),
                    **metrics,
                    "evidence_status": "descriptive_sample_sufficient" if not missing else "insufficient_evidence",
                    "missing_evidence": missing,
                })
    return {
        "ok": True,
        "contract_version": CROSSOVER_CONDITIONING_CONTRACT,
        "parameters": {
            "sma_windows": [20, 50],
            "ema_windows": [20, 50],
            "execution": "completed_close_signal_executed_next_session_open",
            "cost_model": EQUITY_DELIVERY_COST_SCHEDULE["schedule_version"],
            "minimum_completed_trades": CROSSOVER_CONDITIONING_MINIMUM_TRADES,
            "parameters_optimised": False,
        },
        "joined_completed_trades": len(joined_trades),
        "rows": rows,
    }


def build_crossover_state_conditioning(
    market_db: Path = DEFAULT_MARKET_DB, return_internal: bool = False,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        reference_context = _backtest_reference_context(connection)
        instruments = connection.execute(
            """SELECT DISTINCT i.instrument_id,i.display_name
               FROM instruments i JOIN stock_leadership_ledger s USING(instrument_id)
               WHERE i.kind='stock' ORDER BY i.instrument_id"""
        ).fetchall()
        market_states = {
            str(row["session_date"]): str(row["regime_family"])
            for row in connection.execute(
                """SELECT session_date,regime_family FROM market_regime_ledger
                   WHERE contract_version=? AND classification_status!='gated' AND regime_family IS NOT NULL""",
                (MARKET_REGIME_CONTRACT,),
            )
        }
        stock_states = {
            (str(row["session_date"]), str(row["symbol"])): dict(row)
            for row in connection.execute(
                """SELECT session_date,symbol,sector_state,leadership_state,setup_state,
                          fno_eligible,fno_state
                   FROM stock_leadership_ledger
                   WHERE contract_version=? AND classification_status!='gated'""",
                (STOCK_LEADERSHIP_CONTRACT,),
            )
        }
        state_observations: dict[tuple[str, str], int] = defaultdict(int)
        for state in market_states.values():
            state_observations[("market_regime", state)] += 1
        for row in stock_states.values():
            for dimension, key in (
                ("sector_state", "sector_state"),
                ("stock_leadership", "leadership_state"),
                ("setup_state", "setup_state"),
            ):
                if row.get(key):
                    state_observations[(dimension, str(row[key]))] += 1
            if row.get("fno_state"):
                normalised_fno_state = (
                    "neutral" if row["fno_state"] == "no_clear_signal" else str(row["fno_state"])
                )
                state_observations[("fno_state", normalised_fno_state)] += 1

        joined_trades: list[dict[str, Any]] = []
        baseline_trades: list[dict[str, Any]] = []
        quality_gated = 0
        completed_before_join = 0
        for instrument in instruments:
            prepared, audit = _quality_checked_backtest_series(
                connection, instrument["instrument_id"], 51, reference_context, "comparison"
            )
            if audit.get("status") != "ready":
                quality_gated += 1
                continue
            symbol = str(instrument["instrument_id"]).split(":", 1)[-1]
            for strategy, runner in (
                ("sma_20_50", _moving_average_crossover_series),
                ("ema_20_50", _ema_crossover_series),
            ):
                result = runner(prepared, 20, 50)
                trades = result.get("_completed_trades") or []
                completed_before_join += len(trades)
                for trade in trades:
                    baseline_trades.append({**trade, "strategy": strategy, "symbol": symbol})
                    entry_date = str(trade.get("entry_signal_date") or "")
                    stock_state = stock_states.get((entry_date, symbol))
                    market_state = market_states.get(entry_date)
                    if not stock_state or not market_state:
                        continue
                    joined_trades.append({
                        **trade,
                        "strategy": strategy,
                        "symbol": symbol,
                        "market_regime": market_state,
                        "sector_state": stock_state.get("sector_state"),
                        "stock_leadership": stock_state.get("leadership_state"),
                        "setup_state": stock_state.get("setup_state"),
                        "fno_state": (
                            "neutral" if stock_state.get("fno_state") == "no_clear_signal"
                            else stock_state.get("fno_state")
                        ),
                    })
        contract = crossover_state_conditioning_contract(joined_trades, state_observations)
        generated_at = datetime.now(timezone.utc).isoformat()
        connection.execute(
            "DELETE FROM crossover_conditioning_ledger WHERE contract_version=?",
            (CROSSOVER_CONDITIONING_CONTRACT,),
        )
        for row in contract["rows"]:
            connection.execute(
                """INSERT INTO crossover_conditioning_ledger(
                       contract_version,generated_at,strategy,dimension,state,state_observations,
                       completed_trades,gross_trade_expectancy_pct,net_trade_expectancy_pct,
                       gross_profit_factor,net_profit_factor,net_maximum_drawdown_pct,
                       average_move_capture_ratio,evidence_status,missing_evidence_json,schema_version)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
                (
                    row["contract_version"], generated_at, row["strategy"], row["dimension"],
                    row["state"], row["state_observations"], row["completed_trades"],
                    row["gross_trade_expectancy_pct"], row["net_trade_expectancy_pct"],
                    row["gross_profit_factor"], row["net_profit_factor"],
                    row["net_maximum_drawdown_pct"], row["average_move_capture_ratio"],
                    row["evidence_status"], json.dumps(row["missing_evidence"], sort_keys=True),
                ),
            )
        connection.commit()
        contract.update({
            "generated_at": generated_at,
            "instruments_requested": len(instruments),
            "quality_gated_instruments": quality_gated,
            "completed_trades_before_state_join": completed_before_join,
            "completed_trades_after_state_join": len(joined_trades),
            "fno_available_state_observations": sum(
                count for (dimension, _state), count in state_observations.items()
                if dimension == "fno_state"
            ),
            "fno_unavailable_state_observations": sum(
                not bool(row.get("fno_state")) for row in stock_states.values()
            ),
            "completed_trades_with_fno_state": sum(bool(row.get("fno_state")) for row in joined_trades),
            "completed_trades_without_fno_state": sum(not bool(row.get("fno_state")) for row in joined_trades),
            "persisted_rows": len(contract["rows"]),
            "sample_boundary": "Only completed trades whose entry signal date has admissible frozen state ledgers are joined; sample sufficiency is not out-of-sample edge validation.",
            "fno_boundary": "Same front-month contract is compared only with its prior completed observation; stale gaps, missing baselines and expiry/roll changes remain unavailable rather than inferred.",
        })
        if return_internal:
            contract["_baseline_trades"] = baseline_trades
            contract["_joined_trades"] = joined_trades
        return contract
    finally:
        connection.close()


def crossover_state_conditioning_history(
    market_db: Path = DEFAULT_MARKET_DB, strategy: str | None = None,
    dimension: str | None = None,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        filters = ["contract_version=?"]
        parameters: list[Any] = [CROSSOVER_CONDITIONING_CONTRACT]
        for value, expression in ((strategy, "strategy=?"), (dimension, "dimension=?")):
            if value:
                filters.append(expression)
                parameters.append(value)
        stored = connection.execute(
            f"SELECT * FROM crossover_conditioning_ledger WHERE {' AND '.join(filters)} ORDER BY strategy,dimension,state",
            parameters,
        ).fetchall()
        rows = []
        for stored_row in stored:
            row = dict(stored_row)
            row["missing_evidence"] = json.loads(row.pop("missing_evidence_json"))
            rows.append(row)
        return {
            "ok": bool(rows),
            "reason": None if rows else "crossover_conditioning_ledger_empty",
            "contract_version": CROSSOVER_CONDITIONING_CONTRACT,
            "rows": rows,
            "decision_boundary": "Descriptive conditioning of frozen baselines; no parameter search and no ranking is published for insufficient samples.",
        }
    finally:
        connection.close()


CROSSOVER_ABLATION_CONTRACT = "crossover-top-down-ablation-v1"


def crossover_ablation_contract(
    baseline_trades: list[dict[str, Any]], joined_trades: list[dict[str, Any]],
) -> dict[str, Any]:
    stage_definitions = (
        {
            "key": "crossover_only", "label": "Crossover only",
            "rule": "All completed trades from the unchanged 20/50 baseline.",
        },
        {
            "key": "market_regime", "label": "+ market regime",
            "rule": "Entry state is bullish_trend or transition.",
        },
        {
            "key": "sector_trend", "label": "+ sector trend",
            "rule": "Sector state is leading or improving.",
        },
        {
            "key": "stock_priming", "label": "+ stock leadership / priming",
            "rule": "Stock is leading/improving and setup is primed, approaching_trigger or triggered.",
        },
        {
            "key": "fno_alignment", "label": "+ F&O state",
            "rule": "Same-contract F&O state is long_build_up or short_covering.",
        },
    )
    rows = []
    for strategy in ("sma_20_50", "ema_20_50"):
        baseline = [row for row in baseline_trades if row.get("strategy") == strategy]
        state_joined = [row for row in joined_trades if row.get("strategy") == strategy]
        selected = baseline
        previous_metrics = None
        for order, definition in enumerate(stage_definitions, start=1):
            prior_selected = selected
            if order == 1:
                eligible = baseline
                selected = baseline
            elif order == 2:
                eligible = state_joined
                selected = [
                    row for row in eligible
                    if row.get("market_regime") in ("bullish_trend", "transition")
                ]
            elif order == 3:
                eligible = [row for row in prior_selected if row.get("sector_state")]
                selected = [
                    row for row in eligible
                    if row.get("sector_state") in ("leading", "improving")
                ]
            elif order == 4:
                eligible = [
                    row for row in prior_selected
                    if row.get("stock_leadership") and row.get("setup_state")
                ]
                selected = [
                    row for row in eligible
                    if row.get("stock_leadership") in ("leading", "improving")
                    and row.get("setup_state") in ("primed", "approaching_trigger", "triggered")
                ]
            else:
                eligible = [row for row in prior_selected if row.get("fno_state")]
                selected = [
                    row for row in eligible
                    if row.get("fno_state") in ("long_build_up", "short_covering")
                ]
            metrics = _conditioned_trade_metrics(selected)

            def delta(key: str) -> float | None:
                if not previous_metrics or metrics.get(key) is None or previous_metrics.get(key) is None:
                    return None
                return _round(float(metrics[key]) - float(previous_metrics[key]), 6)

            delta_expectancy = delta("net_trade_expectancy_pct")
            delta_pf = delta("net_profit_factor")
            delta_drawdown = delta("net_maximum_drawdown_pct")
            delta_capture = delta("average_move_capture_ratio")
            sample_status = (
                "descriptive_sample_sufficient"
                if metrics["completed_trades"] >= CROSSOVER_CONDITIONING_MINIMUM_TRADES
                else "insufficient_evidence"
            )
            if order == 1:
                incremental_status = "baseline"
            elif sample_status != "descriptive_sample_sufficient":
                incremental_status = "insufficient_evidence"
            elif (
                delta_expectancy is not None and delta_expectancy > 0
                and any(value is not None and value > 0 for value in (
                    delta_pf, delta_drawdown, delta_capture,
                ))
            ):
                incremental_status = "incremental_improvement_observed"
            else:
                incremental_status = "no_incremental_improvement"
            rows.append({
                "contract_version": CROSSOVER_ABLATION_CONTRACT,
                "strategy": strategy,
                "stage_order": order,
                "stage_key": definition["key"],
                "stage_label": definition["label"],
                "stage_rule": definition["rule"],
                "prior_stage_trades": None if order == 1 else len(prior_selected),
                "state_eligible_trades": len(eligible),
                "unavailable_state_trades": 0 if order == 1 else len(prior_selected) - len(eligible),
                "filter_excluded_trades": len(eligible) - len(selected),
                "baseline_retention_ratio": _round(len(selected) / len(baseline), 6) if baseline else 0.0,
                **metrics,
                "delta_net_expectancy_pct": delta_expectancy,
                "delta_net_profit_factor": delta_pf,
                "delta_net_drawdown_pct": delta_drawdown,
                "delta_move_capture_ratio": delta_capture,
                "sample_status": sample_status,
                "incremental_status": incremental_status,
            })
            previous_metrics = metrics
    return {
        "ok": True,
        "contract_version": CROSSOVER_ABLATION_CONTRACT,
        "stage_definitions": list(stage_definitions),
        "rows": rows,
        "decision_boundary": (
            "Rules are frozen before inspecting stage returns. Results are descriptive; "
            "incremental improvement is not an out-of-sample edge claim."
        ),
    }


def build_crossover_ablation_ladder(
    market_db: Path = DEFAULT_MARKET_DB,
) -> dict[str, Any]:
    conditioning = build_crossover_state_conditioning(market_db, return_internal=True)
    baseline = conditioning.pop("_baseline_trades")
    joined = conditioning.pop("_joined_trades")
    contract = crossover_ablation_contract(baseline, joined)
    generated_at = datetime.now(timezone.utc).isoformat()
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        connection.execute(
            "DELETE FROM crossover_ablation_ledger WHERE contract_version=?",
            (CROSSOVER_ABLATION_CONTRACT,),
        )
        for row in contract["rows"]:
            connection.execute(
                """INSERT INTO crossover_ablation_ledger(
                       contract_version,generated_at,strategy,stage_order,stage_key,stage_label,
                       prior_stage_trades,state_eligible_trades,unavailable_state_trades,
                       filter_excluded_trades,completed_trades,baseline_retention_ratio,
                       gross_trade_expectancy_pct,net_trade_expectancy_pct,gross_profit_factor,
                       net_profit_factor,net_maximum_drawdown_pct,average_move_capture_ratio,
                       delta_net_expectancy_pct,delta_net_profit_factor,delta_net_drawdown_pct,
                       delta_move_capture_ratio,sample_status,incremental_status,schema_version)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
                (
                    row["contract_version"], generated_at, row["strategy"], row["stage_order"],
                    row["stage_key"], row["stage_label"], row["prior_stage_trades"],
                    row["state_eligible_trades"], row["unavailable_state_trades"],
                    row["filter_excluded_trades"], row["completed_trades"],
                    row["baseline_retention_ratio"], row["gross_trade_expectancy_pct"],
                    row["net_trade_expectancy_pct"], row["gross_profit_factor"],
                    row["net_profit_factor"], row["net_maximum_drawdown_pct"],
                    row["average_move_capture_ratio"], row["delta_net_expectancy_pct"],
                    row["delta_net_profit_factor"], row["delta_net_drawdown_pct"],
                    row["delta_move_capture_ratio"], row["sample_status"],
                    row["incremental_status"],
                ),
            )
        connection.commit()
    finally:
        connection.close()
    contract.update({
        "generated_at": generated_at,
        "persisted_rows": len(contract["rows"]),
        "baseline_completed_trades": len(baseline),
        "state_joined_completed_trades": len(joined),
    })
    return contract


def crossover_ablation_history(
    market_db: Path = DEFAULT_MARKET_DB, strategy: str | None = None,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        filters = ["contract_version=?"]
        parameters: list[Any] = [CROSSOVER_ABLATION_CONTRACT]
        if strategy:
            filters.append("strategy=?")
            parameters.append(strategy)
        rows = [dict(row) for row in connection.execute(
            f"SELECT * FROM crossover_ablation_ledger WHERE {' AND '.join(filters)} ORDER BY strategy,stage_order",
            parameters,
        )]
        return {
            "ok": bool(rows),
            "reason": None if rows else "crossover_ablation_ledger_empty",
            "contract_version": CROSSOVER_ABLATION_CONTRACT,
            "rows": rows,
            "decision_boundary": "Frozen descriptive ladder only; OOS validation is the next milestone.",
        }
    finally:
        connection.close()


TOP_DOWN_OOS_ADMISSION_CONTRACT = "top-down-oos-admission-v1"
TOP_DOWN_OOS_MINIMUM_TRADES = 15
TOP_DOWN_OOS_MINIMUM_STATE_COVERAGE = 0.95


def top_down_oos_admission_contract(
    ablation_rows: list[dict[str, Any]], oos_family_audit: dict[str, Any],
) -> dict[str, Any]:
    rows = []
    for strategy in ("sma_20_50", "ema_20_50"):
        stages = sorted(
            (dict(row) for row in ablation_rows if row.get("strategy") == strategy),
            key=lambda row: int(row.get("stage_order") or 0),
        )
        if not stages:
            continue
        baseline = stages[0]
        final_stage = stages[-1]
        market_stage = next((row for row in stages if row.get("stage_key") == "market_regime"), {})
        baseline_trades = int(baseline.get("completed_trades") or 0)
        state_coverage_ratio = (
            int(market_stage.get("state_eligible_trades") or 0) / baseline_trades
            if baseline_trades else 0.0
        )
        ablation_pass = all(
            row.get("incremental_status") == "incremental_improvement_observed"
            for row in stages[1:]
        )
        trade_pass = int(final_stage.get("completed_trades") or 0) >= TOP_DOWN_OOS_MINIMUM_TRADES
        expectancy = final_stage.get("net_trade_expectancy_pct")
        expectancy_pass = expectancy is not None and float(expectancy) > 0
        profit_factor = final_stage.get("net_profit_factor")
        profit_factor_pass = profit_factor is not None and float(profit_factor) > 1.0
        coverage_pass = state_coverage_ratio >= TOP_DOWN_OOS_MINIMUM_STATE_COVERAGE
        pre_admission_pass = all((
            ablation_pass, trade_pass, expectancy_pass, profit_factor_pass, coverage_pass,
        ))
        failure_reasons = []
        if not ablation_pass:
            failure_reasons.append("ablation_gate_failed")
        if not trade_pass:
            failure_reasons.append("final_stage_completed_trades_below_15")
        if not expectancy_pass:
            failure_reasons.append("final_stage_net_expectancy_not_positive")
        if not profit_factor_pass:
            failure_reasons.append("final_stage_net_profit_factor_not_above_one")
        if not coverage_pass:
            failure_reasons.append("dated_state_coverage_below_95_pct")
        if not bool(oos_family_audit.get("coverage_complete")):
            failure_reasons.append("cross_strategy_oos_family_incomplete")
        if int(oos_family_audit.get("fdr_q_below_alpha") or 0) < 1:
            failure_reasons.append("no_cross_strategy_fdr_admission")
        execution_status = (
            "pending_complete_model_oos" if pre_admission_pass
            else "not_run_pre_admission_failed"
        )
        rows.append({
            "contract_version": TOP_DOWN_OOS_ADMISSION_CONTRACT,
            "strategy": strategy,
            "admission_decision": "pending_oos" if pre_admission_pass else "rejected",
            "ablation_gate_passed": ablation_pass,
            "minimum_trade_gate_passed": trade_pass,
            "positive_net_expectancy_gate_passed": expectancy_pass,
            "profit_factor_gate_passed": profit_factor_pass,
            "state_coverage_gate_passed": coverage_pass,
            "final_stage_trades": int(final_stage.get("completed_trades") or 0),
            "final_stage_net_expectancy_pct": expectancy,
            "final_stage_net_profit_factor": profit_factor,
            "state_coverage_ratio": _round(state_coverage_ratio, 6),
            "walk_forward_status": execution_status,
            "bootstrap_status": execution_status,
            "within_strategy_fdr_status": execution_status,
            "cross_strategy_fdr_status": (
                "complete" if oos_family_audit.get("coverage_complete") else "incomplete"
            ),
            "oos_score": None,
            "failure_reasons": sorted(set(failure_reasons)),
            "audit": {
                "ablation_contract": CROSSOVER_ABLATION_CONTRACT,
                "final_stage": final_stage.get("stage_key"),
                "baseline_completed_trades": baseline_trades,
                "state_eligible_trades": int(market_stage.get("state_eligible_trades") or 0),
                "required_minimum_trades": TOP_DOWN_OOS_MINIMUM_TRADES,
                "required_state_coverage_ratio": TOP_DOWN_OOS_MINIMUM_STATE_COVERAGE,
                "cross_strategy_contract": oos_family_audit.get("contract_version"),
                "cross_strategy_tested_hypotheses": int(oos_family_audit.get("tested_hypotheses") or 0),
                "cross_strategy_fdr_admitted": int(oos_family_audit.get("fdr_q_below_alpha") or 0),
                "cost_model": EQUITY_DELIVERY_COST_SCHEDULE["schedule_version"],
                "point_in_time_state_join_required": True,
            },
        })
    return {
        "ok": bool(rows),
        "contract_version": TOP_DOWN_OOS_ADMISSION_CONTRACT,
        "rows": rows,
        "admitted_strategies": sum(row["admission_decision"] == "admitted" for row in rows),
        "rejected_strategies": sum(row["admission_decision"] == "rejected" for row in rows),
        "decision_boundary": (
            "A failed pre-admission gate stops complete-model OOS execution. Missing "
            "walk-forward, bootstrap or FDR evidence is never treated as a pass."
        ),
    }


def build_top_down_oos_admission(
    market_db: Path = DEFAULT_MARKET_DB,
) -> dict[str, Any]:
    ablation = build_crossover_ablation_ladder(market_db)
    oos_audit = cross_strategy_multiple_testing(market_db, start_background=False)
    contract = top_down_oos_admission_contract(ablation["rows"], oos_audit)
    generated_at = datetime.now(timezone.utc).isoformat()
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        connection.execute(
            "DELETE FROM top_down_oos_admission_ledger WHERE contract_version=?",
            (TOP_DOWN_OOS_ADMISSION_CONTRACT,),
        )
        for row in contract["rows"]:
            connection.execute(
                """INSERT INTO top_down_oos_admission_ledger(
                       contract_version,generated_at,strategy,admission_decision,
                       ablation_gate_passed,minimum_trade_gate_passed,
                       positive_net_expectancy_gate_passed,profit_factor_gate_passed,
                       state_coverage_gate_passed,final_stage_trades,
                       final_stage_net_expectancy_pct,final_stage_net_profit_factor,
                       state_coverage_ratio,walk_forward_status,bootstrap_status,
                       within_strategy_fdr_status,cross_strategy_fdr_status,oos_score,
                       failure_reasons_json,audit_json,schema_version)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
                (
                    row["contract_version"], generated_at, row["strategy"],
                    row["admission_decision"], int(row["ablation_gate_passed"]),
                    int(row["minimum_trade_gate_passed"]),
                    int(row["positive_net_expectancy_gate_passed"]),
                    int(row["profit_factor_gate_passed"]),
                    int(row["state_coverage_gate_passed"]), row["final_stage_trades"],
                    row["final_stage_net_expectancy_pct"], row["final_stage_net_profit_factor"],
                    row["state_coverage_ratio"], row["walk_forward_status"],
                    row["bootstrap_status"], row["within_strategy_fdr_status"],
                    row["cross_strategy_fdr_status"], row["oos_score"],
                    json.dumps(row["failure_reasons"], sort_keys=True),
                    json.dumps(row["audit"], sort_keys=True),
                ),
            )
        connection.commit()
    finally:
        connection.close()
    contract.update({
        "generated_at": generated_at,
        "persisted_rows": len(contract["rows"]),
        "oos_family_audit": {
            "contract_version": oos_audit.get("contract_version"),
            "coverage_complete": bool(oos_audit.get("coverage_complete")),
            "tested_hypotheses": int(oos_audit.get("tested_hypotheses") or 0),
            "fdr_q_below_alpha": int(oos_audit.get("fdr_q_below_alpha") or 0),
            "missing_engines": oos_audit.get("missing_engines") or [],
        },
    })
    return contract


def top_down_oos_admission_history(
    market_db: Path = DEFAULT_MARKET_DB,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        stored = connection.execute(
            "SELECT * FROM top_down_oos_admission_ledger WHERE contract_version=? ORDER BY strategy",
            (TOP_DOWN_OOS_ADMISSION_CONTRACT,),
        ).fetchall()
        rows = []
        for stored_row in stored:
            row = dict(stored_row)
            for key in (
                "ablation_gate_passed", "minimum_trade_gate_passed",
                "positive_net_expectancy_gate_passed", "profit_factor_gate_passed",
                "state_coverage_gate_passed",
            ):
                row[key] = bool(row[key])
            row["failure_reasons"] = json.loads(row.pop("failure_reasons_json"))
            row["audit"] = json.loads(row.pop("audit_json"))
            rows.append(row)
        return {
            "ok": bool(rows),
            "reason": None if rows else "top_down_oos_admission_empty",
            "contract_version": TOP_DOWN_OOS_ADMISSION_CONTRACT,
            "rows": rows,
            "opportunity_board_authorised": any(
                row["admission_decision"] == "admitted" for row in rows
            ),
        }
    finally:
        connection.close()


OPPORTUNITY_BOARD_RELEASE_CONTRACT = "opportunity-board-release-v1"


def opportunity_board_release_contract(
    admission_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    rows = [dict(row) for row in admission_rows]
    admitted = [row for row in rows if row.get("admission_decision") == "admitted"]
    rejected = [row for row in rows if row.get("admission_decision") == "rejected"]
    authorised = bool(admitted)
    reasons = sorted({
        str(reason)
        for row in rows for reason in (row.get("failure_reasons") or [])
    })
    if authorised:
        decision = "authorised"
        ui_mode = "validated_opportunity_board"
        headline = "Validated opportunity board authorised"
        explanation = (
            f"{len(admitted)} strategy contract(s) passed every frozen admission gate."
        )
        next_action = "Build the sector-first board strictly from admitted strategy contracts."
    else:
        decision = "withheld"
        ui_mode = "research_status_only"
        headline = "Opportunity board withheld"
        explanation = (
            "No strategy passed ablation and out-of-sample admission. Rankings, "
            "opportunity scores, trade labels and order controls remain disabled."
        )
        next_action = (
            "Begin a separately specified research cycle with hypotheses frozen before results are inspected."
        )
    return {
        "ok": bool(rows),
        "reason": None if rows else "oos_admission_unavailable",
        "contract_version": OPPORTUNITY_BOARD_RELEASE_CONTRACT,
        "release_decision": decision,
        "board_authorised": authorised,
        "ui_mode": ui_mode,
        "admitted_strategies": len(admitted),
        "rejected_strategies": len(rejected),
        "headline": headline,
        "explanation": explanation,
        "failure_reasons": reasons,
        "next_action": next_action,
        "prohibited_outputs": [] if authorised else [
            "opportunity_rank", "opportunity_score", "trade_label",
            "target_price", "position_size", "order_control",
        ],
    }


def build_opportunity_board_release(
    market_db: Path = DEFAULT_MARKET_DB,
) -> dict[str, Any]:
    admission = top_down_oos_admission_history(market_db)
    if not admission.get("ok"):
        build_top_down_oos_admission(market_db)
        admission = top_down_oos_admission_history(market_db)
    contract = opportunity_board_release_contract(admission.get("rows") or [])
    if not contract.get("ok"):
        return contract
    generated_at = datetime.now(timezone.utc).isoformat()
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        connection.execute(
            """INSERT OR REPLACE INTO opportunity_board_release_ledger(
                   contract_version,generated_at,release_decision,board_authorised,ui_mode,
                   admitted_strategies,rejected_strategies,headline,explanation,
                   failure_reasons_json,next_action,schema_version)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,1)""",
            (
                contract["contract_version"], generated_at, contract["release_decision"],
                int(contract["board_authorised"]), contract["ui_mode"],
                contract["admitted_strategies"], contract["rejected_strategies"],
                contract["headline"], contract["explanation"],
                json.dumps(contract["failure_reasons"], sort_keys=True), contract["next_action"],
            ),
        )
        connection.commit()
    finally:
        connection.close()
    contract["generated_at"] = generated_at
    return contract


def opportunity_board_release_status(
    market_db: Path = DEFAULT_MARKET_DB,
) -> dict[str, Any]:
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        stored = connection.execute(
            "SELECT * FROM opportunity_board_release_ledger WHERE contract_version=?",
            (OPPORTUNITY_BOARD_RELEASE_CONTRACT,),
        ).fetchone()
        if not stored:
            return {"ok": False, "reason": "opportunity_board_release_unavailable"}
        row = dict(stored)
        row["board_authorised"] = bool(row["board_authorised"])
        row["failure_reasons"] = json.loads(row.pop("failure_reasons_json"))
        row["prohibited_outputs"] = [] if row["board_authorised"] else [
            "opportunity_rank", "opportunity_score", "trade_label",
            "target_price", "position_size", "order_control",
        ]
        row["ok"] = True
        return row
    finally:
        connection.close()


BACKTEST_COMPARISON_SESSIONS = 252
BACKTEST_COMMON_COVERAGE_RATIO = 0.95
BACKTEST_ABNORMAL_JUMP_PCT = 50.0
POINT_IN_TIME_HISTORY_SESSIONS = 451


def _backtest_reference_context(connection: sqlite3.Connection) -> dict[str, Any]:
    stored_stock_count = int(connection.execute("SELECT COUNT(*) FROM instruments WHERE kind='stock'").fetchone()[0])
    latest_reference_session = connection.execute(
        "SELECT MAX(session_date) FROM candles WHERE instrument_id='index:Nifty 50'"
    ).fetchone()[0]
    active_stock_count = 0 if not latest_reference_session else int(connection.execute(
        """SELECT COUNT(DISTINCT c.instrument_id) FROM candles c
           JOIN instruments i USING(instrument_id)
           WHERE i.kind='stock' AND c.session_date=?""",
        (latest_reference_session,),
    ).fetchone()[0])
    # Historical/delisted instruments remain in the store for point-in-time
    # research, but must not force the present comparable window backwards.
    # Anchor the coverage gate to stocks actually observed on the latest index
    # session; individual inactive or incomplete histories are still gated below.
    comparison_universe = active_stock_count or stored_stock_count
    minimum_coverage = max(1, math.ceil(comparison_universe * BACKTEST_COMMON_COVERAGE_RATIO))
    cutoff_row = connection.execute(
        """SELECT c.session_date, COUNT(DISTINCT c.instrument_id) AS covered
           FROM candles c JOIN instruments i USING(instrument_id)
           WHERE i.kind='stock'
           GROUP BY c.session_date HAVING covered >= ?
           ORDER BY c.session_date DESC LIMIT 1""",
        (minimum_coverage,),
    ).fetchone()
    common_as_of = cutoff_row["session_date"] if cutoff_row else None
    reference_dates: list[str] = []
    if common_as_of:
        reference_dates = [row[0] for row in connection.execute(
            "SELECT session_date FROM candles WHERE instrument_id='index:Nifty 50' AND session_date<=? ORDER BY session_date",
            (common_as_of,),
        )]
        if not reference_dates:
            reference_dates = [row[0] for row in connection.execute(
                "SELECT DISTINCT session_date FROM candles WHERE session_date<=? ORDER BY session_date",
                (common_as_of,),
            )]
    return {
        "common_as_of_date": common_as_of,
        "stocks": comparison_universe,
        "stored_stock_instruments": stored_stock_count,
        "latest_reference_session": latest_reference_session,
        "minimum_coverage": minimum_coverage,
        "covered_on_common_as_of": int(cutoff_row["covered"]) if cutoff_row else 0,
        "reference_dates": reference_dates,
        "comparison_sessions": BACKTEST_COMPARISON_SESSIONS,
        "reference_instrument": "Nifty 50 completed sessions",
    }


def removed_constituent_history_audit(
    market_db: Path = DEFAULT_MARKET_DB,
) -> dict[str, Any]:
    """Audit stored candles for every reviewed constituent exit.

    Ordinary removals require up to 451 sessions ending immediately before the
    effective exclusion date (252 comparison sessions plus the longest current
    strategy warm-up). Resulting demerger companies require only their actual
    listed-and-index-eligible interval; zero-price dummies never require candles.
    """
    ensure_workspace_schema(market_db)
    connection = _connect(market_db)
    try:
        reference_dates = [str(row[0]) for row in connection.execute(
            "SELECT session_date FROM candles WHERE instrument_id='index:Nifty 50' ORDER BY session_date"
        )]
        if not reference_dates:
            reference_dates = [str(row[0]) for row in connection.execute(
                "SELECT DISTINCT session_date FROM candles ORDER BY session_date"
            )]

        exits = [
            {
                "symbol": row["symbol"],
                "exclusion_date": row["effective_date"],
                "eligible_from": None,
                "membership_role": "ordinary_constituent",
                "source_url": row["source_url"],
            }
            for row in connection.execute(
                """SELECT symbol,effective_date,source_url
                   FROM universe_membership_events
                   WHERE index_name='NIFTY 500' AND action='excluded'
                   ORDER BY effective_date,symbol"""
            )
        ]
        exits.extend(
            {
                "symbol": row["listed_symbol"],
                "exclusion_date": row["exclusion_date"],
                "eligible_from": row["listing_date"],
                "membership_role": "listed_resulting_company",
                "source_url": row["exclusion_source_url"],
            }
            for row in connection.execute(
                """SELECT listed_symbol,listing_date,exclusion_date,exclusion_source_url
                   FROM universe_lineage_intervals
                   WHERE index_name='NIFTY 500' ORDER BY exclusion_date,listed_symbol"""
            )
            if reference_dates and str(row["listing_date"]) <= reference_dates[-1]
        )

        rows = []
        for exit_row in exits:
            exclusion_date = str(exit_row["exclusion_date"])
            eligible_from = exit_row["eligible_from"]
            reference_window = [
                session for session in reference_dates
                if session < exclusion_date and (not eligible_from or session >= str(eligible_from))
            ][-POINT_IN_TIME_HISTORY_SESSIONS:]
            confirmed_absent_dates = {
                str(row[0]) for row in connection.execute(
                    """SELECT session_date FROM cash_archive_symbol_checks
                       WHERE symbol=? AND present=0 AND session_date<?""",
                    (exit_row["symbol"], exclusion_date),
                )
            }
            # A completed official cash archive proves that the security did not
            # trade under this symbol on that session (typically before listing
            # or after a symbol change). Do not manufacture a missing candle for
            # a market session on which the security did not exist or trade.
            eligible_dates = [session for session in reference_window if session not in confirmed_absent_dates]
            instrument_id = f"stock:{exit_row['symbol']}"
            instrument = connection.execute(
                "SELECT instrument_id,source FROM instruments WHERE instrument_id=?",
                (instrument_id,),
            ).fetchone()
            stored_dates = {
                str(row[0]) for row in connection.execute(
                    "SELECT session_date FROM candles WHERE instrument_id=? AND session_date<?",
                    (instrument_id, exclusion_date),
                )
            }
            missing_dates = [session for session in eligible_dates if session not in stored_dates]
            required = len(eligible_dates)
            observed = required - len(missing_dates)
            if not instrument:
                status = "missing_instrument"
            elif not required:
                status = "reference_window_unavailable"
            elif missing_dates:
                status = "partial_history"
            else:
                status = "complete"
            rows.append({
                **exit_row,
                "instrument_id": instrument_id,
                "instrument_source": instrument["source"] if instrument else None,
                "required_sessions": required,
                "stored_sessions": observed,
                "coverage_ratio": _round(observed / required, 6) if required else None,
                "window_start": eligible_dates[0] if eligible_dates else None,
                "window_end": eligible_dates[-1] if eligible_dates else None,
                "reference_window_sessions": len(reference_window),
                "archive_confirmed_absent_sessions": len(set(reference_window) & confirmed_absent_dates),
                "missing_session_count": len(missing_dates),
                "first_missing_session": missing_dates[0] if missing_dates else None,
                "last_missing_session": missing_dates[-1] if missing_dates else None,
                "status": status,
            })

        required_total = sum(int(row["required_sessions"]) for row in rows)
        stored_total = sum(int(row["stored_sessions"]) for row in rows)
        missing_symbols = sorted({row["symbol"] for row in rows if row["status"] == "missing_instrument"})
        partial_symbols = sorted({row["symbol"] for row in rows if row["status"] == "partial_history"})
        complete_rows = sum(row["status"] == "complete" for row in rows)
        complete = bool(rows) and complete_rows == len(rows)
        return {
            "ok": True,
            "contract_version": "removed-constituent-history-audit-v1",
            "index_name": "NIFTY 500",
            "history_requirement": {
                "ordinary_exit_sessions": POINT_IN_TIME_HISTORY_SESSIONS,
                "comparison_sessions": BACKTEST_COMPARISON_SESSIONS,
                "warm_up_sessions": POINT_IN_TIME_HISTORY_SESSIONS - BACKTEST_COMPARISON_SESSIONS,
                "lineage_rule": "listed-and-index-eligible sessions only; zero-price dummies excluded",
            },
            "coverage": {
                "reviewed_exits": len(rows),
                "complete_exits": complete_rows,
                "required_sessions": required_total,
                "stored_sessions": stored_total,
                "coverage_ratio": _round(stored_total / required_total, 6) if required_total else None,
                "missing_instruments": len(missing_symbols),
                "partial_histories": len(partial_symbols),
                "complete": complete,
            },
            "missing_symbols": missing_symbols,
            "partial_symbols": partial_symbols,
            "exits": rows,
        }
    finally:
        connection.close()


def _quality_checked_backtest_series(
    connection: sqlite3.Connection,
    instrument_id: str,
    required_observations: int,
    reference_context: dict[str, Any],
    history_mode: str = "comparison",
) -> tuple[list[sqlite3.Row], dict[str, Any]]:
    return quality_checked_backtest_series(
        connection,
        instrument_id,
        required_observations,
        reference_context,
        history_mode,
        instrument_series=_instrument_series,
        comparison_sessions=BACKTEST_COMPARISON_SESSIONS,
        abnormal_jump_pct=BACKTEST_ABNORMAL_JUMP_PCT,
    )


def nifty50_ytd(path: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    connection = _connect(path)
    try:
        rows = _instrument_series(connection, "index:Nifty 50")
    finally:
        connection.close()
    if not rows:
        return {"ok": False, "reason": "nifty50_history_unavailable"}
    latest = rows[-1]
    year = str(latest["session_date"])[:4]
    baseline = next((row for row in reversed(rows) if str(row["session_date"]) < f"{year}-01-01"), None)
    if baseline is None:
        return {"ok": False, "reason": "nifty50_ytd_baseline_unavailable"}
    return {
        "ok": True,
        "instrument": "Nifty 50",
        "as_of_date": latest["session_date"],
        "close": latest["close"],
        "baseline_date": baseline["session_date"],
        "baseline_close": baseline["close"],
        "ytd_return_pct": _round(_pct(latest["close"], baseline["close"])),
        "source": "Workspace 2.0 local EOD store",
    }


def _breadth(
    connection: sqlite3.Connection,
    snapshot_db: Path = DEFAULT_SNAPSHOT_DB,
) -> dict[str, Any]:
    snapshot = _connect(snapshot_db)
    try:
        membership_date = snapshot.execute(
            """SELECT MAX(snapshot_date) FROM index_membership_snapshots
               WHERE index_name='NIFTY 500'"""
        ).fetchone()[0]
        members = {
            row[0]
            for row in snapshot.execute(
                """SELECT DISTINCT symbol FROM index_membership_snapshots
                   WHERE index_name='NIFTY 500' AND snapshot_date=?""",
                (membership_date,),
            ).fetchall()
        } if membership_date else set()
    finally:
        snapshot.close()
    registered = connection.execute(
        "SELECT instrument_id FROM instruments WHERE kind='stock'"
    ).fetchall()
    instruments = [
        row for row in registered
        if row["instrument_id"].split(":", 1)[-1] in members
    ]
    above50 = above200 = positive20 = evaluated50 = evaluated200 = evaluated20 = 0
    latest_date = None
    for item in instruments:
        rows = _instrument_series(connection, item[0])
        if not rows:
            continue
        latest_date = max(latest_date or rows[-1]["session_date"], rows[-1]["session_date"])
        closes = [float(row["close"]) for row in rows]
        current = closes[-1]
        if len(closes) >= 50:
            evaluated50 += 1
            above50 += current >= mean(closes[-50:])
        if len(closes) >= 200:
            evaluated200 += 1
            above200 += current >= mean(closes[-200:])
        if len(closes) >= 21:
            evaluated20 += 1
            positive20 += current >= closes[-21]
    evaluated = min(value for value in (evaluated50, evaluated200) if value) if evaluated50 and evaluated200 else max(evaluated50, evaluated200)
    return {
        "as_of_date": latest_date,
        "membership_as_of_date": membership_date,
        "official_count": len(members),
        "universe_count": len(instruments),
        "registered_history_count": len(registered),
        "excluded_noncurrent_count": max(0, len(registered) - len(instruments)),
        "evaluated": evaluated,
        "evaluated_50dma": evaluated50,
        "evaluated_200dma": evaluated200,
        "evaluated_20d": evaluated20,
        "above_50dma": above50,
        "above_200dma": above200,
        "positive_20d": positive20,
        "above_50dma_pct": _round(100 * above50 / evaluated50) if evaluated50 else None,
        "above_200dma_pct": _round(100 * above200 / evaluated200) if evaluated200 else None,
        "positive_20d_pct": _round(100 * positive20 / evaluated20) if evaluated20 else None,
        "coverage_pct": _round(100 * evaluated / len(members)) if members and evaluated else 0,
        "universe_contract": "latest_official_nifty500_snapshot",
    }


def _fno_state(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """WITH ranked AS (
               SELECT f.contract_key, f.underlying, f.tradingsymbol, f.expiry_date,
                      s.session_date, s.close, s.volume, s.open_interest,
                      ROW_NUMBER() OVER (
                          PARTITION BY f.contract_key ORDER BY s.session_date DESC
                      ) AS recency
               FROM futures_contracts f
               JOIN futures_eod_snapshots s ON s.contract_key=f.contract_key
           )
           SELECT contract_key,underlying,tradingsymbol,expiry_date,
                  session_date,close,volume,open_interest
           FROM ranked WHERE recency <= 2
           ORDER BY underlying,expiry_date,session_date"""
    ).fetchall()
    grouped: dict[str, dict[str, list[sqlite3.Row]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        grouped[row["underlying"]][row["contract_key"]].append(row)
    states = []
    for underlying, contracts in grouped.items():
        candidates = [series for series in contracts.values() if len(series) >= 2]
        if not candidates:
            continue
        latest_session = max(values[-1]["session_date"] for values in candidates)
        live = [
            values for values in candidates
            if values[-1]["session_date"] == latest_session
            and values[-1]["expiry_date"] >= latest_session
        ]
        series = min(live, key=lambda values: values[-1]["expiry_date"]) if live else max(
            candidates, key=lambda values: (values[-1]["session_date"], values[-1]["expiry_date"])
        )
        previous, latest = series[-2], series[-1]
        price = _pct(float(latest["close"]), float(previous["close"])) or 0.0
        oi = _pct(float(latest["open_interest"]), float(previous["open_interest"])) or 0.0
        if abs(price) < 0.15 or abs(oi) < 0.5:
            state = "no_clear_signal"
        elif price >= 0 and oi >= 0:
            state = "long_build_up"
        elif price < 0 and oi >= 0:
            state = "short_build_up"
        elif price < 0 and oi < 0:
            state = "long_unwinding"
        else:
            state = "short_covering"
        states.append({
            "underlying": underlying,
            "tradingsymbol": latest["tradingsymbol"],
            "expiry": latest["expiry_date"],
            "as_of_date": latest["session_date"],
            "price_change_pct": _round(price),
            "oi_change_pct": _round(oi),
            "volume": latest["volume"],
            "open_interest": latest["open_interest"],
            "state": state,
        })
    latest = max((row["as_of_date"] for row in states), default=None)
    latest_states = [row for row in states if row["as_of_date"] == latest] if latest else []
    counts = defaultdict(int)
    for row in latest_states:
        counts[row["state"]] += 1
    latest_coverage = len(latest_states)
    current_underlyings = {
        row["underlying"] for row in rows if latest and row["session_date"] == latest
    }
    history = connection.execute(
        "SELECT COUNT(DISTINCT session_date) FROM futures_eod_snapshots"
    ).fetchone()[0]
    comparable_history = connection.execute(
        """SELECT COUNT(DISTINCT session_date)
           FROM (
               SELECT session_date,open_interest,
                      LAG(session_date) OVER (
                          PARTITION BY contract_key ORDER BY session_date
                      ) AS previous_date,
                      LAG(open_interest) OVER (
                          PARTITION BY contract_key ORDER BY session_date
                      ) AS previous_open_interest
               FROM futures_eod_snapshots
           )
           WHERE open_interest > 0 AND previous_open_interest > 0
             AND julianday(session_date)-julianday(previous_date) BETWEEN 1 AND 7"""
    ).fetchone()[0]
    archive_rows = connection.execute(
        """SELECT status, COUNT(*) AS files, COALESCE(SUM(records_written),0) AS records,
                  MIN(CASE WHEN status='imported' THEN session_date END) AS first_imported,
                  MAX(CASE WHEN status='imported' THEN session_date END) AS last_imported
           FROM fno_archive_files GROUP BY status"""
    ).fetchall()
    archive_coverage = {
        row["status"]: {"files": row["files"], "records": row["records"]}
        for row in archive_rows
    }
    imported_rows = [row for row in archive_rows if row["status"] == "imported"]
    bullish = counts["long_build_up"] + counts["short_covering"]
    bearish = counts["short_build_up"] + counts["long_unwinding"]
    neutral = counts["no_clear_signal"]
    expected_underlyings = len(current_underlyings) or latest_coverage
    unavailable = max(0, expected_underlyings - latest_coverage)
    long_build_up = counts["long_build_up"]
    short_build_up = counts["short_build_up"]
    long_unwinding = counts["long_unwinding"]
    short_covering = counts["short_covering"]
    rising_directional = long_build_up + short_covering
    falling_directional = short_build_up + long_unwinding
    position_addition = long_build_up + short_build_up
    position_reduction = short_covering + long_unwinding
    movement_attribution = {
        "long_build_up": long_build_up,
        "short_build_up": short_build_up,
        "long_unwinding": long_unwinding,
        "short_covering": short_covering,
        "position_addition": position_addition,
        "position_reduction": position_reduction,
        "directional_total": position_addition + position_reduction,
        "neutral": neutral,
        "unavailable": unavailable,
        "rising": {
            "total": rising_directional,
            "build_up": long_build_up,
            "position_closure": short_covering,
            "build_up_share_pct": _round(100 * long_build_up / rising_directional, 1) if rising_directional else None,
        },
        "falling": {
            "total": falling_directional,
            "build_up": short_build_up,
            "position_closure": long_unwinding,
            "build_up_share_pct": _round(100 * short_build_up / falling_directional, 1) if falling_directional else None,
        },
        "method": "Same-contract completed-session price and open-interest comparison after noise safeguards.",
    }
    return {
        "as_of_date": latest,
        "status": "descriptive_eod",
        "positioning": {"bullish": bullish, "bearish": bearish, "neutral": neutral, "unavailable": unavailable, "total": expected_underlyings},
        "counts": dict(counts),
        "coverage": {"latest": latest_coverage, "expected": expected_underlyings},
        "coverage_label": f"{latest_coverage}/{expected_underlyings}",
        "stored_history_sessions": comparable_history,
        "raw_history_sessions": history,
        "archive_backfill": {
            "status": "available" if imported_rows else "not_started",
            "coverage": archive_coverage,
            "first_imported": min((row["first_imported"] for row in imported_rows), default=None),
            "last_imported": max((row["last_imported"] for row in imported_rows), default=None),
            "comparison_method": "Same contract, positive OI, maximum seven-day calendar gap.",
        },
        "futures_oi": {
            "as_of_date": latest,
            "states": latest_states,
            "state_counts": dict(counts),
            "movement_attribution": movement_attribution,
            "expected_universe": expected_underlyings,
            "latest_coverage": latest_coverage,
            "latest_coverage_pct": _round(100 * latest_coverage / expected_underlyings) if expected_underlyings else 0,
            "comparable_count": latest_coverage,
            "rollover_baseline_count": max(0, history - comparable_history),
            "classification_status": "descriptive_eod",
            "stale_gap_count": max(0, len(states) - latest_coverage),
            "liquidity_excluded_count": 0,
            "latest_missing_count": unavailable,
            "safeguards": {
                "minimum_absolute_price_change_pct": 0.15,
                "minimum_absolute_oi_change_pct": 0.5,
                "maximum_calendar_gap_days": 7,
            },
        },
    }


def _relative_strength(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute("SELECT instrument_id, display_name FROM instruments WHERE kind='index'").fetchall()
    returns = []
    for item in rows:
        if "VIX" in str(item["display_name"]).upper() or "VOLATILITY" in str(item["display_name"]).upper():
            continue
        series = _instrument_series(connection, item["instrument_id"])
        if len(series) < 21:
            continue
        latest = series[-1]
        latest_date = date.fromisoformat(latest["session_date"])
        prior_year = next(
            (row for row in reversed(series) if row["session_date"] < f"{latest_date.year}-01-01"),
            None,
        )
        value = _pct(float(latest["close"]), float(series[-21]["close"]))
        ytd = None if prior_year is None else _pct(float(latest["close"]), float(prior_year["close"]))
        retrieval = connection.execute(
            """SELECT retrieved_at FROM candles
               WHERE instrument_id=? AND session_date=? ORDER BY retrieved_at DESC LIMIT 1""",
            (item["instrument_id"], latest["session_date"]),
        ).fetchone()
        returns.append({
            "name": item["display_name"],
            "return_pct": _round(value),
            "ytd_return_pct": _round(ytd),
            "as_of_date": latest["session_date"],
            "retrieved_at": retrieval["retrieved_at"] if retrieval else None,
        })
    benchmark = next(
        (row for row in returns if row["name"] == "Nifty 50"),
        {"name": "Nifty 50", "return_pct": None, "ytd_return_pct": None, "as_of_date": None, "retrieved_at": None},
    )
    benchmark_return = benchmark["return_pct"] or 0
    ranked = [
        {**row, "excess_vs_nifty50_pct": _round((row["return_pct"] or 0) - benchmark_return)}
        for row in returns if row["name"] != "Nifty 50"
    ]
    ranked.sort(key=lambda row: row["excess_vs_nifty50_pct"])
    latest_as_of = max((row["as_of_date"] for row in returns if row["as_of_date"]), default=None)
    latest_retrieved = max((row["retrieved_at"] for row in returns if row["retrieved_at"]), default=None)
    return {
        "window_sessions": 20,
        "benchmark": benchmark,
        "as_of_date": latest_as_of,
        "retrieved_at": latest_retrieved,
        "indices": {
            "evaluated": len(ranked),
            "strongest": ranked[-1] if ranked else None,
            "weakest": ranked[0] if ranked else None,
            "leaders": list(reversed(ranked[-3:])),
            "laggards": ranked[:3],
        },
        "sectors": {"evaluated": 0, "strongest": None, "weakest": None},
    }


def _historical_analogue(series: list[sqlite3.Row]) -> dict[str, Any]:
    closes = [float(row["close"]) for row in series]
    if len(closes) < 100:
        return {"label": "Comparable", "sessions_observed": 0, "horizons": [], "method": "Insufficient Nifty 50 history."}
    current20 = _pct(closes[-1], closes[-21]) or 0
    candidates = []
    for index in range(20, len(closes) - 61):
        trailing = _pct(closes[index], closes[index - 20]) or 0
        if abs(trailing - current20) <= 2.0:
            candidates.append(index)
    horizons = []
    for days in (5, 20, 60):
        values = [_pct(closes[index + days], closes[index]) or 0 for index in candidates]
        drawdowns = []
        for index in candidates:
            path = closes[index + 1:index + days + 1]
            drawdowns.append(min((_pct(value, closes[index]) or 0 for value in path), default=0))
        if values:
            horizons.append({
                "sessions": days,
                "mean_return_pct": _round(mean(values)),
                "median_return_pct": _round(median(values)),
                "positive_rate_pct": _round(100 * sum(value > 0 for value in values) / len(values), 1),
                "worst_return_pct": _round(min(values)),
                "mean_max_drawdown_pct": _round(mean(drawdowns)),
            })
    return {
        "label": "Similar 20-session tape",
        "sessions_observed": len(candidates),
        "horizons": horizons,
        "method": "Trailing-only Nifty 50 20-session return matches within ±2 percentage points; overlapping descriptive windows.",
    }


def _global_risk_snapshot(
    connection: sqlite3.Connection,
    reference_date: str | None = None,
) -> dict[str, Any]:
    """Return the latest independently dated global-risk observations.

    Global markets do not always share a session date. Selecting the maximum
    date for the whole table would therefore hide valid rows from markets that
    were closed on that date. Each metric is selected independently and is
    admitted only when it is no more than seven calendar days behind the local
    market reference date.
    """
    rows = connection.execute(
        """SELECT observation_date,metric_key,value,unit,source_series,source,retrieved_at
           FROM global_risk_observations AS observed
           WHERE observation_date=(
               SELECT MAX(candidate.observation_date)
               FROM global_risk_observations AS candidate
               WHERE candidate.metric_key=observed.metric_key
           )
           ORDER BY metric_key"""
    ).fetchall()
    reference = date.fromisoformat(reference_date) if reference_date else date.today()
    latest_rows = {row["metric_key"]: row for row in rows}
    metrics: dict[str, dict[str, Any]] = {}
    stale_labels: list[str] = []
    missing_labels: list[str] = []
    available_core: list[str] = []
    for series in GLOBAL_RISK_SERIES:
        row = latest_rows.get(series["key"])
        label = str(series["label"])
        if row is None:
            missing_labels.append(label)
            continue
        observed = date.fromisoformat(row["observation_date"])
        age_days = max(0, (reference - observed).days)
        change = latest_rows.get(f"{series['key']}_20d_change_pct")
        metrics[series["key"]] = {
            "level": row["value"],
            "unit": row["unit"],
            "as_of_date": row["observation_date"],
            "age_days": age_days,
            "fresh": age_days <= 7,
            "twenty_session_change_pct": None if change is None else change["value"],
            "source_series": row["source_series"],
            "source": row["source"],
            "retrieved_at": row["retrieved_at"],
        }
        if age_days > 7:
            stale_labels.append(label)
        elif series["core"]:
            available_core.append(label)
    required_core = [str(series["label"]) for series in GLOBAL_RISK_SERIES if series["core"]]
    missing_core = [label for label in required_core if label not in available_core]
    available = not missing_core
    latest_date = max((item["as_of_date"] for item in metrics.values()), default=None)
    reason = None
    if not available:
        reason = "Missing or stale core series: " + ", ".join(missing_core)
    return {
        "available": available,
        "as_of_date": latest_date,
        "metrics": metrics,
        "coverage": {
            "core_available": len(available_core),
            "core_required": len(required_core),
            "available_labels": available_core,
            "missing_core_labels": missing_core,
            "missing_labels": missing_labels,
            "stale_labels": stale_labels,
            "freshness_limit_days": 7,
        },
        "reason": reason,
    }


def refresh_global_risk(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    """Collect and store completed daily global-risk observations."""
    connection = _connect(market_db)
    retrieved_at = datetime.now(timezone.utc).isoformat()
    started_at = retrieved_at
    results: list[dict[str, Any]] = []
    records_written = 0
    try:
        for series in GLOBAL_RISK_SERIES:
            symbol = str(series["symbol"])
            source_url = f"https://finance.yahoo.com/quote/{requests.utils.quote(symbol, safe='')}/history"
            try:
                response = requests.get(
                    f"{YAHOO_CHART_API}/{requests.utils.quote(symbol, safe='')}",
                    params={"range": "3mo", "interval": "1d", "events": "div,splits"},
                    headers={"User-Agent": "Workspace-2.0/2.0"},
                    timeout=30,
                )
                response.raise_for_status()
                chart = response.json().get("chart", {})
                if chart.get("error"):
                    raise ValueError(str(chart["error"].get("description") or chart["error"]))
                payload = (chart.get("result") or [None])[0]
                if not payload:
                    raise ValueError("empty_chart_result")
                timestamps = payload.get("timestamp") or []
                quotes = (payload.get("indicators", {}).get("quote") or [{}])[0]
                closes = quotes.get("close") or []
                completed: list[tuple[str, float]] = []
                for timestamp, close in zip(timestamps, closes):
                    if close is None:
                        continue
                    session_date = datetime.fromtimestamp(int(timestamp), timezone.utc).date()
                    if session_date >= date.today():
                        continue
                    completed.append((session_date.isoformat(), float(close)))
                if not completed:
                    raise ValueError("no_completed_daily_observation")
                observation_date, level = completed[-1]
                connection.execute(
                    """INSERT OR REPLACE INTO global_risk_observations(
                           observation_date,metric_key,value,unit,source_series,source,
                           retrieved_at,schema_version)
                       VALUES (?,?,?,?,?,?,?,2)""",
                    (
                        observation_date, series["key"], level, series["unit"], symbol,
                        source_url, retrieved_at,
                    ),
                )
                records_written += 1
                change = None
                if len(completed) >= 21 and completed[-21][1] != 0:
                    change = 100.0 * (level / completed[-21][1] - 1.0)
                    connection.execute(
                        """INSERT OR REPLACE INTO global_risk_observations(
                               observation_date,metric_key,value,unit,source_series,source,
                               retrieved_at,schema_version)
                           VALUES (?,?,?,?,?,?,?,2)""",
                        (
                            observation_date, f"{series['key']}_20d_change_pct", change,
                            "percent", symbol, source_url, retrieved_at,
                        ),
                    )
                    records_written += 1
                results.append({
                    "key": series["key"], "label": series["label"], "status": "successful",
                    "observation_date": observation_date, "level": level,
                    "twenty_session_change_pct": change,
                })
            except (requests.RequestException, ValueError, TypeError, KeyError) as error:
                results.append({
                    "key": series["key"], "label": series["label"],
                    "status": "failed", "detail": str(error),
                })
        connection.commit()
        failed = [item for item in results if item["status"] != "successful"]
        status = "successful" if not failed else ("partial" if records_written else "failed")
        connection.execute(
            """INSERT INTO workspace_source_runs(
                   source_key,started_at,completed_at,status,records_written,detail)
               VALUES (?,?,?,?,?,?)""",
            (
                "global_risk_eod", started_at, datetime.now(timezone.utc).isoformat(), status,
                records_written, json.dumps({"series": results}, separators=(",", ":")),
            ),
        )
        connection.commit()
        snapshot = _global_risk_snapshot(connection)
        return {
            "ok": status != "failed" and snapshot["available"],
            "status": status,
            "records_written": records_written,
            "series": results,
            "global_risk": snapshot,
            "source": "Yahoo Finance completed daily chart observations",
        }
    finally:
        connection.close()


def _dashboard_overview_uncached(
    market_db: Path = DEFAULT_MARKET_DB,
    snapshot_db: Path = DEFAULT_SNAPSHOT_DB,
) -> dict[str, Any]:
    connection = _connect(market_db)
    try:
        nifty = _instrument_series(connection, "index:Nifty 50")
        if not nifty:
            return {"ok": False, "reason": "workspace_market_history_unavailable"}
        breadth = _breadth(connection, snapshot_db)
        fno = _fno_state(connection)
        relative = _relative_strength(connection)
        latest = nifty[-1]
        closes = [float(row["close"]) for row in nifty]
        return20 = _pct(closes[-1], closes[-21]) if len(closes) >= 21 else None
        annual_vol = None
        if len(closes) >= 22:
            daily = [math.log(closes[index] / closes[index - 1]) for index in range(len(closes) - 20, len(closes))]
            annual_vol = 100 * (mean((value - mean(daily)) ** 2 for value in daily) ** 0.5) * math.sqrt(252)
        above50 = breadth["above_50dma_pct"]
        above200 = breadth["above_200dma_pct"]
        tape = breadth["positive_20d_pct"]
        score_parts = [
            (0 if return20 is None else max(-25, min(25, return20 * 3))),
            (0 if above50 is None else (above50 - 50) * 0.5),
            (0 if above200 is None else (above200 - 50) * 0.35),
            (0 if tape is None else (tape - 50) * 0.35),
        ]
        score = max(-100.0, min(100.0, sum(score_parts)))
        label = "High-risk market" if score <= -25 else ("Risk-on market" if score >= 25 else "Balanced market")
        tone = "high-risk" if score <= -25 else ("risk-on" if score >= 25 else "balanced")
        bullish = fno["positioning"]["bullish"]
        bearish = fno["positioning"]["bearish"]
        neutral = fno["positioning"]["neutral"]
        unavailable = fno["positioning"]["unavailable"]
        total_fno = fno["positioning"]["total"]
        latest_flow = connection.execute(
            "SELECT MAX(session_date) FROM institutional_flows"
        ).fetchone()[0]
        global_risk = _global_risk_snapshot(connection, latest["session_date"])
        snapshot_connection = _connect(snapshot_db)
        try:
            latest_rates_fx = snapshot_connection.execute(
                "SELECT MAX(observation_date) FROM sovereign_yield_observations"
            ).fetchone()[0]
        finally:
            snapshot_connection.close()
        catalysts = _upcoming_macro_events(date.today())
        seasonality_payload = seasonality_summary(market_db)
        current_seasonality = seasonality_payload.get("current_month", {})
        nifty_seasonality = current_seasonality.get("nifty50", {})
        month_number = int(current_seasonality.get("number") or date.today().month)
        seasonality = {
            "month": calendar.month_name[month_number],
            "average_return_pct": nifty_seasonality.get("average_return_pct"),
            "count": nifty_seasonality.get("count"),
        }
        signals = [
            {"key": "regime", "label": "Regime", "value": label, "detail": f"Validated score {score:.1f}; Workspace-owned close history.", "meter_pct": (score + 100) / 2, "state": tone},
            {"key": "participation", "label": "Participation", "value": f"{above50 or 0:.0f}% / {above200 or 0:.0f}%", "detail": f"Current NIFTY 500: {breadth['evaluated_50dma']} stocks evaluated for 50DMA and {breadth['evaluated_200dma']} for 200DMA.", "meter_pct": above50 or 0, "state": "risk" if (above50 or 0) < 40 else "ready"},
            {"key": "tape", "label": "20-session tape", "value": "—" if return20 is None else f"{return20:+.2f}%", "detail": f"{breadth['positive_20d']}/{breadth['evaluated_20d']} equities positive over 20 sessions.", "meter_pct": tape or 0, "state": "risk" if (return20 or 0) < 0 else "ready"},
            {"key": "futures", "label": "Futures positioning", "value": f"{bearish} bearish / {bullish} bullish / {neutral} neutral", "detail": f"{fno['coverage']['latest']} current classifications; {unavailable} of {total_fno} unavailable.", "meter_pct": 100 * bullish / max(1, bullish + bearish + neutral), "state": "risk" if bearish > bullish else "ready"},
            {"key": "volatility", "label": "Realised volatility", "value": "—" if annual_vol is None else f"{annual_vol:.1f}% annualised", "detail": "20-session Nifty 50 close-to-close realised volatility.", "meter_pct": min(100, annual_vol or 0), "state": "risk" if (annual_vol or 0) > 25 else "ready"},
        ]
        evidence_clusters = [
            {
                "key": "trend",
                "label": "Trend",
                "available": return20 is not None,
                "as_of_date": latest["session_date"] if return20 is not None else None,
                "missing_reason": None if return20 is not None else "Nifty 50 return history is insufficient.",
            },
            {
                "key": "breadth_strength",
                "label": "Breadth / strength",
                "available": above50 is not None and above200 is not None and tape is not None,
                "as_of_date": latest["session_date"] if above50 is not None and above200 is not None and tape is not None else None,
                "missing_reason": None if above50 is not None and above200 is not None and tape is not None else "NIFTY 500 moving-average breadth is incomplete.",
            },
            {
                "key": "volatility",
                "label": "Volatility",
                "available": annual_vol is not None,
                "as_of_date": latest["session_date"] if annual_vol is not None else None,
                "missing_reason": None if annual_vol is not None else "Nifty 50 realised-volatility history is insufficient.",
            },
            {
                "key": "flows",
                "label": "Flows",
                "available": latest_flow is not None,
                "as_of_date": latest_flow,
                "missing_reason": None if latest_flow is not None else "No admitted institutional-flow observation is stored.",
            },
            {
                "key": "rates_fx",
                "label": "Rates / FX",
                "available": latest_rates_fx is not None,
                "as_of_date": latest_rates_fx,
                "missing_reason": None if latest_rates_fx is not None else "No admitted sovereign-rate or FX observation is stored.",
            },
            {
                "key": "global_risk",
                "label": "Global risk",
                "available": global_risk["available"],
                "as_of_date": global_risk["as_of_date"],
                "missing_reason": global_risk["reason"],
                "coverage": global_risk["coverage"],
            },
        ]
        available_clusters = [cluster for cluster in evidence_clusters if cluster["available"]]
        missing_clusters = [cluster for cluster in evidence_clusters if not cluster["available"]]
        confidence = {
            "label": "Ready" if not missing_clusters else "Limited",
            "available_clusters": len(available_clusters),
            "total_clusters": len(evidence_clusters),
            "clusters": evidence_clusters,
            "available_labels": [cluster["label"] for cluster in available_clusters],
            "missing_labels": [cluster["label"] for cluster in missing_clusters],
        }
        headline = "Market evidence is balanced across trend, participation and derivatives." if -25 < score < 25 else ("Risk appetite and broad participation dominate the tape." if score >= 25 else "Downside, weak participation and risk evidence dominate the tape.")
        narrative = f"Nifty 50 closed at {float(latest['close']):,.2f}. The 20-session return is {return20 or 0:+.2f}%; {above50 or 0:.1f}% of eligible current NIFTY 500 constituents are above their 50-day average and {above200 or 0:.1f}% are above their 200-day average. Futures positioning has {bullish} bullish, {bearish} bearish and {neutral} neutral states; {unavailable} of {total_fno} stocks are unavailable."
        story = {
            "as_of_date": latest["session_date"],
            "regime": {"label": label, "score": _round(score, 1), "close": latest["close"], "tone": tone},
            "confidence": confidence,
            "headline": headline,
            "narrative": narrative,
            "signals": signals,
            "market_context": {"as_of_date": latest["session_date"], "breadth": breadth, "relative_strength": relative},
            "historical_analogue": _historical_analogue(nifty),
            "catalysts": catalysts,
            "seasonality": seasonality,
        }
        return {
            "ok": True,
            "contract_version": "workspace-overview-v3",
            "as_of_date": latest["session_date"],
            "market_story": story,
            "fno": fno,
            "sentiment": {"status": "local_evidence", "decision_counts": {}, "evidence": confidence},
            "screener": {"status": "local_eod", "positive_20_session": breadth["positive_20d"], "calculation_ready": breadth["evaluated_20d"]},
            "events": {
                "status": "reviewed_official_calendar_snapshot",
                "next_event": catalysts[0] if catalysts else None,
                "events": catalysts,
                "coverage": {
                    "upcoming": len(catalysts),
                    "next_30_days": sum(1 for row in catalysts if row["days_until"] <= 30),
                },
            },
        }
    finally:
        connection.close()


def dashboard_overview(
    market_db: Path = DEFAULT_MARKET_DB,
    snapshot_db: Path = DEFAULT_SNAPSHOT_DB,
) -> dict[str, Any]:
    """Return the overview from a semantic, completed-session result cache."""
    contract_version = "workspace-overview-v3"
    source_version = semantic_source_version(
        (
            (market_db, (
                ("candles", "session_date", "retrieved_at"),
                ("futures_contracts", "expiry_date", "last_seen_at"),
                ("futures_eod_snapshots", "session_date", "retrieved_at"),
                ("institutional_flows", "session_date", "retrieved_at"),
                ("global_risk_observations", "observation_date", "retrieved_at"),
                ("universe_membership_events", "effective_date", "retrieved_at"),
                ("universe_classification_snapshots", "snapshot_date", "retrieved_at"),
            )),
            (snapshot_db, (
                ("sovereign_yield_observations", "observation_date", "retrieved_at"),
            )),
        ),
        context={"calendar_date": date.today().isoformat()},
    )
    cached = load_versioned_result(
        market_db, "dashboard_overview", contract_version, source_version,
    )
    if cached is not None:
        return cached
    payload = _dashboard_overview_uncached(market_db, snapshot_db)
    if payload.get("ok"):
        store_versioned_result(
            market_db, "dashboard_overview", contract_version, source_version, payload,
        )
    return payload


def domestic_core(market_db: Path = DEFAULT_MARKET_DB, snapshot_db: Path = DEFAULT_SNAPSHOT_DB) -> dict[str, Any]:
    connection = _connect(market_db)
    try:
        overview = dashboard_overview(market_db)
        fno = _fno_state(connection)
        global_risk = _global_risk_snapshot(connection, overview.get("as_of_date"))
    finally:
        connection.close()
    snap = _connect(snapshot_db)
    try:
        india = snap.execute(
            """SELECT observation_date,yield_pct FROM sovereign_yield_observations
               WHERE market='India' AND tenor_years=10 ORDER BY observation_date DESC LIMIT 1"""
        ).fetchone()
    finally:
        snap.close()
    metrics = dict(global_risk["metrics"])
    if india:
        metrics["india_10y_gsec_yield"] = {"level": india["yield_pct"], "as_of_date": india["observation_date"]}
    return {
        "ok": True,
        "as_of_date": overview.get("as_of_date"),
        "futures_oi": fno["futures_oi"],
        "fno_archive_backfill": fno["archive_backfill"],
        "fno_raw_history_sessions": fno["raw_history_sessions"],
        "fno_comparable_history_sessions": fno["stored_history_sessions"],
        "macro_context": {"available": bool(india), "as_of_date": india["observation_date"] if india else None, "metrics": metrics},
        "global_risk": {**global_risk, "metrics": metrics},
    }


def _technical_rows(market_db: Path) -> list[dict[str, Any]]:
    connection = _connect(market_db)
    try:
        instruments = connection.execute("SELECT instrument_id,display_name FROM instruments WHERE kind='stock'").fetchall()
        nifty = _instrument_series(connection, "index:Nifty 50")
        nifty20 = _pct(float(nifty[-1]["close"]), float(nifty[-21]["close"])) if len(nifty) >= 21 else None
        results = []
        for item in instruments:
            series = _instrument_series(connection, item["instrument_id"])
            if not series:
                continue
            closes = [float(row["close"]) for row in series]
            latest = series[-1]
            latest_year = str(latest["session_date"])[:4]
            prior_year_close = next(
                (float(row["close"]) for row in reversed(series) if str(row["session_date"]) < f"{latest_year}-01-01"),
                None,
            )
            ret = lambda days: _pct(closes[-1], closes[-days - 1]) if len(closes) > days else None
            dma = lambda days: _pct(closes[-1], mean(closes[-days:])) if len(closes) >= days else None
            daily = [math.log(closes[index] / closes[index - 1]) for index in range(max(1, len(closes) - 20), len(closes))]
            vol = None if len(daily) < 10 else 100 * (mean((value - mean(daily)) ** 2 for value in daily) ** 0.5) * math.sqrt(252)
            symbol = item["instrument_id"].split(":", 1)[-1]
            results.append({
                "symbol": symbol, "name": item["display_name"], "sector": "Unclassified",
                "close": latest["close"], "as_of_date": latest["session_date"],
                "ytd_return_pct": _round(_pct(closes[-1], prior_year_close)),
                "return_20d_pct": _round(ret(20)), "return_60d_pct": _round(ret(60)),
                "excess_20d_vs_nifty_pct": _round((ret(20) or 0) - (nifty20 or 0)),
                "vs_20dma_pct": _round(dma(20)), "vs_50dma_pct": _round(dma(50)), "vs_200dma_pct": _round(dma(200)),
                "realised_volatility_20d_pct": _round(vol), "status": "ready" if len(closes) >= 200 else "partial",
            })
        return results
    finally:
        connection.close()


def _attach_comparison_evidence_rankings(results: list[dict[str, Any]]) -> dict[str, Any]:
    return attach_comparison_evidence_rankings(results)


_UNIVERSE_OOS_LOCK = threading.Lock()
_UNIVERSE_OOS_LATEST: dict[str, dict[str, Any]] = {}
_UNIVERSE_OOS_CACHE_CONTRACT = "universe-oos-persistent-cache-v1"
_DEFAULT_OOS_SIGNATURES = {
    "moving_average_crossover": ((20, 50), 51),
    "donchian_breakout": ((55, 20), 57),
    "ema_crossover": ((20, 50), 51),
    "time_series_momentum": ((63,), 65),
    "triple_sma_alignment": ((20, 50, 200), 201),
    "rsi_mean_reversion": ((2, 10.0, 70.0), 4),
    "bollinger_mean_reversion": ((20, 2.0), 21),
    "zscore_mean_reversion": ((10, -1.5, 0.0), 11),
    "pullback_uptrend": ((5, 200), 201),
    "selloff_recovery": ((3, 5), 6),
}


def _oos_source_version(
    market_db: Path, snapshot_db: Path = DEFAULT_SNAPSHOT_DB,
) -> str:
    connection = _connect(market_db)
    try:
        coverage = connection.execute(
            "SELECT value,updated_at FROM workspace_metadata WHERE key='corporate_action_coverage'"
        ).fetchone()
        coverage_context = dict(coverage) if coverage else None
    finally:
        connection.close()
    return semantic_source_version(
        (
            (market_db, (
                ("candles", "session_date", "retrieved_at"),
                ("instruments", "updated_at", "updated_at"),
                ("corporate_actions", "ex_date", "retrieved_at"),
                ("symbol_history", "effective_date", "retrieved_at"),
                ("universe_membership_events", "effective_date", "retrieved_at"),
                ("universe_notice_review_windows", "reviewed_through", "retrieved_at"),
                ("universe_classification_snapshots", "snapshot_date", "retrieved_at"),
                ("universe_lineage_intervals", "effective_date", "retrieved_at"),
                ("cash_archive_symbol_checks", "session_date", "retrieved_at"),
            )),
            (snapshot_db, (
                ("equity_size_classifications", "snapshot_date", "retrieved_at"),
            )),
        ),
        context={"corporate_action_coverage": coverage_context},
    )


def _oos_cache_key(
    engine: str, series_arguments: tuple[Any, ...], required_observations: int,
) -> str:
    signature = json.dumps(
        {"engine": engine, "arguments": series_arguments, "required": required_observations},
        sort_keys=True, separators=(",", ":"),
    )
    return f"universe_oos:{hashlib.sha256(signature.encode()).hexdigest()}"


def _hydrate_persistent_oos_cache(market_db: Path, source_version: str) -> None:
    stored = load_versioned_results_by_prefix(
        market_db, "universe_oos:", _UNIVERSE_OOS_CACHE_CONTRACT, source_version,
    )
    with _UNIVERSE_OOS_LOCK:
        for cache_key, envelope in stored.items():
            if not envelope.get("default_family"):
                continue
            engine = str(envelope.get("engine") or "")
            if engine in _DEFAULT_OOS_SIGNATURES:
                _UNIVERSE_OOS_LATEST[engine] = {
                    "database_version": source_version,
                    "cache_key": cache_key,
                    "evidence": envelope.get("evidence", {}),
                }


def _compact_walk_forward_evidence(validation: dict[str, Any], audit: dict[str, Any]) -> dict[str, Any]:
    if validation.get("status") != "ready":
        return {
            "status": "insufficient_full_history",
            "contract_version": "universe-walk-forward-evidence-v1",
            "available_evaluable_sessions": validation.get("available_evaluable_sessions"),
            "required_evaluable_sessions": validation.get("required_evaluable_sessions"),
            "history_start_date": audit.get("history_start_date"),
            "history_end_date": audit.get("latest_observation"),
            "ranking_admissible": False,
            "failed_checks": ["full_history_walk_forward_unavailable"],
        }
    admitted_tests = [
        fold.get("test", {}) for fold in validation.get("folds", [])
        if fold.get("point_in_time_universe", {}).get("ranking_admissible")
        and fold.get("test", {}).get("status") == "ready"
    ]

    def values(name: str) -> list[float]:
        return [float(period[name]) for period in admitted_tests if period.get(name) is not None]

    def median_value(name: str) -> float | None:
        observed = values(name)
        return _round(median(observed)) if observed else None

    admission = validation.get("ranking_admission") or {}
    statistical = validation.get("out_of_sample_statistical_evidence") or {}
    metrics = dict(admission.get("metrics") or {})
    metrics.update({
        "median_excess_return_pct": median_value("excess_return_pct"),
        "median_exposure_pct": median_value("exposure_pct"),
        "median_cost_drag_pct": median_value("estimated_cost_drag_pct"),
        "completed_sells": sum(int(period.get("executed_sells") or 0) for period in admitted_tests),
        "evaluated_test_sessions": sum(int(period.get("evaluated_sessions") or 0) for period in admitted_tests),
    })
    return {
        "status": "ready",
        "contract_version": "universe-walk-forward-evidence-v1",
        "history_start_date": audit.get("history_start_date"),
        "history_end_date": audit.get("latest_observation"),
        "fold_count": validation.get("fold_count"),
        "point_in_time_coverage": validation.get("point_in_time_coverage"),
        "ranking_admissible": bool(admission.get("ranking_admissible")),
        "failed_checks": list(admission.get("failed_checks") or []),
        "metrics": metrics,
        "net_trade_edge_p_value": statistical.get("net_trade_edge_p_value"),
    }


def _universe_oos_evidence_map(
    market_db: Path,
    engine: str,
    series_function: Any,
    series_arguments: tuple[Any, ...],
    required_observations: int,
) -> dict[str, dict[str, Any]]:
    source_version = _oos_source_version(market_db)
    cache_key = _oos_cache_key(engine, series_arguments, required_observations)
    cached = load_versioned_result(
        market_db, cache_key, _UNIVERSE_OOS_CACHE_CONTRACT, source_version,
    )
    if cached is not None:
        evidence = cached.get("evidence", {})
        if cached.get("default_family"):
            with _UNIVERSE_OOS_LOCK:
                _UNIVERSE_OOS_LATEST[engine] = {
                    "database_version": source_version,
                    "cache_key": cache_key,
                    "evidence": evidence,
                }
        return evidence
    connection = _connect(market_db)
    evidence: dict[str, dict[str, Any]] = {}
    dated_universe_cache: dict[str, dict[str, Any]] = {}
    try:
        reference_context = _backtest_reference_context(connection)
        instruments = connection.execute(
            "SELECT instrument_id FROM instruments WHERE kind='stock' ORDER BY instrument_id"
        ).fetchall()
        for instrument in instruments:
            instrument_id = instrument["instrument_id"]
            symbol = instrument_id.split(":", 1)[-1]
            rows, audit = _quality_checked_backtest_series(
                connection, instrument_id, required_observations, reference_context, "full"
            )
            if audit.get("status") != "ready":
                evidence[symbol] = {
                    "status": "full_history_quality_gated",
                    "contract_version": "universe-walk-forward-evidence-v1",
                    "ranking_admissible": False,
                    "failed_checks": list(audit.get("blocking_gates") or ["full_history_data_quality"]),
                    "history_start_date": audit.get("history_start_date"),
                    "history_end_date": audit.get("latest_observation"),
                }
                continue
            validation = _rolling_walk_forward_validation(
                rows, series_function, series_arguments, required_observations
            )
            validation = _attach_walk_forward_universe_gates(
                validation, market_db, symbol=symbol, universe_cache=dated_universe_cache
            )
            evidence[symbol] = _compact_walk_forward_evidence(validation, audit)
    finally:
        connection.close()
    default_family = _DEFAULT_OOS_SIGNATURES.get(engine) == (
        tuple(series_arguments), required_observations,
    )
    envelope = {
        "engine": engine,
        "series_arguments": list(series_arguments),
        "required_observations": required_observations,
        "default_family": default_family,
        "evidence": evidence,
    }
    store_versioned_result(
        market_db, cache_key, _UNIVERSE_OOS_CACHE_CONTRACT, source_version, envelope,
    )
    if default_family:
        with _UNIVERSE_OOS_LOCK:
            _UNIVERSE_OOS_LATEST[engine] = {
                "database_version": source_version,
                "cache_key": cache_key,
                "evidence": evidence,
            }
    return evidence


def _attach_oos_evidence_rankings(
    results: list[dict[str, Any]],
    oos_evidence: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return attach_oos_evidence_rankings(results, oos_evidence)


def _moving_average_crossover_series(
    rows: list[Any],
    fast_window: int = 20,
    slow_window: int = 50,
) -> dict[str, Any]:
    """Calculate a completed-close, long-only SMA crossover without same-row returns.

    A signal observed on session *t* is shifted before return attribution, so the
    strategy never earns session *t*'s return from information first known at that
    session's close. This is a research calculation, not an execution simulator.
    """
    required = slow_window + 1
    if len(rows) < required:
        return {
            "status": "insufficient_history",
            "observations": len(rows),
            "required_observations": required,
            "reason": f"At least {required} completed sessions are required.",
        }

    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    fast_values: list[float | None] = [None] * len(closes)
    slow_values: list[float | None] = [None] * len(closes)
    fast_sum = slow_sum = 0.0
    for index, close in enumerate(closes):
        fast_sum += close
        slow_sum += close
        if index >= fast_window:
            fast_sum -= closes[index - fast_window]
        if index >= slow_window:
            slow_sum -= closes[index - slow_window]
        if index >= fast_window - 1:
            fast_values[index] = fast_sum / fast_window
        if index >= slow_window - 1:
            slow_values[index] = slow_sum / slow_window

    signals: list[int | None] = [None] * len(closes)
    for index in range(slow_window - 1, len(closes)):
        signals[index] = int((fast_values[index] or 0) > (slow_values[index] or 0))

    crossover_events: list[dict[str, Any]] = []
    for index in range(slow_window, len(closes)):
        prior_signal = int(signals[index - 1] or 0)
        current_signal = int(signals[index] or 0)
        if current_signal == prior_signal:
            continue
        event = {
            "signal_date": dates[index],
            "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
            "type": "bullish_cross" if current_signal else "bearish_cross",
            "new_signal": current_signal,
            "close": _round(closes[index]),
            "fast_sma": _round(fast_values[index]),
            "slow_sma": _round(slow_values[index]),
        }
        crossover_events.append(event)
    current_fast = fast_values[-1]
    current_slow = slow_values[-1]
    result = _long_cash_metrics(rows, [int(signal or 0) for signal in signals], slow_window - 1, crossover_events)
    result.update({
        "current_fast_sma": _round(current_fast),
        "current_slow_sma": _round(current_slow),
        "current_spread_pct": _round(_pct(current_fast, current_slow)),
        "crossovers": len(crossover_events),
        "last_crossover": result.get("last_event"),
        "crossover_events": result.pop("events"),
    })
    return result


def moving_average_crossover(
    market_db: Path = DEFAULT_MARKET_DB,
    symbol: str | None = None,
    fast_window: int = 20,
    slow_window: int = 50,
    history_mode: str = "comparison",
    include_oos_ranking: bool = True,
) -> dict[str, Any]:
    """Run the background SMA crossover research engine for one or all stocks."""
    if fast_window < 2 or slow_window <= fast_window or slow_window > 250:
        raise ValueError("invalid_moving_average_windows")
    requested_symbol = (symbol or "").strip().upper()
    connection = _connect(market_db)
    try:
        if requested_symbol:
            instruments = connection.execute(
                "SELECT instrument_id,display_name FROM instruments WHERE kind='stock' AND UPPER(SUBSTR(instrument_id, 7))=?",
                (requested_symbol,),
            ).fetchall()
        else:
            instruments = connection.execute(
                "SELECT instrument_id,display_name FROM instruments WHERE kind='stock' ORDER BY instrument_id"
            ).fetchall()
        reference_context = _backtest_reference_context(connection)
        results = []
        for instrument in instruments:
            prepared_rows, audit = _quality_checked_backtest_series(
                connection, instrument["instrument_id"], slow_window + 1, reference_context, history_mode
            )
            if audit["status"] == "ready":
                result = _moving_average_crossover_series(prepared_rows, fast_window, slow_window)
                if requested_symbol:
                    result["statistical_evidence"] = _bootstrap_trade_evidence(
                        result.get("_net_completed_trade_returns") or []
                    )
                    result["chronological_validation"] = _chronological_holdout_validation(
                        prepared_rows, _moving_average_crossover_series,
                        (fast_window, slow_window), slow_window + 1,
                    )
                    result["rolling_walk_forward"] = _attach_walk_forward_universe_gates(
                        _rolling_walk_forward_validation(
                            prepared_rows, _moving_average_crossover_series,
                            (fast_window, slow_window), slow_window + 1,
                        ),
                        market_db,
                        symbol=requested_symbol,
                    )
            else:
                result = {"status": "data_quality_gated", "observations": audit["stored_observations"],
                          "reason": ", ".join(audit["blocking_gates"])}
            result["data_quality"] = audit
            result["symbol"] = instrument["instrument_id"].split(":", 1)[-1]
            result["name"] = instrument["display_name"]
            result.pop("_completed_trade_returns", None)
            result.pop("_net_completed_trade_returns", None)
            if not requested_symbol:
                result.pop("crossover_events", None)
            results.append(result)
    finally:
        connection.close()
    if requested_symbol and not results:
        return {"ok": False, "reason": "stock_not_found", "symbol": requested_symbol}

    ready = [row for row in results if row["status"] == "ready"]
    quality_gated = [row for row in results if row["status"] == "data_quality_gated"]
    multiple_testing = (
        _attach_benjamini_hochberg_fdr(results) if not requested_symbol else
        {"contract_version": "benjamini-hochberg-fdr-v1", "status": "not_applicable_single_stock"}
    )
    evidence_ranking = _attach_comparison_evidence_rankings(results)
    if not requested_symbol and include_oos_ranking:
        oos_evidence = _universe_oos_evidence_map(
            market_db, "moving_average_crossover", _moving_average_crossover_series,
            (fast_window, slow_window), slow_window + 1,
        )
        evidence_ranking = _attach_oos_evidence_rankings(results, oos_evidence)
    invested = sum(row.get("current_executed_position") == "invested" for row in ready)
    outperforming = sum((row.get("excess_return_pct") or 0) > 0 for row in ready)
    return {
        "ok": True,
        "contract_version": "ma-crossover-backtest-v8",
        "engine": "moving_average_crossover",
        "strategy": {
            "fast_window": fast_window,
            "slow_window": slow_window,
            "average_type": "simple",
            "positioning": "long_or_cash",
            "signal_source": "completed_session_close",
            "execution_price_field": "next_session_open",
            "return_attribution": "completed_close_signal_executed_next_session_open",
            "cost_model": EQUITY_DELIVERY_COST_SCHEDULE["schedule_version"],
            "history_mode": history_mode,
        },
        "scope": "single_stock" if requested_symbol else "stored_stock_universe",
        "multiple_testing": multiple_testing,
        "evidence_ranking": evidence_ranking,
        "as_of_date": max((row.get("as_of_date") or "" for row in ready), default=None) or None,
        "data_quality_contract": {
            "common_as_of_date": reference_context.get("common_as_of_date"),
            "comparison_sessions": BACKTEST_COMPARISON_SESSIONS,
            "history_mode": history_mode,
            "minimum_universe_coverage_ratio": BACKTEST_COMMON_COVERAGE_RATIO,
            "covered_on_common_as_of": reference_context.get("covered_on_common_as_of"),
            "reference_instrument": reference_context.get("reference_instrument"),
            "abnormal_jump_gate_pct": BACKTEST_ABNORMAL_JUMP_PCT,
            "adjustment_provenance_reported": True,
            "adjustment_gate_enforced": True,
            "point_in_time_universe_contract": "nifty500-point-in-time-universe-v2",
            "point_in_time_universe_gate_enforced": False,
            "walk_forward_fold_universe_gate_enforced": True,
            "point_in_time_universe_status": "complete_for_reviewed_window",
        },
        "coverage": {
            "requested": len(instruments),
            "ready": len(ready),
            "data_quality_gated": len(quality_gated),
            "insufficient_history": sum(row["status"] == "insufficient_history" for row in results),
            "currently_invested": invested,
            "currently_cash": len(ready) - invested,
            "pending_next_session_actions": sum(bool(row.get("pending_next_session_action")) for row in ready),
            "rejected_execution_events": sum(int(row.get("rejected_execution_events") or 0) for row in ready),
            "outperformed_buy_and_hold": outperforming,
        },
        "limitations": [
            "Gross and estimated net results are both reported; net results apply the published Zerodha/NSE delivery schedule, ₹50,000 position capital, 5 bps slippage each side and zero market impact.",
            "Signals are observed after a completed close and executed only at the next valid session open; missing opens reject the transition.",
            "The point-in-time membership and classification ledger is complete for its reviewed window, but this current-universe ranking does not yet vary membership on every historical session; the survivorship-bias warning remains until walk-forward evaluation consumes dated universes.",
            ("The selected-stock study uses every continuous stored session after strategy warm-up; its corporate-action ledger may be partial before the audited coverage window."
             if history_mode == "full" else
             "Ranked results use the same 252 completed Nifty 50 sessions after strategy-specific warm-up; stale, incomplete, invalid-OHLC and unaudited extreme-jump series are excluded from ranking."),
            "Kite-reported corporate-action adjustment provenance is enforced. NSE events requiring manual lineage, delisting or unclassified treatment exclude the stock until reviewed; ordinary cash dividends remain outside price-return results.",
            "Trade-edge p-values remain exploratory two-sided t-tests. Comparable-universe ranking results publish Benjamini-Hochberg q-values within each strategy family; selected-stock studies publish deterministic ordinary and circular moving-block bootstrap intervals only after 15 completed net trades.",
            "Results are research evidence, not a trade recommendation or order instruction.",
        ],
        "results": results,
    }


def _donchian_breakout_series(
    rows: list[Any],
    entry_window: int = 55,
    exit_window: int = 20,
) -> dict[str, Any]:
    """Calculate a completed-close Donchian breakout with a shorter exit channel.

    The current close is compared only with prior completed sessions. A signal
    observed on session *t* is shifted before return attribution, matching the
    no-look-ahead convention used by the moving-average engine.
    """
    required = max(entry_window, exit_window) + 2
    if len(rows) < required:
        return {
            "status": "insufficient_history",
            "observations": len(rows),
            "required_observations": required,
            "reason": f"At least {required} completed sessions are required.",
        }

    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    start = max(entry_window, exit_window)
    signals: list[int] = [0] * len(closes)
    upper_channels: list[float | None] = [None] * len(closes)
    lower_channels: list[float | None] = [None] * len(closes)
    events: list[dict[str, Any]] = []

    for index in range(start, len(closes)):
        upper = max(closes[index - entry_window:index])
        lower = min(closes[index - exit_window:index])
        upper_channels[index] = upper
        lower_channels[index] = lower
        prior_signal = signals[index - 1]
        current_signal = prior_signal
        event_type = None
        if not prior_signal and closes[index] > upper:
            current_signal = 1
            event_type = "upside_breakout"
        elif prior_signal and closes[index] < lower:
            current_signal = 0
            event_type = "downside_exit"
        signals[index] = current_signal
        if event_type:
            event = {
                "signal_date": dates[index],
                "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
                "type": event_type,
                "new_signal": current_signal,
                "close": _round(closes[index]),
                "entry_channel": _round(upper),
                "exit_channel": _round(lower),
            }
            events.append(event)
    result = _long_cash_metrics(rows, signals, start, events)
    result.update({
        "current_entry_channel": _round(upper_channels[-1]),
        "current_exit_channel": _round(lower_channels[-1]),
        "distance_to_entry_pct": _round(_pct(closes[-1], upper_channels[-1])),
        "distance_to_exit_pct": _round(_pct(closes[-1], lower_channels[-1])),
        "breakouts": sum(event["type"] == "upside_breakout" for event in events),
    })
    return result


def donchian_breakout(
    market_db: Path = DEFAULT_MARKET_DB,
    symbol: str | None = None,
    entry_window: int = 55,
    exit_window: int = 20,
    history_mode: str = "comparison",
    include_oos_ranking: bool = True,
) -> dict[str, Any]:
    """Run the long-only Donchian breakout research engine for one or all stocks."""
    if exit_window < 2 or entry_window <= exit_window or entry_window > 250:
        raise ValueError("invalid_donchian_windows")
    requested_symbol = (symbol or "").strip().upper()
    connection = _connect(market_db)
    try:
        if requested_symbol:
            instruments = connection.execute(
                "SELECT instrument_id,display_name FROM instruments WHERE kind='stock' AND UPPER(SUBSTR(instrument_id, 7))=?",
                (requested_symbol,),
            ).fetchall()
        else:
            instruments = connection.execute(
                "SELECT instrument_id,display_name FROM instruments WHERE kind='stock' ORDER BY instrument_id"
            ).fetchall()
        reference_context = _backtest_reference_context(connection)
        results = []
        for instrument in instruments:
            required_observations = max(entry_window, exit_window) + 2
            prepared_rows, audit = _quality_checked_backtest_series(
                connection, instrument["instrument_id"], required_observations, reference_context, history_mode
            )
            if audit["status"] == "ready":
                result = _donchian_breakout_series(prepared_rows, entry_window, exit_window)
                if requested_symbol:
                    result["statistical_evidence"] = _bootstrap_trade_evidence(
                        result.get("_net_completed_trade_returns") or []
                    )
                    result["chronological_validation"] = _chronological_holdout_validation(
                        prepared_rows, _donchian_breakout_series,
                        (entry_window, exit_window), required_observations,
                    )
                    result["rolling_walk_forward"] = _attach_walk_forward_universe_gates(
                        _rolling_walk_forward_validation(
                            prepared_rows, _donchian_breakout_series,
                            (entry_window, exit_window), required_observations,
                        ),
                        market_db,
                        symbol=requested_symbol,
                    )
            else:
                result = {"status": "data_quality_gated", "observations": audit["stored_observations"],
                          "reason": ", ".join(audit["blocking_gates"])}
            result["data_quality"] = audit
            result["symbol"] = instrument["instrument_id"].split(":", 1)[-1]
            result["name"] = instrument["display_name"]
            result.pop("_completed_trade_returns", None)
            result.pop("_net_completed_trade_returns", None)
            if not requested_symbol:
                result.pop("events", None)
            results.append(result)
    finally:
        connection.close()
    if requested_symbol and not results:
        return {"ok": False, "reason": "stock_not_found", "symbol": requested_symbol}

    ready = [row for row in results if row["status"] == "ready"]
    quality_gated = [row for row in results if row["status"] == "data_quality_gated"]
    multiple_testing = (
        _attach_benjamini_hochberg_fdr(results) if not requested_symbol else
        {"contract_version": "benjamini-hochberg-fdr-v1", "status": "not_applicable_single_stock"}
    )
    evidence_ranking = _attach_comparison_evidence_rankings(results)
    if not requested_symbol and include_oos_ranking:
        oos_evidence = _universe_oos_evidence_map(
            market_db, "donchian_breakout", _donchian_breakout_series,
            (entry_window, exit_window), required_observations,
        )
        evidence_ranking = _attach_oos_evidence_rankings(results, oos_evidence)
    invested = sum(row.get("current_executed_position") == "invested" for row in ready)
    outperforming = sum((row.get("excess_return_pct") or 0) > 0 for row in ready)
    return {
        "ok": True,
        "contract_version": "donchian-breakout-backtest-v8",
        "engine": "donchian_breakout",
        "strategy": {
            "entry_window": entry_window,
            "exit_window": exit_window,
            "positioning": "long_or_cash",
            "signal_source": "completed_session_close",
            "entry_rule": "close_above_prior_entry_channel",
            "exit_rule": "close_below_prior_exit_channel",
            "execution_price_field": "next_session_open",
            "return_attribution": "completed_close_signal_executed_next_session_open",
            "cost_model": EQUITY_DELIVERY_COST_SCHEDULE["schedule_version"],
            "history_mode": history_mode,
        },
        "scope": "single_stock" if requested_symbol else "stored_stock_universe",
        "multiple_testing": multiple_testing,
        "evidence_ranking": evidence_ranking,
        "as_of_date": max((row.get("as_of_date") or "" for row in ready), default=None) or None,
        "data_quality_contract": {
            "common_as_of_date": reference_context.get("common_as_of_date"),
            "comparison_sessions": BACKTEST_COMPARISON_SESSIONS,
            "history_mode": history_mode,
            "minimum_universe_coverage_ratio": BACKTEST_COMMON_COVERAGE_RATIO,
            "covered_on_common_as_of": reference_context.get("covered_on_common_as_of"),
            "reference_instrument": reference_context.get("reference_instrument"),
            "abnormal_jump_gate_pct": BACKTEST_ABNORMAL_JUMP_PCT,
            "adjustment_provenance_reported": True,
            "adjustment_gate_enforced": True,
            "point_in_time_universe_contract": "nifty500-point-in-time-universe-v2",
            "point_in_time_universe_gate_enforced": False,
            "walk_forward_fold_universe_gate_enforced": True,
            "point_in_time_universe_status": "complete_for_reviewed_window",
        },
        "coverage": {
            "requested": len(instruments),
            "ready": len(ready),
            "data_quality_gated": len(quality_gated),
            "insufficient_history": sum(row["status"] == "insufficient_history" for row in results),
            "currently_invested": invested,
            "currently_cash": len(ready) - invested,
            "pending_next_session_actions": sum(bool(row.get("pending_next_session_action")) for row in ready),
            "rejected_execution_events": sum(int(row.get("rejected_execution_events") or 0) for row in ready),
            "outperformed_buy_and_hold": outperforming,
        },
        "limitations": [
            "Gross and estimated net results are both reported; net results apply the published Zerodha/NSE delivery schedule, ₹50,000 position capital, 5 bps slippage each side and zero market impact.",
            "Channel levels use prior completed sessions; state changes execute only at the next valid session open.",
            "The point-in-time membership and classification ledger is complete for its reviewed window, but this current-universe ranking does not yet vary membership on every historical session; the survivorship-bias warning remains until walk-forward evaluation consumes dated universes.",
            ("The selected-stock study uses every continuous stored session after strategy warm-up; its corporate-action ledger may be partial before the audited coverage window."
             if history_mode == "full" else
             "Ranked results use the same 252 completed Nifty 50 sessions after strategy-specific warm-up; stale, incomplete, invalid-OHLC and unaudited extreme-jump series are excluded from ranking."),
            "Kite-reported corporate-action adjustment provenance is enforced. NSE events requiring manual lineage, delisting or unclassified treatment exclude the stock until reviewed; ordinary cash dividends remain outside price-return results.",
            "Trade-edge p-values remain exploratory two-sided t-tests. Comparable-universe ranking results publish Benjamini-Hochberg q-values within each strategy family; selected-stock studies publish deterministic ordinary and circular moving-block bootstrap intervals only after 15 completed net trades.",
            "Results are research evidence, not a trade recommendation or order instruction.",
        ],
        "results": results,
    }


def _long_cash_metrics(
    rows: list[Any],
    signals: list[int],
    start: int,
    events: list[dict[str, Any]],
    cost_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build executable next-open metrics for a completed-close long/cash signal.

    A state change observed after session ``t`` closes is eligible for execution
    only at session ``t + 1``'s valid opening price.  The position that existed at
    the previous close earns the overnight gap; the newly executed position earns
    the open-to-close move.  If a required opening price is absent or invalid, the
    transition is rejected and the previous position remains in force.
    """
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    cost_assumptions = _normalise_cost_config(cost_config)
    capital_inr = float(cost_assumptions["position_capital_inr"])

    def opening_price(index: int) -> float | None:
        try:
            value = float(rows[index]["open"])
        except (KeyError, IndexError, TypeError, ValueError):
            return None
        return value if math.isfinite(value) and value > 0 else None

    events_by_date = {str(event["signal_date"]): event for event in events}
    execution_events: list[dict[str, Any]] = []
    strategy_returns: list[float] = []
    equity_curve = [1.0]
    net_equity_curve = [1.0]
    invested_sessions = 0
    rejected_executions = 0
    executed_position = 0
    entry_price: float | None = None
    entry_cost_inr: float | None = None
    entry_index: int | None = None
    entry_signal_date: str | None = None
    entry_execution_date: str | None = None
    completed_trade_returns: list[float] = []
    net_completed_trade_returns: list[float] = []
    completed_trades: list[dict[str, Any]] = []
    cost_breakdown = {key: 0.0 for key in (
        "brokerage", "stt", "exchange_transaction", "nse_ipft", "sebi", "gst",
        "stamp_duty", "dp_charge", "slippage", "market_impact", "total",
    )}
    executed_buys = executed_sells = 0

    for index in range(start + 1, len(closes)):
        previous_close = closes[index - 1]
        current_close = closes[index]
        target_position = int(signals[index - 1] or 0)
        current_open = opening_price(index)
        signal_event = events_by_date.get(dates[index - 1])

        if target_position != executed_position and current_open is not None:
            overnight_factor = current_open / previous_close if executed_position else 1.0
            value_at_open = net_equity_curve[-1] * overnight_factor
            turnover_inr = value_at_open * capital_inr
            side = "buy" if target_position else "sell"
            charges = _equity_delivery_side_charges(turnover_inr, side, cost_assumptions)
            for key, value in charges.items():
                cost_breakdown[key] += value
            cost_in_equity_units = charges["total"] / capital_inr
            executed_position = target_position
            intraday_factor = current_close / current_open if executed_position else 1.0
            session_factor = overnight_factor * intraday_factor
            net_value_after_cost = max(0.0, value_at_open - cost_in_equity_units)
            net_equity = net_value_after_cost * intraday_factor
            if signal_event is not None:
                execution_event = dict(signal_event)
                execution_event.update({
                    "execution_status": "executed",
                    "execution_date": dates[index],
                    "execution_price": _round(current_open),
                    "execution_price_field": "open",
                    "estimated_charges_inr": _round(charges["total"], 4),
                })
                execution_events.append(execution_event)
            if executed_position:
                entry_price = current_open
                entry_cost_inr = _equity_delivery_side_charges(capital_inr, "buy", cost_assumptions)["total"]
                entry_index = index
                entry_signal_date = str(signal_event.get("signal_date")) if signal_event else dates[index - 1]
                entry_execution_date = dates[index]
                executed_buys += 1
            elif entry_price is not None:
                gross_trade_return = 100 * (current_open / entry_price - 1.0)
                completed_trade_returns.append(gross_trade_return)
                entry_net_capital = capital_inr - float(entry_cost_inr or 0.0)
                exit_turnover = entry_net_capital * current_open / entry_price
                exit_cost = _equity_delivery_side_charges(exit_turnover, "sell", cost_assumptions)["total"]
                net_trade_return = 100 * ((exit_turnover - exit_cost) / capital_inr - 1.0)
                net_completed_trade_returns.append(net_trade_return)
                holding_rows = rows[entry_index:index + 1] if entry_index is not None else []
                maximum_favourable = (
                    100 * (max(float(_price_row_value(row, "high", _price_row_value(row, "close"))) for row in holding_rows) / entry_price - 1.0)
                    if holding_rows else None
                )
                maximum_adverse = (
                    100 * (min(float(_price_row_value(row, "low", _price_row_value(row, "close"))) for row in holding_rows) / entry_price - 1.0)
                    if holding_rows else None
                )
                completed_trades.append({
                    "entry_signal_date": entry_signal_date,
                    "entry_execution_date": entry_execution_date,
                    "exit_signal_date": str(signal_event.get("signal_date")) if signal_event else dates[index - 1],
                    "exit_execution_date": dates[index],
                    "holding_sessions": (index - entry_index) if entry_index is not None else None,
                    "gross_return_pct": _round(gross_trade_return, 6),
                    "net_return_pct": _round(net_trade_return, 6),
                    "maximum_favourable_excursion_pct": _round(maximum_favourable, 6),
                    "maximum_adverse_excursion_pct": _round(maximum_adverse, 6),
                    "move_capture_ratio": (
                        _round(gross_trade_return / maximum_favourable, 6)
                        if maximum_favourable is not None and maximum_favourable > 0 else None
                    ),
                })
                entry_price = None
                entry_cost_inr = None
                entry_index = None
                entry_signal_date = None
                entry_execution_date = None
                executed_sells += 1
        elif target_position != executed_position:
            rejected_executions += 1
            session_factor = current_close / previous_close if executed_position else 1.0
            net_equity = net_equity_curve[-1] * session_factor
            if signal_event is not None:
                execution_event = dict(signal_event)
                execution_event.update({
                    "execution_status": "rejected_missing_open",
                    "execution_date": dates[index],
                    "execution_price": None,
                    "execution_price_field": "open",
                })
                execution_events.append(execution_event)
        else:
            session_factor = current_close / previous_close if executed_position else 1.0
            net_equity = net_equity_curve[-1] * session_factor

        session_return = session_factor - 1.0
        strategy_returns.append(session_return)
        invested_sessions += executed_position
        equity_curve.append(equity_curve[-1] * session_factor)
        net_equity_curve.append(net_equity)

    pending_events = []
    if events and events[-1]["signal_date"] == dates[-1]:
        pending_event = dict(events[-1])
        pending_event.update({
            "execution_status": "pending_next_session",
            "execution_date": None,
            "execution_price": None,
            "execution_price_field": "open",
        })
        pending_events.append(pending_event)
        execution_events.append(pending_event)

    compounded_return = 100 * (equity_curve[-1] - 1.0)
    net_compounded_return = 100 * (net_equity_curve[-1] - 1.0)
    benchmark_open = opening_price(start + 1) if start + 1 < len(rows) else None
    benchmark_return = None if benchmark_open is None else 100 * (closes[-1] / benchmark_open - 1.0)
    sessions = len(strategy_returns)
    annualised_return = 100 * (equity_curve[-1] ** (252 / sessions) - 1.0) if sessions and equity_curve[-1] > 0 else None
    net_annualised_return = 100 * (net_equity_curve[-1] ** (252 / sessions) - 1.0) if sessions and net_equity_curve[-1] > 0 else None
    annualised_volatility = None
    if len(strategy_returns) >= 10:
        average_return = mean(strategy_returns)
        variance = mean((value - average_return) ** 2 for value in strategy_returns)
        annualised_volatility = 100 * math.sqrt(variance) * math.sqrt(252)
    winning_returns = [value for value in completed_trade_returns if value > 0]
    losing_returns = [value for value in completed_trade_returns if value < 0]
    breakeven_returns = [value for value in completed_trade_returns if value == 0]
    gross_profit = sum(winning_returns)
    gross_loss = abs(sum(losing_returns))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None
    net_winning_returns = [value for value in net_completed_trade_returns if value > 0]
    net_losing_returns = [value for value in net_completed_trade_returns if value < 0]
    net_gross_profit = sum(net_winning_returns)
    net_gross_loss = abs(sum(net_losing_returns))
    net_profit_factor = net_gross_profit / net_gross_loss if net_gross_loss > 0 else None
    buy_hold_cost = None
    net_benchmark_return = None
    if benchmark_open is not None:
        buy_cost = _equity_delivery_side_charges(capital_inr, "buy", cost_assumptions)["total"]
        buy_hold_shares_value = capital_inr - buy_cost
        sell_turnover = buy_hold_shares_value * closes[-1] / benchmark_open
        sell_cost = _equity_delivery_side_charges(sell_turnover, "sell", cost_assumptions)["total"]
        buy_hold_cost = buy_cost + sell_cost
        net_benchmark_return = 100 * ((sell_turnover - sell_cost) / capital_inr - 1.0)
    return {
        "status": "ready",
        "observations": len(rows),
        "evaluation_start": dates[start],
        "first_execution_date": dates[start + 1] if start + 1 < len(dates) else None,
        "as_of_date": dates[-1],
        "current_signal": "invested" if signals[-1] else "cash",
        "current_executed_position": "invested" if executed_position else "cash",
        "pending_next_session_action": int(signals[-1] or 0) != executed_position,
        "execution_convention": "completed_close_signal_next_session_open",
        "strategy_return_pct": _round(compounded_return),
        "gross_strategy_return_pct": _round(compounded_return),
        "net_strategy_return_pct": _round(net_compounded_return),
        "estimated_cost_drag_pct": _round(compounded_return - net_compounded_return),
        "annualised_return_pct": _round(annualised_return),
        "net_annualised_return_pct": _round(net_annualised_return),
        "buy_and_hold_return_pct": _round(benchmark_return),
        "net_buy_and_hold_return_pct": _round(net_benchmark_return),
        "estimated_buy_and_hold_cost_inr": _round(buy_hold_cost, 4),
        "excess_return_pct": _round(compounded_return - benchmark_return) if benchmark_return is not None else None,
        "maximum_drawdown_pct": _maximum_drawdown_pct(equity_curve),
        "net_maximum_drawdown_pct": _maximum_drawdown_pct(net_equity_curve),
        "annualised_volatility_pct": _round(annualised_volatility),
        "evaluated_sessions": sessions,
        "invested_sessions": invested_sessions,
        "exposure_pct": _round(100 * invested_sessions / sessions) if sessions else None,
        "completed_trades": len(completed_trade_returns),
        "winning_trades": len(winning_returns),
        "losing_trades": len(losing_returns),
        "breakeven_trades": len(breakeven_returns),
        "win_rate_pct": _round(100 * len(winning_returns) / len(completed_trade_returns)) if completed_trade_returns else None,
        "average_winner_pct": _round(mean(winning_returns)) if winning_returns else None,
        "average_loser_pct": _round(mean(losing_returns)) if losing_returns else None,
        "gross_profit_pct": _round(gross_profit),
        "gross_loss_pct": _round(gross_loss),
        "profit_factor": _round(profit_factor),
        "net_profit_factor": _round(net_profit_factor),
        "trade_expectancy_pct": _round(mean(completed_trade_returns)) if completed_trade_returns else None,
        "net_trade_expectancy_pct": _round(mean(net_completed_trade_returns)) if net_completed_trade_returns else None,
        "trade_edge_p_value": _trade_edge_p_value(completed_trade_returns),
        "net_trade_edge_p_value": _trade_edge_p_value(net_completed_trade_returns),
        "trade_edge_test": "two_sided_one_sample_t_test_vs_zero",
        "_completed_trade_returns": completed_trade_returns,
        "_net_completed_trade_returns": net_completed_trade_returns,
        "_completed_trades": completed_trades,
        "executed_events": sum(event["execution_status"] == "executed" for event in execution_events),
        "rejected_execution_events": rejected_executions,
        "pending_execution_events": len(pending_events),
        "executed_buys": executed_buys,
        "executed_sells": executed_sells,
        "estimated_total_cost_inr": _round(cost_breakdown["total"], 4),
        "estimated_cost_breakdown_inr": {key: _round(value, 4) for key, value in cost_breakdown.items()},
        "cost_model": {
            "contract_version": "equity-delivery-cost-v1",
            "schedule_version": EQUITY_DELIVERY_COST_SCHEDULE["schedule_version"],
            "assumptions": cost_assumptions,
            "rate_basis": "Current published schedule applied uniformly; historical rate changes are not reconstructed.",
        },
        "last_event": execution_events[-1] if execution_events else None,
        "events": execution_events,
    }


def _ema_crossover_series(rows: list[Any], fast_window: int = 20, slow_window: int = 50) -> dict[str, Any]:
    required = slow_window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows), "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    fast_alpha, slow_alpha = 2 / (fast_window + 1), 2 / (slow_window + 1)
    fast_values, slow_values = [closes[0]], [closes[0]]
    for close in closes[1:]:
        fast_values.append(fast_alpha * close + (1 - fast_alpha) * fast_values[-1])
        slow_values.append(slow_alpha * close + (1 - slow_alpha) * slow_values[-1])
    start = slow_window - 1
    signals = [0] * len(closes)
    events: list[dict[str, Any]] = []
    for index in range(start, len(closes)):
        signals[index] = int(fast_values[index] > slow_values[index])
        prior_signal = signals[index - 1] if index > start else 0
        if signals[index] != prior_signal:
            events.append({"signal_date": dates[index], "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
                           "type": "bullish_cross" if signals[index] else "bearish_cross", "new_signal": signals[index],
                           "close": _round(closes[index]), "fast_ema": _round(fast_values[index]), "slow_ema": _round(slow_values[index])})
    result = _long_cash_metrics(rows, signals, start, events)
    result.update({"current_fast_ema": _round(fast_values[-1]), "current_slow_ema": _round(slow_values[-1]),
                   "current_spread_pct": _round(_pct(fast_values[-1], slow_values[-1])), "crossovers": len(events)})
    return result


def _time_series_momentum_series(rows: list[Any], lookback: int = 63) -> dict[str, Any]:
    required = lookback + 2
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows), "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    signals = [0] * len(closes)
    events: list[dict[str, Any]] = []
    for index in range(lookback, len(closes)):
        momentum = 100 * (closes[index] / closes[index - lookback] - 1.0)
        signals[index] = int(momentum > 0)
        prior_signal = signals[index - 1] if index > lookback else 0
        if signals[index] != prior_signal:
            events.append({"signal_date": dates[index], "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
                           "type": "momentum_positive" if signals[index] else "momentum_non_positive", "new_signal": signals[index],
                           "close": _round(closes[index]), "lookback_return_pct": _round(momentum)})
    result = _long_cash_metrics(rows, signals, lookback, events)
    result.update({"current_lookback_return_pct": _round(100 * (closes[-1] / closes[-1 - lookback] - 1.0)),
                   "signal_changes": len(events)})
    return result


def _triple_sma_alignment_series(
    rows: list[Any], fast_window: int = 20, medium_window: int = 50, slow_window: int = 200
) -> dict[str, Any]:
    required = slow_window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows), "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    def rolling_average(window: int) -> list[float | None]:
        values: list[float | None] = [None] * len(closes)
        running = 0.0
        for index, close in enumerate(closes):
            running += close
            if index >= window:
                running -= closes[index - window]
            if index >= window - 1:
                values[index] = running / window
        return values
    fast_values, medium_values, slow_values = rolling_average(fast_window), rolling_average(medium_window), rolling_average(slow_window)
    start = slow_window - 1
    signals = [0] * len(closes)
    events: list[dict[str, Any]] = []
    for index in range(start, len(closes)):
        signals[index] = int(float(fast_values[index] or 0) > float(medium_values[index] or 0) > float(slow_values[index] or 0))
        prior_signal = signals[index - 1] if index > start else 0
        if signals[index] != prior_signal:
            events.append({"signal_date": dates[index], "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
                           "type": "alignment_on" if signals[index] else "alignment_off", "new_signal": signals[index],
                           "close": _round(closes[index]), "fast_sma": _round(fast_values[index]),
                           "medium_sma": _round(medium_values[index]), "slow_sma": _round(slow_values[index])})
    result = _long_cash_metrics(rows, signals, start, events)
    result.update({"current_fast_sma": _round(fast_values[-1]), "current_medium_sma": _round(medium_values[-1]),
                   "current_slow_sma": _round(slow_values[-1]), "alignment_changes": len(events)})
    return result


def _state_machine_signals(
    dates: list[str], closes: list[float], start: int,
    entry_flags: list[bool], exit_flags: list[bool],
    entry_type: str, exit_type: str, contexts: list[dict[str, Any]],
) -> tuple[list[int], list[dict[str, Any]]]:
    """Translate completed-close entry/exit observations into a long/cash state."""
    signals = [0] * len(closes)
    events: list[dict[str, Any]] = []
    for index in range(start, len(closes)):
        prior_signal = signals[index - 1] if index > start else 0
        current_signal = prior_signal
        event_type = None
        if not prior_signal and entry_flags[index]:
            current_signal, event_type = 1, entry_type
        elif prior_signal and exit_flags[index]:
            current_signal, event_type = 0, exit_type
        signals[index] = current_signal
        if event_type:
            events.append({
                "signal_date": dates[index],
                "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
                "type": event_type,
                "new_signal": current_signal,
                "close": _round(closes[index]),
                **contexts[index],
            })
    return signals, events


def _rsi_mean_reversion_series(
    rows: list[Any], period: int = 2, entry_rsi: float = 10.0, exit_rsi: float = 70.0,
) -> dict[str, Any]:
    required = period + 2
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows), "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    rsi_values: list[float | None] = [None] * len(closes)
    for index in range(period, len(closes)):
        changes = [closes[pos] - closes[pos - 1] for pos in range(index - period + 1, index + 1)]
        gains = sum(max(change, 0.0) for change in changes) / period
        losses = sum(max(-change, 0.0) for change in changes) / period
        rsi_values[index] = 100.0 if losses == 0 else 100.0 - 100.0 / (1.0 + gains / losses)
    entry_flags = [False] * len(closes)
    exit_flags = [False] * len(closes)
    contexts: list[dict[str, Any]] = [{} for _ in closes]
    for index in range(period, len(closes)):
        rsi = float(rsi_values[index] or 0)
        entry_flags[index], exit_flags[index] = rsi <= entry_rsi, rsi >= exit_rsi
        contexts[index] = {"rsi": _round(rsi)}
    signals, events = _state_machine_signals(dates, closes, period, entry_flags, exit_flags,
                                              "oversold_entry", "rsi_recovery_exit", contexts)
    result = _long_cash_metrics(rows, signals, period, events)
    result.update({"current_rsi": _round(rsi_values[-1]), "signal_changes": len(events)})
    return result


def _bollinger_mean_reversion_series(
    rows: list[Any], window: int = 20, deviations: float = 2.0,
) -> dict[str, Any]:
    required = window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows), "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    means: list[float | None] = [None] * len(closes)
    lower_bands: list[float | None] = [None] * len(closes)
    entry_flags, exit_flags = [False] * len(closes), [False] * len(closes)
    contexts: list[dict[str, Any]] = [{} for _ in closes]
    start = window - 1
    for index in range(start, len(closes)):
        sample = closes[index - window + 1:index + 1]
        average = mean(sample)
        deviation = math.sqrt(mean((value - average) ** 2 for value in sample))
        lower = average - deviations * deviation
        means[index], lower_bands[index] = average, lower
        entry_flags[index], exit_flags[index] = closes[index] < lower, closes[index] >= average
        contexts[index] = {"middle_band": _round(average), "lower_band": _round(lower)}
    signals, events = _state_machine_signals(dates, closes, start, entry_flags, exit_flags,
                                              "lower_band_entry", "middle_band_exit", contexts)
    result = _long_cash_metrics(rows, signals, start, events)
    result.update({"current_middle_band": _round(means[-1]), "current_lower_band": _round(lower_bands[-1]),
                   "distance_to_middle_pct": _round(_pct(closes[-1], means[-1])), "signal_changes": len(events)})
    return result


def _zscore_mean_reversion_series(
    rows: list[Any], window: int = 10, entry_z: float = -1.5, exit_z: float = 0.0,
) -> dict[str, Any]:
    required = window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows), "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    zscores: list[float | None] = [None] * len(closes)
    entry_flags, exit_flags = [False] * len(closes), [False] * len(closes)
    contexts: list[dict[str, Any]] = [{} for _ in closes]
    start = window - 1
    for index in range(start, len(closes)):
        sample = closes[index - window + 1:index + 1]
        average = mean(sample)
        deviation = math.sqrt(mean((value - average) ** 2 for value in sample))
        zscore = (closes[index] - average) / deviation if deviation > 0 else 0.0
        zscores[index] = zscore
        entry_flags[index], exit_flags[index] = zscore <= entry_z, zscore >= exit_z
        contexts[index] = {"zscore": _round(zscore), "rolling_mean": _round(average)}
    signals, events = _state_machine_signals(dates, closes, start, entry_flags, exit_flags,
                                              "negative_zscore_entry", "mean_recovery_exit", contexts)
    result = _long_cash_metrics(rows, signals, start, events)
    result.update({"current_zscore": _round(zscores[-1]), "signal_changes": len(events)})
    return result


def _pullback_uptrend_series(
    rows: list[Any], pullback_window: int = 5, trend_window: int = 200,
) -> dict[str, Any]:
    required = trend_window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows), "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    fast_values: list[float | None] = [None] * len(closes)
    trend_values: list[float | None] = [None] * len(closes)
    entry_flags, exit_flags = [False] * len(closes), [False] * len(closes)
    contexts: list[dict[str, Any]] = [{} for _ in closes]
    start = trend_window - 1
    for index in range(start, len(closes)):
        fast = mean(closes[index - pullback_window + 1:index + 1])
        trend = mean(closes[index - trend_window + 1:index + 1])
        fast_values[index], trend_values[index] = fast, trend
        entry_flags[index] = closes[index] < fast and closes[index] > trend
        exit_flags[index] = closes[index] >= fast or closes[index] <= trend
        contexts[index] = {"pullback_sma": _round(fast), "trend_sma": _round(trend)}
    signals, events = _state_machine_signals(dates, closes, start, entry_flags, exit_flags,
                                              "uptrend_pullback_entry", "pullback_recovery_exit", contexts)
    result = _long_cash_metrics(rows, signals, start, events)
    result.update({"current_pullback_sma": _round(fast_values[-1]), "current_trend_sma": _round(trend_values[-1]),
                   "distance_to_pullback_sma_pct": _round(_pct(closes[-1], fast_values[-1])), "signal_changes": len(events)})
    return result


def _selloff_recovery_series(
    rows: list[Any], down_sessions: int = 3, exit_window: int = 5,
) -> dict[str, Any]:
    start = max(down_sessions, exit_window - 1)
    required = start + 2
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows), "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    exit_averages: list[float | None] = [None] * len(closes)
    entry_flags, exit_flags = [False] * len(closes), [False] * len(closes)
    contexts: list[dict[str, Any]] = [{} for _ in closes]
    for index in range(start, len(closes)):
        exit_average = mean(closes[index - exit_window + 1:index + 1])
        selloff = all(closes[pos] < closes[pos - 1] for pos in range(index - down_sessions + 1, index + 1))
        exit_averages[index] = exit_average
        entry_flags[index], exit_flags[index] = selloff and closes[index] < exit_average, closes[index] >= exit_average
        contexts[index] = {"exit_sma": _round(exit_average), "down_sessions": down_sessions}
    signals, events = _state_machine_signals(dates, closes, start, entry_flags, exit_flags,
                                              "selloff_entry", "recovery_exit", contexts)
    result = _long_cash_metrics(rows, signals, start, events)
    result.update({"current_exit_sma": _round(exit_averages[-1]),
                   "distance_to_exit_sma_pct": _round(_pct(closes[-1], exit_averages[-1])), "signal_changes": len(events)})
    return result


# Compatibility adapters preserve every existing engine callback signature while
# the frozen signal rules live in focused strategy-family components. Execution,
# costs and portfolio metrics remain owned by the engine and are injected here.
def _moving_average_crossover_series(
    rows: list[Any], fast_window: int = 20, slow_window: int = 50,
) -> dict[str, Any]:
    return _moving_average_crossover_component(
        rows, fast_window, slow_window, metrics=_long_cash_metrics,
    )


def _donchian_breakout_series(
    rows: list[Any], entry_window: int = 55, exit_window: int = 20,
) -> dict[str, Any]:
    return _donchian_breakout_component(
        rows, entry_window, exit_window, metrics=_long_cash_metrics,
    )


def _ema_crossover_series(
    rows: list[Any], fast_window: int = 20, slow_window: int = 50,
) -> dict[str, Any]:
    return _ema_crossover_component(
        rows, fast_window, slow_window, metrics=_long_cash_metrics,
    )


def _time_series_momentum_series(
    rows: list[Any], lookback: int = 63,
) -> dict[str, Any]:
    return _time_series_momentum_component(rows, lookback, metrics=_long_cash_metrics)


def _triple_sma_alignment_series(
    rows: list[Any], fast_window: int = 20, medium_window: int = 50,
    slow_window: int = 200,
) -> dict[str, Any]:
    return _triple_sma_alignment_component(
        rows, fast_window, medium_window, slow_window, metrics=_long_cash_metrics,
    )


def _rsi_mean_reversion_series(
    rows: list[Any], period: int = 2, entry_rsi: float = 10.0,
    exit_rsi: float = 70.0,
) -> dict[str, Any]:
    return _rsi_mean_reversion_component(
        rows, period, entry_rsi, exit_rsi, metrics=_long_cash_metrics,
    )


def _bollinger_mean_reversion_series(
    rows: list[Any], window: int = 20, deviations: float = 2.0,
) -> dict[str, Any]:
    return _bollinger_mean_reversion_component(
        rows, window, deviations, metrics=_long_cash_metrics,
    )


def _zscore_mean_reversion_series(
    rows: list[Any], window: int = 10, entry_z: float = -1.5,
    exit_z: float = 0.0,
) -> dict[str, Any]:
    return _zscore_mean_reversion_component(
        rows, window, entry_z, exit_z, metrics=_long_cash_metrics,
    )


def _pullback_uptrend_series(
    rows: list[Any], pullback_window: int = 5, trend_window: int = 200,
) -> dict[str, Any]:
    return _pullback_uptrend_component(
        rows, pullback_window, trend_window, metrics=_long_cash_metrics,
    )


def _selloff_recovery_series(
    rows: list[Any], down_sessions: int = 3, exit_window: int = 5,
) -> dict[str, Any]:
    return _selloff_recovery_component(
        rows, down_sessions, exit_window, metrics=_long_cash_metrics,
    )


def _chronological_holdout_validation(
    rows: list[Any],
    series_function: Any,
    series_arguments: tuple[Any, ...],
    required_observations: int,
) -> dict[str, Any]:
    """Evaluate frozen parameters on chronological 60/20/20 holdout periods.

    Each period receives only the preceding strategy warm-up and resets its
    position at the boundary. This is intentionally a first holdout contract;
    rolling walk-forward folds are added only after the static split is audited.
    """
    warmup = max(1, required_observations - 2)
    evaluable = len(rows) - warmup
    if evaluable < 60:
        return {
            "status": "insufficient_history",
            "required_evaluable_sessions": 60,
            "available_evaluable_sessions": max(0, evaluable),
        }

    train_sessions = max(30, int(evaluable * 0.60))
    validation_sessions = max(10, int(evaluable * 0.20))
    if train_sessions + validation_sessions >= evaluable:
        validation_sessions = max(10, evaluable - train_sessions - 10)
    test_sessions = evaluable - train_sessions - validation_sessions
    boundaries = (
        ("training", 0, train_sessions),
        ("validation", train_sessions, train_sessions + validation_sessions),
        ("untouched_test", train_sessions + validation_sessions, evaluable),
    )
    metric_names = (
        "evaluated_sessions", "completed_trades", "net_strategy_return_pct",
        "net_annualised_return_pct", "net_profit_factor",
        "net_trade_expectancy_pct", "net_trade_edge_p_value",
        "net_maximum_drawdown_pct", "excess_return_pct", "exposure_pct",
        "estimated_cost_drag_pct", "executed_sells",
    )
    periods = []
    for label, evaluation_start, evaluation_end in boundaries:
        absolute_start = warmup + evaluation_start
        absolute_end = warmup + evaluation_end
        period_rows = rows[absolute_start - warmup:absolute_end]
        result = series_function(period_rows, *series_arguments)
        periods.append({
            "period": label,
            "start_date": str(rows[absolute_start]["session_date"]),
            "end_date": str(rows[absolute_end - 1]["session_date"]),
            "parameters_frozen": True,
            "position_reset_at_boundary": True,
            "status": result.get("status"),
            **{name: result.get(name) for name in metric_names},
        })
    return {
        "status": "ready",
        "contract_version": "chronological-holdout-v1",
        "split": {"training_pct": 60, "validation_pct": 20, "untouched_test_pct": 20},
        "warmup_sessions": warmup,
        "parameter_policy": "strategy parameters frozen before validation and untouched test periods",
        "primary_published_period": "untouched_test",
        "periods": periods,
        "limitations": [
            "Period positions reset at each boundary so evidence cannot leak across the split.",
            "This first contract is a single chronological holdout, not yet a rolling walk-forward study.",
            "No parameter search is performed inside this contract; the published baseline parameters remain frozen.",
        ],
    }


def _rolling_walk_forward_validation(
    rows: list[Any],
    series_function: Any,
    series_arguments: tuple[Any, ...],
    required_observations: int,
) -> dict[str, Any]:
    """Run expanding 504/126/126-session walk-forward folds.

    Each test block becomes historical information only after its fold has been
    evaluated. Parameters remain the published baseline parameters throughout;
    this contract does not optimise against validation or test evidence.
    """
    warmup = max(1, required_observations - 2)
    evaluable = len(rows) - warmup
    minimum_training = 504
    validation_sessions = 126
    test_sessions = 126
    step_sessions = 126
    required_evaluable = minimum_training + validation_sessions + test_sessions
    if evaluable < required_evaluable:
        return {
            "status": "insufficient_history",
            "contract_version": "rolling-walk-forward-v1",
            "required_evaluable_sessions": required_evaluable,
            "available_evaluable_sessions": max(0, evaluable),
        }

    metric_names = (
        "evaluated_sessions", "completed_trades", "net_strategy_return_pct",
        "net_annualised_return_pct", "net_profit_factor",
        "net_trade_expectancy_pct", "net_trade_edge_p_value",
        "net_maximum_drawdown_pct", "excess_return_pct", "exposure_pct",
        "estimated_cost_drag_pct", "executed_sells",
    )

    def evaluate_period(start: int, end: int) -> dict[str, Any]:
        absolute_start = warmup + start
        absolute_end = warmup + end
        period_rows = rows[absolute_start - warmup:absolute_end]
        result = series_function(period_rows, *series_arguments)
        return {
            "start_date": str(rows[absolute_start]["session_date"]),
            "end_date": str(rows[absolute_end - 1]["session_date"]),
            "status": result.get("status"),
            "parameters_frozen": True,
            "position_reset_at_boundary": True,
            **{name: result.get(name) for name in metric_names},
            "_net_completed_trade_returns": result.get("_net_completed_trade_returns") or [],
        }

    folds = []
    training_end = minimum_training
    fold_number = 1
    while training_end + validation_sessions + test_sessions <= evaluable:
        validation_end = training_end + validation_sessions
        test_end = validation_end + test_sessions
        folds.append({
            "fold": fold_number,
            "training": evaluate_period(0, training_end),
            "validation": evaluate_period(training_end, validation_end),
            "test": evaluate_period(validation_end, test_end),
        })
        fold_number += 1
        training_end += step_sessions

    test_results = [fold["test"] for fold in folds if fold["test"]["status"] == "ready"]

    def metric_values(name: str) -> list[float]:
        return [float(period[name]) for period in test_results if period.get(name) is not None]

    returns = metric_values("net_strategy_return_pct")
    profit_factors = metric_values("net_profit_factor")
    expectancies = metric_values("net_trade_expectancy_pct")
    drawdowns = metric_values("net_maximum_drawdown_pct")
    return {
        "status": "ready" if folds else "insufficient_history",
        "contract_version": "rolling-walk-forward-v1",
        "fold_design": {
            "training": "expanding minimum 504 sessions",
            "validation_sessions": validation_sessions,
            "untouched_test_sessions": test_sessions,
            "step_sessions": step_sessions,
            "parameters": "frozen published baseline",
        },
        "warmup_sessions": warmup,
        "fold_count": len(folds),
        "primary_published_period": "test",
        "out_of_sample_summary": {
            "ready_test_folds": len(test_results),
            "positive_test_folds": sum(value > 0 for value in returns),
            "positive_test_fold_ratio": _round(sum(value > 0 for value in returns) / len(returns), 4) if returns else None,
            "median_test_net_return_pct": _round(median(returns)) if returns else None,
            "median_test_net_profit_factor": _round(median(profit_factors)) if profit_factors else None,
            "median_test_net_trade_expectancy_pct": _round(median(expectancies)) if expectancies else None,
            "worst_test_net_drawdown_pct": _round(min(drawdowns)) if drawdowns else None,
        },
        "folds": folds,
        "limitations": [
            "The fold schedule is fixed before results are read and does not optimise strategy parameters.",
            "Later training windows may include earlier test periods only after those tests have completed, matching expanding walk-forward chronology.",
            "Point-in-time universe admission is reported per test fold and remains unavailable before the reviewed membership window.",
        ],
    }


def _attach_walk_forward_universe_gates(
    validation: dict[str, Any],
    market_db: Path,
    snapshot_db: Path = DEFAULT_SNAPSHOT_DB,
    symbol: str | None = None,
    universe_cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if validation.get("status") != "ready":
        return validation
    reviewed_from = date.fromisoformat(NIFTY500_NOTICE_REVIEW_WINDOWS[0]["reviewed_from"])
    for fold in validation.get("folds", []):
        test_end = date.fromisoformat(fold["test"]["end_date"])
        if test_end < reviewed_from:
            fold["point_in_time_universe"] = {
                "status": "outside_reviewed_window",
                "ranking_admissible": False,
                "reviewed_from": reviewed_from.isoformat(),
                "as_of_date": test_end.isoformat(),
            }
            continue
        try:
            cache_key = test_end.isoformat()
            universe = (universe_cache or {}).get(cache_key)
            if universe is None:
                universe = point_in_time_universe_contract(
                    market_db=market_db, snapshot_db=snapshot_db, as_of_date=cache_key
                )
                if universe_cache is not None:
                    universe_cache[cache_key] = universe
            member_symbols = {str(member.get("symbol") or "").upper() for member in universe.get("members", [])}
            symbol_member = not symbol or symbol.upper() in member_symbols
            fold["point_in_time_universe"] = {
                "status": universe.get("membership_status"),
                "ranking_admissible": bool(universe.get("ranking_admissible") and symbol_member),
                "as_of_date": test_end.isoformat(),
                "member_count": universe.get("member_count"),
                "classification_status": universe.get("classification_status"),
                "symbol": symbol,
                "symbol_member": symbol_member,
            }
        except ValueError as error:
            fold["point_in_time_universe"] = {
                "status": str(error),
                "ranking_admissible": False,
                "as_of_date": test_end.isoformat(),
            }
    admitted = [
        fold for fold in validation.get("folds", [])
        if fold.get("point_in_time_universe", {}).get("ranking_admissible")
    ]
    validation["point_in_time_coverage"] = {
        "reviewed_from": reviewed_from.isoformat(),
        "admitted_test_folds": len(admitted),
        "total_test_folds": len(validation.get("folds", [])),
        "ranking_ready": bool(admitted),
    }
    admitted_trade_returns = [
        float(value)
        for fold in admitted
        for value in fold.get("test", {}).get("_net_completed_trade_returns", [])
    ]
    validation["out_of_sample_statistical_evidence"] = {
        "contract_version": "walk-forward-oos-trade-evidence-v1",
        "scope": "point_in_time_admitted_non_overlapping_test_folds",
        "admitted_test_folds": len(admitted),
        "completed_net_trades": len(admitted_trade_returns),
        "net_trade_edge_p_value": _trade_edge_p_value(admitted_trade_returns),
        "bootstrap": _bootstrap_trade_evidence(admitted_trade_returns),
    }
    for fold in validation.get("folds", []):
        for period_name in ("training", "validation", "test"):
            fold.get(period_name, {}).pop("_net_completed_trade_returns", None)
    validation["ranking_admission"] = _walk_forward_stability_gate(validation)
    return validation


def _walk_forward_stability_gate(validation: dict[str, Any]) -> dict[str, Any]:
    """Admit only stable, dated out-of-sample evidence to future rankings."""
    thresholds = {
        "minimum_admitted_test_folds": 3,
        "minimum_completed_trades": 15,
        "minimum_positive_fold_ratio": 0.60,
        "minimum_median_net_profit_factor": 1.10,
        "minimum_median_net_trade_expectancy_pct": 0.0,
        "maximum_worst_net_drawdown_pct": -25.0,
    }
    admitted_periods = [
        fold["test"] for fold in validation.get("folds", [])
        if fold.get("point_in_time_universe", {}).get("ranking_admissible")
        and fold.get("test", {}).get("status") == "ready"
    ]

    def values(name: str) -> list[float]:
        return [float(period[name]) for period in admitted_periods if period.get(name) is not None]

    returns = values("net_strategy_return_pct")
    profit_factors = values("net_profit_factor")
    expectancies = values("net_trade_expectancy_pct")
    drawdowns = values("net_maximum_drawdown_pct")
    trades = sum(int(period.get("completed_trades") or 0) for period in admitted_periods)
    positive_ratio = sum(value > 0 for value in returns) / len(returns) if returns else None
    median_profit_factor = median(profit_factors) if profit_factors else None
    median_expectancy = median(expectancies) if expectancies else None
    worst_drawdown = min(drawdowns) if drawdowns else None
    metrics = {
        "admitted_test_folds": len(admitted_periods),
        "completed_trades": trades,
        "positive_fold_ratio": _round(positive_ratio, 4),
        "median_net_profit_factor": _round(median_profit_factor),
        "median_net_trade_expectancy_pct": _round(median_expectancy),
        "worst_net_drawdown_pct": _round(worst_drawdown),
    }
    checks = (
        ("admitted_test_folds", len(admitted_periods) >= thresholds["minimum_admitted_test_folds"]),
        ("completed_trades", trades >= thresholds["minimum_completed_trades"]),
        ("positive_fold_ratio", positive_ratio is not None and positive_ratio >= thresholds["minimum_positive_fold_ratio"]),
        ("median_net_profit_factor", median_profit_factor is not None and median_profit_factor >= thresholds["minimum_median_net_profit_factor"]),
        ("median_net_trade_expectancy_pct", median_expectancy is not None and median_expectancy > thresholds["minimum_median_net_trade_expectancy_pct"]),
        ("worst_net_drawdown_pct", worst_drawdown is not None and worst_drawdown >= thresholds["maximum_worst_net_drawdown_pct"]),
    )
    failed = [name for name, passed in checks if not passed]
    return {
        "contract_version": "walk-forward-stability-gate-v1",
        "status": "admitted" if not failed else "gated",
        "ranking_admissible": not failed,
        "thresholds": thresholds,
        "metrics": metrics,
        "checks": [{"name": name, "passed": passed} for name, passed in checks],
        "failed_checks": failed,
        "decision_boundary": (
            "Eligible for future out-of-sample ranking; this contract does not issue a trade instruction."
            if not failed else
            "Excluded from out-of-sample ranking until every stability and point-in-time evidence check passes."
        ),
    }


def _run_signal_backtest(
    market_db: Path, symbol: str | None, series_function: Any, series_arguments: tuple[Any, ...],
    engine: str, contract_version: str, strategy: dict[str, Any], required_observations: int,
    history_mode: str = "comparison",
    include_oos_ranking: bool = True,
) -> dict[str, Any]:
    requested_symbol = (symbol or "").strip().upper()
    connection = _connect(market_db)
    try:
        if requested_symbol:
            instruments = connection.execute(
                "SELECT instrument_id,display_name FROM instruments WHERE kind='stock' AND UPPER(SUBSTR(instrument_id, 7))=?",
                (requested_symbol,),
            ).fetchall()
        else:
            instruments = connection.execute("SELECT instrument_id,display_name FROM instruments WHERE kind='stock' ORDER BY instrument_id").fetchall()
        reference_context = _backtest_reference_context(connection)
        results = []
        for instrument in instruments:
            prepared_rows, audit = _quality_checked_backtest_series(
                connection, instrument["instrument_id"], required_observations, reference_context, history_mode
            )
            if audit["status"] == "ready":
                result = series_function(prepared_rows, *series_arguments)
                if requested_symbol:
                    result["statistical_evidence"] = _bootstrap_trade_evidence(
                        result.get("_net_completed_trade_returns") or []
                    )
                    result["chronological_validation"] = _chronological_holdout_validation(
                        prepared_rows, series_function, series_arguments, required_observations
                    )
                    result["rolling_walk_forward"] = _attach_walk_forward_universe_gates(
                        _rolling_walk_forward_validation(
                            prepared_rows, series_function, series_arguments, required_observations
                        ),
                        market_db,
                        symbol=requested_symbol,
                    )
            else:
                result = {
                    "status": "data_quality_gated",
                    "observations": audit["stored_observations"],
                    "reason": ", ".join(audit["blocking_gates"]),
                }
            result["data_quality"] = audit
            result["symbol"] = instrument["instrument_id"].split(":", 1)[-1]
            result["name"] = instrument["display_name"]
            result.pop("_completed_trade_returns", None)
            result.pop("_net_completed_trade_returns", None)
            if not requested_symbol:
                result.pop("events", None)
            results.append(result)
    finally:
        connection.close()
    if requested_symbol and not results:
        return {"ok": False, "reason": "stock_not_found", "symbol": requested_symbol}
    ready = [row for row in results if row["status"] == "ready"]
    quality_gated = [row for row in results if row["status"] == "data_quality_gated"]
    multiple_testing = (
        _attach_benjamini_hochberg_fdr(results) if not requested_symbol else
        {"contract_version": "benjamini-hochberg-fdr-v1", "status": "not_applicable_single_stock"}
    )
    evidence_ranking = _attach_comparison_evidence_rankings(results)
    if not requested_symbol and include_oos_ranking:
        oos_evidence = _universe_oos_evidence_map(
            market_db, engine, series_function, series_arguments, required_observations
        )
        evidence_ranking = _attach_oos_evidence_rankings(results, oos_evidence)
    invested = sum(row.get("current_executed_position") == "invested" for row in ready)
    strategy_contract = dict(strategy)
    strategy_contract.update({
        "signal_source": "completed_session_close",
        "execution_price_field": "next_session_open",
        "return_attribution": "completed_close_signal_executed_next_session_open",
        "cost_model": EQUITY_DELIVERY_COST_SCHEDULE["schedule_version"],
        "history_mode": history_mode,
    })
    return {
        "ok": True, "contract_version": contract_version, "engine": engine, "strategy": strategy_contract,
        "scope": "single_stock" if requested_symbol else "stored_stock_universe",
        "multiple_testing": multiple_testing,
        "evidence_ranking": evidence_ranking,
        "as_of_date": max((row.get("as_of_date") or "" for row in ready), default=None) or None,
        "data_quality_contract": {
            "common_as_of_date": reference_context.get("common_as_of_date"),
            "comparison_sessions": BACKTEST_COMPARISON_SESSIONS,
            "history_mode": history_mode,
            "minimum_universe_coverage_ratio": BACKTEST_COMMON_COVERAGE_RATIO,
            "covered_on_common_as_of": reference_context.get("covered_on_common_as_of"),
            "reference_instrument": reference_context.get("reference_instrument"),
            "abnormal_jump_gate_pct": BACKTEST_ABNORMAL_JUMP_PCT,
            "adjustment_provenance_reported": True,
            "adjustment_gate_enforced": True,
            "point_in_time_universe_contract": "nifty500-point-in-time-universe-v2",
            "point_in_time_universe_gate_enforced": False,
            "walk_forward_fold_universe_gate_enforced": True,
            "point_in_time_universe_status": "complete_for_reviewed_window",
        },
        "coverage": {"requested": len(instruments), "ready": len(ready),
                     "data_quality_gated": len(quality_gated),
                     "insufficient_history": sum(row["status"] == "insufficient_history" for row in results),
                     "currently_invested": invested, "currently_cash": len(ready) - invested,
                     "pending_next_session_actions": sum(bool(row.get("pending_next_session_action")) for row in ready),
                     "rejected_execution_events": sum(int(row.get("rejected_execution_events") or 0) for row in ready),
                     "outperformed_buy_and_hold": sum((row.get("excess_return_pct") or 0) > 0 for row in ready)},
        "limitations": [
            "Gross and estimated net results are both reported; net results apply the published Zerodha/NSE delivery schedule, ₹50,000 position capital, 5 bps slippage each side and zero market impact.",
            "Signals use completed-session closes and execute only at the next valid session open; missing opens reject the transition.",
            "The point-in-time membership and classification ledger is complete for its reviewed window, but this current-universe ranking does not yet vary membership on every historical session; the survivorship-bias warning remains until walk-forward evaluation consumes dated universes.",
            ("The selected-stock study uses every continuous stored session after strategy warm-up; its corporate-action ledger may be partial before the audited coverage window."
             if history_mode == "full" else
             "Ranked results use the same 252 completed Nifty 50 sessions after strategy-specific warm-up; stale, incomplete, invalid-OHLC and unaudited extreme-jump series are excluded from ranking."),
            "Kite-reported corporate-action adjustment provenance is enforced. NSE events requiring manual lineage, delisting or unclassified treatment exclude the stock until reviewed; ordinary cash dividends remain outside price-return results.",
            "Trade-edge p-values remain exploratory two-sided t-tests. Comparable-universe ranking results publish Benjamini-Hochberg q-values within each strategy family; selected-stock studies publish deterministic ordinary and circular moving-block bootstrap intervals only after 15 completed net trades.",
            "Results are research evidence, not a trade recommendation or order instruction.",
        ],
        "results": results,
    }


def ema_crossover(market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None, fast_window: int = 20, slow_window: int = 50, history_mode: str = "comparison", include_oos_ranking: bool = True) -> dict[str, Any]:
    if fast_window < 2 or slow_window <= fast_window or slow_window > 250:
        raise ValueError("invalid_ema_windows")
    return _run_signal_backtest(market_db, symbol, _ema_crossover_series, (fast_window, slow_window), "ema_crossover",
                                "ema-crossover-backtest-v8", {"fast_window": fast_window, "slow_window": slow_window,
                                "average_type": "exponential", "positioning": "long_or_cash",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, slow_window + 1, history_mode, include_oos_ranking)


def time_series_momentum(market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None, lookback: int = 63, history_mode: str = "comparison", include_oos_ranking: bool = True) -> dict[str, Any]:
    if lookback < 5 or lookback > 250:
        raise ValueError("invalid_momentum_lookback")
    return _run_signal_backtest(market_db, symbol, _time_series_momentum_series, (lookback,), "time_series_momentum",
                                "time-series-momentum-backtest-v8", {"lookback": lookback, "positioning": "long_or_cash",
                                "entry_rule": "lookback_return_positive", "exit_rule": "lookback_return_non_positive",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, lookback + 2, history_mode, include_oos_ranking)


def triple_sma_alignment(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    fast_window: int = 20, medium_window: int = 50, slow_window: int = 200,
    history_mode: str = "comparison",
    include_oos_ranking: bool = True,
) -> dict[str, Any]:
    if fast_window < 2 or medium_window <= fast_window or slow_window <= medium_window or slow_window > 300:
        raise ValueError("invalid_triple_sma_windows")
    return _run_signal_backtest(market_db, symbol, _triple_sma_alignment_series,
                                (fast_window, medium_window, slow_window), "triple_sma_alignment",
                                "triple-sma-alignment-backtest-v8", {"fast_window": fast_window, "medium_window": medium_window,
                                "slow_window": slow_window, "average_type": "simple", "positioning": "long_or_cash",
                                "entry_rule": "fast_above_medium_above_slow", "exit_rule": "alignment_breaks",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, slow_window + 1, history_mode, include_oos_ranking)


def rsi_mean_reversion(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    period: int = 2, entry_rsi: float = 10.0, exit_rsi: float = 70.0,
    history_mode: str = "comparison",
    include_oos_ranking: bool = True,
) -> dict[str, Any]:
    if period < 2 or period > 50 or entry_rsi <= 0 or exit_rsi >= 100 or entry_rsi >= exit_rsi:
        raise ValueError("invalid_rsi_mean_reversion_parameters")
    return _run_signal_backtest(market_db, symbol, _rsi_mean_reversion_series,
                                (period, entry_rsi, exit_rsi), "rsi_mean_reversion",
                                "rsi-mean-reversion-backtest-v8", {"period": period, "entry_rsi": entry_rsi,
                                "exit_rsi": exit_rsi, "positioning": "long_or_cash",
                                "entry_rule": "rsi_at_or_below_entry", "exit_rule": "rsi_at_or_above_exit",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, period + 2, history_mode, include_oos_ranking)


def bollinger_mean_reversion(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    window: int = 20, deviations: float = 2.0, history_mode: str = "comparison",
    include_oos_ranking: bool = True,
) -> dict[str, Any]:
    if window < 5 or window > 250 or deviations <= 0 or deviations > 5:
        raise ValueError("invalid_bollinger_parameters")
    return _run_signal_backtest(market_db, symbol, _bollinger_mean_reversion_series,
                                (window, deviations), "bollinger_mean_reversion",
                                "bollinger-mean-reversion-backtest-v8", {"window": window, "deviations": deviations,
                                "positioning": "long_or_cash", "entry_rule": "close_below_lower_band",
                                "exit_rule": "close_at_or_above_middle_band",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, window + 1, history_mode, include_oos_ranking)


def zscore_mean_reversion(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    window: int = 10, entry_z: float = -1.5, exit_z: float = 0.0,
    history_mode: str = "comparison",
    include_oos_ranking: bool = True,
) -> dict[str, Any]:
    if window < 5 or window > 250 or entry_z >= exit_z or entry_z >= 0:
        raise ValueError("invalid_zscore_parameters")
    return _run_signal_backtest(market_db, symbol, _zscore_mean_reversion_series,
                                (window, entry_z, exit_z), "zscore_mean_reversion",
                                "zscore-mean-reversion-backtest-v8", {"window": window, "entry_z": entry_z,
                                "exit_z": exit_z, "positioning": "long_or_cash",
                                "entry_rule": "zscore_at_or_below_entry", "exit_rule": "zscore_at_or_above_exit",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, window + 1, history_mode, include_oos_ranking)


def pullback_uptrend(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    pullback_window: int = 5, trend_window: int = 200, history_mode: str = "comparison",
    include_oos_ranking: bool = True,
) -> dict[str, Any]:
    if pullback_window < 2 or trend_window <= pullback_window or trend_window > 300:
        raise ValueError("invalid_pullback_uptrend_windows")
    return _run_signal_backtest(market_db, symbol, _pullback_uptrend_series,
                                (pullback_window, trend_window), "pullback_uptrend",
                                "pullback-uptrend-backtest-v8", {"pullback_window": pullback_window,
                                "trend_window": trend_window, "positioning": "long_or_cash",
                                "entry_rule": "close_below_pullback_sma_and_above_trend_sma",
                                "exit_rule": "close_recovers_pullback_sma_or_breaks_trend_sma",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, trend_window + 1, history_mode, include_oos_ranking)


def selloff_recovery(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    down_sessions: int = 3, exit_window: int = 5, history_mode: str = "comparison",
    include_oos_ranking: bool = True,
) -> dict[str, Any]:
    if down_sessions < 2 or down_sessions > 10 or exit_window < 3 or exit_window > 50:
        raise ValueError("invalid_selloff_recovery_parameters")
    return _run_signal_backtest(market_db, symbol, _selloff_recovery_series,
                                (down_sessions, exit_window), "selloff_recovery",
                                "selloff-recovery-backtest-v8", {"down_sessions": down_sessions,
                                "exit_window": exit_window, "positioning": "long_or_cash",
                                "entry_rule": "consecutive_down_closes_below_exit_sma",
                                "exit_rule": "close_at_or_above_exit_sma",
                                "return_attribution": "completed_close_signal_executed_next_session_open"},
                                max(down_sessions, exit_window - 1) + 2, history_mode, include_oos_ranking)


_CROSS_STRATEGY_FDR_LOCK = threading.Lock()
_ACTIVE_BACKGROUND_JOBS: set[str] = set()


def _oos_strategy_runners() -> tuple[tuple[str, Any], ...]:
    return (
        ("moving_average_crossover", moving_average_crossover),
        ("donchian_breakout", donchian_breakout),
        ("ema_crossover", ema_crossover),
        ("time_series_momentum", time_series_momentum),
        ("triple_sma_alignment", triple_sma_alignment),
        ("rsi_mean_reversion", rsi_mean_reversion),
        ("bollinger_mean_reversion", bollinger_mean_reversion),
        ("zscore_mean_reversion", zscore_mean_reversion),
        ("pullback_uptrend", pullback_uptrend),
        ("selloff_recovery", selloff_recovery),
    )


def _precompute_missing_oos_strategies(
    market_db: Path, database_version: str, job_id: str,
) -> None:
    errors: dict[str, str] = {}
    try:
        for engine, runner in _oos_strategy_runners():
            with _UNIVERSE_OOS_LOCK:
                cached = _UNIVERSE_OOS_LATEST.get(engine)
                completed = sorted(
                    name for name, item in _UNIVERSE_OOS_LATEST.items()
                    if item.get("database_version") == database_version
                )
            if cached and cached.get("database_version") == database_version:
                continue
            update_job(
                market_db, job_id, len(completed), current_stage=engine,
                errors=errors, detail={"completed_engines": completed},
            )
            try:
                runner(market_db)
            except Exception as error:  # one family must not hide later progress
                errors[engine] = str(error)
            with _UNIVERSE_OOS_LOCK:
                completed = sorted(
                    name for name, item in _UNIVERSE_OOS_LATEST.items()
                    if item.get("database_version") == database_version
                )
            update_job(
                market_db, job_id, len(completed), current_stage=engine,
                errors=errors, detail={"completed_engines": completed},
            )
        finish_job(
            market_db, job_id,
            "complete" if not errors else "complete_with_errors", errors,
        )
    except Exception as error:
        errors["background_job"] = str(error)
        finish_job(market_db, job_id, "failed", errors)
    finally:
        with _CROSS_STRATEGY_FDR_LOCK:
            _ACTIVE_BACKGROUND_JOBS.discard(job_id)


def background_research_jobs(
    market_db: Path = DEFAULT_MARKET_DB, limit: int = 20,
) -> dict[str, Any]:
    return list_jobs(market_db, limit)


def _cross_strategy_fdr_from_payloads(payloads: list[dict[str, Any]], alpha: float = 0.05) -> dict[str, Any]:
    return cross_strategy_fdr_from_payloads(payloads, alpha)


def cross_strategy_multiple_testing(
    market_db: Path = DEFAULT_MARKET_DB,
    start_background: bool = False,
) -> dict[str, Any]:
    """Return the incremental full-history OOS family.

    Missing strategy families are deliberately not started by ordinary page
    reads. Computing all ten full-history universes in a Python thread can
    starve the request-serving process and make cheap dashboard contracts look
    hung. Individual strategy endpoints populate this cache on demand; an
    explicit maintenance request may still opt into the complete build.
    """
    database_version = _oos_source_version(market_db)
    _hydrate_persistent_oos_cache(market_db, database_version)
    required_engines = [engine for engine, _runner in _oos_strategy_runners()]
    job_id = f"oos-family:{database_version}"
    with _UNIVERSE_OOS_LOCK:
        available = {
            engine: item for engine, item in _UNIVERSE_OOS_LATEST.items()
            if item.get("database_version") == database_version
        }
    with _CROSS_STRATEGY_FDR_LOCK:
        running_for_version = job_id in _ACTIVE_BACKGROUND_JOBS
        if start_background and len(available) < len(required_engines) and not running_for_version:
            start_job(
                market_db, job_id, "full_history_oos_strategy_family",
                database_version, len(required_engines), len(available),
                {"completed_engines": sorted(available)},
            )
            _ACTIVE_BACKGROUND_JOBS.add(job_id)
            threading.Thread(
                target=_precompute_missing_oos_strategies,
                args=(market_db, database_version, job_id),
                name="workspace-oos-precompute", daemon=True,
            ).start()
        active_for_version = job_id in _ACTIVE_BACKGROUND_JOBS
    progress = job_status(market_db, job_id) or {
        "contract_version": "background-research-job-v1",
        "job_id": job_id,
        "job_type": "full_history_oos_strategy_family",
        "source_version": database_version,
        "status": "idle",
        "completed_units": len(available),
        "total_units": len(required_engines),
        "current_stage": None,
        "progress_pct": round(100 * len(available) / len(required_engines), 1),
        "errors": {},
        "detail": {"completed_engines": sorted(available)},
        "terminal": False,
    }
    if progress.get("status") == "running" and not active_for_version:
        progress = finish_job(
            market_db, job_id, "interrupted",
            {"process": "server_restarted_before_job_completed"},
        )
    progress["current_engine"] = progress.get("current_stage")
    progress["completed_engines"] = progress.get("detail", {}).get("completed_engines", [])

    payloads = []
    for engine, item in available.items():
        payloads.append({
            "engine": engine,
            "results": [
                {
                    "status": "ready" if evidence.get("status") == "ready" else "gated",
                    "symbol": symbol,
                    "evidence_ranking": {
                        "ranking_admissible": bool(evidence.get("ranking_admissible")),
                        "diagnostic_score": None,
                        "failed_checks": list(evidence.get("failed_checks") or []),
                        "oos_net_trade_edge_p_value": evidence.get("net_trade_edge_p_value"),
                    },
                }
                for symbol, evidence in item.get("evidence", {}).items()
            ],
        })
    result = _cross_strategy_fdr_from_payloads(payloads)
    coverage_complete = len(available) == len(required_engines)
    if not coverage_complete:
        for rows in result.get("by_engine", {}).values():
            for item in rows.values():
                item["cross_strategy_q_value"] = None
                item["status"] = "pending_complete_strategy_family"
                item["evidence_ranking"]["ranking_admissible"] = False
                item["evidence_ranking"]["score"] = None
                item["evidence_ranking"]["global_fdr_passed"] = False
                item["evidence_ranking"]["failed_checks"] = list(dict.fromkeys(
                    [*(item["evidence_ranking"].get("failed_checks") or []), "all_strategy_fdr"]
                ))
        result["fdr_q_below_alpha"] = 0
        result["ranking_admitted"] = 0
    connection = _connect(market_db)
    try:
        as_of_date = _backtest_reference_context(connection).get("common_as_of_date")
    finally:
        connection.close()
    background_running = (
        progress.get("status") == "running"
        and progress.get("source_version") == database_version
    )
    result.update({
        "status": (
            "ready" if coverage_complete else
            "building_oos_strategy_family" if background_running else
            "incremental_oos_strategy_family"
        ),
        "database_version": database_version,
        "strategy_count": len(available),
        "required_strategy_count": len(required_engines),
        "coverage_complete": coverage_complete,
        "missing_engines": [engine for engine in required_engines if engine not in available],
        "background_progress": progress,
        "background_requested": bool(start_background),
        "as_of_date": as_of_date,
    })
    return result


def _screener_local_uncached(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    rows = _technical_rows(market_db)
    return {"ok": True, "as_of_date": max((row["as_of_date"] for row in rows), default=None), "rows": rows, "coverage": {"ready": sum(row["status"] == "ready" for row in rows)}}


def _factor_research_uncached(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    connection = _connect(market_db)
    try:
        cached = _latest_snapshot(connection, "factor-research-foundation")
        nifty_series = _instrument_series(connection, "index:Nifty 50")
        nifty_closes = [float(row["close"]) for row in nifty_series]
        nifty_momentum = {
            "12_1": _pct(nifty_closes[-22], nifty_closes[-253]) if len(nifty_closes) >= 253 else None,
            "6_1": _pct(nifty_closes[-22], nifty_closes[-127]) if len(nifty_closes) >= 127 else None,
            "3_1": _pct(nifty_closes[-22], nifty_closes[-64]) if len(nifty_closes) >= 64 else None,
        }
        if cached:
            latest_dates: list[str] = []
            for row in cached.get("rows", []):
                series = _instrument_series(connection, f"stock:{row.get('symbol', '')}")
                if not series:
                    continue
                closes = [float(item["close"]) for item in series]
                latest_dates.append(str(series[-1]["session_date"]))
                row["last_session"] = series[-1]["session_date"]
                row["sessions"] = len(series)
                momentum = {
                    "12_1": _pct(closes[-22], closes[-253]) if len(closes) >= 253 else None,
                    "6_1": _pct(closes[-22], closes[-127]) if len(closes) >= 127 else None,
                    "3_1": _pct(closes[-22], closes[-64]) if len(closes) >= 64 else None,
                }
                for window, value in momentum.items():
                    row[f"momentum_{window}_pct"] = _round(value)
                    benchmark = nifty_momentum.get(window)
                    row[f"excess_{window}_vs_nifty_pct"] = _round(value - benchmark) if value is not None and benchmark is not None else None
                turnovers = [float(item["close"]) * float(item["volume"] or 0) for item in series[-20:] if item["volume"] is not None]
                row["median_daily_turnover_inr"] = _round(median(turnovers)) if turnovers else None
                row["positive_volume_coverage_pct"] = _round(100 * sum(float(item["volume"] or 0) > 0 for item in series) / len(series))
            if latest_dates:
                cached["as_of_date"] = max(latest_dates)
                cached["expected_through"] = max(latest_dates)
    finally:
        connection.close()
    if cached:
        cached["source"] = "Workspace 2.0 local EOD store"
        return cached
    technical = _technical_rows(market_db)
    rows = [{
        "symbol": row["symbol"], "sector": row["sector"],
        "momentum_12_1_pct": row["return_60d_pct"], "median_daily_turnover_inr": None,
        "research_eligible": row["status"] == "ready",
    } for row in technical]
    return {"ok": True, "as_of_date": max((row["as_of_date"] for row in technical), default=None), "rows": rows, "coverage": {"research_eligible": sum(row["research_eligible"] for row in rows)}}


def _stock_ytd_uncached(market_db: Path = DEFAULT_MARKET_DB, snapshot_db: Path = DEFAULT_SNAPSHOT_DB) -> dict[str, Any]:
    connection = _connect(market_db)
    try:
        cached = _latest_snapshot(connection, "nifty500-stock-ytd")
    finally:
        connection.close()
    if cached:
        technical = {row["symbol"]: row for row in _technical_rows(market_db)}

        def enrich(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            enriched = []
            for source_row in rows:
                row = dict(source_row)
                observed = technical.get(row.get("symbol"), {})
                if observed:
                    row["ltp"] = observed.get("close")
                    row["price_date"] = observed.get("as_of_date")
                    row["ytd_performance_pct"] = observed.get("ytd_return_pct")
                    row["status"] = observed.get("status", row.get("status"))
                    row["from_20dma_pct"] = observed.get("vs_20dma_pct")
                    row["from_50dma_pct"] = observed.get("vs_50dma_pct")
                    row["from_200dma_pct"] = observed.get("vs_200dma_pct")
                else:
                    row["from_50dma_pct"] = None
                enriched.append(row)
            return enriched

        years = [int(value) for value in cached.get("available_years", [])]
        selected = max(years) if years else None
        if selected is not None and isinstance(cached.get("by_year"), dict):
            payload = dict(cached["by_year"].get(str(selected), {}))
            payload["rows"] = enrich(payload.get("rows", []))
            payload["as_of_date"] = max((row.get("price_date") or "" for row in payload["rows"]), default=None) or None
            payload["available_years"] = years
            payload["selected_year"] = selected
            payload["source"] = "Workspace 2.0 local EOD store"
            return payload
        cached["rows"] = enrich(cached.get("rows", []))
        cached["source"] = "Workspace 2.0 local EOD store"
        return cached
    technical = _technical_rows(market_db)
    rows = [{"symbol": row["symbol"], "name": row["name"], "sector": row["sector"], "ltp": row["close"], "price_date": row["as_of_date"], "ytd_performance_pct": row["ytd_return_pct"], "from_20dma_pct": row["vs_20dma_pct"], "from_50dma_pct": row["vs_50dma_pct"], "from_200dma_pct": row["vs_200dma_pct"], "status": row["status"]} for row in technical]
    return {"ok": True, "as_of_date": max((row["as_of_date"] for row in technical), default=None), "rows": rows, "coverage": {"ready": sum(row["status"] == "ready" for row in rows), "partial_history": sum(row["status"] != "ready" for row in rows)}}


_STABLE_STOCK_RESEARCH_SOURCES = (
    ("candles", "session_date", "retrieved_at"),
    ("instruments", "updated_at", "updated_at"),
    ("workspace_snapshots", "as_of_date", "created_at"),
)


def _stable_stock_research_contract(
    market_db: Path, cache_key: str, contract_version: str, builder: Any,
) -> dict[str, Any]:
    source_version = semantic_source_version(
        ((market_db, _STABLE_STOCK_RESEARCH_SOURCES),)
    )
    cached = load_versioned_result(
        market_db, cache_key, contract_version, source_version,
    )
    if cached is not None:
        return cached
    payload = builder()
    if payload.get("ok"):
        store_versioned_result(
            market_db, cache_key, contract_version, source_version, payload,
        )
    return payload


def screener_local(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    return _stable_stock_research_contract(
        market_db, "screener_local", "local-technical-screen-v1",
        lambda: _screener_local_uncached(market_db),
    )


def factor_research(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    return _stable_stock_research_contract(
        market_db, "factor_research", "factor-research-v1",
        lambda: _factor_research_uncached(market_db),
    )


def stock_ytd(
    market_db: Path = DEFAULT_MARKET_DB,
    snapshot_db: Path = DEFAULT_SNAPSHOT_DB,
) -> dict[str, Any]:
    return _stable_stock_research_contract(
        market_db, "stock_ytd", "stock-ytd-v1",
        lambda: _stock_ytd_uncached(market_db, snapshot_db),
    )


def seasonality_summary(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    connection = _connect(market_db)
    try:
        instruments = connection.execute("SELECT instrument_id,display_name FROM instruments WHERE kind='index'").fetchall()
        observations = []
        target_month = date.today().month
        for item in instruments:
            rows = _instrument_series(connection, item["instrument_id"])
            by_month: dict[tuple[int, int], list[sqlite3.Row]] = defaultdict(list)
            for row in rows:
                moment = datetime.strptime(row["session_date"], "%Y-%m-%d").date()
                by_month[(moment.year, moment.month)].append(row)
            values = []
            for (year, month), month_rows in by_month.items():
                if month != target_month or (year == date.today().year and month == date.today().month):
                    continue
                values.append(_pct(float(month_rows[-1]["close"]), float(month_rows[0]["open"])) or 0)
            if values:
                observations.append({"instrument": item["display_name"], "average_return_pct": _round(mean(values)), "count": len(values)})
    finally:
        connection.close()
    observations.sort(key=lambda row: row["average_return_pct"])
    nifty = next((row for row in observations if row["instrument"] == "Nifty 50"), {})
    return {"ok": True, "current_month": {"number": target_month, "nifty50": nifty, "leading_index": observations[-1] if observations else {}, "lagging_index": observations[0] if observations else {}, "indices_evaluated": len(observations), "return_definition": "Completed historical calendar-month open-to-close returns; current incomplete month excluded."}}


def refresh_kite_indices(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    if not KITE.connected:
        raise ValueError("kite_not_connected")
    connection = _connect(market_db)
    written = 0
    try:
        rows = connection.execute("SELECT instrument_id,provider_token FROM instruments WHERE kind='index'").fetchall()
        for item in rows:
            latest = connection.execute("SELECT MAX(session_date) FROM candles WHERE instrument_id=?", (item["instrument_id"],)).fetchone()[0]
            start = (datetime.strptime(latest, "%Y-%m-%d").date() + timedelta(days=1)).isoformat() if latest else (date.today() - timedelta(days=450)).isoformat()
            candles = KITE.candles(item["provider_token"], start, date.today().isoformat())
            for candle in candles:
                session = str(candle[0])[:10]
                connection.execute(
                    """INSERT OR REPLACE INTO candles(instrument_id,session_date,open,high,low,close,volume,source,retrieved_at,validation_status,schema_version)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (item["instrument_id"], session, candle[1], candle[2], candle[3], candle[4], candle[5], "Kite Connect direct Workspace EOD", datetime.now(timezone.utc).isoformat(), "validated", 3),
                )
                written += 1
        connection.commit()
    finally:
        connection.close()
    return {"ok": True, "records_written": written, "source": "Kite Connect direct Workspace EOD"}


def refresh_kite_breadth(market_db: Path = DEFAULT_MARKET_DB, snapshot_db: Path = DEFAULT_SNAPSHOT_DB) -> dict[str, Any]:
    """Backfill current NIFTY 500 histories directly from Kite, then calculate breadth."""
    if not KITE.connected:
        raise ValueError("kite_not_connected")
    snap = _connect(snapshot_db)
    try:
        snapshot_date = snap.execute("SELECT MAX(snapshot_date) FROM index_membership_snapshots WHERE index_name='NIFTY 500'").fetchone()[0]
        members = [] if not snapshot_date else [row[0] for row in snap.execute("SELECT symbol FROM index_membership_snapshots WHERE index_name='NIFTY 500' AND snapshot_date=?", (snapshot_date,))]
    finally:
        snap.close()
    instrument_rows = KITE.instruments("NSE")
    tokens = {row["tradingsymbol"]: row for row in instrument_rows if row.get("segment") == "NSE" and row.get("instrument_type") == "EQ"}
    connection = _connect(market_db)
    written = attempted = 0
    try:
        for symbol in members:
            provider = tokens.get(symbol)
            if not provider:
                continue
            attempted += 1
            instrument_id = f"stock:{symbol}"
            connection.execute(
                """INSERT OR REPLACE INTO instruments(instrument_id,kind,display_name,provider_token,source,updated_at,schema_version,adjustment_status,adjustment_source_url)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (instrument_id, "stock", symbol, provider["instrument_token"], "Kite instrument master direct Workspace", datetime.now(timezone.utc).isoformat(), 3, "provider_unspecified", "https://kite.trade/docs/connect/v3/market-quotes/#instruments"),
            )
            latest = connection.execute("SELECT MAX(session_date) FROM candles WHERE instrument_id=?", (instrument_id,)).fetchone()[0]
            start = (datetime.strptime(latest, "%Y-%m-%d").date() + timedelta(days=1)).isoformat() if latest else (date.today() - timedelta(days=450)).isoformat()
            if start > date.today().isoformat():
                continue
            try:
                candles = KITE.candles(provider["instrument_token"], start, date.today().isoformat())
            except ValueError:
                continue
            for candle in candles:
                connection.execute(
                    """INSERT OR REPLACE INTO candles(instrument_id,session_date,open,high,low,close,volume,source,retrieved_at,validation_status,schema_version)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (instrument_id, str(candle[0])[:10], candle[1], candle[2], candle[3], candle[4], candle[5], "Kite Connect direct Workspace EOD", datetime.now(timezone.utc).isoformat(), "validated", 3),
                )
                written += 1
            connection.commit()
    finally:
        connection.close()
    return {"ok": True, "members": len(members), "attempted": attempted, "records_written": written, "source": "Kite Connect direct Workspace EOD"}


def refresh_removed_constituent_histories(
    market_db: Path = DEFAULT_MARKET_DB,
) -> dict[str, Any]:
    """Backfill the audited pre-exit history windows still available from Kite."""
    if not KITE.connected:
        raise ValueError("kite_not_connected")
    before = removed_constituent_history_audit(market_db)
    targets_by_symbol: dict[str, dict[str, Any]] = {}
    for row in before["exits"]:
        if row["status"] == "complete" or not row["window_start"] or not row["window_end"]:
            continue
        existing = targets_by_symbol.get(row["symbol"])
        if existing is None or str(row["window_start"]) < str(existing["window_start"]):
            targets_by_symbol[row["symbol"]] = row

    instrument_rows = KITE.instruments("NSE")
    tokens = {
        row["tradingsymbol"]: row for row in instrument_rows
        if row.get("segment") == "NSE" and row.get("instrument_type") == "EQ"
    }
    connection = _connect(market_db)
    attempted = written = 0
    unavailable = []
    failed = []
    now = datetime.now(timezone.utc).isoformat()
    try:
        for symbol, target in sorted(targets_by_symbol.items()):
            provider = tokens.get(symbol)
            if not provider:
                unavailable.append(symbol)
                continue
            attempted += 1
            instrument_id = f"stock:{symbol}"
            connection.execute(
                """INSERT INTO instruments(
                       instrument_id,kind,display_name,provider_token,source,updated_at,
                       schema_version,adjustment_status,adjustment_source_url)
                   VALUES (?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(instrument_id) DO UPDATE SET
                       display_name=excluded.display_name,
                       provider_token=excluded.provider_token,
                       source=excluded.source,
                       updated_at=excluded.updated_at""",
                (
                    instrument_id, "stock", symbol, provider["instrument_token"],
                    "Kite removed-constituent history direct Workspace", now, 3,
                    "provider_reported_adjusted", KITE_ADJUSTMENT_POLICY_URL,
                ),
            )
            try:
                candles = KITE.candles(
                    provider["instrument_token"],
                    str(target["window_start"]),
                    str(target["window_end"]),
                )
            except ValueError as error:
                failed.append({"symbol": symbol, "reason": str(error)})
                continue
            for candle in candles:
                connection.execute(
                    """INSERT OR REPLACE INTO candles(
                           instrument_id,session_date,open,high,low,close,volume,source,
                           retrieved_at,validation_status,schema_version)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        instrument_id, str(candle[0])[:10], candle[1], candle[2], candle[3],
                        candle[4], candle[5], "Kite Connect removed-constituent EOD",
                        now, "validated", 3,
                    ),
                )
                written += 1
            connection.commit()
    finally:
        connection.close()
    after = removed_constituent_history_audit(market_db)
    return {
        "ok": True,
        "contract_version": "removed-constituent-history-refresh-v1",
        "source": "Kite Connect direct Workspace EOD",
        "targets": len(targets_by_symbol),
        "attempted": attempted,
        "records_written": written,
        "unavailable_symbols": unavailable,
        "failed": failed,
        "coverage_before": before["coverage"],
        "coverage_after": after["coverage"],
    }


def refresh_nse_removed_constituent_archive(
    market_db: Path = DEFAULT_MARKET_DB,
    workers: int = 8,
) -> dict[str, Any]:
    """Backfill reviewed constituent exits from official NSE cash archives.

    Every requested symbol/date pair is recorded as present or absent.  The
    absence ledger lets the point-in-time audit distinguish a genuine data gap
    from a session before an IPO/listing (or another non-trading session).
    """
    ensure_workspace_schema(market_db)
    before = removed_constituent_history_audit(market_db)
    targets = [
        row for row in before["exits"]
        if row["status"] != "complete" and row.get("window_start") and row.get("window_end")
    ]
    connection = _connect(market_db)
    try:
        reference_dates = [str(row[0]) for row in connection.execute(
            "SELECT session_date FROM candles WHERE instrument_id='index:Nifty 50' ORDER BY session_date"
        )]
        work_by_date: dict[str, set[str]] = defaultdict(set)
        for target in targets:
            symbol = str(target["symbol"])
            stored = {
                str(row[0]) for row in connection.execute(
                    "SELECT session_date FROM candles WHERE instrument_id=?",
                    (f"stock:{symbol}",),
                )
            }
            checked_absent = {
                str(row[0]) for row in connection.execute(
                    "SELECT session_date FROM cash_archive_symbol_checks WHERE symbol=? AND present=0",
                    (symbol,),
                )
            }
            for session in reference_dates:
                if str(target["window_start"]) <= session <= str(target["window_end"]):
                    if session not in stored and session not in checked_absent:
                        work_by_date[session].add(symbol)
    finally:
        connection.close()

    NSE_CM_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    def load_session(session_text: str) -> dict[str, Any]:
        session = date.fromisoformat(session_text)
        key = session.strftime("%d%m%Y")
        url = NSE_CM_FULL_TEMPLATE.format(date=key)
        cache_path = NSE_CM_ARCHIVE_DIR / f"sec_bhavdata_full_{key}.csv"
        from_cache = cache_path.exists()
        if from_cache:
            payload = cache_path.read_bytes()
        else:
            response = requests.get(
                url,
                timeout=45,
                headers={"User-Agent": NSE_CM_USER_AGENT, "Accept": "text/csv,*/*"},
            )
            response.raise_for_status()
            payload = response.content
            if not payload.startswith((b"SYMBOL", b"\xef\xbb\xbfSYMBOL", b"PK")):
                raise ValueError("unexpected_cash_bhavcopy_payload")
            cache_path.write_bytes(payload)
        rows = parse_nse_cash_bhavcopy(payload, session)
        wanted = work_by_date[session_text]
        return {
            "session_date": session_text,
            "url": url,
            "from_cache": from_cache,
            "rows": {row["symbol"]: row for row in rows if row["symbol"] in wanted},
            "wanted": wanted,
        }

    loaded: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(int(workers), 12))) as pool:
        future_map = {pool.submit(load_session, session): session for session in sorted(work_by_date)}
        for future in as_completed(future_map):
            session = future_map[future]
            try:
                loaded.append(future.result())
            except (OSError, ValueError, requests.RequestException, zipfile.BadZipFile) as error:
                failures.append({"session_date": session, "reason": str(error)})

    now = datetime.now(timezone.utc).isoformat()
    written = present_checks = absent_checks = 0
    connection = _connect(market_db)
    try:
        for result in sorted(loaded, key=lambda item: item["session_date"]):
            for symbol in sorted(result["wanted"]):
                row = result["rows"].get(symbol)
                present = int(row is not None)
                connection.execute(
                    """INSERT OR REPLACE INTO cash_archive_symbol_checks(
                           session_date,symbol,present,source_url,retrieved_at,schema_version)
                       VALUES (?,?,?,?,?,?)""",
                    (result["session_date"], symbol, present, result["url"], now, 1),
                )
                if not row:
                    absent_checks += 1
                    continue
                present_checks += 1
                instrument_id = f"stock:{symbol}"
                connection.execute(
                    """INSERT INTO instruments(
                           instrument_id,kind,display_name,provider_token,source,updated_at,
                           schema_version,adjustment_status,adjustment_source_url)
                       VALUES (?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(instrument_id) DO UPDATE SET
                           source=excluded.source,updated_at=excluded.updated_at""",
                    (
                        instrument_id, "stock", symbol, f"NSE:{symbol}",
                        "NSE official cash bhavcopy archive", now, 3,
                        "raw_unadjusted", result["url"],
                    ),
                )
                connection.execute(
                    """INSERT OR REPLACE INTO candles(
                           instrument_id,session_date,open,high,low,close,volume,source,
                           retrieved_at,validation_status,schema_version)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        instrument_id, row["session_date"], row["open"], row["high"],
                        row["low"], row["close"], row["volume"],
                        "NSE official cash bhavcopy archive", now, "validated", 3,
                    ),
                )
                written += 1
        connection.commit()
    finally:
        connection.close()

    after = removed_constituent_history_audit(market_db)
    return {
        "ok": not failures,
        "contract_version": "removed-constituent-nse-archive-v1",
        "source": "NSE official full cash bhavcopy archive",
        "target_symbols": len({row["symbol"] for row in targets}),
        "requested_sessions": len(work_by_date),
        "loaded_sessions": len(loaded),
        "cached_sessions": sum(bool(row["from_cache"]) for row in loaded),
        "records_written": written,
        "present_checks": present_checks,
        "absent_checks": absent_checks,
        "failures": failures,
        "coverage_before": before["coverage"],
        "coverage_after": after["coverage"],
    }


def refresh_kite_futures(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    """Store the nearest listed NFO futures contract and its EOD price/OI history."""
    if not KITE.connected:
        raise ValueError("kite_not_connected")
    today = date.today()
    candidates: dict[str, dict[str, str]] = {}
    for row in KITE.instruments("NFO"):
        if row.get("instrument_type") != "FUT" or not row.get("expiry"):
            continue
        try:
            expiry = datetime.strptime(row["expiry"], "%Y-%m-%d").date()
        except ValueError:
            continue
        if expiry < today or expiry > today + timedelta(days=120):
            continue
        underlying = (row.get("name") or row.get("tradingsymbol") or "").strip()
        if not underlying:
            continue
        current = candidates.get(underlying)
        if current is None or row["expiry"] < current["expiry"]:
            candidates[underlying] = row

    connection = _connect(market_db)
    written = attempted = failed = 0
    now = datetime.now(timezone.utc).isoformat()
    try:
        for underlying, row in candidates.items():
            attempted += 1
            contract_key = f"NFO:{row['tradingsymbol']}"
            existing = connection.execute(
                "SELECT first_seen_at FROM futures_contracts WHERE contract_key=?", (contract_key,)
            ).fetchone()
            connection.execute(
                """INSERT OR REPLACE INTO futures_contracts(
                       contract_key,underlying,tradingsymbol,exchange,provider_token,expiry_date,
                       lot_size,source,first_seen_at,last_seen_at,schema_version
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    contract_key, underlying, row["tradingsymbol"], "NFO", row["instrument_token"],
                    row["expiry"], int(float(row.get("lot_size") or 0)),
                    "Kite instrument master direct Workspace", existing[0] if existing else now, now, 3,
                ),
            )
            latest = connection.execute(
                "SELECT MAX(session_date) FROM futures_eod_snapshots WHERE contract_key=?", (contract_key,)
            ).fetchone()[0]
            start_date = datetime.strptime(latest, "%Y-%m-%d").date() + timedelta(days=1) if latest else today - timedelta(days=14)
            if start_date > today:
                continue
            try:
                candles = KITE.candles(row["instrument_token"], start_date.isoformat(), today.isoformat(), oi=True)
            except ValueError:
                failed += 1
                continue
            for candle in candles:
                connection.execute(
                    """INSERT OR REPLACE INTO futures_eod_snapshots(
                           contract_key,session_date,open,high,low,close,volume,open_interest,
                           source,retrieved_at,schema_version
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        contract_key, str(candle[0])[:10], candle[1], candle[2], candle[3], candle[4],
                        int(candle[5] or 0), int(candle[6] or 0) if len(candle) > 6 else 0,
                        "Kite Connect direct Workspace EOD", now, 3,
                    ),
                )
                written += 1
        connection.commit()
    finally:
        connection.close()
    return {
        "ok": True,
        "contracts_selected": len(candidates),
        "attempted": attempted,
        "failed": failed,
        "records_written": written,
        "source": "Kite Connect direct Workspace EOD",
    }


def _canonical_bhavcopy_row(row: dict[str, str]) -> dict[str, str]:
    return {
        re.sub(r"[^A-Z0-9]", "", str(key).upper()): str(value or "").strip()
        for key, value in row.items()
    }


def _bhavcopy_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = row.get(re.sub(r"[^A-Z0-9]", "", key.upper()), "")
        if value not in ("", "-", "NA", "N/A"):
            return value
    return ""


def _parse_nse_date(value: str) -> date | None:
    value = str(value or "").strip()
    for pattern in ("%Y-%m-%d", "%d-%b-%Y", "%d-%b-%y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            continue
    return None


def _number(value: str, integer: bool = False) -> float | int:
    cleaned = str(value or "0").replace(",", "").strip()
    try:
        parsed = float(cleaned or 0)
    except ValueError:
        parsed = 0.0
    return int(parsed) if integer else parsed


def parse_nse_cash_bhavcopy(payload: bytes, expected_date: date | None = None) -> list[dict[str, Any]]:
    """Normalize the official NSE full cash bhavcopy to equity EOD rows."""
    if payload[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError("cash_bhavcopy_csv_missing")
            raw = archive.read(csv_names[0])
    else:
        raw = payload
    text = raw.decode("utf-8-sig", errors="replace")
    rows: list[dict[str, Any]] = []
    for original in csv.DictReader(io.StringIO(text)):
        row = _canonical_bhavcopy_row(original)
        symbol = _bhavcopy_value(row, "SYMBOL", "TckrSymb").upper()
        series = _bhavcopy_value(row, "SERIES", "SctySrs").upper()
        session = _parse_nse_date(_bhavcopy_value(row, "DATE1", "TradDt", "BizDt")) or expected_date
        if not symbol or not session or series not in {"EQ", "BE", "BZ"}:
            continue
        close = float(_number(_bhavcopy_value(row, "CLOSE_PRICE", "CLOSE", "ClsPric")))
        if close <= 0:
            continue
        rows.append({
            "symbol": symbol,
            "series": series,
            "session_date": session.isoformat(),
            "open": float(_number(_bhavcopy_value(row, "OPEN_PRICE", "OPEN", "OpnPric"))),
            "high": float(_number(_bhavcopy_value(row, "HIGH_PRICE", "HIGH", "HghPric"))),
            "low": float(_number(_bhavcopy_value(row, "LOW_PRICE", "LOW", "LwPric"))),
            "close": close,
            "volume": int(_number(_bhavcopy_value(row, "TTL_TRD_QNTY", "TOTTRDQTY", "TtlTradgVol"), True)),
        })
    return rows


def parse_nse_futures_bhavcopy(payload: bytes, expected_date: date | None = None) -> list[dict[str, Any]]:
    """Normalize legacy or UDiFF NSE F&O bhavcopy bytes to front-month futures rows."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError("nse_archive_csv_missing")
        raw = archive.read(csv_names[0])
    text = raw.decode("utf-8-sig", errors="replace")
    parsed: list[dict[str, Any]] = []
    for original in csv.DictReader(io.StringIO(text)):
        row = _canonical_bhavcopy_row(original)
        instrument = _bhavcopy_value(row, "INSTRUMENT", "FinInstrmTp").upper()
        option_type = _bhavcopy_value(row, "OPTION_TYP", "OptnTp").upper()
        strike = float(_number(_bhavcopy_value(row, "STRIKE_PR", "StrkPric")))
        is_future = instrument in {"FUTIDX", "FUTSTK", "IDF", "STF", "FUTURE", "FUT"}
        if not is_future or option_type in {"CE", "PE", "CA", "PA"} or strike != 0:
            continue
        symbol = _bhavcopy_value(row, "SYMBOL", "TckrSymb").upper()
        expiry = _parse_nse_date(_bhavcopy_value(row, "EXPIRY_DT", "XpryDt", "FininstrmActlXpryDt"))
        session = _parse_nse_date(_bhavcopy_value(row, "TIMESTAMP", "TradDt", "BizDt")) or expected_date
        if not symbol or not expiry or not session or expiry < session:
            continue
        parsed.append({
            "session_date": session.isoformat(),
            "instrument_type": instrument,
            "underlying": symbol,
            "expiry_date": expiry.isoformat(),
            "open": float(_number(_bhavcopy_value(row, "OPEN", "OpnPric"))),
            "high": float(_number(_bhavcopy_value(row, "HIGH", "HghPric"))),
            "low": float(_number(_bhavcopy_value(row, "LOW", "LwPric"))),
            "close": float(_number(_bhavcopy_value(row, "CLOSE", "ClsPric", "SttlmPric"))),
            "volume": int(_number(_bhavcopy_value(row, "CONTRACTS", "TtlTradgVol"), True)),
            "open_interest": int(_number(_bhavcopy_value(row, "OPEN_INT", "OpnIntrst"), True)),
        })
    front_month: dict[tuple[str, str], dict[str, Any]] = {}
    for row in parsed:
        key = (row["session_date"], row["underlying"])
        current = front_month.get(key)
        if current is None or row["expiry_date"] < current["expiry_date"]:
            front_month[key] = row
    return sorted(front_month.values(), key=lambda row: (row["session_date"], row["underlying"]))


def _nse_fo_archive_url(session: date) -> tuple[str, str]:
    if session >= NSE_FO_UDIFF_START:
        return "udiff", NSE_FO_UDIFF_TEMPLATE.format(date=session.strftime("%Y%m%d"))
    month = session.strftime("%b").upper()
    return "legacy", NSE_FO_LEGACY_TEMPLATE.format(
        year=session.strftime("%Y"), month=month, day=session.strftime("%d")
    )


def refresh_nse_futures_archive(
    market_db: Path = DEFAULT_MARKET_DB,
    start_date: date | None = None,
    end_date: date | None = None,
    target_sessions: int = 252,
    force: bool = False,
) -> dict[str, Any]:
    """Backfill official NSE front-month futures EOD price/OI observations."""
    ensure_workspace_schema(market_db)
    end_date = end_date or date.today()
    start_date = start_date or (end_date - timedelta(days=max(400, int(target_sessions * 1.7))))
    if start_date > end_date:
        raise ValueError("invalid_archive_date_range")
    NSE_FO_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    connection = _connect(market_db)
    imported = unavailable = failed = records_written = attempted = cached = 0
    errors: list[dict[str, str]] = []
    retrieved_at = datetime.now(timezone.utc).isoformat()
    http = requests.Session()
    http.headers.update({
        "User-Agent": NSE_FO_USER_AGENT,
        "Accept": "application/zip,application/octet-stream,*/*",
        "Referer": "https://www.nseindia.com/all-reports-derivatives",
    })
    try:
        cursor = start_date
        while cursor <= end_date:
            if cursor.weekday() >= 5:
                cursor += timedelta(days=1)
                continue
            previous = connection.execute(
                "SELECT status FROM fno_archive_files WHERE session_date=?", (cursor.isoformat(),)
            ).fetchone()
            if previous and not force and previous["status"] in {"imported", "unavailable"}:
                cursor += timedelta(days=1)
                continue
            attempted += 1
            archive_format, url = _nse_fo_archive_url(cursor)
            cache_path = NSE_FO_ARCHIVE_DIR / f"nse_fo_{cursor.isoformat()}_{archive_format}.zip"
            try:
                if cache_path.exists() and cache_path.stat().st_size > 100:
                    payload = cache_path.read_bytes()
                    cached += 1
                else:
                    response = http.get(url, timeout=45)
                    if response.status_code in {403, 404}:
                        unavailable += 1
                        connection.execute(
                            """INSERT OR REPLACE INTO fno_archive_files
                               (session_date,format,source_url,cache_path,sha256,status,records_written,detail,retrieved_at,schema_version)
                               VALUES (?,?,?,?,?,'unavailable',0,?,?,1)""",
                            (cursor.isoformat(), archive_format, url, None, None,
                             f"HTTP {response.status_code}", retrieved_at),
                        )
                        connection.commit()
                        cursor += timedelta(days=1)
                        continue
                    response.raise_for_status()
                    payload = response.content
                    cache_path.write_bytes(payload)
                    time.sleep(0.08)
                rows = parse_nse_futures_bhavcopy(payload, cursor)
                if not rows:
                    raise ValueError("no_front_month_futures_rows")
                digest = hashlib.sha256(payload).hexdigest()
                day_written = 0
                for row in rows:
                    expiry = date.fromisoformat(row["expiry_date"])
                    contract_key = f"NSEFO:{row['instrument_type']}:{row['underlying']}:{row['expiry_date']}"
                    tradingsymbol = f"{row['underlying']}{expiry.strftime('%d%b%y').upper()}FUT"
                    existing = connection.execute(
                        "SELECT first_seen_at FROM futures_contracts WHERE contract_key=?", (contract_key,)
                    ).fetchone()
                    connection.execute(
                        """INSERT OR REPLACE INTO futures_contracts(
                               contract_key,underlying,tradingsymbol,exchange,provider_token,expiry_date,
                               lot_size,source,first_seen_at,last_seen_at,schema_version)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (contract_key, row["underlying"], tradingsymbol, "NFO", "NSE_ARCHIVE",
                         row["expiry_date"], 0, "NSE official F&O bhavcopy archive",
                         existing[0] if existing else retrieved_at, retrieved_at, 4),
                    )
                    connection.execute(
                        """INSERT INTO futures_eod_snapshots(
                               contract_key,session_date,open,high,low,close,volume,open_interest,
                               source,retrieved_at,schema_version)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)
                           ON CONFLICT(contract_key,session_date) DO UPDATE SET
                               open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,
                               volume=excluded.volume,open_interest=excluded.open_interest,
                               source=excluded.source,retrieved_at=excluded.retrieved_at,schema_version=excluded.schema_version""",
                        (contract_key, row["session_date"], row["open"], row["high"], row["low"],
                         row["close"], row["volume"], row["open_interest"],
                         "NSE official F&O bhavcopy archive", retrieved_at, 4),
                    )
                    day_written += 1
                connection.execute(
                    """INSERT OR REPLACE INTO fno_archive_files
                       (session_date,format,source_url,cache_path,sha256,status,records_written,detail,retrieved_at,schema_version)
                       VALUES (?,?,?,?,?,'imported',?,'front-month futures only',?,1)""",
                    (cursor.isoformat(), archive_format, url, str(cache_path), digest,
                     day_written, retrieved_at),
                )
                connection.commit()
                imported += 1
                records_written += day_written
            except (OSError, ValueError, zipfile.BadZipFile, requests.RequestException) as error:
                failed += 1
                detail = str(error)[:300]
                errors.append({"date": cursor.isoformat(), "reason": detail})
                connection.execute(
                    """INSERT OR REPLACE INTO fno_archive_files
                       (session_date,format,source_url,cache_path,sha256,status,records_written,detail,retrieved_at,schema_version)
                       VALUES (?,?,?,?,?,'failed',0,?,?,1)""",
                    (cursor.isoformat(), archive_format, url,
                     str(cache_path) if cache_path.exists() else None, None, detail, retrieved_at),
                )
                connection.commit()
            cursor += timedelta(days=1)
        state = _fno_state(connection)
    finally:
        connection.close()
    return {
        "ok": imported > 0 or not attempted,
        "contract_version": "nse-fno-archive-backfill-v1",
        "range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "attempted_files": attempted,
        "imported_files": imported,
        "unavailable_files": unavailable,
        "failed_files": failed,
        "cached_files": cached,
        "records_written": records_written,
        "validated_comparable_sessions": state["stored_history_sessions"],
        "raw_stored_sessions": state["raw_history_sessions"],
        "history_gate_passed": state["stored_history_sessions"] >= target_sessions,
        "errors": errors[:20],
        "source": "NSE official F&O bhavcopy archive",
    }

