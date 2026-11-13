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
import random
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
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


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
            """
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
    if history_mode not in {"comparison", "full"}:
        raise ValueError("invalid_backtest_history_mode")
    common_as_of = reference_context.get("common_as_of_date")
    reference_dates = list(reference_context.get("reference_dates") or [])
    comparison_required_total = required_observations + BACKTEST_COMPARISON_SESSIONS - 1
    all_rows = _instrument_series(connection, instrument_id)
    row_by_date = {str(row["session_date"]): row for row in all_rows if not common_as_of or str(row["session_date"]) <= common_as_of}
    if history_mode == "full" and row_by_date:
        first_observation = min(row_by_date)
        target_dates = [session for session in reference_dates if session >= first_observation]
        required_total = required_observations
    else:
        target_dates = reference_dates[-comparison_required_total:] if len(reference_dates) >= comparison_required_total else []
        required_total = comparison_required_total
    selected_rows = [row_by_date[session] for session in target_dates if session in row_by_date]
    missing_dates = [session for session in target_dates if session not in row_by_date]
    invalid_ohlc: list[str] = []
    abnormal_jumps: list[dict[str, Any]] = []
    previous_close: float | None = None
    for row in selected_rows:
        session = str(row["session_date"])
        try:
            opening, high, low, close = (float(row[key]) for key in ("open", "high", "low", "close"))
        except (TypeError, ValueError):
            invalid_ohlc.append(session)
            continue
        if not all(math.isfinite(value) and value > 0 for value in (opening, high, low, close)) or high < max(opening, close) or low > min(opening, close) or high < low:
            invalid_ohlc.append(session)
        if previous_close and previous_close > 0:
            jump = 100 * (close / previous_close - 1.0)
            if abs(jump) > BACKTEST_ABNORMAL_JUMP_PCT:
                abnormal_jumps.append({"session_date": session, "return_pct": _round(jump)})
        previous_close = close

    metadata = connection.execute(
        "SELECT adjustment_status,adjustment_source_url,source,updated_at FROM instruments WHERE instrument_id=?",
        (instrument_id,),
    ).fetchone()
    source_rows = connection.execute(
        """SELECT source, COUNT(*) AS rows, MAX(retrieved_at) AS retrieved_at
           FROM candles WHERE instrument_id=? AND (? IS NULL OR session_date<=?) GROUP BY source ORDER BY rows DESC""",
        (instrument_id, common_as_of, common_as_of),
    ).fetchall()
    duplicate_rows = connection.execute(
        """SELECT session_date, COUNT(*) AS rows FROM candles
           WHERE instrument_id=? AND (? IS NULL OR session_date<=?)
           GROUP BY session_date HAVING COUNT(*) > 1 ORDER BY session_date""",
        (instrument_id, common_as_of, common_as_of),
    ).fetchall()
    action_rows = connection.execute(
        """SELECT action_type,purpose,ex_date,record_date,adjustment_requirement,
                  review_status,source_url,retrieved_at
           FROM corporate_actions
           WHERE instrument_id=? AND ex_date BETWEEN ? AND ? ORDER BY ex_date""",
        (instrument_id, target_dates[0] if target_dates else "0000-01-01", common_as_of or "9999-12-31"),
    ).fetchall()
    lineage_rows = connection.execute(
        """SELECT old_symbol,new_symbol,effective_date,review_status,source_url,retrieved_at
           FROM symbol_history WHERE instrument_id=? ORDER BY effective_date""",
        (instrument_id,),
    ).fetchall()
    coverage_row = connection.execute(
        "SELECT value,updated_at FROM workspace_metadata WHERE key='corporate_action_coverage'"
    ).fetchone()
    corporate_coverage: dict[str, Any] | None = None
    if coverage_row:
        try:
            corporate_coverage = json.loads(coverage_row["value"])
        except (TypeError, json.JSONDecodeError):
            corporate_coverage = None
    latest_observation = max(row_by_date, default=None)
    gates: list[str] = []
    warnings: list[str] = []
    if not common_as_of or not target_dates:
        gates.append("reference_window_unavailable")
    if latest_observation != common_as_of:
        gates.append("stale_latest_observation")
    insufficient_history = (
        len(selected_rows) < required_observations
        if history_mode == "full"
        else len(all_rows) < required_total
    )
    if insufficient_history:
        gates.append("insufficient_common_history")
    if missing_dates:
        gates.append("missing_reference_sessions")
    if duplicate_rows:
        gates.append("duplicate_sessions")
    if invalid_ohlc:
        gates.append("invalid_ohlc")
    if abnormal_jumps:
        gates.append("abnormal_price_jump")
    adjustment_status = metadata["adjustment_status"] if metadata else "unknown"
    if adjustment_status not in {"provider_reported_adjusted", "verified_adjusted"}:
        gates.append("adjustment_provenance_unverified")
    if (not corporate_coverage or not target_dates
            or corporate_coverage.get("from", "9999-12-31") > target_dates[0]
            or corporate_coverage.get("through", "0000-01-01") < (common_as_of or "9999-12-31")):
        if history_mode == "full":
            warnings.append("corporate_action_ledger_partial_full_history")
        else:
            gates.append("corporate_action_ledger_incomplete")
    unresolved_actions = [row for row in action_rows if row["review_status"] in {"manual_review", "manual_lineage_review"}]
    if unresolved_actions:
        gates.append("corporate_action_manual_review")
    audit = {
        "status": "ready" if not gates else "gated",
        "history_mode": history_mode,
        "common_as_of_date": common_as_of,
        "comparison_sessions": BACKTEST_COMPARISON_SESSIONS if history_mode == "comparison" else None,
        "required_warmup_observations": required_observations - 1,
        "required_total_observations": required_total,
        "stored_observations": len(all_rows),
        "selected_observations": len(selected_rows),
        "evaluation_start_date": target_dates[required_observations - 1] if len(target_dates) >= required_observations else None,
        "history_start_date": target_dates[0] if target_dates else None,
        "latest_observation": latest_observation,
        "missing_session_count": len(missing_dates),
        "missing_sessions": missing_dates[:20],
        "duplicate_session_count": len(duplicate_rows),
        "duplicate_sessions": [dict(row) for row in duplicate_rows[:20]],
        "invalid_ohlc_count": len(invalid_ohlc),
        "invalid_ohlc_sessions": invalid_ohlc[:20],
        "abnormal_jump_count": len(abnormal_jumps),
        "abnormal_jumps": abnormal_jumps[:20],
        "blocking_gates": gates,
        "warnings": warnings,
        "adjustment_status": adjustment_status,
        "adjustment_source_url": metadata["adjustment_source_url"] if metadata else None,
        "adjustment_gate_enforced": True,
        "corporate_action_count": len(action_rows),
        "corporate_actions": [dict(row) for row in action_rows],
        "unresolved_corporate_action_count": len(unresolved_actions),
        "corporate_action_coverage": corporate_coverage,
        "corporate_action_coverage_updated_at": coverage_row["updated_at"] if coverage_row else None,
        "symbol_lineage": [dict(row) for row in lineage_rows],
        "return_contract": "price_return_excluding_ordinary_cash_dividends",
        "instrument_source": metadata["source"] if metadata else None,
        "instrument_updated_at": metadata["updated_at"] if metadata else None,
        "candle_sources": [dict(row) for row in source_rows],
    }
    return (selected_rows if not gates else []), audit


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


def dashboard_overview(
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


def _maximum_drawdown_pct(equity_curve: list[float]) -> float | None:
    if not equity_curve:
        return None
    peak = equity_curve[0]
    maximum_drawdown = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        if peak > 0:
            maximum_drawdown = min(maximum_drawdown, equity / peak - 1.0)
    return _round(100 * maximum_drawdown)


def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    """Numerically evaluate the continued fraction used by incomplete beta."""
    maximum_iterations, epsilon, floor = 200, 3e-14, 1e-300
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < floor:
        d = floor
    d = 1.0 / d
    value = d
    for iteration in range(1, maximum_iterations + 1):
        doubled = 2 * iteration
        coefficient = iteration * (b - iteration) * x / ((qam + doubled) * (a + doubled))
        d = 1.0 + coefficient * d
        c = 1.0 + coefficient / c
        if abs(d) < floor:
            d = floor
        if abs(c) < floor:
            c = floor
        d = 1.0 / d
        value *= d * c
        coefficient = -(a + iteration) * (qab + iteration) * x / ((a + doubled) * (qap + doubled))
        d = 1.0 + coefficient * d
        c = 1.0 + coefficient / c
        if abs(d) < floor:
            d = floor
        if abs(c) < floor:
            c = floor
        d = 1.0 / d
        delta = d * c
        value *= delta
        if abs(delta - 1.0) < epsilon:
            break
    return value


def _regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    factor = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
                      + a * math.log(x) + b * math.log1p(-x))
    if x < (a + 1.0) / (a + b + 2.0):
        return factor * _beta_continued_fraction(a, b, x) / a
    return 1.0 - factor * _beta_continued_fraction(b, a, 1.0 - x) / b


def _trade_edge_p_value(completed_trade_returns: list[float]) -> float | None:
    """Two-sided one-sample Student t-test of mean completed-trade return vs zero."""
    count = len(completed_trade_returns)
    if count < 2:
        return None
    average = mean(completed_trade_returns)
    sample_variance = sum((value - average) ** 2 for value in completed_trade_returns) / (count - 1)
    if sample_variance <= 0:
        return 1.0 if average == 0 else 0.0
    statistic = abs(average) / math.sqrt(sample_variance / count)
    degrees = count - 1
    probability = _regularized_incomplete_beta(degrees / 2.0, 0.5, degrees / (degrees + statistic * statistic))
    return _round(min(1.0, max(0.0, probability)), 6)


def _percentile(values: list[float], probability: float) -> float | None:
    """Linearly interpolated percentile for a non-empty finite sample."""
    finite = sorted(value for value in values if math.isfinite(value))
    if not finite:
        return None
    position = (len(finite) - 1) * min(1.0, max(0.0, probability))
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return finite[lower]
    weight = position - lower
    return finite[lower] * (1.0 - weight) + finite[upper] * weight


def _sample_profit_factor(sample: list[float]) -> float | None:
    gross_profit = sum(value for value in sample if value > 0)
    gross_loss = abs(sum(value for value in sample if value < 0))
    return gross_profit / gross_loss if gross_loss > 0 else None


def _bootstrap_trade_evidence(
    completed_trade_returns: list[float], resamples: int = 1000, minimum_trades: int = 15,
) -> dict[str, Any]:
    """Deterministic ordinary and circular moving-block bootstrap evidence."""
    sample = [float(value) for value in completed_trade_returns if math.isfinite(float(value))]
    count = len(sample)
    block_length = min(count, max(2, int(round(math.sqrt(count))))) if count else None
    base = {
        "contract_version": "trade-bootstrap-evidence-v1",
        "sample": {
            "completed_net_trades": count,
            "minimum_required": minimum_trades,
            "sampling_unit": "ordered_cost_adjusted_completed_trade_return",
        },
        "confidence_level": 0.95,
        "ordinary_bootstrap": None,
        "moving_block_bootstrap": None,
    }
    if count < minimum_trades:
        return {
            **base,
            "status": "insufficient_sample",
            "interpretation": f"Requires at least {minimum_trades} completed cost-adjusted trades; {count} are available.",
        }

    iterations = max(200, min(int(resamples), 5000))
    seed_material = "|".join(f"{value:.10f}" for value in sample)
    seed = int(hashlib.sha256(seed_material.encode("utf-8")).hexdigest()[:16], 16)
    independent_rng = random.Random(seed)
    block_rng = random.Random(seed ^ 0x9E3779B97F4A7C15)

    def summarise(draws: list[list[float]], method: str) -> dict[str, Any]:
        expectancies = [mean(draw) for draw in draws]
        profit_factors = [value for value in (_sample_profit_factor(draw) for draw in draws) if value is not None]
        expectancy_interval = [_percentile(expectancies, 0.025), _percentile(expectancies, 0.975)]
        profit_factor_interval = [_percentile(profit_factors, 0.025), _percentile(profit_factors, 0.975)]
        return {
            "method": method,
            "resamples": iterations,
            "valid_profit_factor_resamples": len(profit_factors),
            "net_expectancy_ci_95_pct": [_round(value) for value in expectancy_interval],
            "net_profit_factor_ci_95": [_round(value) for value in profit_factor_interval],
            "probability_net_expectancy_positive": _round(sum(value > 0 for value in expectancies) / iterations, 4),
        }

    independent_draws = [
        [sample[independent_rng.randrange(count)] for _ in range(count)]
        for _ in range(iterations)
    ]
    block_draws: list[list[float]] = []
    for _ in range(iterations):
        draw: list[float] = []
        while len(draw) < count:
            start = block_rng.randrange(count)
            draw.extend(sample[(start + offset) % count] for offset in range(block_length or 1))
        block_draws.append(draw[:count])

    return {
        **base,
        "status": "ready",
        "ordinary_bootstrap": summarise(independent_draws, "iid_trade_bootstrap"),
        "moving_block_bootstrap": {
            **summarise(block_draws, "circular_moving_block_trade_bootstrap"),
            "block_length_trades": block_length,
        },
        "interpretation": "Intervals quantify sampling uncertainty in net completed-trade evidence; they are not forecasts or trade instructions.",
    }


def _attach_benjamini_hochberg_fdr(
    results: list[dict[str, Any]], alpha: float = 0.05,
) -> dict[str, Any]:
    """Attach monotone Benjamini-Hochberg q-values to one strategy universe."""
    tests = [
        (index, float(row["net_trade_edge_p_value"]))
        for index, row in enumerate(results)
        if row.get("status") == "ready" and row.get("net_trade_edge_p_value") is not None
    ]
    ordered = sorted(tests, key=lambda item: item[1])
    adjusted: dict[int, float] = {}
    running = 1.0
    total = len(ordered)
    for rank in range(total, 0, -1):
        index, probability = ordered[rank - 1]
        running = min(running, probability * total / rank)
        adjusted[index] = min(1.0, max(0.0, running))
    for index, row in enumerate(results):
        q_value = adjusted.get(index)
        row["net_trade_edge_q_value"] = _round(q_value, 6)
        row["multiple_testing_status"] = (
            "fdr_significant" if q_value is not None and q_value <= alpha else
            "tested_not_significant" if q_value is not None else "not_tested"
        )
    return {
        "contract_version": "benjamini-hochberg-fdr-v1",
        "family": "one_strategy_across_comparable_stocks",
        "alpha": alpha,
        "tested_hypotheses": total,
        "raw_p_below_alpha": sum(probability <= alpha for _, probability in tests),
        "fdr_q_below_alpha": sum(value <= alpha for value in adjusted.values()),
        "interpretation": "Controls the expected false-discovery proportion within this strategy's comparable stock family; it does not correct across strategy families.",
    }


EQUITY_DELIVERY_COST_SCHEDULE = {
    "schedule_version": "zerodha-nse-equity-delivery-2026-03-v1",
    "effective_from": "2026-03-01",
    "brokerage_rate": 0.0,
    "stt_rate_each_side": 0.001,
    "nse_transaction_charge_per_crore": 306.99,
    "nse_ipft_per_crore": 0.01,
    "sebi_charge_per_crore": 10.0,
    "gst_rate": 0.18,
    "stamp_duty_buy_rate": 0.00015,
    "dp_charge_sell_inr": 15.34,
    "sources": [
        "https://zerodha.com/charges/",
        "https://zerodha.com/brokerage-calculator/",
        "https://nsearchives.nseindia.com/content/circulars/FA73061.pdf",
    ],
}

DEFAULT_BACKTEST_COST_CONFIG = {
    "position_capital_inr": 50000.0,
    "slippage_bps_each_side": 5.0,
    "market_impact_bps_each_side": 0.0,
    "include_dp_charge": True,
}


def _normalise_cost_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    values = dict(DEFAULT_BACKTEST_COST_CONFIG)
    if config:
        values.update(config)
    capital = float(values["position_capital_inr"])
    slippage = float(values["slippage_bps_each_side"])
    impact = float(values["market_impact_bps_each_side"])
    if not math.isfinite(capital) or capital <= 0:
        raise ValueError("invalid_position_capital")
    if not math.isfinite(slippage) or slippage < 0 or slippage > 500:
        raise ValueError("invalid_slippage_bps")
    if not math.isfinite(impact) or impact < 0 or impact > 500:
        raise ValueError("invalid_market_impact_bps")
    return {
        "position_capital_inr": capital,
        "slippage_bps_each_side": slippage,
        "market_impact_bps_each_side": impact,
        "include_dp_charge": bool(values.get("include_dp_charge", True)),
    }


def _equity_delivery_side_charges(
    turnover_inr: float,
    side: str,
    config: dict[str, Any],
) -> dict[str, float]:
    turnover = max(0.0, float(turnover_inr))
    if side not in {"buy", "sell"}:
        raise ValueError("invalid_trade_side")
    schedule = EQUITY_DELIVERY_COST_SCHEDULE
    brokerage = turnover * float(schedule["brokerage_rate"])
    stt = turnover * float(schedule["stt_rate_each_side"])
    exchange = turnover * float(schedule["nse_transaction_charge_per_crore"]) / 10_000_000
    ipft = turnover * float(schedule["nse_ipft_per_crore"]) / 10_000_000
    sebi = turnover * float(schedule["sebi_charge_per_crore"]) / 10_000_000
    gst = float(schedule["gst_rate"]) * (brokerage + exchange + ipft + sebi)
    stamp = turnover * float(schedule["stamp_duty_buy_rate"]) if side == "buy" else 0.0
    dp = float(schedule["dp_charge_sell_inr"]) if side == "sell" and config["include_dp_charge"] and turnover > 0 else 0.0
    slippage = turnover * float(config["slippage_bps_each_side"]) / 10_000
    market_impact = turnover * float(config["market_impact_bps_each_side"]) / 10_000
    components = {
        "brokerage": brokerage,
        "stt": stt,
        "exchange_transaction": exchange,
        "nse_ipft": ipft,
        "sebi": sebi,
        "gst": gst,
        "stamp_duty": stamp,
        "dp_charge": dp,
        "slippage": slippage,
        "market_impact": market_impact,
    }
    components["total"] = sum(components.values())
    return components


def equity_delivery_cost_calculator(
    buy_turnover_inr: float,
    sell_turnover_inr: float,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Estimate Zerodha/NSE equity-delivery costs for one buy and one sell."""
    assumptions = _normalise_cost_config(config)
    buy = _equity_delivery_side_charges(buy_turnover_inr, "buy", assumptions)
    sell = _equity_delivery_side_charges(sell_turnover_inr, "sell", assumptions)
    total = buy["total"] + sell["total"]
    gross_turnover = max(0.0, float(buy_turnover_inr)) + max(0.0, float(sell_turnover_inr))
    return {
        "ok": True,
        "contract_version": "equity-delivery-cost-v1",
        "segment": "NSE equity delivery",
        "schedule": EQUITY_DELIVERY_COST_SCHEDULE,
        "assumptions": assumptions,
        "buy_turnover_inr": _round(float(buy_turnover_inr)),
        "sell_turnover_inr": _round(float(sell_turnover_inr)),
        "buy_charges": {key: _round(value, 4) for key, value in buy.items()},
        "sell_charges": {key: _round(value, 4) for key, value in sell.items()},
        "total_cost_inr": _round(total, 4),
        "total_cost_pct_of_gross_turnover": _round(100 * total / gross_turnover, 6) if gross_turnover else None,
        "rate_basis": "Current published schedule applied uniformly; historical rate changes are not reconstructed.",
    }


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
    completed_trade_returns: list[float] = []
    net_completed_trade_returns: list[float] = []
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
                executed_buys += 1
            elif entry_price is not None:
                completed_trade_returns.append(100 * (current_open / entry_price - 1.0))
                entry_net_capital = capital_inr - float(entry_cost_inr or 0.0)
                exit_turnover = entry_net_capital * current_open / entry_price
                exit_cost = _equity_delivery_side_charges(exit_turnover, "sell", cost_assumptions)["total"]
                net_completed_trade_returns.append(100 * ((exit_turnover - exit_cost) / capital_inr - 1.0))
                entry_price = None
                entry_cost_inr = None
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
            universe = point_in_time_universe_contract(
                market_db=market_db, snapshot_db=snapshot_db, as_of_date=test_end.isoformat()
            )
            fold["point_in_time_universe"] = {
                "status": universe.get("membership_status"),
                "ranking_admissible": bool(universe.get("ranking_admissible")),
                "as_of_date": test_end.isoformat(),
                "member_count": universe.get("member_count"),
                "classification_status": universe.get("classification_status"),
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


def ema_crossover(market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None, fast_window: int = 20, slow_window: int = 50, history_mode: str = "comparison") -> dict[str, Any]:
    if fast_window < 2 or slow_window <= fast_window or slow_window > 250:
        raise ValueError("invalid_ema_windows")
    return _run_signal_backtest(market_db, symbol, _ema_crossover_series, (fast_window, slow_window), "ema_crossover",
                                "ema-crossover-backtest-v8", {"fast_window": fast_window, "slow_window": slow_window,
                                "average_type": "exponential", "positioning": "long_or_cash",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, slow_window + 1, history_mode)


def time_series_momentum(market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None, lookback: int = 63, history_mode: str = "comparison") -> dict[str, Any]:
    if lookback < 5 or lookback > 250:
        raise ValueError("invalid_momentum_lookback")
    return _run_signal_backtest(market_db, symbol, _time_series_momentum_series, (lookback,), "time_series_momentum",
                                "time-series-momentum-backtest-v8", {"lookback": lookback, "positioning": "long_or_cash",
                                "entry_rule": "lookback_return_positive", "exit_rule": "lookback_return_non_positive",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, lookback + 2, history_mode)


def triple_sma_alignment(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    fast_window: int = 20, medium_window: int = 50, slow_window: int = 200,
    history_mode: str = "comparison",
) -> dict[str, Any]:
    if fast_window < 2 or medium_window <= fast_window or slow_window <= medium_window or slow_window > 300:
        raise ValueError("invalid_triple_sma_windows")
    return _run_signal_backtest(market_db, symbol, _triple_sma_alignment_series,
                                (fast_window, medium_window, slow_window), "triple_sma_alignment",
                                "triple-sma-alignment-backtest-v8", {"fast_window": fast_window, "medium_window": medium_window,
                                "slow_window": slow_window, "average_type": "simple", "positioning": "long_or_cash",
                                "entry_rule": "fast_above_medium_above_slow", "exit_rule": "alignment_breaks",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, slow_window + 1, history_mode)


def rsi_mean_reversion(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    period: int = 2, entry_rsi: float = 10.0, exit_rsi: float = 70.0,
    history_mode: str = "comparison",
) -> dict[str, Any]:
    if period < 2 or period > 50 or entry_rsi <= 0 or exit_rsi >= 100 or entry_rsi >= exit_rsi:
        raise ValueError("invalid_rsi_mean_reversion_parameters")
    return _run_signal_backtest(market_db, symbol, _rsi_mean_reversion_series,
                                (period, entry_rsi, exit_rsi), "rsi_mean_reversion",
                                "rsi-mean-reversion-backtest-v8", {"period": period, "entry_rsi": entry_rsi,
                                "exit_rsi": exit_rsi, "positioning": "long_or_cash",
                                "entry_rule": "rsi_at_or_below_entry", "exit_rule": "rsi_at_or_above_exit",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, period + 2, history_mode)


def bollinger_mean_reversion(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    window: int = 20, deviations: float = 2.0, history_mode: str = "comparison",
) -> dict[str, Any]:
    if window < 5 or window > 250 or deviations <= 0 or deviations > 5:
        raise ValueError("invalid_bollinger_parameters")
    return _run_signal_backtest(market_db, symbol, _bollinger_mean_reversion_series,
                                (window, deviations), "bollinger_mean_reversion",
                                "bollinger-mean-reversion-backtest-v8", {"window": window, "deviations": deviations,
                                "positioning": "long_or_cash", "entry_rule": "close_below_lower_band",
                                "exit_rule": "close_at_or_above_middle_band",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, window + 1, history_mode)


def zscore_mean_reversion(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    window: int = 10, entry_z: float = -1.5, exit_z: float = 0.0,
    history_mode: str = "comparison",
) -> dict[str, Any]:
    if window < 5 or window > 250 or entry_z >= exit_z or entry_z >= 0:
        raise ValueError("invalid_zscore_parameters")
    return _run_signal_backtest(market_db, symbol, _zscore_mean_reversion_series,
                                (window, entry_z, exit_z), "zscore_mean_reversion",
                                "zscore-mean-reversion-backtest-v8", {"window": window, "entry_z": entry_z,
                                "exit_z": exit_z, "positioning": "long_or_cash",
                                "entry_rule": "zscore_at_or_below_entry", "exit_rule": "zscore_at_or_above_exit",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, window + 1, history_mode)


def pullback_uptrend(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    pullback_window: int = 5, trend_window: int = 200, history_mode: str = "comparison",
) -> dict[str, Any]:
    if pullback_window < 2 or trend_window <= pullback_window or trend_window > 300:
        raise ValueError("invalid_pullback_uptrend_windows")
    return _run_signal_backtest(market_db, symbol, _pullback_uptrend_series,
                                (pullback_window, trend_window), "pullback_uptrend",
                                "pullback-uptrend-backtest-v8", {"pullback_window": pullback_window,
                                "trend_window": trend_window, "positioning": "long_or_cash",
                                "entry_rule": "close_below_pullback_sma_and_above_trend_sma",
                                "exit_rule": "close_recovers_pullback_sma_or_breaks_trend_sma",
                                "return_attribution": "completed_close_signal_executed_next_session_open"}, trend_window + 1, history_mode)


def selloff_recovery(
    market_db: Path = DEFAULT_MARKET_DB, symbol: str | None = None,
    down_sessions: int = 3, exit_window: int = 5, history_mode: str = "comparison",
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
                                max(down_sessions, exit_window - 1) + 2, history_mode)


_CROSS_STRATEGY_FDR_LOCK = threading.Lock()
_CROSS_STRATEGY_FDR_CACHE: dict[str, Any] = {}


def _cross_strategy_fdr_from_payloads(payloads: list[dict[str, Any]], alpha: float = 0.05) -> dict[str, Any]:
    hypotheses: list[dict[str, Any]] = []
    for payload in payloads:
        engine = str(payload.get("engine") or "unknown")
        for row in payload.get("results", []):
            probability = row.get("net_trade_edge_p_value")
            if row.get("status") != "ready" or probability is None:
                continue
            hypotheses.append({
                "status": "ready",
                "engine": engine,
                "symbol": row.get("symbol"),
                "net_trade_edge_p_value": probability,
            })
    contract = _attach_benjamini_hochberg_fdr(hypotheses, alpha)
    by_engine: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in hypotheses:
        by_engine[row["engine"]][str(row["symbol"])] = {
            "raw_p_value": row["net_trade_edge_p_value"],
            "cross_strategy_q_value": row["net_trade_edge_q_value"],
            "status": row["multiple_testing_status"],
        }
    strategy_counts = {
        engine: {
            "tested_hypotheses": len(rows),
            "fdr_q_below_alpha": sum(item["cross_strategy_q_value"] <= alpha for item in rows.values()),
        }
        for engine, rows in by_engine.items()
    }
    return {
        "ok": True,
        "contract_version": "cross-strategy-benjamini-hochberg-fdr-v1",
        "family": "all_default_strategy_stock_pairs",
        "alpha": alpha,
        "tested_hypotheses": contract["tested_hypotheses"],
        "raw_p_below_alpha": contract["raw_p_below_alpha"],
        "fdr_q_below_alpha": contract["fdr_q_below_alpha"],
        "strategy_counts": strategy_counts,
        "by_engine": dict(by_engine),
        "interpretation": "Controls the expected false-discovery proportion across all default stock-by-strategy comparisons in one combined family.",
    }


def cross_strategy_multiple_testing(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    """Compute and cache the combined stock-by-strategy FDR contract."""
    stat = market_db.stat()
    cache_key = f"{stat.st_mtime_ns}:{stat.st_size}"
    with _CROSS_STRATEGY_FDR_LOCK:
        cached = _CROSS_STRATEGY_FDR_CACHE.get(cache_key)
        if cached is not None:
            return cached
        runners = (
            moving_average_crossover, donchian_breakout, ema_crossover,
            time_series_momentum, triple_sma_alignment, rsi_mean_reversion,
            bollinger_mean_reversion, zscore_mean_reversion, pullback_uptrend,
            selloff_recovery,
        )
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(runner, market_db) for runner in runners]
            payloads = [future.result() for future in futures]
        result = _cross_strategy_fdr_from_payloads(payloads)
        result["database_version"] = cache_key
        result["strategy_count"] = len(payloads)
        result["as_of_date"] = max((payload.get("as_of_date") or "" for payload in payloads), default=None) or None
        _CROSS_STRATEGY_FDR_CACHE.clear()
        _CROSS_STRATEGY_FDR_CACHE[cache_key] = result
        return result


def screener_local(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
    rows = _technical_rows(market_db)
    return {"ok": True, "as_of_date": max((row["as_of_date"] for row in rows), default=None), "rows": rows, "coverage": {"ready": sum(row["status"] == "ready" for row in rows)}}


def factor_research(market_db: Path = DEFAULT_MARKET_DB) -> dict[str, Any]:
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


def stock_ytd(market_db: Path = DEFAULT_MARKET_DB, snapshot_db: Path = DEFAULT_SNAPSHOT_DB) -> dict[str, Any]:
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

