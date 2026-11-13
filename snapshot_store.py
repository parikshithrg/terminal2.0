#!/usr/bin/env python3
"""Store auditable Workspace 2.0 market snapshots from official public sources."""

from __future__ import annotations

import argparse
import csv
import html
import io
import json
import math
import re
import sqlite3
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import requests


ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "data" / "workspace_snapshots.sqlite3"
USER_AGENT = "Workspace-2.0-EOD-Snapshot/1.0"

NIFTY_500_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"
SIZE_INDEX_URLS = {
    "Large": ("NIFTY 100", "https://www.niftyindices.com/IndexConstituent/ind_nifty100list.csv", 90),
    "Mid": ("NIFTY MIDCAP 150", "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv", 140),
    "Small": ("NIFTY SMALLCAP 250", "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv", 230),
}
NSE_INTEGRATED_PAGE = (
    "https://www.nseindia.com/companies-listing/corporate-integrated-filing"
    "?integratedType=integratedfilingfinancials&tabIndex=equity"
)
NSE_INTEGRATED_API = "https://www.nseindia.com/api/integrated-filing-results"
NSE_BHAVCOPY_TEMPLATE = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
FACTOR_METHODOLOGY_VERSION = "nifty500-size-style-v1"
CCIL_CURVE_URL = "https://www.ccilindia.com/en/curve"
US_TREASURY_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
    "?data=daily_treasury_yield_curve&field_tdr_date_value={year}"
)
BOE_CURVE_URL = "https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves/latest-yield-curve-data.zip"
BOE_MONTHLY_ARCHIVE_URL = "https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves/glcnominalmonthedata.zip"
JAPAN_MOF_URL = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/historical/jgbcme_all.csv"
BUNDESBANK_SERIES = {
    2: "BBSSY/D.REN.EUR.A610.000000WT0202.A",
    5: "BBSSY/D.REN.EUR.A620.000000WT0505.A",
    10: "BBSSY/D.REN.EUR.A630.000000WT1010.A",
    30: "BBSSY/D.REN.EUR.A640.000000WT3030.A",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch(url: str, timeout: int = 90) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def connect(path: Path = DEFAULT_DB) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS snapshot_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_key TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('successful', 'failed')),
            records_written INTEGER NOT NULL DEFAULT 0,
            detail TEXT
        );

        CREATE TABLE IF NOT EXISTS index_membership_snapshots (
            index_name TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            company_name TEXT NOT NULL,
            industry TEXT,
            series TEXT,
            isin TEXT,
            source_url TEXT NOT NULL,
            retrieved_at TEXT NOT NULL,
            validation_status TEXT NOT NULL CHECK (validation_status = 'validated'),
            PRIMARY KEY (index_name, snapshot_date, symbol)
        );

        CREATE TABLE IF NOT EXISTS sovereign_yield_observations (
            market TEXT NOT NULL,
            observation_date TEXT NOT NULL,
            tenor_years INTEGER NOT NULL,
            yield_pct REAL NOT NULL,
            curve_type TEXT NOT NULL,
            source_url TEXT NOT NULL,
            retrieved_at TEXT NOT NULL,
            validation_status TEXT NOT NULL CHECK (validation_status = 'validated'),
            PRIMARY KEY (market, observation_date, tenor_years, curve_type)
        );

        CREATE TABLE IF NOT EXISTS equity_size_classifications (
            snapshot_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            size_bucket TEXT NOT NULL CHECK (size_bucket IN ('Large', 'Mid', 'Small')),
            source_index TEXT NOT NULL,
            source_url TEXT NOT NULL,
            retrieved_at TEXT NOT NULL,
            validation_status TEXT NOT NULL CHECK (validation_status = 'validated'),
            PRIMARY KEY (snapshot_date, symbol, size_bucket)
        );

        CREATE TABLE IF NOT EXISTS nse_integrated_financial_filings (
            sequence_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            company_name TEXT,
            quarter_end_date TEXT,
            filing_timestamp TEXT,
            filing_subtype TEXT,
            audited_status TEXT,
            consolidation_scope TEXT,
            xbrl_url TEXT NOT NULL,
            ixbrl_url TEXT,
            source_url TEXT NOT NULL,
            retrieved_at TEXT NOT NULL,
            validation_status TEXT NOT NULL CHECK (validation_status = 'metadata_validated')
        );

        CREATE TABLE IF NOT EXISTS nse_financial_facts (
            sequence_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            quarter_end_date TEXT,
            consolidation_scope TEXT,
            metric TEXT NOT NULL,
            value REAL NOT NULL,
            unit_ref TEXT,
            decimals_value TEXT,
            xbrl_url TEXT NOT NULL,
            retrieved_at TEXT NOT NULL,
            validation_status TEXT NOT NULL CHECK (validation_status = 'taxonomy_validated'),
            PRIMARY KEY (sequence_id, metric),
            FOREIGN KEY (sequence_id) REFERENCES nse_integrated_financial_filings(sequence_id)
        );

        CREATE TABLE IF NOT EXISTS nse_filing_parse_audits (
            sequence_id TEXT PRIMARY KEY,
            parsed_at TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('successful', 'failed')),
            facts_written INTEGER NOT NULL DEFAULT 0,
            detail TEXT,
            FOREIGN KEY (sequence_id) REFERENCES nse_integrated_financial_filings(sequence_id)
        );

        CREATE TABLE IF NOT EXISTS equity_factor_classifications (
            snapshot_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            size_bucket TEXT NOT NULL,
            style_bucket TEXT NOT NULL,
            market_cap REAL,
            value_score REAL,
            growth_score REAL,
            source_detail TEXT NOT NULL,
            PRIMARY KEY (snapshot_date, symbol)
        );

        CREATE TABLE IF NOT EXISTS equity_price_observations (
            session_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            previous_close REAL NOT NULL,
            close_price REAL NOT NULL,
            return_pct REAL NOT NULL,
            source_url TEXT NOT NULL,
            retrieved_at TEXT NOT NULL,
            validation_status TEXT NOT NULL CHECK (validation_status = 'validated'),
            PRIMARY KEY (session_date, symbol)
        );

        CREATE TABLE IF NOT EXISTS equity_factor_returns (
            session_date TEXT NOT NULL,
            size_bucket TEXT NOT NULL,
            style_bucket TEXT NOT NULL,
            return_pct REAL NOT NULL,
            constituents_count INTEGER NOT NULL,
            methodology_version TEXT NOT NULL,
            PRIMARY KEY (session_date, size_bucket, style_bucket, methodology_version)
        );
        """
    )
    return connection


def record_run(connection: sqlite3.Connection, source: str, started: str, status: str, count: int, detail: str = "") -> None:
    connection.execute(
        "INSERT INTO snapshot_runs(source_key, started_at, completed_at, status, records_written, detail) VALUES (?, ?, ?, ?, ?, ?)",
        (source, started, utc_now(), status, count, detail[:1000]),
    )
    connection.commit()


def store_membership(connection: sqlite3.Connection) -> int:
    retrieved = utc_now()
    snapshot_date = date.today().isoformat()
    rows = list(csv.DictReader(io.StringIO(fetch(NIFTY_500_URL).decode("utf-8-sig"))))
    if len(rows) < 450:
        raise ValueError(f"NIFTY 500 constituent file returned only {len(rows)} rows")
    values = [
        (
            "NIFTY 500", snapshot_date, row["Symbol"].strip(), row["Company Name"].strip(),
            row.get("Industry", "").strip(), row.get("Series", "").strip(), row.get("ISIN Code", "").strip(),
            NIFTY_500_URL, retrieved, "validated",
        )
        for row in rows if row.get("Symbol") and row.get("Company Name")
    ]
    connection.executemany(
        """INSERT OR REPLACE INTO index_membership_snapshots
           (index_name, snapshot_date, symbol, company_name, industry, series, isin, source_url, retrieved_at, validation_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        values,
    )
    connection.commit()
    return len(values)


def store_size_classifications(connection: sqlite3.Connection) -> int:
    retrieved = utc_now()
    snapshot_date = date.today().isoformat()
    latest_membership = connection.execute(
        "SELECT MAX(snapshot_date) FROM index_membership_snapshots WHERE index_name = 'NIFTY 500'"
    ).fetchone()[0]
    if not latest_membership:
        raise ValueError("NIFTY 500 membership must be stored before size classification")
    nifty_500 = {
        row[0] for row in connection.execute(
            "SELECT symbol FROM index_membership_snapshots WHERE index_name = 'NIFTY 500' AND snapshot_date = ?",
            (latest_membership,),
        )
    }
    values: list[tuple[str, str, str, str, str, str, str]] = []
    seen: dict[str, str] = {}
    for bucket, (index_name, url, minimum_rows) in SIZE_INDEX_URLS.items():
        rows = list(csv.DictReader(io.StringIO(fetch(url).decode("utf-8-sig"))))
        if len(rows) < minimum_rows:
            raise ValueError(f"{index_name} constituent file returned only {len(rows)} rows")
        for row in rows:
            symbol = row.get("Symbol", "").strip()
            if not symbol or symbol not in nifty_500:
                continue
            if symbol in seen and seen[symbol] != bucket:
                raise ValueError(f"{symbol} appears in both {seen[symbol]} and {bucket} size universes")
            seen[symbol] = bucket
            values.append((snapshot_date, symbol, bucket, index_name, url, retrieved, "validated"))
    if len(seen) < 480:
        raise ValueError(f"official size universes classified only {len(seen)} current NIFTY 500 symbols")
    connection.execute("DELETE FROM equity_size_classifications WHERE snapshot_date = ?", (snapshot_date,))
    connection.executemany(
        """INSERT INTO equity_size_classifications
           (snapshot_date, symbol, size_bucket, source_index, source_url, retrieved_at, validation_status)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        values,
    )
    connection.commit()
    return len(seen)


def _nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/141.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    response = session.get(NSE_INTEGRATED_PAGE, timeout=45)
    response.raise_for_status()
    return session


def store_nse_filing_metadata(connection: sqlite3.Connection) -> int:
    latest_membership = connection.execute(
        "SELECT MAX(snapshot_date) FROM index_membership_snapshots WHERE index_name = 'NIFTY 500'"
    ).fetchone()[0]
    if not latest_membership:
        raise ValueError("NIFTY 500 membership must be stored before filing metadata")
    symbols = {
        row[0] for row in connection.execute(
            "SELECT symbol FROM index_membership_snapshots WHERE index_name = 'NIFTY 500' AND snapshot_date = ?",
            (latest_membership,),
        )
    }
    today = date.today()
    session = _nse_session()
    records: list[dict[str, object]] = []
    window_start = today - timedelta(days=550)
    while window_start <= today:
        window_end = min(today, window_start + timedelta(days=119))
        params = {
            "index": "equities",
            "from_date": window_start.strftime("%d-%m-%Y"),
            "to_date": window_end.strftime("%d-%m-%Y"),
            "page": 1,
            "size": 10000,
        }
        response = session.get(
            NSE_INTEGRATED_API,
            params=params,
            headers={"Accept": "application/json, text/plain, */*", "Referer": NSE_INTEGRATED_PAGE},
            timeout=120,
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(batch, list):
            raise ValueError(f"NSE integrated-filing response was invalid for {window_start} to {window_end}")
        records.extend(batch)
        window_start = window_end + timedelta(days=1)
    if not records:
        raise ValueError("NSE integrated-filing response did not contain records")
    retrieved = utc_now()
    values = []
    for row in records:
        symbol = str(row.get("symbol") or "").strip()
        xbrl_url = str(row.get("xbrl") or "").strip()
        if symbol not in symbols or row.get("type") != "Integrated Filing- Financials" or not xbrl_url:
            continue
        values.append((
            str(row.get("seq_Id") or "").strip(), symbol, row.get("cmName") or row.get("smName"),
            row.get("qe_Date"), row.get("broadcast_Date") or row.get("creation_Date"), row.get("type_Sub"),
            row.get("audited"), row.get("consolidated"), xbrl_url, row.get("ixbrl"),
            NSE_INTEGRATED_API, retrieved, "metadata_validated",
        ))
    values = [row for row in values if row[0]]
    if not values:
        raise ValueError("NSE returned no current NIFTY 500 financial filings in the review window")
    connection.executemany(
        """INSERT INTO nse_integrated_financial_filings
           (sequence_id, symbol, company_name, quarter_end_date, filing_timestamp, filing_subtype,
            audited_status, consolidation_scope, xbrl_url, ixbrl_url, source_url, retrieved_at, validation_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(sequence_id) DO UPDATE SET
             company_name=excluded.company_name, quarter_end_date=excluded.quarter_end_date,
             filing_timestamp=excluded.filing_timestamp, filing_subtype=excluded.filing_subtype,
             audited_status=excluded.audited_status, consolidation_scope=excluded.consolidation_scope,
             xbrl_url=excluded.xbrl_url, ixbrl_url=excluded.ixbrl_url,
             source_url=excluded.source_url, retrieved_at=excluded.retrieved_at,
             validation_status=excluded.validation_status""",
        values,
    )
    connection.commit()
    return len(values)


FACTOR_FACT_ALIASES = {
    "RevenueFromOperations": "RevenueFromOperations",
    "Income": "RevenueFromOperations",
    "OperatingIncome": "RevenueFromOperations",
    "GrossPremiumIncome": "RevenueFromOperations",
    "ProfitBeforeTax": "ProfitBeforeTax",
    "ProfitLossFromOrdinaryActivitiesBeforeTax": "ProfitBeforeTax",
    "ProfitOrLossBeforeTax": "ProfitBeforeTax",
    "ProfitLossForPeriod": "ProfitLossForPeriod",
    "ProfitLossForThePeriod": "ProfitLossForPeriod",
    "ProfitLossAfterTax": "ProfitLossForPeriod",
    "ProfitLossAfterTaxAndExtraordinaryItems": "ProfitLossForPeriod",
    "ProfitOrLossAttributableToOwnersOfParent": "ProfitOrLossAttributableToOwnersOfParent",
    "PaidUpValueOfEquityShareCapital": "PaidUpValueOfEquityShareCapital",
    "PaidUpEquityShareCapital": "PaidUpValueOfEquityShareCapital",
    "PaidUpEquityCapital": "PaidUpValueOfEquityShareCapital",
    "FaceValueOfEquityShareCapital": "FaceValueOfEquityShareCapital",
    "BasicEarningsLossPerShareFromContinuingOperations": "BasicEarningsLossPerShareFromContinuingOperations",
    "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations": "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
    "BasicEarningsPerShareBeforeExtraordinaryItems": "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
    "BasicEarningsPerShareAfterExtraordinaryItems": "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
    "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations": "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
    "DilutedEarningsPerShareBeforeExtraordinaryItems": "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
    "DilutedEarningsPerShareAfterExtraordinaryItems": "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
}


def _filing_date(value: str | None) -> date:
    if not value:
        return date.min
    for pattern in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.title(), pattern).date()
        except ValueError:
            continue
    return date.min


def _filing_timestamp_date(value: str | None) -> date:
    if not value:
        return date.min
    for pattern in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value.title(), pattern).date()
        except ValueError:
            continue
    return date.min


def _select_latest_filings(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    rows = connection.execute("SELECT * FROM nse_integrated_financial_filings").fetchall()
    successful = {
        row[0] for row in connection.execute(
            "SELECT sequence_id FROM nse_filing_parse_audits WHERE status = 'successful'"
        )
    }
    by_symbol: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        by_symbol.setdefault(row["symbol"], []).append(row)
    selected = []
    for candidates in by_symbol.values():
        by_period: dict[date, list[sqlite3.Row]] = {}
        for row in candidates:
            period = _filing_date(row["quarter_end_date"])
            if period != date.min:
                by_period.setdefault(period, []).append(row)
        for period in sorted(by_period, reverse=True)[:6]:
            current = by_period[period]
            consolidated = [row for row in current if row["consolidation_scope"] == "Consolidated"]
            pool = consolidated or current
            choice = max(pool, key=lambda row: (row["filing_subtype"] == "Revision", row["filing_timestamp"] or "", row["sequence_id"]))
            if choice["sequence_id"] not in successful:
                selected.append(choice)
    return selected


def _parse_xbrl_row(row: dict[str, object]) -> tuple[str, list[tuple[object, ...]], str | None]:
    sequence_id = str(row["sequence_id"])
    try:
        response = requests.get(str(row["xbrl_url"]), headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        retrieved = utc_now()
        contexts: dict[str, tuple[date, date]] = {}
        for context in (element for element in root.iter() if _local_name(element.tag) == "context"):
            starts = [node.text for node in context.iter() if _local_name(node.tag) == "startDate" and node.text]
            ends = [node.text for node in context.iter() if _local_name(node.tag) in {"endDate", "instant"} and node.text]
            if ends:
                start = _filing_date(starts[0]) if starts else _filing_date(ends[0])
                contexts[context.attrib.get("id", "")] = (start, _filing_date(ends[0]))
        report_end = _filing_date(str(row.get("quarter_end_date") or ""))
        candidates: dict[str, list[tuple[int, ET.Element, float]]] = {}
        for element in root.iter():
            source_metric = _local_name(element.tag)
            metric = FACTOR_FACT_ALIASES.get(source_metric)
            if not metric:
                continue
            raw = (element.text or "").strip().replace(",", "")
            try:
                value = float(raw)
            except ValueError:
                continue
            start, end = contexts.get(element.attrib.get("contextRef", ""), (date.min, date.min))
            if report_end != date.min and end not in {date.min, report_end}:
                continue
            duration = max(0, (end - start).days)
            candidates.setdefault(metric, []).append((duration, element, value))
        facts: dict[str, tuple[object, ...]] = {}
        for metric, options in candidates.items():
            _, element, value = min(options, key=lambda option: option[0])
            facts[metric] = (
                sequence_id, row["symbol"], row["quarter_end_date"], row["consolidation_scope"],
                metric, value, element.attrib.get("unitRef"), element.attrib.get("decimals"),
                row["xbrl_url"], retrieved, "taxonomy_validated",
            )
        if len(facts) < 3:
            raise ValueError(f"only {len(facts)} approved factor facts found")
        return sequence_id, list(facts.values()), None
    except Exception as error:
        return sequence_id, [], str(error)


def normalize_latest_xbrl_facts(connection: sqlite3.Connection) -> int:
    selected = _select_latest_filings(connection)
    if not selected:
        return 0
    rows = [dict(row) for row in selected]
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = [executor.submit(_parse_xbrl_row, row) for row in rows]
        for future in as_completed(futures):
            results.append(future.result())
    fact_rows: list[tuple[object, ...]] = []
    audits = []
    parsed_at = utc_now()
    for sequence_id, facts, error in results:
        fact_rows.extend(facts)
        audits.append((sequence_id, parsed_at, "failed" if error else "successful", len(facts), (error or "")[:1000]))
    connection.executemany(
        """INSERT OR REPLACE INTO nse_financial_facts
           (sequence_id, symbol, quarter_end_date, consolidation_scope, metric, value, unit_ref,
            decimals_value, xbrl_url, retrieved_at, validation_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        fact_rows,
    )
    connection.executemany(
        """INSERT OR REPLACE INTO nse_filing_parse_audits
           (sequence_id, parsed_at, status, facts_written, detail) VALUES (?, ?, ?, ?, ?)""",
        audits,
    )
    connection.commit()
    successful_symbols = connection.execute(
        "SELECT COUNT(DISTINCT symbol) FROM nse_financial_facts"
    ).fetchone()[0]
    if successful_symbols < 350:
        raise ValueError(f"only {successful_symbols} NIFTY 500 symbols have normalized factor facts")
    return len(fact_rows)


def store_equity_prices(connection: sqlite3.Connection) -> int:
    latest_membership = connection.execute(
        "SELECT MAX(snapshot_date) FROM index_membership_snapshots WHERE index_name = 'NIFTY 500'"
    ).fetchone()[0]
    if not latest_membership:
        raise ValueError("NIFTY 500 membership must be stored before prices")
    symbols = {
        row[0] for row in connection.execute(
            "SELECT symbol FROM index_membership_snapshots WHERE index_name = 'NIFTY 500' AND snapshot_date = ?",
            (latest_membership,),
        )
    }
    retrieved = utc_now()
    selected: tuple[str, str, list[tuple[object, ...]]] | None = None
    for offset in range(0, 11):
        candidate = date.today() - timedelta(days=offset)
        url = NSE_BHAVCOPY_TEMPLATE.format(date=candidate.strftime("%d%m%Y"))
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=45)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            rows = list(csv.DictReader(io.StringIO(response.content.decode("utf-8-sig"))))
        except (requests.RequestException, UnicodeDecodeError):
            continue
        values = []
        for raw_row in rows:
            row = {str(key).strip(): str(value or "").strip() for key, value in raw_row.items()}
            symbol = row.get("SYMBOL", "")
            if symbol not in symbols or row.get("SERIES") != "EQ":
                continue
            try:
                previous = float(row["PREV_CLOSE"])
                close = float(row["CLOSE_PRICE"])
            except (KeyError, ValueError):
                continue
            if previous <= 0 or close <= 0:
                continue
            session = _filing_date(row.get("DATE1"))
            session_date = (session if session != date.min else candidate).isoformat()
            values.append((session_date, symbol, previous, close, (close / previous - 1.0) * 100.0, url, retrieved, "validated"))
        if len(values) >= 450:
            selected = (values[0][0], url, values)
            break
    if selected is None:
        raise ValueError("no completed NSE bhavcopy with at least 450 NIFTY 500 observations was found")
    session_date, _, values = selected
    connection.execute("DELETE FROM equity_price_observations WHERE session_date = ?", (session_date,))
    connection.executemany(
        """INSERT INTO equity_price_observations
           (session_date, symbol, previous_close, close_price, return_pct, source_url, retrieved_at, validation_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        values,
    )
    connection.commit()
    return len(values)


def _percentile_ranks(values: dict[str, float]) -> dict[str, float]:
    ordered = sorted(values.items(), key=lambda item: item[1])
    if len(ordered) <= 1:
        return {symbol: 0.5 for symbol in values}
    result: dict[str, float] = {}
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][1] == ordered[index][1]:
            end += 1
        average_rank = ((index + end - 1) / 2) / (len(ordered) - 1)
        for position in range(index, end):
            result[ordered[position][0]] = average_rank
        index = end
    return result


def calculate_equity_factor_returns(connection: sqlite3.Connection) -> int:
    session_date = connection.execute("SELECT MAX(session_date) FROM equity_price_observations").fetchone()[0]
    size_date = connection.execute("SELECT MAX(snapshot_date) FROM equity_size_classifications").fetchone()[0]
    if not session_date or not size_date:
        raise ValueError("price and size snapshots are required before factor calculation")
    evidence_cutoff = _filing_date(session_date)
    prices = {
        row["symbol"]: dict(row) for row in connection.execute(
            "SELECT symbol, close_price, return_pct FROM equity_price_observations WHERE session_date = ?",
            (session_date,),
        )
    }
    sizes = {
        row["symbol"]: row["size_bucket"] for row in connection.execute(
            "SELECT symbol, size_bucket FROM equity_size_classifications WHERE snapshot_date = ?",
            (size_date,),
        )
    }
    filing_rows = connection.execute(
        """SELECT f.sequence_id, f.symbol, f.quarter_end_date, f.filing_timestamp, f.filing_subtype,
                  f.consolidation_scope, x.metric, x.value
           FROM nse_integrated_financial_filings f
           JOIN nse_financial_facts x ON x.sequence_id = f.sequence_id"""
    ).fetchall()
    sequences: dict[str, dict[str, object]] = {}
    for row in filing_rows:
        filing_available = _filing_timestamp_date(row["filing_timestamp"])
        if filing_available == date.min or filing_available > evidence_cutoff:
            continue
        item = sequences.setdefault(row["sequence_id"], {
            "sequence_id": row["sequence_id"], "symbol": row["symbol"],
            "period": _filing_date(row["quarter_end_date"]), "filing_timestamp": row["filing_timestamp"] or "",
            "subtype": row["filing_subtype"] or "", "scope": row["consolidation_scope"] or "",
            "facts": {},
        })
        item["facts"][row["metric"]] = float(row["value"])
    by_symbol_period: dict[str, dict[date, list[dict[str, object]]]] = {}
    for item in sequences.values():
        if item["period"] == date.min:
            continue
        by_symbol_period.setdefault(str(item["symbol"]), {}).setdefault(item["period"], []).append(item)

    observations: dict[str, dict[str, object]] = {}
    for symbol, periods in by_symbol_period.items():
        if symbol not in prices or symbol not in sizes:
            continue
        preferred: dict[date, dict[str, object]] = {}
        for period, candidates in periods.items():
            consolidated = [item for item in candidates if item["scope"] == "Consolidated"]
            pool = consolidated or candidates
            preferred[period] = max(pool, key=lambda item: (item["subtype"] == "Revision", item["filing_timestamp"], item["sequence_id"]))
        current_period = max(preferred)
        target = current_period - timedelta(days=365)
        prior_candidates = [period for period in preferred if period < current_period and abs((period - target).days) <= 50]
        if not prior_candidates:
            continue
        prior_period = min(prior_candidates, key=lambda period: abs((period - target).days))
        current = preferred[current_period]["facts"]
        prior = preferred[prior_period]["facts"]
        close_price = float(prices[symbol]["close_price"])
        eps = current.get("BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations")
        if eps is None:
            paid = current.get("PaidUpValueOfEquityShareCapital")
            face = current.get("FaceValueOfEquityShareCapital")
            profit = current.get("ProfitLossForPeriod")
            if paid and face and profit and paid > 0 and face > 0:
                eps = profit / (paid / face)
        if eps is None or close_price <= 0:
            continue
        earnings_yield = (float(eps) * 4.0) / close_price
        growth_components: dict[str, float] = {}
        for metric, label in (
            ("RevenueFromOperations", "revenue_yoy"),
            ("ProfitLossForPeriod", "profit_yoy"),
            ("BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations", "eps_yoy"),
        ):
            current_value = current.get(metric)
            prior_value = prior.get(metric)
            if current_value is not None and prior_value is not None and float(prior_value) > 0:
                growth_components[label] = max(-3.0, min(3.0, float(current_value) / float(prior_value) - 1.0))
        if len(growth_components) < 2 or not math.isfinite(earnings_yield):
            continue
        growth_score = sum(growth_components.values()) / len(growth_components)
        observations[symbol] = {
            "size": sizes[symbol], "value": earnings_yield, "growth": growth_score,
            "return": float(prices[symbol]["return_pct"]), "current_period": current_period.isoformat(),
            "prior_period": prior_period.isoformat(), "growth_components": growth_components,
        }

    classified: dict[str, dict[str, object]] = {}
    for size_bucket in ("Large", "Mid", "Small"):
        cohort = {symbol: row for symbol, row in observations.items() if row["size"] == size_bucket}
        if len(cohort) < 30:
            continue
        value_ranks = _percentile_ranks({symbol: float(row["value"]) for symbol, row in cohort.items()})
        growth_ranks = _percentile_ranks({symbol: float(row["growth"]) for symbol, row in cohort.items()})
        style_signal = {symbol: growth_ranks[symbol] - value_ranks[symbol] for symbol in cohort}
        style_ranks = _percentile_ranks(style_signal)
        for symbol, row in cohort.items():
            rank = style_ranks[symbol]
            style = "Value" if rank < (1 / 3) else ("Growth" if rank >= (2 / 3) else "Core")
            classified[symbol] = {**row, "style": style, "style_signal": style_signal[symbol]}

    minimum_coverage = math.ceil(len(sizes) * 0.70)
    if len(classified) < minimum_coverage:
        raise ValueError(f"style coverage {len(classified)}/{len(sizes)} is below the 70% publication gate")
    groups: dict[tuple[str, str], list[float]] = {}
    for row in classified.values():
        groups.setdefault((str(row["size"]), str(row["style"])), []).append(float(row["return"]))
    expected = {(size, style) for size in ("Large", "Mid", "Small") for style in ("Value", "Core", "Growth")}
    missing = sorted(f"{size}/{style}" for size, style in expected if len(groups.get((size, style), [])) < 5)
    if missing:
        raise ValueError(f"factor cells below five constituents: {', '.join(missing)}")

    connection.execute("DELETE FROM equity_factor_classifications WHERE snapshot_date = ?", (session_date,))
    classification_rows = []
    for symbol, row in classified.items():
        detail = json.dumps({
            "methodology": FACTOR_METHODOLOGY_VERSION, "current_period": row["current_period"],
            "prior_period": row["prior_period"], "growth_components": row["growth_components"],
            "style_signal": row["style_signal"], "price_session": session_date,
        }, sort_keys=True)
        classification_rows.append((
            session_date, symbol, row["size"], row["style"], None, row["value"], row["growth"], detail,
        ))
    connection.executemany(
        """INSERT INTO equity_factor_classifications
           (snapshot_date, symbol, size_bucket, style_bucket, market_cap, value_score, growth_score, source_detail)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        classification_rows,
    )
    connection.execute(
        "DELETE FROM equity_factor_returns WHERE session_date = ? AND methodology_version = ?",
        (session_date, FACTOR_METHODOLOGY_VERSION),
    )
    factor_rows = [
        (session_date, size, style, sum(returns) / len(returns), len(returns), FACTOR_METHODOLOGY_VERSION)
        for (size, style), returns in groups.items()
    ]
    connection.executemany(
        """INSERT INTO equity_factor_returns
           (session_date, size_bucket, style_bucket, return_pct, constituents_count, methodology_version)
           VALUES (?, ?, ?, ?, ?, ?)""",
        factor_rows,
    )
    connection.commit()
    return len(factor_rows)


def store_yields(connection: sqlite3.Connection, market: str, observed: str, curve_type: str, source_url: str, values: dict[int, float]) -> int:
    retrieved = utc_now()
    rows = [(market, observed, tenor, value, curve_type, source_url, retrieved, "validated") for tenor, value in values.items()]
    if len(rows) != 4 or set(values) != {2, 5, 10, 30}:
        raise ValueError(f"{market} curve does not contain all required tenors")
    connection.executemany(
        """INSERT OR REPLACE INTO sovereign_yield_observations
           (market, observation_date, tenor_years, yield_pct, curve_type, source_url, retrieved_at, validation_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    connection.commit()
    return len(rows)


def store_yield_history(
    connection: sqlite3.Connection,
    market: str,
    curve_type: str,
    source_url: str,
    observations: list[tuple[str, dict[int, float]]],
) -> int:
    cutoff = date.today() - timedelta(days=366 * 5)
    retrieved = utc_now()
    rows = []
    for observed, values in observations:
        parsed = _filing_date(observed)
        if parsed == date.min or parsed < cutoff or set(values) != {2, 5, 10, 30}:
            continue
        for tenor, value in values.items():
            if math.isfinite(float(value)):
                rows.append((market, parsed.isoformat(), tenor, float(value), curve_type, source_url, retrieved, "validated"))
    if len(rows) < 4:
        raise ValueError(f"{market} did not provide a complete yield observation")
    connection.executemany(
        """INSERT OR REPLACE INTO sovereign_yield_observations
           (market, observation_date, tenor_years, yield_pct, curve_type, source_url, retrieved_at, validation_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    connection.commit()
    return len(rows)


def import_india(connection: sqlite3.Connection) -> int:
    session = requests.Session()
    headers = {"User-Agent": "Mozilla/5.0"}
    page = session.get(CCIL_CURVE_URL, headers=headers, timeout=60).text
    assignments = re.findall(r"(?:var\s+)?records\s*=\s*(\[.*?\]);", page, re.S)
    machine = next((value for value in assignments if value.startswith('[{"')), None)
    if machine is None:
        raise ValueError("CCIL machine-readable curve was not found")
    records = json.loads(machine)
    records_by_date = {row["date"]: row for row in records if row.get("date")}
    stored_history = connection.execute(
        "SELECT COUNT(DISTINCT observation_date) FROM sovereign_yield_observations WHERE market = 'India' AND tenor_years = 10"
    ).fetchone()[0]
    if stored_history < 30:
        form_match = re.search(r'<form[^>]+action="([^"]+)"[^>]+method="post"[^>]+id="daterange"', page, re.I)
        if form_match:
            action = html.unescape(form_match.group(1))
            namespace = "_CcilNSSCurve_CcilNSSCurvePortlet_INSTANCE_iexc_"
            for quarter in range(1, 21):
                center = date.today() - timedelta(days=91 * quarter)
                response = session.post(
                    action,
                    headers={**headers, "Referer": CCIL_CURVE_URL},
                    data={
                        f"{namespace}fromDate": (center - timedelta(days=7)).isoformat(),
                        f"{namespace}toDate": (center + timedelta(days=7)).isoformat(),
                    },
                    timeout=60,
                )
                response.raise_for_status()
                historical = re.findall(r"(?:var\s+)?records\s*=\s*(\[.*?\]);", response.text, re.S)
                payload = next((value for value in historical if value.startswith('[{"')), None)
                if payload:
                    for row in json.loads(payload):
                        if row.get("date"):
                            records_by_date[row["date"]] = row
    observations = [
        (row["date"], {2: row["zeroRate02"], 5: row["zeroRate05"], 10: row["zeroRate_10"], 30: row["zeroRate_30"]})
        for row in records_by_date.values()
    ]
    return store_yield_history(connection, "India", "NSS zero-coupon curve", CCIL_CURVE_URL, observations)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def import_united_states(connection: sqlite3.Connection) -> int:
    records: list[dict[str, str]] = []
    for year in range(date.today().year - 5, date.today().year + 1):
        root = ET.fromstring(fetch(US_TREASURY_URL.format(year=year)))
        for entry in (node for node in root.iter() if _local_name(node.tag) == "entry"):
            properties = next((node for node in entry.iter() if _local_name(node.tag) == "properties"), None)
            if properties is not None:
                records.append({_local_name(node.tag): node.text or "" for node in properties})
    observations = [
        (row["NEW_DATE"][:10], {2: float(row["BC_2YEAR"]), 5: float(row["BC_5YEAR"]), 10: float(row["BC_10YEAR"]), 30: float(row["BC_30YEAR"])})
        for row in records if all(row.get(key) not in {None, ""} for key in ("NEW_DATE", "BC_2YEAR", "BC_5YEAR", "BC_10YEAR", "BC_30YEAR"))
    ]
    source = US_TREASURY_URL.format(year=date.today().year)
    return store_yield_history(connection, "United States", "Treasury par yield curve", source, observations)


def _bundesbank_history(series: str) -> tuple[dict[str, float], str]:
    url = f"https://api.statistiken.bundesbank.de/rest/data/{series}?format=csv&lang=en"
    rows = csv.reader(io.StringIO(fetch(url).decode("utf-8-sig")))
    valid = [(row[0], float(row[1])) for row in rows if len(row) > 1 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", row[0]) and row[1] not in {"", "."}]
    return dict(valid), url


def import_germany(connection: sqlite3.Connection) -> int:
    histories = {tenor: _bundesbank_history(series)[0] for tenor, series in BUNDESBANK_SERIES.items()}
    common_dates = set.intersection(*(set(history) for history in histories.values()))
    observations = [(observed, {tenor: histories[tenor][observed] for tenor in histories}) for observed in common_dates]
    source = "https://www.bundesbank.de/en/statistics/overview-of-the-statistical-series/-/3-yields-of-current-federal-securities-914558"
    return store_yield_history(connection, "Germany", "Current federal securities", source, observations)


def _xlsx_sheet_rows(payload: bytes, sheet_name: str) -> list[dict[str, str]]:
    workbook = zipfile.ZipFile(io.BytesIO(payload))
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main", "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}
    shared: list[str] = []
    if "xl/sharedStrings.xml" in workbook.namelist():
        root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
        shared = ["".join(node.text or "" for node in item.findall(".//m:t", ns)) for item in root.findall("m:si", ns)]
    book = ET.fromstring(workbook.read("xl/workbook.xml"))
    rel_id = next(node.attrib[f"{{{ns['r']}}}id"] for node in book.findall(".//m:sheet", ns) if node.attrib["name"] == sheet_name)
    rels = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
    target = next(node.attrib["Target"] for node in rels if node.attrib["Id"] == rel_id)
    target = target.lstrip("/")
    if not target.startswith("xl/"):
        target = f"xl/{target}"
    sheet = ET.fromstring(workbook.read(target))
    result: list[dict[str, str]] = []
    for row in sheet.findall(".//m:row", ns):
        values: dict[str, str] = {}
        for cell in row.findall("m:c", ns):
            column = re.match(r"[A-Z]+", cell.attrib["r"]).group(0)
            value_node = cell.find("m:v", ns)
            value = "" if value_node is None else (value_node.text or "")
            if cell.attrib.get("t") == "s" and value:
                value = shared[int(value)]
            values[column] = value
        result.append(values)
    return result


def import_united_kingdom(connection: sqlite3.Connection) -> int:
    outer = zipfile.ZipFile(io.BytesIO(fetch(BOE_CURVE_URL, timeout=120)))
    workbook_name = next(name for name in outer.namelist() if "GLC Nominal daily data" in name)
    rows = _xlsx_sheet_rows(outer.read(workbook_name), "4. spot curve")
    valid = [row for row in rows if row.get("A", "").replace(".", "", 1).isdigit() and all(row.get(column) for column in ("E", "K", "U", "BI"))]
    observations = [
        ((date(1899, 12, 30) + timedelta(days=int(float(row["A"])))).isoformat(),
         {2: float(row["E"]), 5: float(row["K"]), 10: float(row["U"]), 30: float(row["BI"])})
        for row in valid
    ]
    archive_headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/141.0.0.0 Safari/537.36",
        "Referer": "https://www.bankofengland.co.uk/statistics/yield-curves",
    }
    archive_response = requests.get(BOE_MONTHLY_ARCHIVE_URL, headers=archive_headers, timeout=120)
    archive_response.raise_for_status()
    archive = zipfile.ZipFile(io.BytesIO(archive_response.content))
    for workbook_name in archive.namelist():
        if not ("2016 to 2024" in workbook_name or "2025 to present" in workbook_name):
            continue
        for row in _xlsx_sheet_rows(archive.read(workbook_name), "4. spot curve"):
            if not (row.get("A", "").replace(".", "", 1).isdigit() and all(row.get(column) for column in ("E", "K", "U", "BI"))):
                continue
            observations.append((
                (date(1899, 12, 30) + timedelta(days=int(float(row["A"])))).isoformat(),
                {2: float(row["E"]), 5: float(row["K"]), 10: float(row["U"]), 30: float(row["BI"])},
            ))
    return store_yield_history(connection, "United Kingdom", "Nominal government spot curve", BOE_CURVE_URL, observations)


def import_japan(connection: sqlite3.Connection) -> int:
    content = fetch(JAPAN_MOF_URL).decode("utf-8-sig").splitlines()
    header_index = next(index for index, line in enumerate(content) if line.startswith("Date,"))
    rows = list(csv.DictReader(content[header_index:], skipinitialspace=True))
    valid = [row for row in rows if row.get("Date") and all(row.get(key) not in {None, "", "-"} for key in ("2Y", "5Y", "10Y", "30Y"))]
    observations = [
        (datetime.strptime(row["Date"], "%Y/%m/%d").date().isoformat(),
         {2: float(row["2Y"]), 5: float(row["5Y"]), 10: float(row["10Y"]), 30: float(row["30Y"])})
        for row in valid
    ]
    return store_yield_history(connection, "Japan", "JGB constant-maturity curve", JAPAN_MOF_URL, observations)


def update_all(path: Path = DEFAULT_DB) -> dict[str, object]:
    connection = connect(path)
    tasks: list[tuple[str, Callable[[sqlite3.Connection], int]]] = [
        ("nifty_500_membership", store_membership),
        ("nifty_size_classification", store_size_classifications),
        ("nse_integrated_financial_filings", store_nse_filing_metadata),
        ("nse_xbrl_factor_facts", normalize_latest_xbrl_facts),
        ("nse_equity_prices", store_equity_prices),
        ("nifty_500_factor_returns", calculate_equity_factor_returns),
        ("india_yields", import_india),
        ("us_yields", import_united_states),
        ("germany_yields", import_germany),
        ("uk_yields", import_united_kingdom),
        ("japan_yields", import_japan),
    ]
    results: dict[str, object] = {}
    try:
        for name, task in tasks:
            started = utc_now()
            try:
                count = task(connection)
                record_run(connection, name, started, "successful", count)
                results[name] = {"status": "successful", "records": count}
            except Exception as error:  # keep independent sources from blocking one another
                record_run(connection, name, started, "failed", 0, str(error))
                results[name] = {"status": "failed", "detail": str(error)}
    finally:
        connection.close()
    return results


def snapshot_overview(path: Path = DEFAULT_DB) -> dict[str, object]:
    if not path.is_file():
        return {"ok": True, "membership": None, "yields": [], "factor_returns": [], "runs": []}
    connection = connect(path)
    try:
        membership = connection.execute(
            """SELECT snapshot_date, COUNT(*) AS constituents, MAX(retrieved_at) AS retrieved_at
               FROM index_membership_snapshots WHERE index_name = 'NIFTY 500'
               GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 1"""
        ).fetchone()
        markets = ["India", "United States", "Germany", "United Kingdom", "Japan"]
        yields: list[dict[str, object]] = []
        for market in markets:
            observed = connection.execute(
                "SELECT MAX(observation_date) FROM sovereign_yield_observations WHERE market = ?",
                (market,),
            ).fetchone()[0]
            values = [] if not observed else connection.execute(
                """SELECT tenor_years, yield_pct, curve_type, source_url, retrieved_at
                   FROM sovereign_yield_observations WHERE market = ? AND observation_date = ? ORDER BY tenor_years""",
                (market, observed),
            ).fetchall()
            tenor_context: dict[str, dict[str, object]] = {}
            for row in values:
                cutoff = (_filing_date(observed) - timedelta(days=366 * 5)).isoformat()
                history = [float(item[0]) for item in connection.execute(
                    """SELECT yield_pct FROM sovereign_yield_observations
                       WHERE market = ? AND tenor_years = ? AND observation_date BETWEEN ? AND ?
                       ORDER BY observation_date""",
                    (market, row["tenor_years"], cutoff, observed),
                )]
                current = float(row["yield_pct"])
                percentile = None if len(history) < 30 else 100.0 * sum(value <= current for value in history) / len(history)
                state = "limited" if percentile is None else ("high" if percentile >= 85 else ("elevated" if percentile >= 65 else "normal"))
                tenor_context[str(row["tenor_years"])] = {
                    "percentile": None if percentile is None else round(percentile, 1),
                    "state": state,
                    "observations": len(history),
                    "window_years": 5,
                }
            yields.append({
                "market": market,
                "observation_date": observed,
                "tenors": {str(row["tenor_years"]): row["yield_pct"] for row in values},
                "tenor_context": tenor_context,
                "curve_type": values[0]["curve_type"] if values else None,
                "source_url": values[0]["source_url"] if values else None,
                "retrieved_at": values[0]["retrieved_at"] if values else None,
            })
        latest_factor_date = connection.execute("SELECT MAX(session_date) FROM equity_factor_returns").fetchone()[0]
        factors = [] if not latest_factor_date else [dict(row) for row in connection.execute(
            "SELECT size_bucket, style_bucket, return_pct, constituents_count FROM equity_factor_returns WHERE session_date = ?",
            (latest_factor_date,),
        )]
        runs = [dict(row) for row in connection.execute(
            """SELECT source_key, completed_at, status, records_written, detail FROM snapshot_runs
               WHERE run_id IN (SELECT MAX(run_id) FROM snapshot_runs GROUP BY source_key) ORDER BY source_key"""
        )]
        latest_size_date = connection.execute("SELECT MAX(snapshot_date) FROM equity_size_classifications").fetchone()[0]
        size_rows = [] if not latest_size_date else [dict(row) for row in connection.execute(
            """SELECT size_bucket, COUNT(DISTINCT symbol) AS constituents
               FROM equity_size_classifications WHERE snapshot_date = ? GROUP BY size_bucket ORDER BY size_bucket""",
            (latest_size_date,),
        )]
        filing_row = connection.execute(
            """SELECT COUNT(*) AS filings, COUNT(DISTINCT symbol) AS symbols, MAX(retrieved_at) AS retrieved_at
               FROM nse_integrated_financial_filings"""
        ).fetchone()
        fact_row = connection.execute(
            """SELECT COUNT(*) AS facts, COUNT(DISTINCT symbol) AS symbols, MAX(retrieved_at) AS retrieved_at
               FROM nse_financial_facts"""
        ).fetchone()
        parse_row = connection.execute(
            """SELECT SUM(CASE WHEN status = 'successful' THEN 1 ELSE 0 END) AS successful,
                      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
               FROM nse_filing_parse_audits"""
        ).fetchone()
        price_row = connection.execute(
            """SELECT session_date, COUNT(*) AS observations, MAX(retrieved_at) AS retrieved_at
               FROM equity_price_observations GROUP BY session_date ORDER BY session_date DESC LIMIT 1"""
        ).fetchone()
        return {
            "ok": True,
            "membership": dict(membership) if membership else None,
            "yields": yields,
            "factor_as_of": latest_factor_date,
            "factor_returns": factors,
            "size_classification": {"snapshot_date": latest_size_date, "buckets": size_rows},
            "filing_intake": dict(filing_row),
            "fundamental_facts": {**dict(fact_row), **dict(parse_row)},
            "price_snapshot": dict(price_row) if price_row else None,
            "factor_methodology": FACTOR_METHODOLOGY_VERSION,
            "runs": runs,
        }
    finally:
        connection.close()


def factor_constituents(
    path: Path = DEFAULT_DB,
    size_bucket: str = "",
    style_bucket: str = "",
) -> dict[str, object]:
    """Return the latest admitted stocks behind one published factor cell."""
    if size_bucket not in {"Large", "Mid", "Small"}:
        raise ValueError("invalid_size_bucket")
    if style_bucket not in {"Value", "Core", "Growth"}:
        raise ValueError("invalid_style_bucket")

    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        snapshot_date = connection.execute(
            "SELECT MAX(snapshot_date) FROM equity_factor_classifications"
        ).fetchone()[0]
        if not snapshot_date:
            return {
                "ok": True,
                "snapshot_date": None,
                "size_bucket": size_bucket,
                "style_bucket": style_bucket,
                "methodology_version": FACTOR_METHODOLOGY_VERSION,
                "rows": [],
            }
        membership_date = connection.execute(
            "SELECT MAX(snapshot_date) FROM index_membership_snapshots WHERE index_name = 'NIFTY 500'"
        ).fetchone()[0]
        rows = connection.execute(
            """SELECT c.symbol, m.company_name, m.industry, m.isin,
                      p.close_price, p.return_pct, p.retrieved_at,
                      c.size_bucket, c.style_bucket, c.value_score, c.growth_score,
                      c.source_detail
               FROM equity_factor_classifications c
               JOIN equity_price_observations p
                 ON p.session_date = c.snapshot_date AND p.symbol = c.symbol
               LEFT JOIN index_membership_snapshots m
                 ON m.index_name = 'NIFTY 500' AND m.snapshot_date = ? AND m.symbol = c.symbol
               WHERE c.snapshot_date = ? AND c.size_bucket = ? AND c.style_bucket = ?
               ORDER BY p.return_pct DESC, c.symbol""",
            (membership_date, snapshot_date, size_bucket, style_bucket),
        ).fetchall()
        result = []
        for row in rows:
            detail = json.loads(row["source_detail"] or "{}")
            result.append({
                "symbol": row["symbol"],
                "company_name": row["company_name"] or row["symbol"],
                "industry": row["industry"] or "Unclassified",
                "isin": row["isin"],
                "close_price": row["close_price"],
                "return_pct": row["return_pct"],
                "size_bucket": row["size_bucket"],
                "style_bucket": row["style_bucket"],
                "value_score": row["value_score"],
                "growth_score": row["growth_score"],
                "current_period": detail.get("current_period"),
                "prior_period": detail.get("prior_period"),
                "growth_components": detail.get("growth_components", {}),
                "style_signal": detail.get("style_signal"),
                "evidence_timestamp": row["retrieved_at"],
            })
        return {
            "ok": True,
            "snapshot_date": snapshot_date,
            "membership_date": membership_date,
            "size_bucket": size_bucket,
            "style_bucket": style_bucket,
            "methodology_version": FACTOR_METHODOLOGY_VERSION,
            "rows": result,
        }
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Update or inspect Workspace 2.0 local snapshots")
    parser.add_argument("command", choices=("update", "status"))
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    args = parser.parse_args()
    result = update_all(args.db) if args.command == "update" else snapshot_overview(args.db)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
