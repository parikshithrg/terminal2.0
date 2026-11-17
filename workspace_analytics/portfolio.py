"""Local-only long-term portfolio ledger and completed-session valuation."""

from __future__ import annotations

import csv
import hashlib
import io
import math
import sqlite3
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from workspace_analytics.sqlite_store import connect_database


PORTFOLIO_CONTRACT = "portfolio-overview-v1"
PORTFOLIO_PERFORMANCE_CONTRACT = "portfolio-performance-v1"
PORTFOLIO_RISK_CONTRACT = "portfolio-risk-analysis-v1"
CASH_FLOW_TYPES = {"CONTRIBUTION", "WITHDRAWAL", "DIVIDEND", "INTEREST", "TAX", "FEE"}


def ensure_portfolio_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """CREATE TABLE IF NOT EXISTS portfolio_transactions (
               transaction_id TEXT PRIMARY KEY,
               trade_date TEXT NOT NULL,
               symbol TEXT NOT NULL,
               side TEXT NOT NULL CHECK(side IN ('BUY', 'SELL')),
               quantity REAL NOT NULL CHECK(quantity > 0),
               price_inr REAL NOT NULL CHECK(price_inr > 0),
               fees_inr REAL NOT NULL DEFAULT 0 CHECK(fees_inr >= 0),
               notes TEXT NOT NULL DEFAULT '',
               created_at TEXT NOT NULL,
               schema_version INTEGER NOT NULL DEFAULT 1
           )"""
    )
    connection.execute(
        """CREATE INDEX IF NOT EXISTS portfolio_transactions_symbol_date_idx
               ON portfolio_transactions(symbol, trade_date, created_at)"""
    )
    connection.execute(
        """CREATE INDEX IF NOT EXISTS portfolio_transactions_date_idx
               ON portfolio_transactions(trade_date, created_at)"""
    )
    connection.execute(
        """CREATE TABLE IF NOT EXISTS portfolio_cash_flows (
               cash_flow_id TEXT PRIMARY KEY,
               flow_date TEXT NOT NULL,
               flow_type TEXT NOT NULL CHECK(flow_type IN (
                   'CONTRIBUTION','WITHDRAWAL','DIVIDEND','INTEREST','TAX','FEE'
               )),
               amount_inr REAL NOT NULL CHECK(amount_inr > 0),
               notes TEXT NOT NULL DEFAULT '',
               created_at TEXT NOT NULL,
               schema_version INTEGER NOT NULL DEFAULT 1
           )"""
    )
    connection.execute(
        """CREATE INDEX IF NOT EXISTS portfolio_cash_flows_date_idx
               ON portfolio_cash_flows(flow_date, created_at)"""
    )
    connection.execute(
        """CREATE TABLE IF NOT EXISTS portfolio_imports (
               import_id TEXT PRIMARY KEY,
               file_name TEXT NOT NULL,
               content_sha256 TEXT NOT NULL UNIQUE,
               import_format TEXT NOT NULL,
               records_imported INTEGER NOT NULL,
               imported_at TEXT NOT NULL,
               schema_version INTEGER NOT NULL DEFAULT 1
           )"""
    )


def _finite_positive(value: Any, reason: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(reason) from error
    if not math.isfinite(number) or number <= 0:
        raise ValueError(reason)
    return number


def _finite_non_negative(value: Any, reason: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(reason) from error
    if not math.isfinite(number) or number < 0:
        raise ValueError(reason)
    return number


def _calculate_ledger(rows: list[sqlite3.Row | dict[str, Any]]) -> dict[str, dict[str, float]]:
    positions: dict[str, dict[str, float]] = {}
    for source in rows:
        row = dict(source)
        symbol = str(row["symbol"])
        position = positions.setdefault(symbol, {
            "quantity": 0.0,
            "cost_basis_inr": 0.0,
            "realised_pnl_inr": 0.0,
            "buy_turnover_inr": 0.0,
            "sell_turnover_inr": 0.0,
        })
        quantity = float(row["quantity"])
        price = float(row["price_inr"])
        fees = float(row["fees_inr"])
        if row["side"] == "BUY":
            position["quantity"] += quantity
            position["cost_basis_inr"] += quantity * price + fees
            position["buy_turnover_inr"] += quantity * price
            continue
        if quantity > position["quantity"] + 1e-9:
            raise ValueError("sell_quantity_exceeds_available_holding")
        average_cost = (
            position["cost_basis_inr"] / position["quantity"]
            if position["quantity"] else 0.0
        )
        removed_cost = average_cost * quantity
        position["realised_pnl_inr"] += quantity * price - fees - removed_cost
        position["sell_turnover_inr"] += quantity * price
        position["quantity"] -= quantity
        position["cost_basis_inr"] -= removed_cost
        if abs(position["quantity"]) < 1e-9:
            position["quantity"] = 0.0
            position["cost_basis_inr"] = 0.0
    return positions


def _transaction_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """SELECT transaction_id,trade_date,symbol,side,quantity,price_inr,
                  fees_inr,notes,created_at
           FROM portfolio_transactions
           ORDER BY trade_date,created_at,transaction_id"""
    ).fetchall()


def _cash_flow_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """SELECT cash_flow_id,flow_date,flow_type,amount_inr,notes,created_at
           FROM portfolio_cash_flows ORDER BY flow_date,created_at,cash_flow_id"""
    ).fetchall()


def _parse_trade_date(value: Any) -> str:
    text = str(value or "").strip()
    candidates = [text, text[:10]]
    for candidate in candidates:
        try:
            parsed = date.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed > date.today():
            raise ValueError("portfolio_trade_date_in_future")
        return parsed.isoformat()
    for pattern in ("%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(text[:11], pattern).date()
        except ValueError:
            continue
        if parsed > date.today():
            raise ValueError("portfolio_trade_date_in_future")
        return parsed.isoformat()
    raise ValueError("invalid_portfolio_trade_date")


def _normalise_header(value: str) -> str:
    return "_".join(str(value or "").strip().lower().replace("/", " ").split())


def _first_value(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    for alias in aliases:
        value = row.get(alias)
        if value not in (None, ""):
            return value
    return None


def record_portfolio_transaction(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    symbol = str(payload.get("symbol") or "").strip().upper()
    if not symbol or len(symbol) > 40 or not all(character.isalnum() or character in {"-", "&"} for character in symbol):
        raise ValueError("invalid_portfolio_symbol")
    side = str(payload.get("side") or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError("invalid_portfolio_side")
    quantity = _finite_positive(payload.get("quantity"), "invalid_portfolio_quantity")
    price = _finite_positive(payload.get("price_inr"), "invalid_portfolio_price")
    fees = _finite_non_negative(payload.get("fees_inr", 0), "invalid_portfolio_fees")
    trade_date = _parse_trade_date(payload.get("trade_date"))
    notes = str(payload.get("notes") or "").strip()
    if len(notes) > 500:
        raise ValueError("portfolio_notes_too_long")

    connection = connect_database(path)
    try:
        ensure_portfolio_schema(connection)
        instrument = connection.execute(
            "SELECT display_name FROM instruments WHERE instrument_id=? AND kind='stock'",
            (f"stock:{symbol}",),
        ).fetchone()
        if instrument is None:
            raise ValueError("portfolio_symbol_not_in_workspace_store")
        transaction_id = uuid.uuid4().hex
        created_at = datetime.now(timezone.utc).isoformat()
        connection.execute(
            """INSERT INTO portfolio_transactions(
                   transaction_id,trade_date,symbol,side,quantity,price_inr,
                   fees_inr,notes,created_at,schema_version)
               VALUES (?,?,?,?,?,?,?,?,?,1)""",
            (transaction_id, trade_date, symbol, side, quantity, price, fees, notes, created_at),
        )
        _calculate_ledger(_transaction_rows(connection))
        connection.commit()
        return {
            "ok": True,
            "contract_version": "portfolio-transaction-v1",
            "transaction_id": transaction_id,
            "symbol": symbol,
            "display_name": instrument["display_name"],
            "side": side,
            "trade_date": trade_date,
            "quantity": quantity,
            "price_inr": price,
            "fees_inr": fees,
            "local_only": True,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def record_portfolio_cash_flow(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    flow_type = str(payload.get("flow_type") or "").strip().upper()
    if flow_type not in CASH_FLOW_TYPES:
        raise ValueError("invalid_portfolio_cash_flow_type")
    amount = _finite_positive(payload.get("amount_inr"), "invalid_portfolio_cash_flow_amount")
    flow_date = _parse_trade_date(payload.get("flow_date"))
    notes = str(payload.get("notes") or "").strip()
    if len(notes) > 500:
        raise ValueError("portfolio_notes_too_long")
    connection = connect_database(path)
    try:
        ensure_portfolio_schema(connection)
        cash_flow_id = uuid.uuid4().hex
        created_at = datetime.now(timezone.utc).isoformat()
        connection.execute(
            """INSERT INTO portfolio_cash_flows(
                   cash_flow_id,flow_date,flow_type,amount_inr,notes,created_at,schema_version)
               VALUES (?,?,?,?,?,?,1)""",
            (cash_flow_id, flow_date, flow_type, amount, notes, created_at),
        )
        connection.commit()
        return {
            "ok": True,
            "contract_version": "portfolio-cash-flow-v1",
            "cash_flow_id": cash_flow_id,
            "flow_date": flow_date,
            "flow_type": flow_type,
            "amount_inr": amount,
            "local_only": True,
        }
    finally:
        connection.close()


def import_portfolio_tradebook(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    csv_text = str(payload.get("csv_text") or "")
    if not csv_text.strip():
        raise ValueError("portfolio_import_empty")
    if len(csv_text.encode("utf-8")) > 1_500_000:
        raise ValueError("portfolio_import_too_large")
    file_name = str(payload.get("file_name") or "tradebook.csv").strip()[:240] or "tradebook.csv"
    digest = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()
    reader = csv.DictReader(io.StringIO(csv_text.lstrip("\ufeff")))
    if not reader.fieldnames:
        raise ValueError("portfolio_import_headers_missing")
    raw_rows = []
    for index, source in enumerate(reader, start=2):
        if index > 10_001:
            raise ValueError("portfolio_import_row_limit_exceeded")
        row = {_normalise_header(key): value.strip() if isinstance(value, str) else value for key, value in source.items() if key}
        if not any(value not in (None, "") for value in row.values()):
            continue
        try:
            symbol = str(_first_value(row, ("tradingsymbol", "trading_symbol", "symbol", "scrip")) or "").strip().upper()
            symbol = symbol.split(":")[-1]
            side = str(_first_value(row, ("trade_type", "transaction_type", "side", "buy_sell")) or "").strip().upper()
            trade_date = _parse_trade_date(_first_value(row, ("trade_date", "trade_date_time", "order_execution_time", "date")))
            quantity = _finite_positive(_first_value(row, ("quantity", "qty", "traded_quantity")), "invalid_portfolio_quantity")
            price = _finite_positive(_first_value(row, ("price", "trade_price", "average_price", "avg_price")), "invalid_portfolio_price")
            fees_value = _first_value(row, ("fees", "charges", "fee", "brokerage"))
            fees = _finite_non_negative(fees_value or 0, "invalid_portfolio_fees")
            if side not in {"BUY", "SELL"}:
                raise ValueError("invalid_portfolio_side")
            if not symbol:
                raise ValueError("invalid_portfolio_symbol")
        except ValueError as error:
            raise ValueError(f"portfolio_import_row_{index}_{error}") from error
        raw_rows.append({
            "row_number": index, "trade_date": trade_date, "symbol": symbol,
            "side": side, "quantity": quantity, "price_inr": price, "fees_inr": fees,
        })
    if not raw_rows:
        raise ValueError("portfolio_import_no_trade_rows")

    connection = connect_database(path)
    try:
        ensure_portfolio_schema(connection)
        existing = connection.execute(
            "SELECT import_id,records_imported,imported_at FROM portfolio_imports WHERE content_sha256=?",
            (digest,),
        ).fetchone()
        if existing:
            return {
                "ok": True,
                "contract_version": "portfolio-tradebook-import-v1",
                "status": "already_imported",
                "duplicate": True,
                "import_id": existing["import_id"],
                "records_imported": existing["records_imported"],
                "imported_at": existing["imported_at"],
                "local_only": True,
            }
        known = {
            str(row[0])[6:] for row in connection.execute(
                "SELECT instrument_id FROM instruments WHERE kind='stock'"
            )
        }
        unknown = sorted({row["symbol"] for row in raw_rows if row["symbol"] not in known})
        if unknown:
            raise ValueError(f"portfolio_import_unknown_symbols:{','.join(unknown[:20])}")
        import_id = uuid.uuid4().hex
        imported_at = datetime.now(timezone.utc).isoformat()
        for sequence, row in enumerate(raw_rows):
            created_at = f"{imported_at}:{sequence:05d}"
            transaction_id = hashlib.sha256(
                f"{digest}|{row['row_number']}".encode("utf-8")
            ).hexdigest()
            connection.execute(
                """INSERT INTO portfolio_transactions(
                       transaction_id,trade_date,symbol,side,quantity,price_inr,
                       fees_inr,notes,created_at,schema_version)
                   VALUES (?,?,?,?,?,?,?,?,?,1)""",
                (
                    transaction_id, row["trade_date"], row["symbol"], row["side"],
                    row["quantity"], row["price_inr"], row["fees_inr"],
                    f"Imported from {file_name} row {row['row_number']}", created_at,
                ),
            )
        _calculate_ledger(_transaction_rows(connection))
        connection.execute(
            """INSERT INTO portfolio_imports(
                   import_id,file_name,content_sha256,import_format,records_imported,
                   imported_at,schema_version) VALUES (?,?,?,?,?,?,1)""",
            (import_id, file_name, digest, "zerodha_or_generic_equity_tradebook_csv", len(raw_rows), imported_at),
        )
        connection.commit()
        return {
            "ok": True,
            "contract_version": "portfolio-tradebook-import-v1",
            "status": "imported",
            "duplicate": False,
            "import_id": import_id,
            "records_imported": len(raw_rows),
            "file_name": file_name,
            "local_only": True,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def portfolio_overview(path: Path) -> dict[str, Any]:
    connection = connect_database(path)
    try:
        ensure_portfolio_schema(connection)
        transactions = _transaction_rows(connection)
        cash_flows = _cash_flow_rows(connection)
        ledger = _calculate_ledger(transactions)
        names = {
            str(row["instrument_id"])[6:]: str(row["display_name"])
            for row in connection.execute(
                "SELECT instrument_id,display_name FROM instruments WHERE kind='stock'"
            )
        }
        holdings = []
        for symbol, position in ledger.items():
            if position["quantity"] <= 0:
                continue
            price_row = connection.execute(
                """SELECT session_date,close FROM candles
                   WHERE instrument_id=? ORDER BY session_date DESC LIMIT 1""",
                (f"stock:{symbol}",),
            ).fetchone()
            market_price = float(price_row["close"]) if price_row else None
            market_value = position["quantity"] * market_price if market_price is not None else None
            unrealised = market_value - position["cost_basis_inr"] if market_value is not None else None
            holdings.append({
                "symbol": symbol,
                "display_name": names.get(symbol, symbol),
                "quantity": round(position["quantity"], 6),
                "average_cost_inr": round(position["cost_basis_inr"] / position["quantity"], 4),
                "cost_basis_inr": round(position["cost_basis_inr"], 2),
                "market_price_inr": round(market_price, 2) if market_price is not None else None,
                "market_value_inr": round(market_value, 2) if market_value is not None else None,
                "unrealised_pnl_inr": round(unrealised, 2) if unrealised is not None else None,
                "unrealised_return_pct": (
                    round(100 * unrealised / position["cost_basis_inr"], 2)
                    if unrealised is not None and position["cost_basis_inr"] else None
                ),
                "realised_pnl_inr": round(position["realised_pnl_inr"], 2),
                "price_as_of_date": str(price_row["session_date"]) if price_row else None,
                "allocation_pct": None,
            })
        total_market_value = sum(
            float(row["market_value_inr"]) for row in holdings
            if row["market_value_inr"] is not None
        )
        for row in holdings:
            if row["market_value_inr"] is not None and total_market_value:
                row["allocation_pct"] = round(100 * float(row["market_value_inr"]) / total_market_value, 2)
        holdings.sort(key=lambda row: (-(row["market_value_inr"] or 0), row["symbol"]))
        total_cost = sum(float(row["cost_basis_inr"]) for row in holdings)
        unrealised_total = sum(
            float(row["unrealised_pnl_inr"]) for row in holdings
            if row["unrealised_pnl_inr"] is not None
        )
        realised_total = sum(float(position["realised_pnl_inr"]) for position in ledger.values())
        contribution_total = sum(float(row["amount_inr"]) for row in cash_flows if row["flow_type"] == "CONTRIBUTION")
        withdrawal_total = sum(float(row["amount_inr"]) for row in cash_flows if row["flow_type"] == "WITHDRAWAL")
        income_total = sum(float(row["amount_inr"]) for row in cash_flows if row["flow_type"] in {"DIVIDEND", "INTEREST"})
        cash_expense_total = sum(float(row["amount_inr"]) for row in cash_flows if row["flow_type"] in {"TAX", "FEE"})
        trade_cash = sum(
            (float(row["quantity"]) * float(row["price_inr"]) - float(row["fees_inr"]))
            if row["side"] == "SELL"
            else -(float(row["quantity"]) * float(row["price_inr"]) + float(row["fees_inr"]))
            for row in transactions
        )
        cash_balance = contribution_total - withdrawal_total + income_total - cash_expense_total + trade_cash
        dates = [row["price_as_of_date"] for row in holdings if row["price_as_of_date"]]
        imports = connection.execute(
            """SELECT import_id,file_name,import_format,records_imported,imported_at
               FROM portfolio_imports ORDER BY imported_at DESC LIMIT 10"""
        ).fetchall()
        issues = []
        if transactions and not cash_flows:
            issues.append("cash_flow_history_missing")
        if holdings and any(row["market_price_inr"] is None for row in holdings):
            issues.append("holding_price_unavailable")
        if cash_flows and cash_balance < -0.01:
            issues.append("negative_reconciled_cash")
        if not transactions and not cash_flows:
            reconciliation_status = "empty"
        elif issues:
            reconciliation_status = "review_required"
        else:
            reconciliation_status = "reconciled"
        connection.commit()
        return {
            "ok": True,
            "contract_version": PORTFOLIO_CONTRACT,
            "as_of_date": max(dates, default=None),
            "summary": {
                "positions": len(holdings),
                "priced_positions": sum(row["market_price_inr"] is not None for row in holdings),
                "cost_basis_inr": round(total_cost, 2),
                "market_value_inr": round(total_market_value, 2),
                "unrealised_pnl_inr": round(unrealised_total, 2),
                "unrealised_return_pct": round(100 * unrealised_total / total_cost, 2) if total_cost else None,
                "realised_pnl_inr": round(realised_total, 2),
                "transaction_count": len(transactions),
                "cash_balance_inr": round(cash_balance, 2),
                "cash_flow_count": len(cash_flows),
            },
            "holdings": holdings,
            "recent_transactions": [dict(row) for row in reversed(transactions[-20:])],
            "recent_cash_flows": [dict(row) for row in reversed(cash_flows[-20:])],
            "recent_imports": [dict(row) for row in imports],
            "reconciliation": {
                "status": reconciliation_status,
                "issues": issues,
                "cash_history_available": bool(cash_flows),
                "cash_balance_inr": round(cash_balance, 2),
                "contributions_inr": round(contribution_total, 2),
                "withdrawals_inr": round(withdrawal_total, 2),
                "income_inr": round(income_total, 2),
                "cash_expenses_inr": round(cash_expense_total, 2),
                "priced_positions": sum(row["market_price_inr"] is not None for row in holdings),
                "positions": len(holdings),
                "import_count": len(imports),
            },
            "methodology": {
                "cost_basis": "moving_weighted_average_including_recorded_fees",
                "valuation": "latest_stored_completed_session_close",
                "performance_status": "point_in_time_valuation_foundation_only",
                "cash_flows_included": bool(cash_flows),
                "benchmark_included": False,
            },
            "local_only": True,
            "trading_research_separated": True,
            "orders_enabled": False,
        }
    finally:
        connection.close()


def _xirr(cash_flows: list[tuple[date, float]]) -> float | None:
    """Return the annual money-weighted rate for irregular dated cash flows."""
    if len(cash_flows) < 2 or not any(value < 0 for _, value in cash_flows) or not any(value > 0 for _, value in cash_flows):
        return None
    origin = min(flow_date for flow_date, _ in cash_flows)

    def net_present_value(rate: float) -> float:
        return sum(
            value / ((1.0 + rate) ** ((flow_date - origin).days / 365.0))
            for flow_date, value in cash_flows
        )

    lower = -0.999999
    upper = 1.0
    lower_value = net_present_value(lower)
    upper_value = net_present_value(upper)
    while lower_value * upper_value > 0 and upper < 1_000_000:
        upper *= 2.0
        upper_value = net_present_value(upper)
    if lower_value * upper_value > 0:
        return None
    for _ in range(160):
        midpoint = (lower + upper) / 2.0
        midpoint_value = net_present_value(midpoint)
        if abs(midpoint_value) < 1e-8:
            return midpoint
        if lower_value * midpoint_value <= 0:
            upper = midpoint
        else:
            lower = midpoint
            lower_value = midpoint_value
    return (lower + upper) / 2.0


def _performance_gate(reasons: list[str], benchmark_instrument_id: str) -> dict[str, Any]:
    return {
        "ok": True,
        "contract_version": PORTFOLIO_PERFORMANCE_CONTRACT,
        "status": "gated",
        "gates": sorted(set(reasons)),
        "from_date": None,
        "through_date": None,
        "observations": 0,
        "portfolio": {},
        "benchmark": {"instrument_id": benchmark_instrument_id, "label": benchmark_instrument_id.split(":", 1)[-1]},
        "comparison": {},
        "methodology": {
            "time_weighted_return": "daily_chain_linked_return_net_of_contributions_and_withdrawals",
            "money_weighted_return": "dated_xirr_with_terminal_portfolio_value",
            "benchmark": "same_external_cash_flows_invested_at_stored_completed_session_closes",
            "cash_income_and_costs": "dividends_and_interest_are_return; taxes_fees_and_trade_costs_reduce_return",
        },
        "local_only": True,
        "trading_research_separated": True,
        "orders_enabled": False,
    }


def portfolio_performance(
    path: Path, benchmark_instrument_id: str = "index:Nifty 50"
) -> dict[str, Any]:
    """Calculate cash-flow-neutral portfolio and benchmark performance.

    Events dated between exchange sessions are applied to the first stored
    benchmark session on or after their date. This keeps valuation and flow
    timing on the same completed-session calendar.
    """
    connection = connect_database(path)
    try:
        ensure_portfolio_schema(connection)
        transactions = [dict(row) for row in _transaction_rows(connection)]
        cash_flows = [dict(row) for row in _cash_flow_rows(connection)]
        gates: list[str] = []
        if not transactions:
            gates.append("transaction_history_missing")
        external_flows = [row for row in cash_flows if row["flow_type"] in {"CONTRIBUTION", "WITHDRAWAL"}]
        if not external_flows:
            gates.append("external_cash_flow_history_missing")
        if transactions and not cash_flows:
            gates.append("cash_flow_history_missing")
        if gates:
            return _performance_gate(gates, benchmark_instrument_id)

        event_dates = [str(row["trade_date"]) for row in transactions] + [str(row["flow_date"]) for row in cash_flows]
        first_event_date = min(event_dates)
        benchmark_rows = connection.execute(
            """SELECT session_date,close FROM candles
               WHERE instrument_id=? AND session_date>=?
               ORDER BY session_date""",
            (benchmark_instrument_id, first_event_date),
        ).fetchall()
        if not benchmark_rows:
            return _performance_gate(["benchmark_history_missing"], benchmark_instrument_id)

        sessions = [str(row["session_date"]) for row in benchmark_rows]
        benchmark_closes = {str(row["session_date"]): float(row["close"]) for row in benchmark_rows}
        last_session = sessions[-1]
        if max(event_dates) > last_session:
            return _performance_gate(["events_after_latest_benchmark_session"], benchmark_instrument_id)

        symbols = sorted({str(row["symbol"]) for row in transactions})
        price_history: dict[str, dict[str, float]] = {}
        for symbol in symbols:
            rows = connection.execute(
                """SELECT session_date,close FROM candles
                   WHERE instrument_id=? AND session_date>=?
                   ORDER BY session_date""",
                (f"stock:{symbol}", first_event_date),
            ).fetchall()
            price_history[symbol] = {str(row["session_date"]): float(row["close"]) for row in rows}

        transaction_events: dict[str, list[dict[str, Any]]] = {}
        cash_events: dict[str, list[dict[str, Any]]] = {}
        for row in transactions:
            session = next((value for value in sessions if value >= str(row["trade_date"])), None)
            if session is None:
                gates.append("transaction_outside_valuation_calendar")
            else:
                transaction_events.setdefault(session, []).append(row)
        for row in cash_flows:
            session = next((value for value in sessions if value >= str(row["flow_date"])), None)
            if session is None:
                gates.append("cash_flow_outside_valuation_calendar")
            else:
                cash_events.setdefault(session, []).append(row)
        if gates:
            return _performance_gate(gates, benchmark_instrument_id)

        positions = {symbol: 0.0 for symbol in symbols}
        latest_prices: dict[str, float] = {}
        cash_balance = 0.0
        benchmark_units = 0.0
        previous_portfolio_value: float | None = None
        previous_benchmark_value: float | None = None
        portfolio_growth = 1.0
        benchmark_growth = 1.0
        observations = 0
        valuation_start: str | None = None
        performance_series: list[dict[str, Any]] = []
        investor_flows: list[tuple[date, float]] = []
        benchmark_investor_flows: list[tuple[date, float]] = []

        for session in sessions:
            for symbol in symbols:
                if session in price_history[symbol]:
                    latest_prices[symbol] = price_history[symbol][session]
            external_flow = 0.0
            for flow in cash_events.get(session, []):
                amount = float(flow["amount_inr"])
                flow_type = str(flow["flow_type"])
                if flow_type == "CONTRIBUTION":
                    cash_balance += amount
                    external_flow += amount
                    investor_flows.append((date.fromisoformat(str(flow["flow_date"])), -amount))
                    benchmark_investor_flows.append((date.fromisoformat(str(flow["flow_date"])), -amount))
                    benchmark_units += amount / benchmark_closes[session]
                elif flow_type == "WITHDRAWAL":
                    cash_balance -= amount
                    external_flow -= amount
                    investor_flows.append((date.fromisoformat(str(flow["flow_date"])), amount))
                    benchmark_investor_flows.append((date.fromisoformat(str(flow["flow_date"])), amount))
                    benchmark_units -= amount / benchmark_closes[session]
                elif flow_type in {"DIVIDEND", "INTEREST"}:
                    cash_balance += amount
                else:
                    cash_balance -= amount
            for trade in transaction_events.get(session, []):
                symbol = str(trade["symbol"])
                quantity = float(trade["quantity"])
                value = quantity * float(trade["price_inr"])
                fees = float(trade["fees_inr"])
                if trade["side"] == "BUY":
                    positions[symbol] += quantity
                    cash_balance -= value + fees
                else:
                    positions[symbol] -= quantity
                    cash_balance += value - fees
            missing = [symbol for symbol, quantity in positions.items() if quantity > 1e-9 and symbol not in latest_prices]
            if missing:
                return _performance_gate([f"historical_price_missing:{symbol}" for symbol in missing], benchmark_instrument_id)
            portfolio_value = cash_balance + sum(
                quantity * latest_prices[symbol] for symbol, quantity in positions.items() if quantity > 1e-9
            )
            benchmark_value = benchmark_units * benchmark_closes[session]
            if previous_portfolio_value is None:
                if external_flow <= 0:
                    continue
                valuation_start = session
            else:
                if previous_portfolio_value <= 0 or previous_benchmark_value is None or previous_benchmark_value <= 0:
                    return _performance_gate(["non_positive_prior_valuation"], benchmark_instrument_id)
                portfolio_growth *= (portfolio_value - external_flow) / previous_portfolio_value
                benchmark_growth *= (benchmark_value - external_flow) / previous_benchmark_value
                observations += 1
            performance_series.append({
                "session_date": session,
                "portfolio_value_inr": round(portfolio_value, 2),
                "portfolio_index": round(portfolio_growth * 100.0, 6),
                "benchmark_index": round(benchmark_growth * 100.0, 6),
            })
            previous_portfolio_value = portfolio_value
            previous_benchmark_value = benchmark_value

        if previous_portfolio_value is None or valuation_start is None:
            return _performance_gate(["positive_initial_contribution_missing"], benchmark_instrument_id)
        if previous_portfolio_value < -0.01:
            return _performance_gate(["negative_terminal_portfolio_value"], benchmark_instrument_id)

        terminal_date = date.fromisoformat(last_session)
        investor_flows.append((terminal_date, previous_portfolio_value))
        benchmark_investor_flows.append((terminal_date, previous_benchmark_value or 0.0))
        portfolio_xirr = _xirr(investor_flows)
        benchmark_xirr = _xirr(benchmark_investor_flows)
        elapsed_days = max((terminal_date - date.fromisoformat(valuation_start)).days, 0)
        portfolio_twr = portfolio_growth - 1.0
        benchmark_twr = benchmark_growth - 1.0
        annualised_twr = (portfolio_growth ** (365.0 / elapsed_days) - 1.0) if elapsed_days > 0 and portfolio_growth > 0 else None
        benchmark_annualised = (benchmark_growth ** (365.0 / elapsed_days) - 1.0) if elapsed_days > 0 and benchmark_growth > 0 else None

        def percentage(value: float | None) -> float | None:
            return round(value * 100.0, 4) if value is not None and math.isfinite(value) else None

        return {
            "ok": True,
            "contract_version": PORTFOLIO_PERFORMANCE_CONTRACT,
            "status": "ready",
            "gates": [],
            "from_date": valuation_start,
            "through_date": last_session,
            "observations": observations,
            "portfolio": {
                "terminal_value_inr": round(previous_portfolio_value, 2),
                "time_weighted_return_pct": percentage(portfolio_twr),
                "annualised_twr_pct": percentage(annualised_twr),
                "money_weighted_return_pct": percentage(portfolio_xirr),
            },
            "benchmark": {
                "instrument_id": benchmark_instrument_id,
                "label": benchmark_instrument_id.split(":", 1)[-1],
                "terminal_value_inr": round(previous_benchmark_value or 0.0, 2),
                "time_weighted_return_pct": percentage(benchmark_twr),
                "annualised_twr_pct": percentage(benchmark_annualised),
                "money_weighted_return_pct": percentage(benchmark_xirr),
            },
            "comparison": {
                "excess_twr_pct": percentage(portfolio_twr - benchmark_twr),
                "excess_money_weighted_return_pct": (
                    percentage(portfolio_xirr - benchmark_xirr)
                    if portfolio_xirr is not None and benchmark_xirr is not None else None
                ),
            },
            "series": performance_series,
            "methodology": {
                "time_weighted_return": "daily_chain_linked_return_net_of_contributions_and_withdrawals",
                "money_weighted_return": "dated_xirr_with_terminal_portfolio_value",
                "benchmark": "same_external_cash_flows_invested_at_stored_completed_session_closes",
                "event_mapping": "first_stored_benchmark_session_on_or_after_event_date",
                "cash_income_and_costs": "dividends_and_interest_are_return; taxes_fees_and_trade_costs_reduce_return",
            },
            "local_only": True,
            "trading_research_separated": True,
            "orders_enabled": False,
        }
    finally:
        connection.close()


def portfolio_risk_analysis(path: Path) -> dict[str, Any]:
    """Publish allocation, concentration, drawdown and P&L contribution evidence."""
    performance = portfolio_performance(path)
    overview = portfolio_overview(path)
    if performance.get("status") != "ready":
        return {
            "ok": True,
            "contract_version": PORTFOLIO_RISK_CONTRACT,
            "status": "gated",
            "gates": list(performance.get("gates") or []),
            "as_of_date": overview.get("as_of_date"),
            "allocation": [],
            "concentration": {},
            "drawdown": {},
            "contributions": [],
            "methodology": {
                "allocation": "latest_stored_completed_session_market_value_weights",
                "concentration": "position_weight_hhi_and_top_weight_shares",
                "drawdown": "peak_to_trough_on_cash_flow_neutral_twr_index",
                "contribution": "symbol_realised_plus_unrealised_pnl_over_net_external_capital",
            },
            "local_only": True,
            "trading_research_separated": True,
            "orders_enabled": False,
        }

    holdings = list(overview.get("holdings") or [])
    allocations = [
        {
            "symbol": row["symbol"],
            "display_name": row["display_name"],
            "market_value_inr": row["market_value_inr"],
            "allocation_pct": row["allocation_pct"],
        }
        for row in holdings
    ]
    weights = sorted((float(row["allocation_pct"] or 0.0) for row in holdings), reverse=True)
    hhi = sum((weight / 100.0) ** 2 for weight in weights)
    largest = weights[0] if weights else 0.0
    top_three = sum(weights[:3])
    top_five = sum(weights[:5])
    if largest >= 25.0 or top_three >= 60.0 or hhi >= 0.18:
        concentration_state = "concentrated"
    elif largest >= 15.0 or top_three >= 45.0 or hhi >= 0.12:
        concentration_state = "moderate"
    else:
        concentration_state = "diversified"

    series = list(performance.get("series") or [])
    peak_value = -math.inf
    peak_date: str | None = None
    maximum_drawdown = 0.0
    maximum_peak_date: str | None = None
    trough_date: str | None = None
    current_drawdown = 0.0
    for row in series:
        index_value = float(row["portfolio_index"])
        if index_value > peak_value:
            peak_value = index_value
            peak_date = str(row["session_date"])
        drawdown = index_value / peak_value - 1.0 if peak_value > 0 else 0.0
        current_drawdown = drawdown
        if drawdown < maximum_drawdown:
            maximum_drawdown = drawdown
            maximum_peak_date = peak_date
            trough_date = str(row["session_date"])

    connection = connect_database(path)
    try:
        ensure_portfolio_schema(connection)
        transactions = _transaction_rows(connection)
        cash_flows = _cash_flow_rows(connection)
        ledger = _calculate_ledger(transactions)
        names = {
            str(row["instrument_id"])[6:]: str(row["display_name"])
            for row in connection.execute(
                "SELECT instrument_id,display_name FROM instruments WHERE kind='stock'"
            )
        }
    finally:
        connection.close()
    holding_by_symbol = {str(row["symbol"]): row for row in holdings}
    contributions = []
    contribution_total = sum(float(row["amount_inr"]) for row in cash_flows if row["flow_type"] == "CONTRIBUTION")
    withdrawal_total = sum(float(row["amount_inr"]) for row in cash_flows if row["flow_type"] == "WITHDRAWAL")
    net_external_capital = contribution_total - withdrawal_total
    for symbol, position in ledger.items():
        holding = holding_by_symbol.get(symbol)
        unrealised = float(holding["unrealised_pnl_inr"]) if holding and holding["unrealised_pnl_inr"] is not None else 0.0
        realised = float(position["realised_pnl_inr"])
        pnl = realised + unrealised
        contributions.append({
            "symbol": symbol,
            "display_name": names.get(symbol, symbol),
            "realised_pnl_inr": round(realised, 2),
            "unrealised_pnl_inr": round(unrealised, 2),
            "total_pnl_inr": round(pnl, 2),
            "return_contribution_pct": round(100.0 * pnl / net_external_capital, 4) if net_external_capital else None,
        })
    income = sum(float(row["amount_inr"]) for row in cash_flows if row["flow_type"] in {"DIVIDEND", "INTEREST"})
    expenses = sum(float(row["amount_inr"]) for row in cash_flows if row["flow_type"] in {"TAX", "FEE"})
    if income or expenses:
        net_cash_return = income - expenses
        contributions.append({
            "symbol": "CASH",
            "display_name": "Income, tax and cash fees",
            "realised_pnl_inr": round(net_cash_return, 2),
            "unrealised_pnl_inr": 0.0,
            "total_pnl_inr": round(net_cash_return, 2),
            "return_contribution_pct": round(100.0 * net_cash_return / net_external_capital, 4) if net_external_capital else None,
        })
    contributions.sort(key=lambda row: (-float(row["total_pnl_inr"]), row["symbol"]))

    return {
        "ok": True,
        "contract_version": PORTFOLIO_RISK_CONTRACT,
        "status": "ready",
        "gates": [],
        "as_of_date": performance.get("through_date"),
        "allocation": allocations,
        "concentration": {
            "state": concentration_state,
            "largest_position_pct": round(largest, 2),
            "top_three_pct": round(top_three, 2),
            "top_five_pct": round(top_five, 2),
            "hhi": round(hhi, 6),
            "effective_positions": round(1.0 / hhi, 2) if hhi else 0.0,
            "position_count": len(weights),
        },
        "drawdown": {
            "maximum_drawdown_pct": round(maximum_drawdown * 100.0, 4),
            "current_drawdown_pct": round(current_drawdown * 100.0, 4),
            "peak_date": maximum_peak_date,
            "trough_date": trough_date,
            "observations": len(series),
        },
        "contributions": contributions,
        "net_external_capital_inr": round(net_external_capital, 2),
        "methodology": {
            "allocation": "latest_stored_completed_session_market_value_weights",
            "concentration": "position_weight_hhi_and_top_weight_shares",
            "drawdown": "peak_to_trough_on_cash_flow_neutral_twr_index",
            "contribution": "symbol_realised_plus_unrealised_pnl_over_net_external_capital",
            "unallocated_income": "dividend_and_interest_records_are_cash_level_until_symbol_tagging_exists",
        },
        "local_only": True,
        "trading_research_separated": True,
        "orders_enabled": False,
    }
