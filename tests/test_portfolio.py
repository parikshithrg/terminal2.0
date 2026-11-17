import tempfile
import unittest
from datetime import date
from pathlib import Path

from workspace_analytics.portfolio import (
    import_portfolio_tradebook,
    portfolio_overview,
    portfolio_performance,
    portfolio_risk_analysis,
    record_portfolio_cash_flow,
    record_portfolio_transaction,
)
from workspace_analytics.sqlite_store import connect_database


class PortfolioFoundationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "portfolio.sqlite3"
        connection = connect_database(self.database)
        connection.executescript(
            """
            CREATE TABLE instruments(
                instrument_id TEXT PRIMARY KEY, kind TEXT, display_name TEXT
            );
            CREATE TABLE candles(
                instrument_id TEXT, session_date TEXT, close REAL,
                PRIMARY KEY(instrument_id, session_date)
            );
            INSERT INTO instruments VALUES ('stock:AAA','stock','Alpha Ltd');
            INSERT INTO instruments VALUES ('index:Nifty 50','index','Nifty 50');
            INSERT INTO candles VALUES ('stock:AAA','2026-11-13',120.0);
            INSERT INTO candles VALUES ('index:Nifty 50','2026-11-13',25000.0);
            """
        )
        connection.commit()
        connection.close()

    def tearDown(self) -> None:
        self.directory.cleanup()

    def add(self, side: str, quantity: float, price: float, fees: float = 0) -> None:
        record_portfolio_transaction(self.database, {
            "trade_date": min(date.today(), date(2026, 11, 13)).isoformat(),
            "symbol": "AAA",
            "side": side,
            "quantity": quantity,
            "price_inr": price,
            "fees_inr": fees,
        })

    def test_weighted_cost_valuation_and_realised_pnl(self) -> None:
        self.add("BUY", 10, 100, 10)
        self.add("BUY", 10, 110, 10)
        self.add("SELL", 5, 130, 5)
        result = portfolio_overview(self.database)
        holding = result["holdings"][0]
        self.assertEqual(result["contract_version"], "portfolio-overview-v1")
        self.assertEqual(holding["quantity"], 15.0)
        self.assertEqual(holding["average_cost_inr"], 106.0)
        self.assertEqual(holding["market_value_inr"], 1800.0)
        self.assertEqual(holding["unrealised_pnl_inr"], 210.0)
        self.assertEqual(holding["realised_pnl_inr"], 115.0)
        self.assertEqual(result["summary"]["transaction_count"], 3)
        self.assertFalse(result["orders_enabled"])
        self.assertTrue(result["trading_research_separated"])

    def test_oversell_is_rejected_and_rolled_back(self) -> None:
        self.add("BUY", 2, 100)
        with self.assertRaisesRegex(ValueError, "sell_quantity_exceeds_available_holding"):
            self.add("SELL", 3, 120)
        result = portfolio_overview(self.database)
        self.assertEqual(result["summary"]["transaction_count"], 1)
        self.assertEqual(result["holdings"][0]["quantity"], 2.0)

    def test_unknown_symbol_and_future_date_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "portfolio_symbol_not_in_workspace_store"):
            record_portfolio_transaction(self.database, {
                "trade_date": min(date.today(), date(2026, 11, 13)).isoformat(),
                "symbol": "MISSING", "side": "BUY", "quantity": 1, "price_inr": 100,
            })
        with self.assertRaisesRegex(ValueError, "portfolio_trade_date_in_future"):
            record_portfolio_transaction(self.database, {
                "trade_date": "2999-01-01", "symbol": "AAA", "side": "BUY",
                "quantity": 1, "price_inr": 100,
            })

    def test_cash_flows_reconcile_trade_cash_and_income(self) -> None:
        flow_date = min(date.today(), date(2026, 11, 13)).isoformat()
        record_portfolio_cash_flow(self.database, {
            "flow_date": flow_date, "flow_type": "CONTRIBUTION",
            "amount_inr": 2000, "notes": "Initial capital",
        })
        record_portfolio_cash_flow(self.database, {
            "flow_date": flow_date, "flow_type": "DIVIDEND",
            "amount_inr": 50, "notes": "Cash distribution",
        })
        self.add("BUY", 10, 100, 10)
        result = portfolio_overview(self.database)
        self.assertEqual(result["summary"]["cash_balance_inr"], 1040.0)
        self.assertEqual(result["summary"]["cash_flow_count"], 2)
        self.assertEqual(result["reconciliation"]["status"], "reconciled")
        self.assertEqual(result["reconciliation"]["income_inr"], 50.0)
        self.assertTrue(result["methodology"]["cash_flows_included"])

    def test_tradebook_import_is_atomic_and_idempotent(self) -> None:
        trade_date = min(date.today(), date(2026, 11, 13)).isoformat()
        tradebook = (
            "trade_date,tradingsymbol,trade_type,quantity,price,fees\n"
            f"{trade_date},AAA,BUY,10,100,5\n"
            f"{trade_date},AAA,SELL,2,120,2\n"
        )
        imported = import_portfolio_tradebook(self.database, {
            "file_name": "tradebook.csv", "csv_text": tradebook,
        })
        duplicate = import_portfolio_tradebook(self.database, {
            "file_name": "tradebook.csv", "csv_text": tradebook,
        })
        overview = portfolio_overview(self.database)
        self.assertEqual(imported["records_imported"], 2)
        self.assertFalse(imported["duplicate"])
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(overview["summary"]["transaction_count"], 2)
        self.assertEqual(overview["holdings"][0]["quantity"], 8.0)
        self.assertEqual(overview["reconciliation"]["import_count"], 1)

    def test_unknown_import_symbol_rolls_back_entire_file(self) -> None:
        trade_date = min(date.today(), date(2026, 11, 13)).isoformat()
        tradebook = (
            "date,symbol,side,qty,average_price\n"
            f"{trade_date},AAA,BUY,1,100\n"
            f"{trade_date},UNKNOWN,BUY,1,100\n"
        )
        with self.assertRaisesRegex(ValueError, "portfolio_import_unknown_symbols:UNKNOWN"):
            import_portfolio_tradebook(self.database, {
                "file_name": "bad.csv", "csv_text": tradebook,
            })
        overview = portfolio_overview(self.database)
        self.assertEqual(overview["summary"]["transaction_count"], 0)
        self.assertEqual(overview["reconciliation"]["import_count"], 0)

    def reset_performance_prices(self, rows: list[tuple[str, str, float]]) -> None:
        connection = connect_database(self.database)
        connection.execute("DELETE FROM candles")
        connection.executemany(
            "INSERT INTO candles(instrument_id,session_date,close) VALUES (?,?,?)",
            rows,
        )
        connection.commit()
        connection.close()

    def test_performance_calculates_twr_xirr_and_benchmark_edge(self) -> None:
        self.reset_performance_prices([
            ("stock:AAA", "2025-01-02", 100.0),
            ("stock:AAA", "2026-01-02", 110.0),
            ("index:Nifty 50", "2025-01-02", 1000.0),
            ("index:Nifty 50", "2026-01-02", 1050.0),
        ])
        record_portfolio_cash_flow(self.database, {
            "flow_date": "2025-01-02", "flow_type": "CONTRIBUTION", "amount_inr": 1000,
        })
        record_portfolio_transaction(self.database, {
            "trade_date": "2025-01-02", "symbol": "AAA", "side": "BUY",
            "quantity": 10, "price_inr": 100, "fees_inr": 0,
        })
        result = portfolio_performance(self.database)
        self.assertEqual(result["contract_version"], "portfolio-performance-v1")
        self.assertEqual(result["status"], "ready")
        self.assertAlmostEqual(result["portfolio"]["time_weighted_return_pct"], 10.0, places=3)
        self.assertAlmostEqual(result["portfolio"]["money_weighted_return_pct"], 10.0, places=3)
        self.assertAlmostEqual(result["benchmark"]["time_weighted_return_pct"], 5.0, places=3)
        self.assertAlmostEqual(result["comparison"]["excess_twr_pct"], 5.0, places=3)

    def test_mid_period_contribution_is_not_counted_as_return(self) -> None:
        self.reset_performance_prices([
            ("stock:AAA", "2025-01-02", 100.0),
            ("stock:AAA", "2025-07-02", 110.0),
            ("stock:AAA", "2026-01-02", 121.0),
            ("index:Nifty 50", "2025-01-02", 1000.0),
            ("index:Nifty 50", "2025-07-02", 1000.0),
            ("index:Nifty 50", "2026-01-02", 1000.0),
        ])
        for flow_date, amount in (("2025-01-02", 1000), ("2025-07-02", 1100)):
            record_portfolio_cash_flow(self.database, {
                "flow_date": flow_date, "flow_type": "CONTRIBUTION", "amount_inr": amount,
            })
        for trade_date, price in (("2025-01-02", 100), ("2025-07-02", 110)):
            record_portfolio_transaction(self.database, {
                "trade_date": trade_date, "symbol": "AAA", "side": "BUY",
                "quantity": 10, "price_inr": price, "fees_inr": 0,
            })
        result = portfolio_performance(self.database)
        self.assertEqual(result["status"], "ready")
        self.assertAlmostEqual(result["portfolio"]["time_weighted_return_pct"], 21.0, places=3)
        self.assertAlmostEqual(result["benchmark"]["time_weighted_return_pct"], 0.0, places=3)

    def test_performance_is_gated_without_external_cash_flow_history(self) -> None:
        self.add("BUY", 1, 100)
        result = portfolio_performance(self.database)
        self.assertEqual(result["status"], "gated")
        self.assertIn("external_cash_flow_history_missing", result["gates"])
        self.assertIn("cash_flow_history_missing", result["gates"])

    def test_risk_analysis_reports_concentration_and_contribution(self) -> None:
        self.reset_performance_prices([
            ("stock:AAA", "2025-01-02", 100.0),
            ("stock:AAA", "2026-01-02", 110.0),
            ("index:Nifty 50", "2025-01-02", 1000.0),
            ("index:Nifty 50", "2026-01-02", 1050.0),
        ])
        record_portfolio_cash_flow(self.database, {
            "flow_date": "2025-01-02", "flow_type": "CONTRIBUTION", "amount_inr": 1000,
        })
        record_portfolio_transaction(self.database, {
            "trade_date": "2025-01-02", "symbol": "AAA", "side": "BUY",
            "quantity": 10, "price_inr": 100, "fees_inr": 0,
        })
        result = portfolio_risk_analysis(self.database)
        self.assertEqual(result["contract_version"], "portfolio-risk-analysis-v1")
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["concentration"]["state"], "concentrated")
        self.assertEqual(result["concentration"]["largest_position_pct"], 100.0)
        self.assertEqual(result["concentration"]["effective_positions"], 1.0)
        self.assertAlmostEqual(result["contributions"][0]["total_pnl_inr"], 100.0, places=2)
        self.assertAlmostEqual(result["contributions"][0]["return_contribution_pct"], 10.0, places=2)

    def test_drawdown_uses_cash_flow_neutral_performance_path(self) -> None:
        self.reset_performance_prices([
            ("stock:AAA", "2025-01-02", 100.0),
            ("stock:AAA", "2025-06-02", 80.0),
            ("stock:AAA", "2026-01-02", 120.0),
            ("index:Nifty 50", "2025-01-02", 1000.0),
            ("index:Nifty 50", "2025-06-02", 1000.0),
            ("index:Nifty 50", "2026-01-02", 1000.0),
        ])
        record_portfolio_cash_flow(self.database, {
            "flow_date": "2025-01-02", "flow_type": "CONTRIBUTION", "amount_inr": 1000,
        })
        record_portfolio_transaction(self.database, {
            "trade_date": "2025-01-02", "symbol": "AAA", "side": "BUY",
            "quantity": 10, "price_inr": 100, "fees_inr": 0,
        })
        result = portfolio_risk_analysis(self.database)
        self.assertAlmostEqual(result["drawdown"]["maximum_drawdown_pct"], -20.0, places=3)
        self.assertEqual(result["drawdown"]["peak_date"], "2025-01-02")
        self.assertEqual(result["drawdown"]["trough_date"], "2025-06-02")
        self.assertAlmostEqual(result["drawdown"]["current_drawdown_pct"], 0.0, places=3)

    def test_risk_analysis_preserves_performance_gates(self) -> None:
        result = portfolio_risk_analysis(self.database)
        self.assertEqual(result["status"], "gated")
        self.assertIn("transaction_history_missing", result["gates"])
        self.assertEqual(result["allocation"], [])


if __name__ == "__main__":
    unittest.main()
