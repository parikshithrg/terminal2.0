import json
import re
import tempfile
import unittest
from pathlib import Path

from workspace_analytics.portfolio import (
    portfolio_overview,
    portfolio_performance,
    portfolio_risk_analysis,
    record_portfolio_cash_flow,
    record_portfolio_transaction,
)
from workspace_analytics.sqlite_store import connect_database


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = Path(__file__).parent / "fixtures" / "portfolio_release_v1.json"


def build_fixture_database(path: Path, inputs: dict) -> None:
    connection = connect_database(path)
    connection.executescript(
        """
        CREATE TABLE instruments(
            instrument_id TEXT PRIMARY KEY, kind TEXT, display_name TEXT
        );
        CREATE TABLE candles(
            instrument_id TEXT, session_date TEXT, close REAL,
            PRIMARY KEY(instrument_id, session_date)
        );
        """
    )
    connection.executemany(
        "INSERT INTO instruments(instrument_id,kind,display_name) VALUES (?,?,?)",
        inputs["instruments"],
    )
    connection.executemany(
        "INSERT INTO candles(instrument_id,session_date,close) VALUES (?,?,?)",
        inputs["candles"],
    )
    connection.commit()
    connection.close()
    for row in inputs["cash_flows"]:
        record_portfolio_cash_flow(path, row)
    for row in inputs["transactions"]:
        record_portfolio_transaction(path, row)


def stable_contracts(path: Path) -> dict:
    overview = portfolio_overview(path)
    performance = portfolio_performance(path)
    risk = portfolio_risk_analysis(path)
    return {
        "overview": {
            "contract_version": overview["contract_version"],
            "as_of_date": overview["as_of_date"],
            "summary": overview["summary"],
            "holdings": overview["holdings"],
            "reconciliation": overview["reconciliation"],
            "methodology": overview["methodology"],
            "local_only": overview["local_only"],
            "trading_research_separated": overview["trading_research_separated"],
            "orders_enabled": overview["orders_enabled"],
        },
        "performance": {
            "contract_version": performance["contract_version"],
            "status": performance["status"],
            "from_date": performance["from_date"],
            "through_date": performance["through_date"],
            "observations": performance["observations"],
            "portfolio": performance["portfolio"],
            "benchmark": performance["benchmark"],
            "comparison": performance["comparison"],
            "series": performance["series"],
            "local_only": performance["local_only"],
            "trading_research_separated": performance["trading_research_separated"],
            "orders_enabled": performance["orders_enabled"],
        },
        "risk": {
            "contract_version": risk["contract_version"],
            "status": risk["status"],
            "as_of_date": risk["as_of_date"],
            "allocation": risk["allocation"],
            "concentration": risk["concentration"],
            "drawdown": risk["drawdown"],
            "contributions": risk["contributions"],
            "net_external_capital_inr": risk["net_external_capital_inr"],
            "local_only": risk["local_only"],
            "trading_research_separated": risk["trading_research_separated"],
            "orders_enabled": risk["orders_enabled"],
        },
    }


class PortfolioReleaseRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = json.loads(FIXTURE.read_text())
        if cls.fixture.get("fixture_contract") != "portfolio-release-regression-fixture-v1":
            raise AssertionError("unexpected_portfolio_fixture_contract")

    def test_reconciled_portfolio_contracts_are_frozen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "portfolio.sqlite3"
            build_fixture_database(database, self.fixture["inputs"])
            result = stable_contracts(database)
        self.assertEqual(result, self.fixture["expected"])

    def test_empty_portfolio_preserves_honest_gates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "empty.sqlite3"
            connection = connect_database(database)
            connection.executescript(
                "CREATE TABLE instruments(instrument_id TEXT PRIMARY KEY,kind TEXT,display_name TEXT);"
                "CREATE TABLE candles(instrument_id TEXT,session_date TEXT,close REAL,PRIMARY KEY(instrument_id,session_date));"
            )
            connection.commit()
            connection.close()
            performance = portfolio_performance(database)
            risk = portfolio_risk_analysis(database)
        self.assertEqual(performance["status"], "gated")
        self.assertEqual(risk["status"], "gated")
        self.assertEqual(risk["allocation"], [])
        self.assertEqual(risk["drawdown"], {})
        self.assertFalse(performance["orders_enabled"])
        self.assertTrue(risk["trading_research_separated"])

    def test_portfolio_page_contract_and_endpoints_are_present(self) -> None:
        html = (ROOT / "dist" / "index.html").read_text()
        script = (ROOT / "dist" / "app.js").read_text()
        server = (ROOT / "server.py").read_text()
        ids = re.findall(r'\bid="([^"]+)"', html)
        self.assertEqual(len(ids), len(set(ids)), "duplicate HTML ids")
        required = {
            "portfolioView", "portfolioTransactionForm", "portfolioCashFlowForm",
            "portfolioImportForm", "portfolioPerformanceStatus", "portfolioRiskStatus",
            "portfolioAllocationList", "portfolioContributionList",
        }
        self.assertTrue(required.issubset(set(ids)))
        portfolio_markup = html[html.index('id="portfolioView"'):html.index('id="factorDialog"')]
        self.assertNotIn("Expense Analyser", portfolio_markup)
        for endpoint in (
            "/api/portfolio/overview", "/api/portfolio/performance",
            "/api/portfolio/risk-analysis", "/api/portfolio/transactions",
            "/api/portfolio/cash-flows", "/api/portfolio/import",
        ):
            self.assertIn(endpoint, script + server)
        self.assertIn("renderPortfolioPerformance", script)
        self.assertIn("renderPortfolioRisk", script)

    def test_portfolio_responsive_rules_are_frozen(self) -> None:
        styles = (ROOT / "dist" / "styles.css").read_text()
        for breakpoint in ("@media (max-width: 1100px)", "@media (max-width: 760px)", "@media (max-width: 520px)"):
            self.assertIn(breakpoint, styles)
        self.assertIn(".portfolio-risk-metrics, .portfolio-risk-detail-grid { grid-template-columns: 1fr; }", styles)
        self.assertIn(".portfolio-performance-grid { grid-template-columns: 1fr; }", styles)


if __name__ == "__main__":
    unittest.main()
