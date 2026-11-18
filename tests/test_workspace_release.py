import tempfile
import time
import unittest
from datetime import date
from pathlib import Path

from workspace_analytics.release_audit import (
    WORKSPACE_RELEASE_CONTRACT,
    workspace_release_audit,
)


ROOT = Path(__file__).resolve().parents[1]
MARKET_DB = ROOT / "data" / "workspace_market.sqlite3"
SNAPSHOT_DB = ROOT / "data" / "workspace_snapshots.sqlite3"


class WorkspaceReleaseAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        started = time.perf_counter()
        cls.audit = workspace_release_audit(
            ROOT, MARKET_DB, SNAPSHOT_DB, today=date(2026, 11, 18),
        )
        cls.elapsed = time.perf_counter() - started

    def test_complete_workspace_is_release_ready(self) -> None:
        self.assertEqual(self.audit["contract_version"], WORKSPACE_RELEASE_CONTRACT)
        self.assertEqual(self.audit["status"], "ready")
        self.assertEqual(self.audit["release_decision"], "local_release_ready")
        self.assertEqual(self.audit["passed"], self.audit["total"])
        self.assertEqual(self.audit["coverage"]["pages"], 6)

    def test_release_boundaries_remain_local_and_non_ordering(self) -> None:
        boundaries = self.audit["boundaries"]
        self.assertTrue(boundaries["local_only"])
        self.assertFalse(boundaries["orders_enabled"])
        self.assertFalse(boundaries["runtime_proxy_dependency"])

    def test_missing_databases_withhold_release(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory)
            audit = workspace_release_audit(
                ROOT, missing / "market.sqlite3", missing / "snapshots.sqlite3",
                today=date(2026, 11, 18),
            )
        checks = {check["key"]: check for check in audit["checks"]}
        self.assertEqual(audit["status"], "withheld")
        self.assertFalse(checks["market_database"]["passed"])
        self.assertFalse(checks["snapshot_database"]["passed"])
        self.assertFalse(checks["completed_session_freshness"]["passed"])

    def test_release_audit_completes_within_local_budget(self) -> None:
        self.assertLess(self.elapsed, 8.0, f"release audit took {self.elapsed:.2f}s")

    def test_dashboard_exposes_versioned_release_state(self) -> None:
        html = (ROOT / "dist" / "index.html").read_text()
        script = (ROOT / "dist" / "app.js").read_text()
        server = (ROOT / "server.py").read_text()
        self.assertIn('id="releaseAuditState"', html)
        self.assertIn('id="releaseAuditMeta"', html)
        self.assertIn("loadWorkspaceReleaseAudit", script)
        self.assertIn("/api/workspace/release-audit", script)
        self.assertIn("/api/workspace/release-audit", server)


if __name__ == "__main__":
    unittest.main()
