#!/usr/bin/env python3
"""Serve the independent Workspace 2.0 application and local data contracts."""

from __future__ import annotations

import argparse
import json
import mimetypes
import sqlite3
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import requests

from snapshot_store import DEFAULT_DB as DEFAULT_SNAPSHOT_DB, factor_constituents, snapshot_overview, update_all
from workspace_engine import (
    DEFAULT_MARKET_DB,
    KITE,
    bollinger_mean_reversion,
    corporate_action_contract,
    cross_strategy_multiple_testing,
    dashboard_overview,
    donchian_breakout,
    domestic_core,
    ema_crossover,
    equity_delivery_cost_calculator,
    ensure_workspace_schema,
    factor_research,
    moving_average_crossover,
    nifty50_ytd,
    point_in_time_universe_contract,
    refresh_global_risk,
    refresh_kite_breadth,
    refresh_kite_futures,
    refresh_kite_indices,
    refresh_nse_futures_archive,
    refresh_nse_removed_constituent_archive,
    refresh_removed_constituent_histories,
    refresh_nse_corporate_actions,
    removed_constituent_history_audit,
    rsi_mean_reversion,
    screener_local,
    selloff_recovery,
    seasonality_summary,
    stock_ytd,
    time_series_momentum,
    triple_sma_alignment,
    pullback_uptrend,
    zscore_mean_reversion,
)


ROOT = Path(__file__).resolve().parent
DIST = ROOT / "dist"


class WorkspaceHandler(BaseHTTPRequestHandler):
    market_db = DEFAULT_MARKET_DB
    snapshot_db = DEFAULT_SNAPSHOT_DB

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        path = parsed.path
        if path == "/api/workspace/health":
            self._json(200, {"ok": True, "workspace": "2.0", "data_engine": "workspace-local", "market_db": str(self.market_db)})
            return
        if path == "/api/workspace/nifty50-ytd":
            payload = nifty50_ytd(self.market_db)
            self._json(200 if payload.get("ok") else 503, payload)
            return
        if path == "/api/workspace/snapshots":
            self._json(200, snapshot_overview(self.snapshot_db))
            return
        if path == "/api/workspace/factor-constituents":
            query = parse_qs(parsed.query)
            try:
                payload = factor_constituents(
                    self.snapshot_db,
                    query.get("size", [""])[0],
                    query.get("style", [""])[0],
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            except (sqlite3.Error, OSError) as error:
                self._json(503, {"ok": False, "reason": "factor_constituents_unavailable", "detail": str(error)})
                return
            self._json(200, payload)
            return
        if path == "/api/workspace/corporate-actions":
            query = parse_qs(parsed.query)
            self._local_contract(lambda: corporate_action_contract(
                self.market_db, query.get("symbol", [""])[0] or None
            ))
            return
        if path == "/api/workspace/corporate-actions/refresh":
            query = parse_qs(parsed.query)
            try:
                from datetime import date
                start = date.fromisoformat(query["from"][0]) if query.get("from") else None
                end = date.fromisoformat(query["to"][0]) if query.get("to") else None
                payload = refresh_nse_corporate_actions(self.market_db, start, end)
            except (ValueError, OSError, requests.RequestException) as error:
                self._json(503, {"ok": False, "reason": "corporate_action_refresh_failed", "detail": str(error)})
                return
            self._json(200, payload)
            return
        if path == "/api/workspace/point-in-time-universe":
            query = parse_qs(parsed.query)
            try:
                payload = point_in_time_universe_contract(
                    self.market_db, self.snapshot_db, query.get("as_of", [""])[0] or None
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            except (sqlite3.Error, OSError) as error:
                self._json(503, {"ok": False, "reason": "point_in_time_universe_unavailable", "detail": str(error)})
                return
            self._json(200 if payload.get("ok") else 503, payload)
            return
        if path == "/api/workspace/removed-constituent-history-audit":
            self._local_contract(lambda: removed_constituent_history_audit(self.market_db))
            return
        if path == "/api/workspace/removed-constituent-history-backfill":
            query = parse_qs(parsed.query)
            try:
                workers = int(query.get("workers", ["8"])[0])
                payload = refresh_nse_removed_constituent_archive(self.market_db, workers)
            except (ValueError, OSError, requests.RequestException) as error:
                self._json(503, {"ok": False, "reason": "removed_constituent_archive_failed", "detail": str(error)})
                return
            self._json(200 if payload.get("ok") else 207, payload)
            return
        if path == "/api/dashboard/overview":
            self._local_contract(lambda: dashboard_overview(self.market_db))
            return
        if path == "/api/market-sentiment/domestic-core":
            self._local_contract(lambda: domestic_core(self.market_db, self.snapshot_db))
            return
        if path == "/api/factor-research":
            self._local_contract(lambda: factor_research(self.market_db))
            return
        if path == "/api/dashboard/seasonality-summary":
            self._local_contract(lambda: seasonality_summary(self.market_db))
            return
        if path == "/api/screener/local":
            self._local_contract(lambda: screener_local(self.market_db))
            return
        if path == "/api/stocks/ytd":
            self._local_contract(lambda: stock_ytd(self.market_db, self.snapshot_db))
            return
        if path == "/api/costs/equity-delivery":
            query = parse_qs(parsed.query)
            try:
                buy = float(query.get("buy", ["50000"])[0])
                sell = float(query.get("sell", [str(buy)])[0])
                payload = equity_delivery_cost_calculator(
                    buy,
                    sell,
                    {
                        "position_capital_inr": float(query.get("capital", [str(max(buy, 1))])[0]),
                        "slippage_bps_each_side": float(query.get("slippage_bps", ["5"])[0]),
                        "market_impact_bps_each_side": float(query.get("impact_bps", ["0"])[0]),
                        "include_dp_charge": query.get("include_dp", ["true"])[0].lower() not in {"0", "false", "no"},
                    },
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200, payload)
            return
        if path == "/api/backtests/cross-strategy-fdr":
            self._local_contract(lambda: cross_strategy_multiple_testing(self.market_db))
            return
        if path == "/api/backtests/moving-average-crossover":
            query = parse_qs(parsed.query)
            try:
                fast_window = int(query.get("fast", ["20"])[0])
                slow_window = int(query.get("slow", ["50"])[0])
                symbol = query.get("symbol", [""])[0]
                history_mode = query.get("history", ["comparison"])[0]
                payload = moving_average_crossover(
                    self.market_db,
                    symbol=symbol or None,
                    fast_window=fast_window,
                    slow_window=slow_window,
                    history_mode=history_mode,
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200 if payload.get("ok") else 404, payload)
            return
        if path == "/api/backtests/donchian-breakout":
            query = parse_qs(parsed.query)
            try:
                entry_window = int(query.get("entry", ["55"])[0])
                exit_window = int(query.get("exit", ["20"])[0])
                symbol = query.get("symbol", [""])[0]
                history_mode = query.get("history", ["comparison"])[0]
                payload = donchian_breakout(
                    self.market_db,
                    symbol=symbol or None,
                    entry_window=entry_window,
                    exit_window=exit_window,
                    history_mode=history_mode,
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200 if payload.get("ok") else 404, payload)
            return
        if path == "/api/backtests/ema-crossover":
            query = parse_qs(parsed.query)
            try:
                payload = ema_crossover(
                    self.market_db,
                    symbol=query.get("symbol", [""])[0] or None,
                    fast_window=int(query.get("fast", ["20"])[0]),
                    slow_window=int(query.get("slow", ["50"])[0]),
                    history_mode=query.get("history", ["comparison"])[0],
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200 if payload.get("ok") else 404, payload)
            return
        if path == "/api/backtests/time-series-momentum":
            query = parse_qs(parsed.query)
            try:
                payload = time_series_momentum(
                    self.market_db,
                    symbol=query.get("symbol", [""])[0] or None,
                    lookback=int(query.get("lookback", ["63"])[0]),
                    history_mode=query.get("history", ["comparison"])[0],
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200 if payload.get("ok") else 404, payload)
            return
        if path == "/api/backtests/triple-sma-alignment":
            query = parse_qs(parsed.query)
            try:
                payload = triple_sma_alignment(
                    self.market_db,
                    symbol=query.get("symbol", [""])[0] or None,
                    fast_window=int(query.get("fast", ["20"])[0]),
                    medium_window=int(query.get("medium", ["50"])[0]),
                    slow_window=int(query.get("slow", ["200"])[0]),
                    history_mode=query.get("history", ["comparison"])[0],
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200 if payload.get("ok") else 404, payload)
            return
        if path == "/api/backtests/rsi-mean-reversion":
            query = parse_qs(parsed.query)
            try:
                payload = rsi_mean_reversion(
                    self.market_db, symbol=query.get("symbol", [""])[0] or None,
                    period=int(query.get("period", ["2"])[0]),
                    entry_rsi=float(query.get("entry", ["10"])[0]),
                    exit_rsi=float(query.get("exit", ["70"])[0]),
                    history_mode=query.get("history", ["comparison"])[0],
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200 if payload.get("ok") else 404, payload)
            return
        if path == "/api/backtests/bollinger-mean-reversion":
            query = parse_qs(parsed.query)
            try:
                payload = bollinger_mean_reversion(
                    self.market_db, symbol=query.get("symbol", [""])[0] or None,
                    window=int(query.get("window", ["20"])[0]),
                    deviations=float(query.get("deviations", ["2"])[0]),
                    history_mode=query.get("history", ["comparison"])[0],
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200 if payload.get("ok") else 404, payload)
            return
        if path == "/api/backtests/zscore-mean-reversion":
            query = parse_qs(parsed.query)
            try:
                payload = zscore_mean_reversion(
                    self.market_db, symbol=query.get("symbol", [""])[0] or None,
                    window=int(query.get("window", ["10"])[0]),
                    entry_z=float(query.get("entry", ["-1.5"])[0]),
                    exit_z=float(query.get("exit", ["0"])[0]),
                    history_mode=query.get("history", ["comparison"])[0],
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200 if payload.get("ok") else 404, payload)
            return
        if path == "/api/backtests/pullback-uptrend":
            query = parse_qs(parsed.query)
            try:
                payload = pullback_uptrend(
                    self.market_db, symbol=query.get("symbol", [""])[0] or None,
                    pullback_window=int(query.get("pullback", ["5"])[0]),
                    trend_window=int(query.get("trend", ["200"])[0]),
                    history_mode=query.get("history", ["comparison"])[0],
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200 if payload.get("ok") else 404, payload)
            return
        if path == "/api/backtests/selloff-recovery":
            query = parse_qs(parsed.query)
            try:
                payload = selloff_recovery(
                    self.market_db, symbol=query.get("symbol", [""])[0] or None,
                    down_sessions=int(query.get("down", ["3"])[0]),
                    exit_window=int(query.get("exit", ["5"])[0]),
                    history_mode=query.get("history", ["comparison"])[0],
                )
            except ValueError as error:
                self._json(400, {"ok": False, "reason": str(error)})
                return
            self._json(200 if payload.get("ok") else 404, payload)
            return
        if path == "/api/kite/status":
            self._json(200, {"ok": True, "connected": KITE.connected, "last_error": KITE.last_error})
            return
        if path == "/api/kite/indices/eod-summary":
            self._local_contract(lambda: refresh_kite_indices(self.market_db))
            return
        if path == "/api/kite/market-breadth":
            self._local_contract(lambda: refresh_kite_breadth(self.market_db, self.snapshot_db))
            return
        if path == "/api/kite/futures/eod-summary":
            self._local_contract(lambda: refresh_kite_futures(self.market_db))
            return
        if path == "/api/workspace/fno-archive/refresh":
            query = parse_qs(parsed.query)
            try:
                start = date.fromisoformat(query["from"][0]) if query.get("from") else None
                end = date.fromisoformat(query["to"][0]) if query.get("to") else None
                target = int(query.get("target", ["252"])[0])
                force = query.get("force", ["false"])[0].lower() in {"1", "true", "yes"}
                payload = refresh_nse_futures_archive(
                    self.market_db, start_date=start, end_date=end,
                    target_sessions=target, force=force,
                )
            except (ValueError, OSError, sqlite3.Error, requests.RequestException) as error:
                self._json(503, {"ok": False, "reason": "fno_archive_refresh_failed", "detail": str(error)})
                return
            self._json(200 if payload.get("ok") else 503, payload)
            return
        if path == "/api/kite/removed-constituent-history":
            self._local_contract(lambda: refresh_removed_constituent_histories(self.market_db))
            return
        if path.startswith("/api/"):
            self._json(404, {"ok": False, "reason": "workspace_api_not_found", "path": path})
            return
        self._serve_static(path)

    def do_POST(self) -> None:  # noqa: N802
        """Handle Workspace's memory-only Kite authentication actions."""
        parsed = urlsplit(self.path)
        if parsed.path not in {
            "/api/kite/session", "/api/kite/disconnect",
            "/api/workspace/snapshots/refresh", "/api/workspace/global-risk/refresh",
        }:
            self._json(404, {"ok": False, "reason": "not_found"})
            return
        if parsed.path == "/api/workspace/snapshots/refresh":
            results = update_all(self.snapshot_db)
            failed = [key for key, value in results.items() if value.get("status") != "successful"]
            self._json(200, {
                "ok": not failed,
                "status": "successful" if not failed else "partial",
                "failed_sources": failed,
                "sources": results,
                "snapshot": snapshot_overview(self.snapshot_db),
            })
            return
        if parsed.path == "/api/workspace/global-risk/refresh":
            try:
                payload = refresh_global_risk(self.market_db)
            except (sqlite3.Error, OSError, requests.RequestException) as error:
                self._json(503, {"ok": False, "reason": "global_risk_refresh_failed", "detail": str(error)})
                return
            self._json(200 if payload.get("ok") else 503, payload)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._json(400, {"ok": False, "reason": "invalid_content_length"})
            return
        if length < 0 or length > 16_384:
            self._json(413, {"ok": False, "reason": "payload_too_large"})
            return
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._json(400, {"ok": False, "reason": "invalid_json"})
            return
        if parsed.path == "/api/kite/disconnect":
            KITE.disconnect()
            self._json(200, {"ok": True, "connected": False})
            return
        try:
            payload = KITE.connect(
                str(body.get("api_key", "")).strip(),
                str(body.get("api_secret", "")),
                str(body.get("request_token", "")).strip(),
            )
            self._json(200, payload)
        except (ValueError, OSError, requests.RequestException) as error:
            self._json(400, {"ok": False, "reason": str(error)})

    def _local_contract(self, builder) -> None:
        try:
            payload = builder()
            self._json(200 if payload.get("ok", True) else 503, payload)
        except (ValueError, sqlite3.Error, OSError, requests.RequestException) as error:
            self._json(503, {"ok": False, "reason": "workspace_data_unavailable", "detail": str(error)})

    def _serve_static(self, path: str) -> None:
        relative = "index.html" if path in {"", "/"} else path.lstrip("/")
        target = (DIST / relative).resolve()
        if DIST.resolve() not in target.parents and target != DIST.resolve():
            self._json(403, {"ok": False, "reason": "forbidden"})
            return
        if not target.is_file():
            target = DIST / "index.html"
        payload = target.read_bytes()
        content_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8" if content_type.startswith("text/") or content_type.endswith("javascript") else content_type)
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _json(self, status: int, body: dict[str, object]) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        print(f"[Workspace 2.0] {format % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve Workspace 2.0")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8520)
    parser.add_argument("--market-db", type=Path, default=DEFAULT_MARKET_DB)
    parser.add_argument("--snapshot-db", type=Path, default=DEFAULT_SNAPSHOT_DB)
    args = parser.parse_args()
    WorkspaceHandler.market_db = args.market_db.resolve()
    WorkspaceHandler.snapshot_db = args.snapshot_db.resolve()
    ensure_workspace_schema(WorkspaceHandler.market_db)
    server = ThreadingHTTPServer((args.host, args.port), WorkspaceHandler)
    print(f"Workspace 2.0 available at http://{args.host}:{args.port}/")
    print(f"Workspace market store: {WorkspaceHandler.market_db}")
    print(f"Workspace official-source snapshots: {WorkspaceHandler.snapshot_db}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
