import csv
import io
import sqlite3
import tempfile
import unittest
import zipfile
from datetime import date
from pathlib import Path

from workspace_engine import (
    _fno_state,
    _connect,
    ensure_workspace_schema,
    parse_nse_futures_bhavcopy,
)


def zipped_csv(name, headers, rows):
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr(name, stream.getvalue())
    return payload.getvalue()


class NseFnoArchiveTests(unittest.TestCase):
    def test_legacy_parser_keeps_front_month_futures_only(self):
        headers = [
            "INSTRUMENT", "SYMBOL", "EXPIRY_DT", "STRIKE_PR", "OPTION_TYP",
            "OPEN", "HIGH", "LOW", "CLOSE", "CONTRACTS", "OPEN_INT", "TIMESTAMP",
        ]
        rows = [
            {"INSTRUMENT": "FUTSTK", "SYMBOL": "ABC", "EXPIRY_DT": "25-Jul-2024", "STRIKE_PR": "0", "OPTION_TYP": "XX", "OPEN": "100", "HIGH": "105", "LOW": "99", "CLOSE": "104", "CONTRACTS": "10", "OPEN_INT": "500", "TIMESTAMP": "01-Jul-2024"},
            {"INSTRUMENT": "FUTSTK", "SYMBOL": "ABC", "EXPIRY_DT": "29-Aug-2024", "STRIKE_PR": "0", "OPTION_TYP": "XX", "OPEN": "101", "HIGH": "106", "LOW": "100", "CLOSE": "105", "CONTRACTS": "9", "OPEN_INT": "400", "TIMESTAMP": "01-Jul-2024"},
            {"INSTRUMENT": "OPTSTK", "SYMBOL": "ABC", "EXPIRY_DT": "25-Jul-2024", "STRIKE_PR": "100", "OPTION_TYP": "CE", "OPEN": "1", "HIGH": "2", "LOW": "1", "CLOSE": "2", "CONTRACTS": "8", "OPEN_INT": "300", "TIMESTAMP": "01-Jul-2024"},
        ]
        parsed = parse_nse_futures_bhavcopy(zipped_csv("legacy.csv", headers, rows))
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["underlying"], "ABC")
        self.assertEqual(parsed[0]["expiry_date"], "2024-07-25")
        self.assertEqual(parsed[0]["open_interest"], 500)

    def test_udiff_parser_normalizes_stock_and_index_futures(self):
        headers = [
            "TradDt", "FinInstrmTp", "TckrSymb", "XpryDt", "StrkPric", "OptnTp",
            "OpnPric", "HghPric", "LwPric", "ClsPric", "TtlTradgVol", "OpnIntrst",
        ]
        rows = [
            {"TradDt": "2024-07-09", "FinInstrmTp": "STF", "TckrSymb": "ABC", "XpryDt": "2024-07-25", "StrkPric": "0", "OptnTp": "", "OpnPric": "100", "HghPric": "103", "LwPric": "98", "ClsPric": "102", "TtlTradgVol": "20", "OpnIntrst": "600"},
            {"TradDt": "2024-07-09", "FinInstrmTp": "IDF", "TckrSymb": "NIFTY", "XpryDt": "2024-07-25", "StrkPric": "0", "OptnTp": "", "OpnPric": "24000", "HghPric": "24100", "LwPric": "23900", "ClsPric": "24050", "TtlTradgVol": "30", "OpnIntrst": "700"},
        ]
        parsed = parse_nse_futures_bhavcopy(zipped_csv("udiff.csv", headers, rows))
        self.assertEqual([row["underlying"] for row in parsed], ["ABC", "NIFTY"])

    def test_history_gate_counts_same_contract_comparisons_not_roll_day(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "market.sqlite3"
            seed = sqlite3.connect(database)
            seed.execute(
                """CREATE TABLE instruments (
                    instrument_id TEXT PRIMARY KEY, kind TEXT, source TEXT,
                    adjustment_status TEXT, adjustment_source_url TEXT
                )"""
            )
            seed.commit()
            seed.close()
            ensure_workspace_schema(database)
            connection = _connect(database)
            try:
                now = "2026-11-11T00:00:00+00:00"
                contracts = [
                    ("NSEFO:STF:ABC:2026-10-29", "2026-10-29"),
                    ("NSEFO:STF:ABC:2026-11-26", "2026-11-26"),
                ]
                for key, expiry in contracts:
                    connection.execute(
                        "INSERT INTO futures_contracts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (key, "ABC", key, "NFO", "NSE_ARCHIVE", expiry, 0, "test", now, now, 4),
                    )
                observations = [
                    (contracts[0][0], "2026-10-28", 100, 1000),
                    (contracts[0][0], "2026-10-29", 101, 1100),
                    (contracts[1][0], "2026-10-30", 102, 1200),
                    (contracts[1][0], "2026-11-02", 103, 1300),
                ]
                for key, session, close, oi in observations:
                    connection.execute(
                        "INSERT INTO futures_eod_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (key, session, close, close, close, close, 1, oi, "test", now, 4),
                    )
                connection.commit()
                state = _fno_state(connection)
                self.assertEqual(state["raw_history_sessions"], 4)
                self.assertEqual(state["stored_history_sessions"], 2)
                self.assertEqual(state["futures_oi"]["rollover_baseline_count"], 2)
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
