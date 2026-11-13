import unittest
from datetime import date

from workspace_engine import parse_nse_cash_bhavcopy


class NseCashArchiveTests(unittest.TestCase):
    def test_parses_official_full_cash_bhavcopy(self):
        payload = (
            "SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, "
            "LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY\n"
            "GUJGASLTD, EQ, 02-Feb-2024, 500, 501, 510, 499, 508, 507.5, 505, 12345\n"
            "NIFTY, INDEX, 02-Feb-2024, 1, 1, 1, 1, 1, 1, 1, 1\n"
        ).encode()

        rows = parse_nse_cash_bhavcopy(payload, date(2024, 2, 2))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "GUJGASLTD")
        self.assertEqual(rows[0]["session_date"], "2024-02-02")
        self.assertEqual(rows[0]["close"], 507.5)
        self.assertEqual(rows[0]["volume"], 12345)


if __name__ == "__main__":
    unittest.main()
