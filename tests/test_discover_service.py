from __future__ import annotations

import unittest

from app.services import discover_service


class DiscoverServiceTests(unittest.TestCase):
    def test_resolve_batch_source_status_prefers_worst_status(self) -> None:
        rows = [
            {"source_status": "primary"},
            {"source_status": "fallback"},
            {"source_status": "limited"},
        ]
        resolved = discover_service._resolve_batch_source_status(rows)
        self.assertEqual("limited", resolved)

    def test_stock_why_ranked_has_default_message(self) -> None:
        reason = discover_service._stock_why_ranked({"source_status": "primary"})
        self.assertTrue(reason)


if __name__ == "__main__":
    unittest.main()
