from __future__ import annotations

import os
import unittest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://econatlas:econatlas@localhost:5432/econatlas",
)

from app.services.discover_service import _resolve_batch_source_status


class DiscoverServiceStatusTests(unittest.TestCase):
    def test_primary_when_majority_is_primary(self) -> None:
        rows = [{"source_status": "primary"} for _ in range(7)] + [{"source_status": "fallback"} for _ in range(3)]
        self.assertEqual("primary", _resolve_batch_source_status(rows))

    def test_fallback_when_primary_not_majority(self) -> None:
        rows = [{"source_status": "primary"} for _ in range(4)] + [{"source_status": "fallback"} for _ in range(6)]
        self.assertEqual("fallback", _resolve_batch_source_status(rows))

    def test_limited_when_no_rows(self) -> None:
        self.assertEqual("limited", _resolve_batch_source_status([]))


if __name__ == "__main__":
    unittest.main()
