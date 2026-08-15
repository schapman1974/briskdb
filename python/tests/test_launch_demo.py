from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path


class LaunchDemoTests(unittest.TestCase):
    def test_public_launch_demo_exercises_every_claimed_surface(self) -> None:
        root = Path(__file__).resolve().parents[2]
        completed = subprocess.run(
            [sys.executable, str(root / "examples" / "launch_demo.py"), "--json"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        summary = json.loads(completed.stdout)
        self.assertEqual(summary["shards"], 4)
        self.assertEqual(summary["writers"], 4)
        self.assertEqual(summary["writes"], 32)
        self.assertEqual(summary["read_rows"], 32)
        self.assertEqual(summary["read_shards"], [0, 1, 2, 3])
        self.assertEqual(sum(summary["shard_counts"]), 32)
        self.assertTrue(all(count > 0 for count in summary["shard_counts"]))
        self.assertEqual(summary["health"], "ok")
        self.assertIsNotNone(summary["postgres_address"])
        self.assertTrue(summary["ordinary_sqlite_files"])


if __name__ == "__main__":
    unittest.main()
