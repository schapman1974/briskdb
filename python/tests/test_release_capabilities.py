"""Installed-artifact checks: compiled capability is distinct from default mode."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import briskdb
from briskdb import _briskdb


class ReleaseCapabilityTests(unittest.TestCase):
    def test_native_overlay_is_present_and_validates_before_io(self):
        native = getattr(_briskdb, "S3OverlayDatabase", None)
        if native is None:
            if os.environ.get("BRISKDB_REQUIRE_S3_OVERLAY") == "1":
                self.fail("release artifact is missing the native S3/EFS engine")
            self.skipTest("custom build without s3-overlay")
        for name in ("create", "query", "execute", "update", "update_status",
                     "update_target", "compact", "settings", "open_stats",
                     "read_stats", "set_parquet_pruning", "close"):
            self.assertTrue(callable(getattr(native, name, None)), name)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "must-not-be-created"
            with self.assertRaises(briskdb.InvalidArgumentError):
                native(str(root), '{"read_only":"not-a-boolean"}')
            self.assertFalse(root.exists())

    def test_sqlite_remains_default_even_with_overlay_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "ordinary"
            overlay_root = Path(directory) / "not-selected"
            with patch.dict(os.environ, {
                "BRISKDB_STORAGE_MODE": "s3-overlay",
                "BRISKDB_OVERLAY_ROOT": str(overlay_root),
            }):
                with briskdb.open(root, shards=2) as database:
                    with database.session(routing_key="release-smoke") as session:
                        self.assertEqual(session.query("SELECT 42")["rows"], [(42,)])
            self.assertTrue((root / "manifest.sqlite").is_file())
            self.assertFalse(overlay_root.exists())


if __name__ == "__main__":
    unittest.main()
