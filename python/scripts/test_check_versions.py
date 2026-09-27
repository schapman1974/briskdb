"""Keep the wheel's explicit feature boundary checked during packaging."""

import subprocess
import sys
import unittest

from check_versions import ROOT, validate_dependency_features


class VersionContractTests(unittest.TestCase):
    def test_workspace_metadata_passes_the_real_release_check(self):
        subprocess.run(
            [sys.executable, str(ROOT / "python/scripts/check_versions.py")],
            check=True,
            capture_output=True,
            text=True,
        )

    def test_tls_is_required_but_shared_auth_is_not_enabled(self):
        expected = ["documents", "listeners", "mongo-tls"]
        validate_dependency_features(
            {"uses_default_features": False, "features": expected}
        )
        for features in (
            ["documents", "listeners", "mongo"],
            expected + ["auth-scram"],
            expected + ["server-cli"],
            ["listeners", "mongo-tls"],
        ):
            with self.subTest(features=features), self.assertRaises(SystemExit):
                validate_dependency_features(
                    {"uses_default_features": False, "features": features}
                )

    def test_default_server_features_are_rejected(self):
        with self.assertRaises(SystemExit):
            validate_dependency_features(
                {
                    "uses_default_features": True,
                    "features": ["documents", "listeners", "mongo-tls"],
                }
            )


if __name__ == "__main__":
    unittest.main()
