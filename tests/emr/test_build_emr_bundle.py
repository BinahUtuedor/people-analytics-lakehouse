"""Tests for deterministic EMR Bronze application packaging."""

from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path

from scripts.build_emr_bundle import build_bundle


class BuildEmrBundleTests(unittest.TestCase):
    """Verify the source-only bundle without network or AWS access."""

    def test_bundle_contains_entry_point_and_importable_project_packages(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            manifest = build_bundle(output, include_dependencies=False)

            self.assertFalse(manifest["dependencies_included"])
            self.assertTrue((output / "bronze-job.py").is_file())
            with zipfile.ZipFile(output / "people-analytics-bronze.zip") as archive:
                names = set(archive.namelist())
            self.assertIn("config/settings.py", names)
            self.assertIn("config/datasets.py", names)
            self.assertIn("spark/utilities.py", names)
            self.assertIn("spark/bronze/job.py", names)
            self.assertFalse(any("__pycache__" in name for name in names))

    def test_source_only_bundle_is_reproducible(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = build_bundle(root / "first", include_dependencies=False)
            second = build_bundle(root / "second", include_dependencies=False)

            self.assertEqual(first["artifacts"], second["artifacts"])


if __name__ == "__main__":
    unittest.main()
