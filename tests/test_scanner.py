import unittest
from pathlib import Path

from backend.scanner import package_file_type


class PackageFileTypeTests(unittest.TestCase):
    def test_expected_graph_contract_files(self):
        self.assertEqual(package_file_type(Path("nodes.csv")), "node_master")
        self.assertEqual(package_file_type(Path("relationships.csv")), "verified_edges")
        self.assertEqual(package_file_type(Path("relationship_candidates.csv")), "candidate_edges")

    def test_supporting_files(self):
        self.assertEqual(package_file_type(Path("domain_maintenance.csv")), "domain_maintenance")
        self.assertEqual(package_file_type(Path("input_metadata/manifest.csv")), "metadata_manifest")
        self.assertEqual(package_file_type(Path("etl_summary.json")), "etl_summary")

    def test_unknown_name(self):
        self.assertIsNone(package_file_type(Path("01_asset_master.xlsx")))
        self.assertIsNone(package_file_type(Path("random.csv")))


if __name__ == "__main__":
    unittest.main()
