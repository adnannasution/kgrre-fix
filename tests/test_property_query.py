import unittest

from backend.app import _query_conditions, _query_fields_for_type


class FakeCursor:
    def __init__(self, items):
        self.items = items

    def fetchall(self):
        return self.items


class FakeConnection:
    def execute(self, query, params):
        # rows() returns fetchall() directly as dict rows (pool uses dict_row).
        return FakeCursor(
            [
                {"properties_json": {"label": "P-101", "derived_status_bucket": "open", "invalid-key": "ignored"}},
                {"properties_json": {"equipment_code_normalized": "P101", "derived_risk_score": 12}},
            ]
        )


class PropertyQueryConditionTests(unittest.TestCase):
    def test_contains_equality_numeric_and_exists(self):
        clauses, params = _query_conditions(
            'label CONTAINS "pump" AND domain = "maintenance" AND risk > 10 AND business_key EXISTS',
            {"label": "label", "domain": "domain", "risk": "risk", "business_key": "business_key"},
        )

        self.assertIn("LIKE %s", clauses[0])
        self.assertIn("= lower(%s)", clauses[1])
        self.assertIn("> %s", clauses[2])
        self.assertIn("IS NOT NULL", clauses[3])
        self.assertEqual(params, ["%pump%", "maintenance", 10.0])

    def test_like_and_not_like(self):
        clauses, params = _query_conditions(
            'label LIKE "%pump%" AND status NOT LIKE "%closed%"',
            {"label": "label", "status": "status"},
        )

        self.assertIn("LIKE %s", clauses[0])
        self.assertIn("NOT LIKE %s", clauses[1])
        self.assertEqual(params, ["%pump%", "%closed%"])

    def test_query_metadata_fields_include_core_and_sampled_properties(self):
        fields = _query_fields_for_type(FakeConnection(), "kg_node", "node_type", "equipment", ["label", "domain"])

        self.assertIn("label", fields)
        self.assertIn("domain", fields)
        self.assertIn("derived_status_bucket", fields)
        self.assertIn("equipment_code_normalized", fields)
        self.assertNotIn("invalid-key", fields)



if __name__ == "__main__":
    unittest.main()
