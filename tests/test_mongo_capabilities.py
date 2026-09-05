import copy
import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "mongo_parity.py"
MANIFEST = ROOT / "compat" / "mongo" / "v1" / "manifest.json"
SPEC = importlib.util.spec_from_file_location("mongo_capability_validation", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
mongo_parity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mongo_parity)


class CapabilityManifestValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.capabilities = json.loads(MANIFEST.read_text(encoding="utf-8"))[
            "capabilities"
        ]

    def assert_invalid(self, mutate, pattern):
        capabilities = copy.deepcopy(self.capabilities)
        mutate(capabilities)
        with self.assertRaisesRegex(mongo_parity.ContractError, pattern):
            mongo_parity._validate_capabilities(capabilities)

    def test_complete_inventory_covers_all_124_operations_and_four_write_results(self):
        mongo_parity._validate_capabilities(copy.deepcopy(self.capabilities))
        operations = self.capabilities["results"]["operation_result_shapes"]
        self.assertEqual(sum(len(methods) for methods in operations.values()), 124)
        self.assertEqual(
            set(self.capabilities["results"]["write_result_classes"]),
            {
                "tinymongo.results.DeleteResult",
                "tinymongo.results.InsertManyResult",
                "tinymongo.results.InsertOneResult",
                "tinymongo.results.UpdateResult",
            },
        )

    def test_missing_owner_method_is_rejected(self):
        self.assert_invalid(
            lambda value: value["api"]["owners"]["sync_client"]["supported"].remove(
                "server_info"
            ),
            "supported methods do not match source",
        )

    def test_missing_method_option_record_is_rejected(self):
        self.assert_invalid(
            lambda value: value["api"]["method_options"]["sync_client"].pop(
                "server_info"
            ),
            "method options keys differ",
        )

    def test_unknown_constructor_option_behavior_is_rejected(self):
        self.assert_invalid(
            lambda value: value["api"]["constructors"]["tinymongo.MongoClient"][
                "options"
            ][0].update(behavior="fabricated"),
            "unknown option behavior",
        )

    def test_duplicate_pymongo_connection_option_is_rejected(self):
        def duplicate(value):
            catalog = value["api"]["connection_options"]["pymongo_fallback_catalog"]
            catalog[-1]["name"] = catalog[0]["name"]

        self.assert_invalid(duplicate, "duplicate option names")

    def test_missing_write_result_class_is_rejected(self):
        self.assert_invalid(
            lambda value: value["results"]["write_result_classes"].pop(
                "tinymongo.results.DeleteResult"
            ),
            "write result classes keys differ",
        )

    def test_wrong_operation_result_shape_is_rejected(self):
        self.assert_invalid(
            lambda value: value["results"]["operation_result_shapes"][
                "sync_collection"
            ].update(list_indexes="cursor_sync"),
            "operation result shapes for sync_collection do not match source",
        )

    def test_undefined_nested_result_shape_is_rejected(self):
        self.assert_invalid(
            lambda value: value["results"]["result_shapes"]["document_list"].update(
                item="fabricated"
            ),
            "references unknown result shape fabricated",
        )

    def test_changed_result_shape_definition_is_rejected(self):
        self.assert_invalid(
            lambda value: value["results"]["result_shapes"]["integer"].update(
                type="number"
            ),
            "result shape definitions do not match TinyMongo v1.3.0",
        )

    def test_invalid_document_must_expose_original_document(self):
        def remove_document(value):
            fields = value["errors"]["class_shapes"]["InvalidDocument"]["fields"]
            fields[:] = [field for field in fields if field["name"] != "document"]

        self.assert_invalid(
            remove_document, "InvalidDocument fields do not match source"
        )

    def test_bulk_write_detail_shape_is_exact(self):
        self.assert_invalid(
            lambda value: value["errors"]["detail_shapes"]["bulk_write_details"][
                "required_fields"
            ].pop("writeErrors"),
            "bulk write detail fields keys differ",
        )

    def test_public_int_and_long_bson_names_are_required(self):
        def use_storage_width_names(value):
            native = value["values"]["native"]
            native[native.index("int")] = "int32"
            native[native.index("long")] = "int64"

        self.assert_invalid(
            use_storage_width_names,
            r"native BSON types do not match supported_bson_types\(\)",
        )

    def test_semantic_capability_reductions_are_rejected(self):
        reductions = {
            "aggregation": lambda category: category["stages"].remove("$group"),
            "commands": lambda category: category["supported"].remove("ping"),
            "indexes": lambda category: category["operations"].remove("create_indexes"),
            "projections": lambda category: category["supported"].remove(
                "array traversal"
            ),
            "queries": lambda category: category["unsupported_operators"].remove(
                "$where"
            ),
            "unsupported": lambda category: category.remove("transactions"),
            "updates": lambda category: category["operators"].remove("$rename"),
            "warnings": lambda category: category["contexts"].remove(
                "create_indexes degraded model reuse"
            ),
        }
        for category, mutate in reductions.items():
            with self.subTest(category=category):
                self.assert_invalid(
                    lambda value, category=category, mutate=mutate: mutate(
                        value[category]
                    ),
                    r"capabilities\.{0} do not match TinyMongo v1\.3\.0".format(
                        category
                    ),
                )

    def test_semantic_capability_fabrications_are_rejected(self):
        fabrications = {
            "aggregation": lambda category: category["stages"].append("$lookup"),
            "commands": lambda category: category["supported"].append("hello"),
            "indexes": lambda category: category["operations"].append("rebuild_index"),
            "projections": lambda category: category["supported"].append(
                "positional projection"
            ),
            "queries": lambda category: category["field_operators"].append("$where"),
            "unsupported": lambda category: category.append(
                "fabricated unsupported feature"
            ),
            "updates": lambda category: category["operators"].append("$mul"),
            "warnings": lambda category: category["contexts"].append(
                "fabricated warning context"
            ),
        }
        for category, mutate in fabrications.items():
            with self.subTest(category=category):
                self.assert_invalid(
                    lambda value, category=category, mutate=mutate: mutate(
                        value[category]
                    ),
                    r"capabilities\.{0} do not match TinyMongo v1\.3\.0".format(
                        category
                    ),
                )

    def test_all_named_unsupported_query_operators_are_frozen(self):
        self.assertEqual(
            self.capabilities["queries"]["unsupported_operators"],
            [
                "$bitsAllClear",
                "$bitsAllSet",
                "$bitsAnyClear",
                "$bitsAnySet",
                "$expr",
                "$geoIntersects",
                "$geoWithin",
                "$jsonSchema",
                "$near",
                "$nearSphere",
                "$text",
                "$where",
            ],
        )

    def test_less_structured_capability_text_is_digest_locked(self):
        self.assert_invalid(
            lambda value: value["values"]["rules"].__setitem__(
                0, "fabricated BSON value rule"
            ),
            "capability definitions do not match the frozen TinyMongo v1.3.0 inventory",
        )


if __name__ == "__main__":
    unittest.main()
