from unittest import TestCase

from pandagg.exceptions import AbsentMappingFieldError
from pandagg.mappings import Keyword, Object, Text, Nested, Integer, Mappings
from pandagg.node.mappings.abstract import Field
from tests.testing_samples.mapping_example import MAPPINGS, EXPECTED_MAPPING_TREE_REPR


class MappingsTreeTestCase(TestCase):
    """All tree logic is tested in utils.
    Here, check that:
     - a dict mappings is correctly parsed into a tree,
     - it has the right representation.
    """

    def test_keyword_with_fields(self):
        unnamed_field = Keyword(fields={"searchable": {"type": "text"}}, fielddata=True)
        self.assertEqual(unnamed_field.body, {"fielddata": True, "type": "keyword"})
        self.assertEqual(unnamed_field.fields, {"searchable": {"type": "text"}})

    def test_deserialization(self):
        mapping_dict = {
            "dynamic": False,
            "properties": {
                "classification_type": {
                    "type": "keyword",
                    "fields": {"raw": {"type": "text"}},
                },
                "local_metrics": {
                    "type": "nested",
                    "dynamic": False,
                    "properties": {
                        "dataset": {
                            "dynamic": False,
                            "properties": {
                                "support_test": {"type": "integer"},
                                "support_train": {"type": "integer"},
                            },
                        }
                    },
                },
            },
        }

        m1 = Mappings(**mapping_dict)

        m2 = Mappings(
            dynamic=False,
            properties={
                "classification_type": Keyword(fields={"raw": Text()}),
                "local_metrics": Nested(
                    dynamic=False,
                    properties={
                        "dataset": Object(
                            dynamic=False,
                            properties={
                                "support_test": Integer(),
                                "support_train": Integer(),
                            },
                        )
                    },
                ),
            },
        )

        expected_repr = """<Mappings>
_
├── classification_type                              Keyword
│   └── raw                                           ~ Text
└── local_metrics                                   [Nested]
    └── dataset                                     {Object}
        ├── support_test                             Integer
        └── support_train                            Integer
"""
        for i, m in enumerate((m1, m2)):
            self.assertEqual(m.__repr__(), expected_repr, "failed at m%d" % (i + 1))
            self.assertEqual(m.to_dict(), mapping_dict, "failed at m%d" % (i + 1))

    def test_parse_tree_from_dict(self):
        mapping_tree = Mappings(**MAPPINGS)

        self.assertEqual(mapping_tree.__str__(), EXPECTED_MAPPING_TREE_REPR)

    def test_nesteds_applied_at_field(self):
        mapping_tree = Mappings(**MAPPINGS)

        self.assertEqual(mapping_tree.nested_at_field("classification_type"), None)
        self.assertEqual(mapping_tree.list_nesteds_at_field("classification_type"), [])
        self.assertEqual(mapping_tree.nested_at_field("date"), None)
        self.assertEqual(mapping_tree.list_nesteds_at_field("date"), [])
        self.assertEqual(mapping_tree.nested_at_field("global_metrics"), None)
        self.assertEqual(mapping_tree.list_nesteds_at_field("global_metrics"), [])

        self.assertEqual(mapping_tree.nested_at_field("local_metrics"), "local_metrics")
        self.assertEqual(
            mapping_tree.list_nesteds_at_field("local_metrics"), ["local_metrics"]
        )
        self.assertEqual(
            mapping_tree.nested_at_field("local_metrics.dataset.support_test"),
            "local_metrics",
        )
        self.assertEqual(
            mapping_tree.list_nesteds_at_field("local_metrics.dataset.support_test"),
            ["local_metrics"],
        )

    def test_mapping_type_of_field(self):
        mapping_tree = Mappings(**MAPPINGS)
        with self.assertRaises(AbsentMappingFieldError):
            self.assertEqual(mapping_tree.mapping_type_of_field("yolo"), False)

        self.assertEqual(mapping_tree.mapping_type_of_field("global_metrics"), "object")
        self.assertEqual(mapping_tree.mapping_type_of_field("local_metrics"), "nested")
        self.assertEqual(
            mapping_tree.mapping_type_of_field("global_metrics.field.name.raw"),
            "keyword",
        )
        self.assertEqual(
            mapping_tree.mapping_type_of_field("local_metrics.dataset.support_test"),
            "integer",
        )

    def test_node_path(self):
        mapping_tree = Mappings(**MAPPINGS)
        # get node by path syntax
        nid = mapping_tree.get_node_id_by_path(
            ["local_metrics", "dataset", "support_test"]
        )
        k, node = mapping_tree.get(nid)
        self.assertIsInstance(node, Field)
        self.assertEqual(
            mapping_tree.get_path(node.identifier),
            ["local_metrics", "dataset", "support_test"],
        )

    def test_validate_doc(self):
        tts = [
            {
                "name": "non nullable",
                "properties": {"pizza": Keyword(nullable=False)},
                "documents_expected_results": [
                    ({"pizza": "yolo"}, None),
                    ({"pizza": None}, "Field <pizza> cannot be null"),
                    ({}, "Field <pizza> cannot be null"),
                    ({"pizza": ["yo", "lo"]}, None),
                ],
            },
            {
                "name": "nullable",
                "properties": {"pizza": Keyword(nullable=True)},
                "documents_expected_results": [
                    ({"pizza": "yolo"}, None),
                    ({"pizza": None}, None),
                    ({}, None),
                    ({"pizza": ["yo", "lo"]}, None),
                ],
            },
            {
                "name": "multiple nullable",
                "properties": {"pizza": Keyword(multiple=True)},
                "documents_expected_results": [
                    ({"pizza": "yolo"}, "Field <pizza> should be a array"),
                    ({"pizza": None}, None),
                    ({}, None),
                    ({"pizza": ["yo", "lo"]}, None),
                ],
            },
            {
                "name": "multiple non nullable",
                "properties": {"pizza": Keyword(multiple=True, nullable=False)},
                "documents_expected_results": [
                    ({"pizza": "yolo"}, "Field <pizza> should be a array"),
                    ({"pizza": None}, "Field <pizza> cannot be null"),
                    ({}, "Field <pizza> cannot be null"),
                    ({"pizza": ["yo", "lo"]}, None),
                ],
            },
            {
                "name": "non multiple",
                "properties": {"pizza": Keyword(multiple=False)},
                "documents_expected_results": [
                    ({"pizza": "yolo"}, None),
                    ({"pizza": None}, None),
                    ({}, None),
                    ({"pizza": ["yo", "lo"]}, "Field <pizza> should not be an array"),
                ],
            },
            {
                "name": "nested multiple non nullable",
                "properties": {
                    "some_good": Object(
                        properties={"pizza": Keyword(multiple=True, nullable=False)}
                    )
                },
                "documents_expected_results": [
                    (
                        {"some_good": {"pizza": "yolo"}},
                        "Field <some_good.pizza> should be a array",
                    ),
                    (
                        {"some_good": {"pizza": None}},
                        "Field <some_good.pizza> cannot be null",
                    ),
                    ({}, "Field <some_good.pizza> cannot be null"),
                    ({"some_good": {"pizza": ["yo", "lo"]}}, None),
                ],
            },
        ]
        for tt in tts:
            mappings = Mappings(properties=tt["properties"])
            for doc, expected_error in tt["documents_expected_results"]:
                if expected_error:
                    with self.assertRaises(ValueError, msg=tt["name"]) as e:
                        mappings.validate_document(doc)
                    self.assertEqual(e.exception.args, (expected_error,), tt["name"])
                else:
                    # must not raise error
                    mappings.validate_document(doc)
