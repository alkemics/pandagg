#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase

from mock import patch

from pandagg.exceptions import AbsentMappingFieldError
from pandagg.node.mapping.abstract import Field
from pandagg.mapping import Keyword, Object, Text, Nested, Integer, Mapping
from tests.testing_samples.mapping_example import MAPPING, EXPECTED_MAPPING_TREE_REPR


class MappingTreeTestCase(TestCase):
    """All tree logic is tested in utils.
    Here, check that:
     - a dict mapping is correctly parsed into a tree,
     - it has the right representation.
    """

    def test_keyword_with_fields(self):
        unnamed_field = Keyword(fields={"searchable": {"type": "text"}}, fielddata=True)
        self.assertEqual(unnamed_field.body, {"fielddata": True})
        self.assertEqual(unnamed_field.fields, {"searchable": {"type": "text"}})
        named_field = unnamed_field.to_named_field(name="path.to.field")
        self.assertIsInstance(named_field, Field)
        self.assertEqual(named_field.KEY, "keyword")
        self.assertEqual(named_field.body, {"fielddata": True, "type": "keyword"})

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

        m1 = Mapping(mapping_dict)

        m2 = Mapping(
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

        m3 = Mapping(m1)

        expected_repr = """<Mapping>
_
├── classification_type                                       Keyword
│   └── raw                                                 ~ Text
└── local_metrics                                            [Nested]
    └── dataset                                              {Object}
        ├── support_test                                      Integer
        └── support_train                                     Integer
"""
        for i, m in enumerate((m1, m2, m3)):
            self.assertEqual(m.__repr__(), expected_repr, "failed at m%d" % (i + 1))
            self.assertEqual(m.to_dict(), mapping_dict, "failed at m%d" % (i + 1))

    def test_mapping_node(self):
        m = Mapping(None)
        self.assertTrue(m.is_empty())

    def test_parse_tree_from_dict(self):
        mapping_tree = Mapping(MAPPING)

        self.assertEqual(mapping_tree.__str__(), EXPECTED_MAPPING_TREE_REPR)

    def test_nesteds_applied_at_field(self):
        mapping_tree = Mapping(MAPPING)

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

    @patch("uuid.uuid4")
    def test_resolve_path_to_id(self, uuid_mock):
        uuid_mock.side_effect = range(100)
        mapping_tree = Mapping(MAPPING)
        # do not resolve
        self.assertEqual(
            mapping_tree.resolve_path_to_id("global_metrics.non_existing_field"),
            "global_metrics.non_existing_field",
        )
        # resolve
        resolved = mapping_tree.resolve_path_to_id("classification_type")
        self.assertIn("classification_type", resolved)
        self.assertIn(resolved, mapping_tree)

        resolved = mapping_tree.resolve_path_to_id("local_metrics.dataset.support_test")
        self.assertIn("support_test", resolved)
        self.assertIn(resolved, mapping_tree)

    def test_mapping_type_of_field(self):
        mapping_tree = Mapping(MAPPING)
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
        mapping_tree = Mapping(MAPPING)
        # get node by path syntax
        node = mapping_tree.get("local_metrics.dataset.support_test")
        self.assertIsInstance(node, Field)
        self.assertEqual(node.name, "support_test")
        self.assertEqual(
            mapping_tree.node_path(node.identifier),
            "local_metrics.dataset.support_test",
        )
