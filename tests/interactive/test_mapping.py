#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pandagg import Search

try:
    # python 2
    from StringIO import StringIO
except ImportError:
    # python 3
    from io import StringIO

import sys

from unittest import TestCase

from pandagg.mappings import Keyword, Text, Nested, Object, Integer
from pandagg.tree.mappings import Mappings
from pandagg.interactive._field_agg_factory import field_classes_per_name
from pandagg.interactive.mappings import IMappings

from tests.testing_samples.mapping_example import MAPPINGS


class IMappingTestCase(TestCase):
    def test_mapping_aggregations(self):
        mapping_tree = Mappings(**MAPPINGS)
        # check that leaves are expanded, based on 'field_name' attribute of nodes
        mappings = IMappings(mapping_tree, depth=1)
        for field_name in (
            "classification_type",
            "date",
            "global_metrics",
            "id",
            "language",
            "local_metrics",
            "workflow",
        ):
            self.assertTrue(hasattr(mappings, field_name))

        dataset = mappings.global_metrics.dataset
        self.assertEqual(
            dataset.__repr__(),
            """<Mappings subpart: global_metrics.dataset>
                                                    {Object}
├── nb_classes                                       Integer
└── support_train                                    Integer
""",
        )
        # capture print statement
        captured_output = StringIO()
        sys.stdout = captured_output
        # what triggers print
        dataset()
        # restore stout
        sys.stdout = sys.__stdout__
        self.assertEqual(
            captured_output.getvalue(),
            """{
  "dynamic": false,
  "properties": {
    "nb_classes": {
      "type": "integer"
    },
    "support_train": {
      "type": "integer"
    }
  }
}
""",
        )

    def test_imapping_init(self):

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

        mapping_tree = Mappings(**mapping_dict)
        client_mock = {}
        index_name = "classification_report_index_name"

        # from dict
        im1 = IMappings(
            Mappings(**mapping_dict), client=client_mock, index=[index_name]
        )
        # from tree
        im2 = IMappings(mapping_tree, client=client_mock, index=[index_name])

        # from nodes
        im3 = IMappings(
            mappings=Mappings(
                **{
                    "properties": {
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
                    "dynamic": False,
                }
            ),
            client=client_mock,
            index=[index_name],
        )
        for i, m in enumerate((im1, im2, im3)):
            self.assertEqual(m._tree.to_dict(), mapping_dict, "failed at m%d" % (i + 1))
            self.assertEqual(m._index, [index_name])
            self.assertIs(m._client, client_mock)

    def test_quick_agg(self):
        """Check that when reaching leaves (fields without children) leaves have the "a" attribute that can generate
        aggregations on that field type.
        """
        client_mock = {}

        mapping_tree = Mappings(**MAPPINGS)
        client_bound_mapping = IMappings(
            mapping_tree, client=client_mock, index=["classification_report_index_name"]
        )

        workflow_field = client_bound_mapping.workflow
        self.assertTrue(hasattr(workflow_field, "a"))
        # workflow type is keyword
        self.assertIsInstance(workflow_field.a, field_classes_per_name["keyword"])

        search = workflow_field.a.terms(size=20)
        self.assertIsInstance(search, Search)
        self.assertTrue(search._repr_auto_execute)
        self.assertEqual(
            search._aggs.to_dict(),
            {"terms_workflow": {"terms": {"field": "workflow", "size": 20}}},
        )
        self.assertEqual(search._index, ["classification_report_index_name"])
        self.assertIs(search._using, client_mock)
