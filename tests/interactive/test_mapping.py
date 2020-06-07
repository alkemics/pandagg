#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    # python 2
    from StringIO import StringIO
except ImportError:
    # python 3
    from io import StringIO

import sys

from mock import Mock
from unittest import TestCase

from pandagg.mapping import Keyword, Text, Nested, Object, Integer
from pandagg.tree.mapping import Mapping
from pandagg.interactive._field_agg_factory import field_classes_per_name
from pandagg.interactive.mapping import IMapping

from tests.testing_samples.mapping_example import MAPPING


class IMappingTestCase(TestCase):
    def test_mapping_aggregations(self):
        mapping_tree = Mapping(MAPPING)
        # check that leaves are expanded, based on 'field_name' attribute of nodes
        mapping = IMapping(mapping_tree, depth=1)
        for field_name in (
            "classification_type",
            "date",
            "global_metrics",
            "id",
            "language",
            "local_metrics",
            "workflow",
        ):
            self.assertTrue(hasattr(mapping, field_name))

        dataset = mapping.global_metrics.dataset
        self.assertEqual(
            dataset.__repr__(),
            """<Mapping subpart: global_metrics.dataset>
dataset                                                      {Object}
├── nb_classes                                                Integer
└── support_train                                             Integer
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

        mapping_tree = Mapping(mapping_dict)
        client_mock = Mock(spec=["search"])
        index_name = "classification_report_index_name"

        # from dict
        im1 = IMapping(mapping_dict, client=client_mock, index=index_name)
        # from tree
        im2 = IMapping(mapping_tree, client=client_mock, index=index_name)

        # from nodes
        im3 = IMapping(
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
            dynamic=False,
            client=client_mock,
            index=index_name,
        )
        for i, m in enumerate((im1, im2, im3)):
            self.assertEqual(m._tree.to_dict(), mapping_dict, "failed at m%d" % (i + 1))
            self.assertEqual(m._index, index_name)
            self.assertIs(m._client, client_mock)

    def test_quick_agg(self):
        """Check that when reaching leaves (fields without children) leaves have the "a" attribute that can generate
        aggregations on that field type.
        """
        client_mock = Mock(spec=["search"])
        es_response_mock = {
            "_shards": {"failed": 0, "successful": 135, "total": 135},
            "aggregations": {
                "terms_agg": {
                    "buckets": [
                        {"doc_count": 25, "key": 1},
                        {"doc_count": 50, "key": 2},
                    ],
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 4,
                }
            },
            "hits": {"hits": [], "max_score": 0.0, "total": 300},
            "timed_out": False,
            "took": 30,
        }
        client_mock.search = Mock(return_value=es_response_mock)

        mapping_tree = Mapping(MAPPING)
        client_bound_mapping = IMapping(
            mapping_tree, client=client_mock, index="classification_report_index_name",
        )

        workflow_field = client_bound_mapping.workflow
        self.assertTrue(hasattr(workflow_field, "a"))
        # workflow type is keyword
        self.assertIsInstance(workflow_field.a, field_classes_per_name["keyword"])

        response = workflow_field.a.terms(
            size=20,
            raw_output=True,
            query={"term": {"classification_type": "multiclass"}},
        )
        self.assertEqual(
            response,
            [(1, {"doc_count": 25, "key": 1}), (2, {"doc_count": 50, "key": 2}),],
        )
        client_mock.search.assert_called_once()
        client_mock.search.assert_called_with(
            body={
                "aggs": {"terms_agg": {"terms": {"field": "workflow", "size": 20}}},
                "size": 0,
                "query": {"term": {"classification_type": "multiclass"}},
            },
            index="classification_report_index_name",
        )

    def test_quick_agg_nested(self):
        """Check that when reaching leaves (fields without children) leaves have the "a" attribute that can generate
        aggregations on that field type, applying nested if necessary.
        """
        client_mock = Mock(spec=["search"])
        es_response_mock = {
            "_shards": {"failed": 0, "successful": 135, "total": 135},
            "aggregations": {"local_metrics": {"avg_agg": {"value": 23},},},
            "hits": {"hits": [], "max_score": 0.0, "total": 300},
            "timed_out": False,
            "took": 30,
        }
        client_mock.search = Mock(return_value=es_response_mock)

        mapping_tree = Mapping(MAPPING)
        client_bound_mapping = IMapping(
            mapping_tree, client=client_mock, index="classification_report_index_name",
        )

        local_train_support = client_bound_mapping.local_metrics.dataset.support_train
        self.assertTrue(hasattr(local_train_support, "a"))
        self.assertIsInstance(local_train_support.a, field_classes_per_name["integer"])

        response = local_train_support.a.avg(
            size=20,
            raw_output=True,
            query={"term": {"classification_type": "multiclass"}},
        )
        self.assertEqual(
            response, [(None, {"value": 23}),],
        )
        client_mock.search.assert_called_once()
        client_mock.search.assert_called_with(
            body={
                "aggs": {
                    "local_metrics": {
                        "nested": {"path": "local_metrics"},
                        "aggs": {
                            "avg_agg": {
                                "avg": {
                                    "field": "local_metrics.dataset.support_train",
                                    "size": 20,
                                }
                            }
                        },
                    }
                },
                "size": 0,
                "query": {"term": {"classification_type": "multiclass"}},
            },
            index="classification_report_index_name",
        )
