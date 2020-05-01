#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase

import pandas as pd

from pandagg.search import Search
from pandagg.tree.response import AggsResponseTree
from pandagg.response import Response, Hits, Hit, Aggregations
from pandagg.tree.aggs import Aggs

import tests.testing_samples.data_sample as sample
from pandagg.utils import equal_queries
from tests.testing_samples.mapping_example import MAPPING


class ResponseTestCase(TestCase):
    def test_hit(self):
        h = Hit(
            {
                "_index": "my_index_01",
                "_type": "_doc",
                "_id": "1",
                "_score": 1.0,
                "_source": {"field_23": 1},
            }
        )
        self.assertEqual(h._index, "my_index_01")
        self.assertEqual(h._type, "_doc")
        self.assertEqual(h._id, "1")
        self.assertEqual(h._score, 1.0)
        self.assertEqual(h._source, {"field_23": 1})

        self.assertEqual(h.__repr__(), "<Hit 1> score=1.00")

    def test_hits(self):
        hits = Hits(
            {
                "total": {"value": 34, "relation": "eq"},
                "max_score": 0.0,
                "hits": [
                    {
                        "_index": "my_index_01",
                        "_type": "_doc",
                        "_id": "1",
                        "_score": 1.0,
                        "_source": {"field_23": 1},
                    },
                    {
                        "_index": "my_index_01",
                        "_type": "_doc",
                        "_id": "2",
                        "_score": 0.2,
                        "_source": {"field_23": 2},
                    },
                ],
            }
        )
        self.assertEqual(hits.total, {"value": 34, "relation": "eq"})
        self.assertEqual(hits._total_repr(), "34")
        self.assertEqual(len(hits), 2)
        self.assertEqual(len(hits.hits), 2)
        for h in hits.hits:
            self.assertIsInstance(h, Hit)
        self.assertEqual(hits.__repr__(), "<Hits> total: 34, contains 2 hits")

    def test_response(self):
        r = Response(
            {
                "took": 42,
                "timed_out": False,
                "_shards": {"total": 10, "successful": 10, "skipped": 0, "failed": 0},
                "hits": {
                    "total": {"value": 34, "relation": "eq"},
                    "max_score": 0.0,
                    "hits": [
                        {
                            "_index": "my_index_01",
                            "_type": "_doc",
                            "_id": "1",
                            "_score": 1.0,
                            "_source": {"field_23": 1},
                        },
                        {
                            "_index": "my_index_01",
                            "_type": "_doc",
                            "_id": "2",
                            "_score": 0.2,
                            "_source": {"field_23": 2},
                        },
                    ],
                },
            },
            Search(),
        )
        self.assertEqual(r.took, 42)
        self.assertEqual(r.timed_out, False)
        self.assertEqual(r.success, True)
        self.assertEqual(
            r._shards, {"total": 10, "successful": 10, "skipped": 0, "failed": 0}
        )
        self.assertIsInstance(r.hits, Hits)
        self.assertEqual(
            r.__repr__(),
            "<Response> took 42ms, success: True, total result 34, contains 2 hits",
        )


class AggregationsResponseTestCase(TestCase):
    def test_parse_as_tree(self, *_):
        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mapping=MAPPING)
        response = Aggregations(
            data=sample.ES_AGG_RESPONSE,
            aggs=my_agg,
            index=None,
            client=None,
            query=None,
        ).serialize_as_tree()
        self.assertIsInstance(response, AggsResponseTree)
        self.assertEqual(response.__str__(), sample.EXPECTED_RESPONSE_TREE_REPR)

    def test_normalize_buckets(self):
        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mapping=MAPPING)
        response = Aggregations(
            data=sample.ES_AGG_RESPONSE,
            aggs=my_agg,
            index=None,
            client=None,
            query=None,
        ).serialize_as_normalized()
        self.assertTrue(equal_queries(response, sample.EXPECTED_NORMALIZED_RESPONSE,))

    def test_parse_as_tabular(self):
        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mapping=MAPPING)
        index, index_names, values = Aggregations(
            data=sample.ES_AGG_RESPONSE,
            aggs=my_agg,
            index=None,
            client=None,
            query=None,
        ).serialize_as_tabular()
        self.assertEqual(
            index_names, ["classification_type", "global_metrics.field.name"]
        )
        self.assertEqual(len(index), len(values))
        self.assertEqual(len(index), 10)
        self.assertEqual(index, sample.EXPECTED_TABULAR_INDEX)
        self.assertEqual(values, sample.EXPECTED_TABULAR_VALUES)

    def test_parse_as_dataframe(self):
        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mapping=MAPPING)
        df = Aggregations(
            data=sample.ES_AGG_RESPONSE,
            aggs=my_agg,
            index=None,
            client=None,
            query=None,
        ).serialize_as_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(
            set(df.index.names), {"classification_type", "global_metrics.field.name"}
        )
        self.assertEqual(
            set(df.columns), {"avg_f1_micro", "avg_nb_classes", "doc_count"}
        )
        self.assertEqual(df.shape, (len(sample.EXPECTED_TABULAR_INDEX), 3))
