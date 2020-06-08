#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tests import PandaggTestCase

import pandas as pd

from pandagg.search import Search
from pandagg.tree.response import AggsResponseTree
from pandagg.response import Response, Hits, Hit, Aggregations
from pandagg.tree.aggs.aggs import Aggs

import tests.testing_samples.data_sample as sample
from pandagg.utils import ordered
from tests.testing_samples.mapping_example import MAPPING


class ResponseTestCase(PandaggTestCase):
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

        hits = list(iter(r))
        self.assertEqual(len(hits), 2)
        for hit in hits:
            self.assertIsInstance(hit, Hit)


class AggregationsResponseTestCase(PandaggTestCase):
    def test_parse_as_tree(self, *_):
        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mapping=MAPPING)
        response = Aggregations(
            data=sample.ES_AGG_RESPONSE,
            aggs=my_agg,
            index=None,
            client=None,
            query=None,
        ).to_tree()
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
        ).to_normalized()
        self.assertEqual(
            ordered(response), ordered(sample.EXPECTED_NORMALIZED_RESPONSE)
        )

    def test_parse_as_tabular(self):
        # with single agg at root
        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mapping=MAPPING)
        index_names, index_values = Aggregations(
            data=sample.ES_AGG_RESPONSE,
            aggs=my_agg,
            index=None,
            client=None,
            query=None,
        ).to_tabular(index_orient=True)

        self.assertEqual(
            index_names, ["classification_type", "global_metrics.field.name"]
        )
        self.assertEqual(
            index_values,
            {
                ("multilabel", "ispracticecompatible"): {
                    "avg_f1_micro": 0.72,
                    "avg_nb_classes": 18.71,
                    "doc_count": 128,
                },
                ("multilabel", "gpc"): {
                    "avg_f1_micro": 0.95,
                    "avg_nb_classes": 183.21,
                    "doc_count": 119,
                },
                ("multilabel", "preservationmethods"): {
                    "avg_f1_micro": 0.8,
                    "avg_nb_classes": 9.97,
                    "doc_count": 76,
                },
                ("multiclass", "kind"): {
                    "avg_f1_micro": 0.89,
                    "avg_nb_classes": 206.5,
                    "doc_count": 370,
                },
                ("multiclass", "gpc"): {
                    "avg_f1_micro": 0.93,
                    "avg_nb_classes": 211.12,
                    "doc_count": 198,
                },
            },
        )

        # index_orient = False
        index_names, index_values = Aggregations(
            data=sample.ES_AGG_RESPONSE,
            aggs=my_agg,
            index=None,
            client=None,
            query=None,
        ).to_tabular(index_orient=False)

        self.assertEqual(
            index_names, ["classification_type", "global_metrics.field.name"]
        )
        self.assertEqual(
            index_values,
            [
                {
                    "avg_f1_micro": 0.72,
                    "avg_nb_classes": 18.71,
                    "classification_type": "multilabel",
                    "doc_count": 128,
                    "global_metrics.field.name": "ispracticecompatible",
                },
                {
                    "avg_f1_micro": 0.95,
                    "avg_nb_classes": 183.21,
                    "classification_type": "multilabel",
                    "doc_count": 119,
                    "global_metrics.field.name": "gpc",
                },
                {
                    "avg_f1_micro": 0.8,
                    "avg_nb_classes": 9.97,
                    "classification_type": "multilabel",
                    "doc_count": 76,
                    "global_metrics.field.name": "preservationmethods",
                },
                {
                    "avg_f1_micro": 0.89,
                    "avg_nb_classes": 206.5,
                    "classification_type": "multiclass",
                    "doc_count": 370,
                    "global_metrics.field.name": "kind",
                },
                {
                    "avg_f1_micro": 0.93,
                    "avg_nb_classes": 211.12,
                    "classification_type": "multiclass",
                    "doc_count": 198,
                    "global_metrics.field.name": "gpc",
                },
            ],
        )

    def test_parse_as_tabular_multiple_roots(self):
        # with multiple aggs at root
        my_agg = Aggs(
            {
                "classification_type": {"terms": {"field": "classification_type"}},
                "avg_f1_score": {
                    "avg": {"field": "global_metrics.performance.test.micro.f1_score"}
                },
            }
        )

        raw_response = {
            "classification_type": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0,
                "buckets": [
                    {"key": "multiclass", "doc_count": 439},
                    {"key": "multilabel", "doc_count": 433},
                ],
            },
            "avg_f1_score": {"value": 0.815},
        }
        index_names, index_values = Aggregations(
            data=raw_response, aggs=my_agg, index=None, client=None, query=None,
        ).to_tabular(index_orient=True, expand_sep=" || ")

        self.assertEqual(index_names, [])
        self.assertEqual(
            index_values,
            {
                (): {
                    "avg_f1_score": 0.815,
                    "classification_type || multiclass": 439,
                    "classification_type || multilabel": 433,
                }
            },
        )

    def test_parse_as_dataframe(self):
        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mapping=MAPPING)
        df = Aggregations(
            data=sample.ES_AGG_RESPONSE,
            aggs=my_agg,
            index=None,
            client=None,
            query=None,
        ).to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(
            set(df.index.names), {"classification_type", "global_metrics.field.name"}
        )
        self.assertEqual(
            set(df.columns), {"avg_f1_micro", "avg_nb_classes", "doc_count"}
        )

        self.assertEqual(
            df.to_dict(orient="index"),
            {
                ("multiclass", "gpc"): {
                    "avg_f1_micro": 0.93,
                    "avg_nb_classes": 211.12,
                    "doc_count": 198,
                },
                ("multiclass", "kind"): {
                    "avg_f1_micro": 0.89,
                    "avg_nb_classes": 206.5,
                    "doc_count": 370,
                },
                ("multilabel", "gpc"): {
                    "avg_f1_micro": 0.95,
                    "avg_nb_classes": 183.21,
                    "doc_count": 119,
                },
                ("multilabel", "ispracticecompatible"): {
                    "avg_f1_micro": 0.72,
                    "avg_nb_classes": 18.71,
                    "doc_count": 128,
                },
                ("multilabel", "preservationmethods"): {
                    "avg_f1_micro": 0.8,
                    "avg_nb_classes": 9.97,
                    "doc_count": 76,
                },
            },
        )

    def test_grouping_agg(self):
        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mapping=MAPPING)
        agg_response = Aggregations(
            data=sample.ES_AGG_RESPONSE,
            aggs=my_agg,
            index=None,
            client=None,
            query=None,
        )

        # none provided
        self.assertEqual(
            agg_response._grouping_agg().identifier, "global_metrics.field.name"
        )
        # fake provided
        with self.assertRaises(ValueError):
            agg_response._grouping_agg("yolo")
        # not bucket provided
        with self.assertRaises(ValueError):
            agg_response._grouping_agg("avg_f1_micro")
        # real provided
        self.assertEqual(
            agg_response._grouping_agg("global_metrics.field.name").identifier,
            "global_metrics.field.name",
        )
