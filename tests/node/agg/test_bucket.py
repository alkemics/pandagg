from pandagg.node.aggs import (
    Terms,
    Filter,
    Filters,
    DateHistogram,
    Nested,
    Range,
    Histogram,
)

from tests import PandaggTestCase


class BucketAggNodesTestCase(PandaggTestCase):
    def test_terms(self):
        es_raw_response = {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [
                {"key": "electronic", "doc_count": 6},
                {"key": "rock", "doc_count": 3},
                {"key": "jazz", "doc_count": 2},
            ],
        }
        # test extract_buckets
        buckets_iterator = Terms("name", "field").extract_buckets(es_raw_response)
        self.assertTrue(hasattr(buckets_iterator, "__iter__"))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                ("electronic", {"doc_count": 6, "key": "electronic"}),
                ("rock", {"doc_count": 3, "key": "rock"}),
                ("jazz", {"doc_count": 2, "key": "jazz"}),
            ],
        )

        # test extract bucket value
        self.assertEqual(
            Terms("name", "field").extract_bucket_value(
                {"doc_count": 6, "key": "electronic"}
            ),
            6,
        )
        self.assertEqual(
            Terms("name", "field").extract_bucket_value(
                {"doc_count": 3, "key": "rock"}
            ),
            3,
        )
        self.assertEqual(
            Terms("name", "field").extract_bucket_value(
                {"doc_count": 2, "key": "jazz"}
            ),
            2,
        )

        # test query dict
        self.assertEqual(
            Terms(field="field", size=10).to_dict(),
            {"terms": {"field": "field", "size": 10}},
        )

    def test_filter(self):
        es_raw_response = {"doc_count": 12, "sub_aggs": {}}
        # test extract_buckets
        buckets_iterator = Filter({}).extract_buckets(es_raw_response)
        self.assertTrue(hasattr(buckets_iterator, "__iter__"))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                (None, {"doc_count": 12, "sub_aggs": {}})
            ],
        )

        # test extract bucket value
        self.assertEqual(Filter({}).extract_bucket_value({"doc_count": 12}), 12)

        # test get_filter
        filter_agg = Filter(filter={"term": {"some_path": 1}})
        self.assertEqual(filter_agg.to_dict(), {"filter": {"term": {"some_path": 1}}})

    def test_nested(self):
        es_raw_response = {"doc_count": 12, "sub_aggs": {}}
        # test extract_buckets
        buckets_iterator = Nested(path="some_nested").extract_buckets(es_raw_response)
        self.assertTrue(hasattr(buckets_iterator, "__iter__"))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                (None, {"doc_count": 12, "sub_aggs": {}})
            ],
        )

        # test extract bucket value
        self.assertEqual(Nested.extract_bucket_value({"doc_count": 12}), 12)

        # test get_filter
        nested_agg = Nested(path="nested_path")
        self.assertEqual(nested_agg.to_dict(), {"nested": {"path": "nested_path"}})

    def test_filters(self):
        es_raw_response = {
            "buckets": {
                "my_first_filter": {"doc_count": 1},
                "my_second_filter": {"doc_count": 2},
            }
        }
        # test extract_buckets
        buckets_iterator = Filters(None).extract_buckets(es_raw_response)
        self.assertTrue(hasattr(buckets_iterator, "__iter__"))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                ("my_first_filter", {"doc_count": 1}),
                ("my_second_filter", {"doc_count": 2}),
            ],
        )

        # test extract bucket value
        self.assertEqual(Filters(None).extract_bucket_value({"doc_count": 1}), 1)
        self.assertEqual(Filters(None).extract_bucket_value({"doc_count": 2}), 2)

        # test get_filter
        filters_agg = Filters(
            filters={
                "first_bucket": {"term": {"some_path": 1}},
                "second_bucket": {"term": {"some_path": 2}},
            },
            other_bucket=True,
            other_bucket_key="neither_one_nor_two",
        )
        self.assertQueryEqual(
            filters_agg.to_dict(),
            {
                "filters": {
                    "filters": {
                        "first_bucket": {"term": {"some_path": 1}},
                        "second_bucket": {"term": {"some_path": 2}},
                    },
                    "other_bucket": True,
                    "other_bucket_key": "neither_one_nor_two",
                }
            },
        )

    def test_range(self):
        query = {
            "range": {
                "field": "price",
                "ranges": [
                    {"to": 100.0},
                    {"from": 100.0, "to": 200.0},
                    {"from": 200.0},
                ],
            }
        }
        es_raw_response = {
            "buckets": [
                {"key": "*-100.0", "to": 100.0, "doc_count": 2},
                {"key": "100.0-200.0", "from": 100.0, "to": 200.0, "doc_count": 2},
                {"key": "200.0-*", "from": 200.0, "doc_count": 3},
            ]
        }

        range_agg = Range(
            field="price",
            ranges=[{"to": 100.0}, {"from": 100.0, "to": 200.0}, {"from": 200.0}],
        )
        self.assertEqual(range_agg.to_dict(), query)

        buckets_iterator = range_agg.extract_buckets(es_raw_response)
        self.assertTrue(hasattr(buckets_iterator, "__iter__"))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                ("*-100.0", {"doc_count": 2, "key": "*-100.0", "to": 100.0}),
                (
                    "100.0-200.0",
                    {"doc_count": 2, "from": 100.0, "key": "100.0-200.0", "to": 200.0},
                ),
                ("200.0-*", {"doc_count": 3, "from": 200.0, "key": "200.0-*"}),
            ],
        )

    def test_range_keyed_response(self):
        query = {
            "range": {
                "field": "price",
                "keyed": True,
                "ranges": [
                    {"to": 100.0},
                    {"from": 100.0, "to": 200.0},
                    {"from": 200.0},
                ],
            }
        }
        es_raw_response = {
            "buckets": {
                "*-100.0": {"to": 100.0, "doc_count": 2},
                "100.0-200.0": {"from": 100.0, "to": 200.0, "doc_count": 2},
                "200.0-*": {"from": 200.0, "doc_count": 3},
            }
        }

        range_agg = Range(
            field="price",
            keyed=True,
            ranges=[{"to": 100.0}, {"from": 100.0, "to": 200.0}, {"from": 200.0}],
        )
        self.assertEqual(range_agg.to_dict(), query)

        buckets_iterator = range_agg.extract_buckets(es_raw_response)
        self.assertTrue(hasattr(buckets_iterator, "__iter__"))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                ("*-100.0", {"doc_count": 2, "to": 100.0}),
                ("100.0-200.0", {"doc_count": 2, "from": 100.0, "to": 200.0}),
                ("200.0-*", {"doc_count": 3, "from": 200.0}),
            ],
        )

    def test_histogram(self):
        query = {"histogram": {"field": "price", "interval": 50}}
        es_raw_response = {
            "buckets": [
                {"key": 0.0, "doc_count": 1},
                {"key": 50.0, "doc_count": 1},
                {"key": 100.0, "doc_count": 0},
                {"key": 150.0, "doc_count": 2},
                {"key": 200.0, "doc_count": 3},
            ]
        }

        hist_agg = Histogram(field="price", interval=50)
        self.assertEqual(hist_agg.to_dict(), query)

        buckets_iterator = hist_agg.extract_buckets(es_raw_response)
        self.assertTrue(hasattr(buckets_iterator, "__iter__"))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                (0.0, {"doc_count": 1, "key": 0.0}),
                (50.0, {"doc_count": 1, "key": 50.0}),
                (100.0, {"doc_count": 0, "key": 100.0}),
                (150.0, {"doc_count": 2, "key": 150.0}),
                (200.0, {"doc_count": 3, "key": 200.0}),
            ],
        )

    def test_date_histogram_key_as_string(self):
        es_raw_response = {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [
                {"key_as_string": "2018-01-01", "key": 1514764800000, "doc_count": 6},
                {"key_as_string": "2018-01-08", "key": 1515369600000, "doc_count": 3},
            ],
        }

        date_hist_agg = DateHistogram(
            name="name", field="field", interval="1w", key_as_string=True
        )
        buckets_iterator = date_hist_agg.extract_buckets(es_raw_response)

        self.assertTrue(hasattr(buckets_iterator, "__iter__"))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key_as_string -> bucket
                (
                    "2018-01-01",
                    {
                        "doc_count": 6,
                        "key": 1514764800000,
                        "key_as_string": "2018-01-01",
                    },
                ),
                (
                    "2018-01-08",
                    {
                        "doc_count": 3,
                        "key": 1515369600000,
                        "key_as_string": "2018-01-08",
                    },
                ),
            ],
        )

        # not using key as string (regular key)
        buckets_iterator = DateHistogram(
            name="name", field="field", interval="1w", key_as_string=False
        ).extract_buckets(es_raw_response)

        self.assertTrue(hasattr(buckets_iterator, "__iter__"))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                (
                    1514764800000,
                    {
                        "doc_count": 6,
                        "key": 1514764800000,
                        "key_as_string": "2018-01-01",
                    },
                ),
                (
                    1515369600000,
                    {
                        "doc_count": 3,
                        "key": 1515369600000,
                        "key_as_string": "2018-01-08",
                    },
                ),
            ],
        )
