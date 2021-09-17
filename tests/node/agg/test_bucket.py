from pandagg.node.aggs import (
    Terms,
    Filter,
    Filters,
    DateHistogram,
    Nested,
    Range,
    Histogram,
    GeoDistance,
    GeoHashGrid,
    AdjacencyMatrix,
    AutoDateHistogram,
    VariableWidthHistogram,
    SignificantTerms,
    RareTerms,
    GeoTileGrid,
    IPRange,
    Sampler,
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


def test_geo_distance():
    # regular (not keyed)
    agg = GeoDistance(
        field="location",
        origin="52.3760, 4.894",
        unit="km",
        distance_type="plane",
        ranges=[{"to": 100}, {"from": 100, "to": 300}, {"from": 300}],
    )
    assert agg.to_dict() == {
        "geo_distance": {
            "distance_type": "plane",
            "field": "location",
            "origin": "52.3760, 4.894",
            "ranges": [{"to": 100}, {"from": 100, "to": 300}, {"from": 300}],
            "unit": "km",
        }
    }

    raw_response = {
        "buckets": [
            {"key": "*-100000.0", "from": 0.0, "to": 100000.0, "doc_count": 3},
            {
                "key": "100000.0-300000.0",
                "from": 100000.0,
                "to": 300000.0,
                "doc_count": 1,
            },
            {"key": "300000.0-*", "from": 300000.0, "doc_count": 2},
        ]
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        (
            "*-100000.0",
            {"doc_count": 3, "from": 0.0, "key": "*-100000.0", "to": 100000.0},
        ),
        (
            "100000.0-300000.0",
            {
                "doc_count": 1,
                "from": 100000.0,
                "key": "100000.0-300000.0",
                "to": 300000.0,
            },
        ),
        ("300000.0-*", {"doc_count": 2, "from": 300000.0, "key": "300000.0-*"}),
    ]

    # keyed
    agg = GeoDistance(
        field="location",
        origin="52.3760, 4.894",
        unit="km",
        distance_type="plane",
        ranges=[{"to": 100}, {"from": 100, "to": 300}, {"from": 300}],
        keyed=True,
    )
    assert agg.to_dict() == {
        "geo_distance": {
            "distance_type": "plane",
            "field": "location",
            "origin": "52.3760, 4.894",
            "ranges": [{"to": 100}, {"from": 100, "to": 300}, {"from": 300}],
            "unit": "km",
            "keyed": True,
        }
    }

    raw_response = {
        "buckets": {
            "2015-01-01": {
                "key_as_string": "2015-01-01",
                "key": 1420070400000,
                "doc_count": 3,
            },
            "2015-02-01": {
                "key_as_string": "2015-02-01",
                "key": 1422748800000,
                "doc_count": 2,
            },
            "2015-03-01": {
                "key_as_string": "2015-03-01",
                "key": 1425168000000,
                "doc_count": 2,
            },
        }
    }

    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        (
            "2015-01-01",
            {"doc_count": 3, "key": 1420070400000, "key_as_string": "2015-01-01"},
        ),
        (
            "2015-02-01",
            {"doc_count": 2, "key": 1422748800000, "key_as_string": "2015-02-01"},
        ),
        (
            "2015-03-01",
            {"doc_count": 2, "key": 1425168000000, "key_as_string": "2015-03-01"},
        ),
    ]


def test_geo_hash_grid():
    agg = GeoHashGrid(field="location", precision=3)
    assert agg.to_dict() == {"geohash_grid": {"field": "location", "precision": 3}}

    raw_response = {
        "buckets": [
            {"key": "u17", "doc_count": 3},
            {"key": "u09", "doc_count": 2},
            {"key": "u15", "doc_count": 1},
        ]
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        ("u17", {"doc_count": 3, "key": "u17"}),
        ("u09", {"doc_count": 2, "key": "u09"}),
        ("u15", {"doc_count": 1, "key": "u15"}),
    ]


def test_adjacency_matrix():
    agg = AdjacencyMatrix(
        filters={
            "grpA": {"terms": {"accounts": ["hillary", "sidney"]}},
            "grpB": {"terms": {"accounts": ["donald", "mitt"]}},
            "grpC": {"terms": {"accounts": ["vladimir", "nigel"]}},
        }
    )
    assert agg.to_dict() == {
        "adjacency_matrix": {
            "filters": {
                "grpA": {"terms": {"accounts": ["hillary", "sidney"]}},
                "grpB": {"terms": {"accounts": ["donald", "mitt"]}},
                "grpC": {"terms": {"accounts": ["vladimir", "nigel"]}},
            }
        }
    }

    raw_response = {
        "buckets": [
            {"key": "grpA", "doc_count": 2},
            {"key": "grpA&grpB", "doc_count": 1},
            {"key": "grpB", "doc_count": 2},
            {"key": "grpB&grpC", "doc_count": 1},
            {"key": "grpC", "doc_count": 1},
        ]
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        ("grpA", {"doc_count": 2, "key": "grpA"}),
        ("grpA&grpB", {"doc_count": 1, "key": "grpA&grpB"}),
        ("grpB", {"doc_count": 2, "key": "grpB"}),
        ("grpB&grpC", {"doc_count": 1, "key": "grpB&grpC"}),
        ("grpC", {"doc_count": 1, "key": "grpC"}),
    ]


def test_auto_date_histogram():
    agg = AutoDateHistogram(field="date", buckets=5, format="yyyy-MM-dd")
    assert agg.to_dict() == {
        "auto_date_histogram": {"field": "date", "buckets": 5, "format": "yyyy-MM-dd"}
    }

    raw_response = {
        "buckets": [
            {"key_as_string": "2015-01-01", "key": 1420070400000, "doc_count": 3},
            {"key_as_string": "2015-02-01", "key": 1422748800000, "doc_count": 2},
            {"key_as_string": "2015-03-01", "key": 1425168000000, "doc_count": 2},
        ],
        "interval": "1M",
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        (
            "2015-01-01",
            {"doc_count": 3, "key": 1420070400000, "key_as_string": "2015-01-01"},
        ),
        (
            "2015-02-01",
            {"doc_count": 2, "key": 1422748800000, "key_as_string": "2015-02-01"},
        ),
        (
            "2015-03-01",
            {"doc_count": 2, "key": 1425168000000, "key_as_string": "2015-03-01"},
        ),
    ]


def test_variable_width_histogram():
    agg = VariableWidthHistogram(field="price", buckets=2)
    assert agg.to_dict() == {
        "variable_width_histogram": {"buckets": 2, "field": "price"}
    }

    raw_response = {
        "buckets": [
            {"min": 10.0, "key": 30.0, "max": 50.0, "doc_count": 2},
            {"min": 150.0, "key": 185.0, "max": 200.0, "doc_count": 5},
        ]
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        (30.0, {"doc_count": 2, "key": 30.0, "max": 50.0, "min": 10.0}),
        (185.0, {"doc_count": 5, "key": 185.0, "max": 200.0, "min": 150.0}),
    ]


def test_significant_terms():
    agg = SignificantTerms(field="crime_type")
    assert agg.to_dict() == {"significant_terms": {"field": "crime_type"}}

    raw_response = {
        "doc_count": 47347,
        "bg_count": 5064554,
        "buckets": [
            {
                "key": "Bicycle theft",
                "doc_count": 3640,
                "score": 0.371235374214817,
                "bg_count": 66799,
            }
        ],
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        (
            "Bicycle theft",
            {
                "bg_count": 66799,
                "doc_count": 3640,
                "key": "Bicycle theft",
                "score": 0.371235374214817,
            },
        )
    ]


def test_rare_terms():
    agg = RareTerms(field="genre")
    assert agg.to_dict() == {"rare_terms": {"field": "genre"}}

    raw_response = {
        "buckets": [{"key": "swing", "doc_count": 1}, {"key": "jazz", "doc_count": 2}]
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        ("swing", {"doc_count": 1, "key": "swing"}),
        ("jazz", {"doc_count": 2, "key": "jazz"}),
    ]


def test_geo_tile_grid():
    agg = GeoTileGrid(field="location", precision=8)
    assert agg.to_dict() == {"geotile_grid": {"field": "location", "precision": 8}}

    raw_response = {
        "buckets": [
            {"key": "8/131/84", "doc_count": 3},
            {"key": "8/129/88", "doc_count": 2},
            {"key": "8/131/85", "doc_count": 1},
        ]
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        ("8/131/84", {"doc_count": 3, "key": "8/131/84"}),
        ("8/129/88", {"doc_count": 2, "key": "8/129/88"}),
        ("8/131/85", {"doc_count": 1, "key": "8/131/85"}),
    ]


def test_ip_range():
    # unkeyed
    agg = IPRange(field="ip", ranges=[{"to": "10.0.0.5"}, {"from": "10.0.0.5"}])
    assert agg.to_dict() == {
        "ip_range": {
            "field": "ip",
            "ranges": [{"to": "10.0.0.5"}, {"from": "10.0.0.5"}],
        }
    }

    raw_response = {
        "buckets": [
            {"key": "*-10.0.0.5", "to": "10.0.0.5", "doc_count": 10},
            {"key": "10.0.0.5-*", "from": "10.0.0.5", "doc_count": 260},
        ]
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        ("*-10.0.0.5", {"doc_count": 10, "key": "*-10.0.0.5", "to": "10.0.0.5"}),
        ("10.0.0.5-*", {"doc_count": 260, "from": "10.0.0.5", "key": "10.0.0.5-*"}),
    ]

    # keyed
    agg = IPRange(
        field="ip", ranges=[{"to": "10.0.0.5"}, {"from": "10.0.0.5"}], keyed=True
    )
    assert agg.to_dict() == {
        "ip_range": {
            "field": "ip",
            "ranges": [{"to": "10.0.0.5"}, {"from": "10.0.0.5"}],
            "keyed": True,
        }
    }

    raw_response = {
        "buckets": {
            "*-10.0.0.5": {"to": "10.0.0.5", "doc_count": 10},
            "10.0.0.5-*": {"from": "10.0.0.5", "doc_count": 260},
        }
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        ("*-10.0.0.5", {"doc_count": 10, "to": "10.0.0.5"}),
        ("10.0.0.5-*", {"doc_count": 260, "from": "10.0.0.5"}),
    ]


def test_sampler():
    agg = Sampler(
        shard_size=200,
        aggs={
            "keywords": {
                "significant_terms": {
                    "field": "tags",
                    "exclude": ["kibana", "javascript"],
                }
            }
        },
    )
    assert agg.to_dict() == {"sampler": {"shard_size": 200}}
    assert agg._children == {
        "keywords": {
            "significant_terms": {"exclude": ["kibana", "javascript"], "field": "tags"}
        }
    }
    raw_response = {
        "doc_count": 200,
        "keywords": {
            "doc_count": 200,
            "bg_count": 650,
            "buckets": [
                {
                    "key": "elasticsearch",
                    "doc_count": 150,
                    "score": 1.078125,
                    "bg_count": 200,
                },
                {"key": "logstash", "doc_count": 50, "score": 0.5625, "bg_count": 50},
            ],
        },
    }
    assert hasattr(agg.extract_buckets(raw_response), "__iter__")
    assert list(agg.extract_buckets(raw_response)) == [
        (
            None,
            {
                "doc_count": 200,
                "keywords": {
                    "bg_count": 650,
                    "buckets": [
                        {
                            "bg_count": 200,
                            "doc_count": 150,
                            "key": "elasticsearch",
                            "score": 1.078125,
                        },
                        {
                            "bg_count": 50,
                            "doc_count": 50,
                            "key": "logstash",
                            "score": 0.5625,
                        },
                    ],
                    "doc_count": 200,
                },
            },
        )
    ]
