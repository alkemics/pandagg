from pandagg.node.aggs.abstract import FieldOrScriptMetricAgg
from pandagg.node.aggs.metric import Avg, TopHits


def test_abstract_metric_agg():
    class MyMetricAgg(FieldOrScriptMetricAgg):
        KEY = "custom"
        VALUE_ATTRS = ["some_attr_name"]

    es_raw_response = {"some_attr_name": 345}

    metric_agg = MyMetricAgg(field="my_field")
    assert metric_agg.field == "my_field"

    buckets_iterator = MyMetricAgg(field="my_field").extract_buckets(es_raw_response)
    assert hasattr(buckets_iterator, "__iter__")
    buckets = list(buckets_iterator)
    assert buckets == [
        # key -> bucket
        (None, {"some_attr_name": 345})
    ]
    assert MyMetricAgg.extract_bucket_value({"some_attr_name": 345}) == 345


def test_average():
    # example for Average metric aggregation
    es_raw_response = {"value": 75.0}
    # test extract_buckets
    buckets_iterator = Avg("field").extract_buckets(es_raw_response)
    assert hasattr(buckets_iterator, "__iter__")
    buckets = list(buckets_iterator)
    assert buckets == [
        # key -> bucket
        (None, {"value": 75.0})
    ]

    # test extract bucket value
    assert Avg.extract_bucket_value({"value": 75.0}) == 75.0


def test_top_hits():
    query = {
        "top_hits": {
            "sort": [{"date": {"order": "desc"}}],
            "_source": {"includes": ["date", "price"]},
            "size": 1,
        }
    }
    top_hits = TopHits(
        sort=[{"date": {"order": "desc"}}],
        _source={"includes": ["date", "price"]},
        size=1,
    )
    assert top_hits.to_dict() == query

    es_raw_answer = {
        "hits": {
            "total": {"value": 3, "relation": "eq"},
            "max_score": None,
            "hits": [
                {
                    "_index": "sales",
                    "_type": "_doc",
                    "_id": "AVnNBmauCQpcRyxw6ChK",
                    "_source": {"date": "2015/03/01 00:00:00", "price": 200},
                    "sort": [1425168000000],
                    "_score": None,
                }
            ],
        }
    }
    assert top_hits.extract_bucket_value(es_raw_answer) == {
        "total": {"value": 3, "relation": "eq"},
        "max_score": None,
        "hits": [
            {
                "_index": "sales",
                "_type": "_doc",
                "_id": "AVnNBmauCQpcRyxw6ChK",
                "_source": {"date": "2015/03/01 00:00:00", "price": 200},
                "sort": [1425168000000],
                "_score": None,
            }
        ],
    }
