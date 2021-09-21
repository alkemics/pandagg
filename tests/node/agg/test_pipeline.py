from pandagg.aggs import BucketSelector


def test_bucket_selector():
    agg_node = BucketSelector(buckets_path={"stuff": "other_agg"}, script="stuff > 100")

    # test query dict
    assert agg_node.to_dict() == {
        "bucket_selector": {
            "buckets_path": {"stuff": "other_agg"},
            "script": "stuff > 100",
        }
    }
