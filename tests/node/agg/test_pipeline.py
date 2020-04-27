from unittest import TestCase

from pandagg.aggs import BucketSelector


class PipelineAggNodesTestCase(TestCase):
    def test_bucket_selector(self):
        agg_node = BucketSelector(
            name="agg_name", buckets_path={"stuff": "other_agg"}, script="stuff > 100"
        )

        # test query dict
        self.assertEqual(
            agg_node.to_dict(with_name=True),
            {
                "agg_name": {
                    "bucket_selector": {
                        "buckets_path": {"stuff": "other_agg"},
                        "script": "stuff > 100",
                    }
                }
            },
        )
