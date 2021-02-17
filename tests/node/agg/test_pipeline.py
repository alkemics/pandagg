from unittest import TestCase

from pandagg.aggs import BucketSelector


class PipelineAggNodesTestCase(TestCase):
    def test_bucket_selector(self):
        agg_node = BucketSelector(
            buckets_path={"stuff": "other_agg"}, script="stuff > 100"
        )

        # test query dict
        self.assertEqual(
            agg_node.to_dict(),
            {
                "bucket_selector": {
                    "buckets_path": {"stuff": "other_agg"},
                    "script": "stuff > 100",
                }
            },
        )
