from unittest import TestCase

from pandagg.base.node.agg.metric import Avg


class MetricAggNodesTestCase(TestCase):

    def test_average(self):
        # example for Average metric aggregation
        es_raw_response = {
            "value": 75.0
        }
        # test extract_buckets
        buckets_iterator = Avg('name', 'field').extract_buckets(es_raw_response)
        self.assertTrue(hasattr(buckets_iterator, '__iter__'))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                (None, {"value": 75.0})
            ]
        )

        # test extract bucket value
        self.assertEqual(Avg.extract_bucket_value({"value": 75.0}), 75.0)
