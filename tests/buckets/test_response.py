from unittest import TestCase

from pandagg.aggs import Agg
from pandagg.buckets.response import ResponseTree
from tests.mapping.mapping_example import MAPPING_NAME, MAPPING_DETAIL
import tests.aggs.data_sample as sample


class ResponseTestCase(TestCase):

    def test_response_tree(self):
        my_agg = Agg(mapping={MAPPING_NAME: MAPPING_DETAIL}, from_=sample.EXPECTED_AGG_QUERY)
        response_tree = ResponseTree(agg_tree=my_agg)
        response_tree.parse_aggregation(sample.ES_AGG_RESPONSE)

        self.assertEqual(
            response_tree.__str__(),
            sample.EXPECTED_RESPONSE_TREE_REPR
        )
        self.assertEqual(len(response_tree.nodes.keys()), 33)
