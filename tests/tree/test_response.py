from collections import OrderedDict
from unittest import TestCase
from mock import patch

from pandagg.tree.aggs import Aggs
from pandagg.tree.response import AggsResponseTree

from tests.testing_samples.mapping_example import MAPPINGS
import tests.testing_samples.data_sample as sample


class ResponseTestCase(TestCase):
    @patch("uuid.uuid4")
    def test_response_tree(self, uuid_mock):
        uuid_mock.side_effect = range(1000)
        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mappings=MAPPINGS)
        response_tree = AggsResponseTree(aggs=my_agg).parse(sample.ES_AGG_RESPONSE)
        self.assertEqual(response_tree.__str__(), sample.EXPECTED_RESPONSE_TREE_REPR)
        self.assertEqual(len(response_tree.list()), 15)

        multiclass_gpc_bucket = next(
            (
                b
                for k, b in response_tree.list()
                if b.level == "global_metrics.field.name" and b.key == "gpc"
            )
        )

        # bucket properties will give parents levels and keys
        self.assertEqual(
            response_tree.bucket_properties(multiclass_gpc_bucket),
            OrderedDict(
                [
                    ("global_metrics.field.name", "gpc"),
                    ("classification_type", "multiclass"),
                ]
            ),
        )
