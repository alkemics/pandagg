from collections import OrderedDict
from unittest import TestCase
from mock import Mock, patch

from pandagg.tree.aggs.aggs import Aggs
from pandagg.tree.response import AggsResponseTree
from pandagg.interactive.response import IResponse
from pandagg.utils import equal_queries
from tests.testing_samples.mapping_example import MAPPING
import tests.testing_samples.data_sample as sample


class ResponseTestCase(TestCase):
    @patch("uuid.uuid4")
    def test_response_tree(self, uuid_mock):
        uuid_mock.side_effect = range(1000)
        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mapping=MAPPING)
        response_tree = AggsResponseTree(aggs=my_agg, index=None).parse(
            sample.ES_AGG_RESPONSE
        )
        self.assertEqual(response_tree.__str__(), sample.EXPECTED_RESPONSE_TREE_REPR)
        self.assertEqual(len(response_tree.list()), 15)

        multiclass_gpc_bucket = next(
            (
                b
                for b in response_tree.list()
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


class ClientBoundResponseTestCase(TestCase):
    @patch("lighttree.node.uuid.uuid4")
    def test_client_bound_response(self, uuid_mock):
        uuid_mock.side_effect = range(1000)
        client_mock = Mock(spec=["search"])

        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mapping=MAPPING)
        response_tree = AggsResponseTree(aggs=my_agg, index=None).parse(
            sample.ES_AGG_RESPONSE
        )

        response = IResponse(
            client=client_mock,
            tree=response_tree,
            index_name="some_index",
            depth=1,
            query={"term": {"some_field": 1}},
        )

        # ensure that navigation to attributes works with autocompletion (dir is used in ipython)
        self.assertIn("classification_type_multiclass", dir(response))
        self.assertIn("classification_type_multilabel", dir(response))

        multiclass = response.classification_type_multiclass
        self.assertIsInstance(multiclass, IResponse)
        self.assertIs(multiclass._initial_tree, response._tree)

        self.assertIn("global_metrics_field_name_gpc", dir(multiclass))
        gpc = multiclass.global_metrics_field_name_gpc
        self.assertIsInstance(gpc, IResponse)
        self.assertIs(gpc._initial_tree, response._tree)

        # test filter query used to list documents belonging to bucket
        self.assertTrue(
            equal_queries(
                gpc.get_bucket_filter(),
                {
                    "bool": {
                        "must": [
                            {"term": {"global_metrics.field.name": {"value": "gpc"}}},
                            {"term": {"classification_type": {"value": "multiclass"}}},
                            {"term": {"some_field": {"value": 1}}},
                        ]
                    }
                },
            )
        )
