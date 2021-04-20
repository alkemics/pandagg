from mock import Mock, patch

from pandagg import Search
from pandagg.tree.aggs import Aggs
from pandagg.tree.response import AggsResponseTree
from pandagg.interactive.response import IResponse
from tests import PandaggTestCase
from tests.testing_samples.mapping_example import MAPPINGS
import tests.testing_samples.data_sample as sample


class ClientBoundResponseTestCase(PandaggTestCase):
    @patch("lighttree.node.uuid.uuid4")
    def test_client_bound_response(self, uuid_mock):
        uuid_mock.side_effect = range(1000)
        client_mock = Mock(spec=["search"])

        my_agg = Aggs(sample.EXPECTED_AGG_QUERY, mappings=MAPPINGS)
        response_tree = AggsResponseTree(aggs=my_agg).parse(sample.ES_AGG_RESPONSE)

        response = IResponse(
            search=Search(index="some_index", using=client_mock).query(
                {"term": {"some_field": 1}}
            ),
            tree=response_tree,
            depth=1,
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
        self.assertQueryEqual(
            gpc.get_bucket_filter(),
            {
                "bool": {
                    "must": [
                        {"term": {"global_metrics.field.name": {"value": "gpc"}}},
                        {"term": {"classification_type": {"value": "multiclass"}}},
                    ]
                }
            },
        )

        # convert to search request (includes initial query clauses)
        srequest = gpc.search()
        self.assertIsInstance(srequest, Search)
        self.assertSearchEqual(
            srequest.to_dict(),
            {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"global_metrics.field.name": {"value": "gpc"}}},
                            {"term": {"classification_type": {"value": "multiclass"}}},
                            {"term": {"some_field": {"value": 1}}},
                        ]
                    }
                }
            },
        )
        self.assertEqual(srequest._aggs.to_dict(), {})
