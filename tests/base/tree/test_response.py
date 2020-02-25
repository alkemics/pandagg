from collections import OrderedDict
from unittest import TestCase
from mock import Mock

from pandagg.tree.agg import Agg
from pandagg.tree.response import ResponseTree
from pandagg.interactive.response import IResponse
from tests.base.mapping_example import MAPPING
import tests.base.data_sample as sample


class ResponseTestCase(TestCase):

    def test_response_tree(self):
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        response_tree = ResponseTree(agg_tree=my_agg)
        response_tree.parse_aggregation(sample.ES_AGG_RESPONSE)

        self.assertEqual(
            response_tree.__str__(),
            sample.EXPECTED_RESPONSE_TREE_REPR
        )
        self.assertEqual(len(response_tree.nodes.keys()), 33)

        multilabel_allergenlist_bucket = next((
            b for b in response_tree.nodes.values()
            if b.tag == 'global_metrics.field.name=allergentypelist'
        ))

        # bucket properties will give parents levels and keys
        self.assertEqual(
            response_tree.bucket_properties(multilabel_allergenlist_bucket),
            OrderedDict([
                ('global_metrics.field.name', 'allergentypelist'),
                ('classification_type', 'multilabel')
            ])
        )

    def test_response(self):
        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        response_tree = ResponseTree(agg_tree=my_agg).parse_aggregation(sample.ES_AGG_RESPONSE)

        response = IResponse(tree=response_tree, depth=1)

        # ensure that navigation to attributes works with autocompletion (dir is used in ipython)
        self.assertIn('classification_type_multiclass', dir(response))
        self.assertIn('classification_type_multilabel', dir(response))

        multilabel = response.classification_type_multilabel
        self.assertIsInstance(multilabel, IResponse)
        self.assertIs(multilabel._initial_tree, response._tree)

        self.assertIn('global_metrics_field_name_allergentypelist', dir(multilabel))
        allergentypelist = multilabel.global_metrics_field_name_allergentypelist
        self.assertIsInstance(allergentypelist, IResponse)
        self.assertIs(allergentypelist._initial_tree, response._tree)

        # test filter query used to list documents belonging to bucket
        expected_query = {
            "bool": {
                "must": [
                    {
                        "term": {
                            "global_metrics.field.name": {
                                "value": "allergentypelist"
                            }
                        }
                    },
                    {
                        "term": {
                            "classification_type": {
                                "value": "multilabel"
                            }
                        }
                    }
                ]
            }
        }
        self.assertEqual(allergentypelist.list_documents(execute=False), expected_query)
        self.assertEqual(allergentypelist.get_bucket_filter(), expected_query)


class ClientBoundResponseTestCase(TestCase):

    def test_client_bound_response(self):
        client_mock = Mock(spec=['search'])

        my_agg = Agg(mapping=MAPPING, from_=sample.EXPECTED_AGG_QUERY)
        response_tree = ResponseTree(agg_tree=my_agg).parse_aggregation(sample.ES_AGG_RESPONSE)

        response = IResponse(
            client=client_mock,
            tree=response_tree,
            index_name='some_index',
            depth=1,
            query={'term': {'some_field': 1}}
        )

        # ensure that navigation to attributes works with autocompletion (dir is used in ipython)
        self.assertIn('classification_type_multiclass', dir(response))
        self.assertIn('classification_type_multilabel', dir(response))

        multilabel = response.classification_type_multilabel
        self.assertIsInstance(multilabel, IResponse)
        self.assertIs(multilabel._initial_tree, response._tree)

        self.assertIn('global_metrics_field_name_allergentypelist', dir(multilabel))
        allergentypelist = multilabel.global_metrics_field_name_allergentypelist
        self.assertIsInstance(allergentypelist, IResponse)
        self.assertIs(allergentypelist._initial_tree, response._tree)

        # test filter query used to list documents belonging to bucket
        self.assertEqual(
            allergentypelist.get_bucket_filter(),
            {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "global_metrics.field.name": {
                                    "value": "allergentypelist"
                                }
                            }
                        },
                        {
                            "term": {
                                "classification_type": {
                                    "value": "multilabel"
                                }
                            }
                        }
                    ]
                }
            }
        )
        self.assertEqual(
            allergentypelist.list_documents(execute=False),
            {
                'bool': {
                    'must': [
                        {'term': {'global_metrics.field.name': {'value': 'allergentypelist'}}},
                        {'term': {'classification_type': {'value': 'multilabel'}}},
                        {'term': {'some_field': {'value': 1}}}
                    ]
                }
            }
        )
