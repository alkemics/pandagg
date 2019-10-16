from pandagg.nodes.agg_nodes import AggNode
from unittest import TestCase


class AggNodesTestCase(TestCase):

    def test_abstract_agg_node(self):

        class CustomAgg(AggNode):
            AGG_TYPE = 'custom_type'
            VALUE_ATTRS = ['bucket_value_path']
            SINGLE_BUCKET = False

        node = CustomAgg(agg_name='custom_agg_name', agg_body={'custom_body': {'stuff': 2}})
        self.assertEqual(
            node.agg_dict(),
            {
                'custom_agg_name': {
                    'custom_type': {
                        'custom_body': {
                            'stuff': 2
                        }
                    }
                }
            }
        )

        node = CustomAgg(agg_name='custom_agg_name', agg_body={'custom_body': {'stuff': 2}}, meta='meta_stuff')
        self.assertEqual(
            node.agg_dict(),
            {
                'custom_agg_name': {
                    'custom_type': {
                        'custom_body': {
                            'stuff': 2
                        }
                    },
                    'meta': 'meta_stuff'
                }
            }
        )

        self.assertEqual(
            node.__repr__().encode('utf-8'),
            u"<CustomAgg, name=custom_agg_name, type=custom_type, body={'custom_body': {'stuff': 2}}>"
        )

        # suppose this aggregation type provide buckets in the following format
        hypothetic_es_response_bucket = {
            'bucket_value_path': 43
        }
        self.assertEqual(node.extract_bucket_value(hypothetic_es_response_bucket), 43)
