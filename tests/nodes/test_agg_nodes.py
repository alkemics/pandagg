
from __future__ import unicode_literals

from pandagg.nodes import Terms, Filters, Avg, DateHistogram
from pandagg.nodes.abstract import AggNode
from unittest import TestCase


class AggNodesTestCase(TestCase):

    def test_abstract_agg_node(self):

        class CustomAgg(AggNode):
            AGG_TYPE = 'custom_type'
            VALUE_ATTRS = ['bucket_value_path']
            # would mean this agg can be applied only on boolean fields
            WHITELISTED_MAPPING_TYPES = ['boolean']
            BLACKLISTED_MAPPING_TYPES = None
            # means it ES response produces a single bucket
            SINGLE_BUCKET = True

            # depends on ElasticSearch aggregation handling, since this is a fake Aggregation this get_filter method
            # doesn't really make sense, just wrote one so that all abstract methods are implemented
            def get_filter(self, key):
                return {'exists': {'field': key}}

            # example for unique bucket agg
            def extract_buckets(self, response_value):
                yield (None, response_value)

        node = CustomAgg(agg_name='custom_agg_name', agg_body={'custom_body': {'stuff': 2}})
        self.assertEqual(
            node.query_dict(),
            {
                'custom_type': {
                    'custom_body': {
                        'stuff': 2
                    }
                }
            }
        )

        node = CustomAgg(agg_name='custom_agg_name', agg_body={'custom_body': {'stuff': 2}}, meta='meta_stuff')
        self.assertEqual(
            node.query_dict(),
            {
                'custom_type': {
                    'custom_body': {
                        'stuff': 2
                    }
                },
                'meta': 'meta_stuff'
            }
        )

        self.assertEqual(
            node.__str__(),
            u"<CustomAgg, name=custom_agg_name, type=custom_type, body={\"custom_body\": {\"stuff\": 2}}>"
        )

        # suppose this aggregation type provide buckets in the following format
        hypothetic_es_response_bucket = {
            'bucket_value_path': 43
        }
        self.assertEqual(node.extract_bucket_value(hypothetic_es_response_bucket), 43)

        self.assertEqual(node.valid_on_field_type('string'), False)
        self.assertEqual(node.valid_on_field_type('boolean'), True)

    def test_terms(self):
        es_raw_response = {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [
                {
                    "key": "electronic",
                    "doc_count": 6
                },
                {
                    "key": "rock",
                    "doc_count": 3
                },
                {
                    "key": "jazz",
                    "doc_count": 2
                }
            ]
        }
        # test extract_buckets
        buckets_iterator = Terms('name', 'field').extract_buckets(es_raw_response)
        self.assertTrue(hasattr(buckets_iterator, '__iter__'))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                ('electronic', {'doc_count': 6, 'key': 'electronic'}),
                ('rock', {'doc_count': 3, 'key': 'rock'}),
                ('jazz', {'doc_count': 2, 'key': 'jazz'})
            ]
        )

        # test extract bucket value
        self.assertEqual(Terms('name', 'field').extract_bucket_value({'doc_count': 6, 'key': 'electronic'}), 6)
        self.assertEqual(Terms('name', 'field').extract_bucket_value({'doc_count': 3, 'key': 'rock'}), 3)
        self.assertEqual(Terms('name', 'field').extract_bucket_value({'doc_count': 2, 'key': 'jazz'}), 2)

    def test_filters(self):
        es_raw_response = {
            "buckets": {
                "my_first_filter": {
                    "doc_count": 1
                },
                "my_second_filter": {
                    "doc_count": 2
                }
            }
        }
        # test extract_buckets
        buckets_iterator = Filters('name', None).extract_buckets(es_raw_response)
        self.assertTrue(hasattr(buckets_iterator, '__iter__'))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                ('my_first_filter', {'doc_count': 1}),
                ('my_second_filter', {'doc_count': 2}),
            ]
        )

        # test extract bucket value
        self.assertEqual(Filters('name', None).extract_bucket_value({'doc_count': 1}), 1)
        self.assertEqual(Filters('name', None).extract_bucket_value({'doc_count': 2}), 2)

    def test_metric_aggs(self):
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

    def test_all_agg_body_to_init_kwargs(self):
        # TODO
        pass

    def test_date_histogram_key_as_string(self):
        es_raw_response = {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [
                {
                    "key_as_string": "2018-01-01",
                    "key": 1514764800000,
                    "doc_count": 6
                },
                {
                    'key_as_string': '2018-01-08',
                    "key": 1515369600000,
                    "doc_count": 3
                }
            ]
        }

        buckets_iterator = DateHistogram(
            agg_name='name',
            field='field',
            interval='1w',
            use_key_as_string=True
        ).extract_buckets(es_raw_response)

        self.assertTrue(hasattr(buckets_iterator, '__iter__'))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key_as_string -> bucket
                ('2018-01-01', {'doc_count': 6, 'key': 1514764800000, 'key_as_string': '2018-01-01'}),
                ('2018-01-08', {'doc_count': 3, 'key': 1515369600000, 'key_as_string': '2018-01-08'})
            ]
        )

        # not using key as string (regular key)
        buckets_iterator = DateHistogram(
            agg_name='name',
            field='field',
            interval='1w',
            use_key_as_string=False
        ).extract_buckets(es_raw_response)

        self.assertTrue(hasattr(buckets_iterator, '__iter__'))
        buckets = list(buckets_iterator)
        self.assertEqual(
            buckets,
            [
                # key -> bucket
                (1514764800000, {'doc_count': 6, 'key': 1514764800000, 'key_as_string': '2018-01-01'}),
                (1515369600000, {'doc_count': 3, 'key': 1515369600000, 'key_as_string': '2018-01-08'})
            ]
        )
