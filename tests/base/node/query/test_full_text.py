
from __future__ import unicode_literals

from unittest import TestCase

from pandagg.base.node.query import Intervals, Match


class FullTextQueriesTestCase(TestCase):

    def test_interval_clause(self):
        body = {'some_field': {
            "all_of": {
                "intervals": [
                    {"match": {"query": "the"}},
                    {"any_of": {"intervals": [
                        {"match": {"query": "big"}},
                        {"match": {"query": "big bad"}}
                    ]}},
                    {"match": {"query": "wolf"}}],
                "max_gaps": 0,
                "ordered": True
            }}}
        expected = {'intervals': body}

        q = Intervals(field='some_field', all_of={
            "intervals": [
                {"match": {"query": "the"}},
                {"any_of": {"intervals": [
                    {"match": {"query": "big"}},
                    {"match": {"query": "big bad"}}
                ]}},
                {"match": {"query": "wolf"}}],
            "max_gaps": 0,
            "ordered": True
        })
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'intervals, field=some_field')

        deserialized = Intervals.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'intervals, field=some_field')

    def test_match_clause(self):
        body = {'message': {
            'query': 'this is a test',
            'operator': 'and'
        }}
        expected = {'match': body}

        q = Match(field='message', query='this is a test', operator='and')
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'match, field=message')

        deserialized = Match.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'match, field=message')

        # short syntax
        deserialized = Match.deserialize(**{'message': 'this is a test'})
        self.assertEqual(deserialized.body, {'message': {'query': 'this is a test'}})
        self.assertEqual(deserialized.serialize(), {"match": {"message": {'query': 'this is a test'}}})
        self.assertEqual(deserialized.tag, 'match, field=message')
