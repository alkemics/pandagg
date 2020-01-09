
from __future__ import unicode_literals

from unittest import TestCase

from pandagg.base.node.query import Intervals, Match, MatchBoolPrefix, MatchPhrase, MatchPhrasePrefix, MultiMatch, \
    QueryString, SimpleQueryString


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

    def test_match_bool_prefix_clause(self):
        body = {'message': {
            'query': 'quick brown f',
            'analyzer': 'keyword'
        }}
        expected = {'match_bool_prefix': body}

        q = MatchBoolPrefix(field='message', query='quick brown f', analyzer='keyword')
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'match_bool_prefix, field=message')

        deserialized = MatchBoolPrefix.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'match_bool_prefix, field=message')

        # short syntax
        deserialized = MatchBoolPrefix.deserialize(**{'message': 'quick brown f'})
        self.assertEqual(deserialized.body, {'message': {'query': 'quick brown f'}})
        self.assertEqual(deserialized.serialize(), {"match_bool_prefix": {"message": {'query': 'quick brown f'}}})
        self.assertEqual(deserialized.tag, 'match_bool_prefix, field=message')

    def test_match_phrase_clause(self):
        body = {'message': {
            'query': 'this is a test',
            'analyzer': 'my_analyzer'
        }}
        expected = {'match_phrase': body}

        q = MatchPhrase(field='message', query='this is a test', analyzer='my_analyzer')
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'match_phrase, field=message')

        deserialized = MatchPhrase.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'match_phrase, field=message')

        # short syntax
        deserialized = MatchPhrase.deserialize(**{'message': 'this is a test'})
        self.assertEqual(deserialized.body, {'message': {'query': 'this is a test'}})
        self.assertEqual(deserialized.serialize(), {"match_phrase": {"message": {'query': 'this is a test'}}})
        self.assertEqual(deserialized.tag, 'match_phrase, field=message')

    def test_match_phrase_prefix_clause(self):
        body = {'message': {
            'query': 'this is a test',
            'analyzer': 'my_analyzer'
        }}
        expected = {'match_phrase_prefix': body}

        q = MatchPhrasePrefix(field='message', query='this is a test', analyzer='my_analyzer')
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'match_phrase_prefix, field=message')

        deserialized = MatchPhrasePrefix.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'match_phrase_prefix, field=message')

        # short syntax
        deserialized = MatchPhrasePrefix.deserialize(**{'message': 'this is a test'})
        self.assertEqual(deserialized.body, {'message': {'query': 'this is a test'}})
        self.assertEqual(deserialized.serialize(), {"match_phrase_prefix": {"message": {'query': 'this is a test'}}})
        self.assertEqual(deserialized.tag, 'match_phrase_prefix, field=message')

    def test_multi_match_clause(self):
        body = {
            'query': 'this is a test',
            'fields': ['subject', 'message'],
            'type': 'best_fields'
        }
        expected = {'multi_match': body}

        q = MultiMatch(fields=['subject', 'message'], query='this is a test', type='best_fields')
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'multi_match, fields=[\'subject\', \'message\']')

        deserialized = MultiMatch.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'multi_match, fields=[\'subject\', \'message\']')

    def test_query_string_clause(self):
        body = {
            'query': '(new york city) OR (big apple)',
            'default_field': 'content'
        }
        expected = {'query_string': body}

        q = QueryString(query='(new york city) OR (big apple)', default_field='content')
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'query_string')

        deserialized = QueryString.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'query_string')

    def test_simple_string_clause(self):
        body = {
            'query': '(new york city) OR (big apple)',
            'default_field': 'content'
        }
        expected = {'simple_string': body}

        q = SimpleQueryString(query='(new york city) OR (big apple)', default_field='content')
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'simple_string')

        deserialized = SimpleQueryString.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'simple_string')
