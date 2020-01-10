
from __future__ import unicode_literals

from unittest import TestCase

from pandagg.query import DistanceFeature, MoreLikeThis, Percolate, RankFeature, Script, ScriptScore


class SpecializedQueriesTestCase(TestCase):

    def test_distance_feature_clause(self):
        body = {
            "field": "production_date",
            "pivot": "7d",
            "origin": "now"
        }
        expected = {'distance_feature': body}

        q = DistanceFeature(
            field='production_date',
            pivot="7d",
            origin="now"
        )
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'distance_feature, field=production_date')

        deserialized = DistanceFeature.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'distance_feature, field=production_date')

    def test_more_like_this_clause(self):
        body = {
            "fields": ["title", "description"],
            "like": "Once upon a time",
            "min_term_freq": 1,
            "max_query_terms": 12
        }
        expected = {'more_like_this': body}

        q = MoreLikeThis(
            fields=["title", "description"],
            like="Once upon a time",
            min_term_freq=1,
            max_query_terms=12
        )
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'more_like_this, fields=[\'title\', \'description\']')

        deserialized = MoreLikeThis.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'more_like_this, fields=[\'title\', \'description\']')
