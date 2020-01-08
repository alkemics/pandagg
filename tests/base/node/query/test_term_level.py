
from __future__ import unicode_literals

from unittest import TestCase

from pandagg.base.node.query import Terms, Term, Fuzzy, Exists, Ids, Prefix, Range, Regexp, TermsSet, Wildcard


class TermLevelQueriesTestCase(TestCase):

    def test_fuzzy_clause(self):
        body = {'user': {'value': 'ki'}}
        expected = {'fuzzy': body}

        q = Fuzzy(field='user', value='ki')
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'fuzzy, field=user')

        deserialized = Fuzzy.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'fuzzy, field=user')

    def test_exists_clause(self):
        body = {'field': 'user'}
        expected = {'exists': body}

        q = Exists(field='user')
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'exists, field=user')

        deserialized = Exists.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'exists, field=user')

    def test_ids_clause(self):
        body = {'values': [1, 4, 100]}
        expected = {'ids': body}

        q = Ids(values=[1, 4, 100])
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'ids, values=[1, 4, 100]')

        deserialized = Ids.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'ids, values=[1, 4, 100]')

    def test_prefix_clause(self):
        body = {'user': {'value': "ki"}}
        expected = {'prefix': body}

        q = Prefix(field="user", value="ki")
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'prefix, field=user')

        deserialized = Prefix.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'prefix, field=user')

    def test_range_clause(self):
        body = {'age': {'gte': 10, 'lte': 20, 'boost': 2}}
        expected = {'range': body}

        q = Range(field="age", gte=10, lte=20, boost=2)
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'range, field=age')

        deserialized = Range.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'range, field=age')

    def test_regexp_clause(self):
        body = {'user': {
            "value": "k.*y",
            "flags": "ALL",
            "max_determinized_states": 10000,
            "rewrite": "constant_score"
        }}
        expected = {'regexp': body}

        q = Regexp(field="user", value="k.*y", flags="ALL", max_determinized_states=10000, rewrite="constant_score")
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'regexp, field=user')

        deserialized = Regexp.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'regexp, field=user')

    def test_term_clause(self):
        body = {'user': {'value': 'Kimchy', 'boost': 1}}
        expected = {'term': body}

        q = Term(field='user', value='Kimchy', boost=1)
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'term, field=user')

        deserialized = Term.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'term, field=user')

        # other format
        deserialized_2 = Term.deserialize(**{'user': 'Kimchy'})
        self.assertEqual(deserialized_2.body, {'user': {'value': 'Kimchy'}})
        self.assertEqual(deserialized_2.serialize(), {'term': {'user': {'value': 'Kimchy'}}})
        self.assertEqual(deserialized_2.tag, 'term, field=user')

    def test_terms_clause(self):
        body = {'user': ['kimchy', 'elasticsearch'], 'boost': 1}
        expected = {'terms': body}

        q = Terms(field='user', terms=['kimchy', 'elasticsearch'], boost=1)
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'terms, field=user')

        deserialized = q.deserialize(**body)
        self.assertEqual(deserialized, expected)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'terms, field=user')

    def test_terms_set_clause(self):
        body = {'programming_languages': {
            "terms": ["c++", "java", "php"],
            "minimum_should_match_field": "required_matches"
        }}
        expected = {'terms_set': body}

        q = TermsSet(
            field='programming_languages',
            terms=["c++", "java", "php"],
            minimum_should_match_field="required_matches"
        )
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'terms_set, field=programming_languages')

        deserialized = q.deserialize(**body)
        self.assertEqual(deserialized, expected)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'terms_set, field=programming_languages')

    def test_wildcard_clause(self):
        body = {'user': {
            "value": "ki*y",
            "boost": 1.0,
            "rewrite": "constant_score"
        }}
        expected = {'wildcard': body}

        q = Wildcard(
            field='user',
            value="ki*y",
            boost=1.0,
            rewrite="constant_score"
        )
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'wildcard, field=user')

        deserialized = q.deserialize(**body)
        self.assertEqual(deserialized, expected)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'wildcard, field=user')