
from __future__ import unicode_literals

from unittest import TestCase

from pandagg.node.query._leaf_clause import deserialize_leaf_clause
from pandagg.query import Terms, Term, Fuzzy, Exists, Ids, Prefix, Range, Regexp, TermsSet, Wildcard


class TermLevelQueriesTestCase(TestCase):

    def test_identifier_deserialization(self):
        node = deserialize_leaf_clause('term', {'user': {'value': 'Kimchy', 'boost': 1}, '_name': 'some_id'})
        self.assertIsInstance(node, Term)

        self.assertEqual(node.body, {'user': {'value': 'Kimchy', 'boost': 1}})
        self.assertEqual(node.serialize(), {'term': {'user': {'value': 'Kimchy', 'boost': 1}}})
        self.assertEqual(node.tag, 'term, field=user, boost=1, value="Kimchy"')

    def test_fuzzy_clause(self):
        body = {'user': {'value': 'ki'}}
        expected = {'fuzzy': body}

        q = Fuzzy(field='user', value='ki')
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'fuzzy, field=user, value="ki"')

        deserialized = Fuzzy.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'fuzzy, field=user, value="ki"')

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
        self.assertEqual(q.tag, 'prefix, field=user, value="ki"')

        deserialized = Prefix.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'prefix, field=user, value="ki"')

    def test_range_clause(self):
        body = {'age': {'gte': 10, 'lte': 20, 'boost': 2}}
        expected = {'range': body}

        q = Range(field="age", gte=10, lte=20, boost=2)
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'range, field=age, boost=2, gte=10, lte=20')

        deserialized = Range.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'range, field=age, boost=2, gte=10, lte=20')

    def test_regexp_clause(self):
        body = {'user': {
            "value": "k.*y",
            "flags": "ALL",
            "max_determinized_states": 10000,
            "rewrite": "constant_score"
        }}
        expected = {'regexp': body}
        tag = 'regexp, field=user, flags="ALL", max_determinized_states=10000, rewrite="constant_score", value="k.*y"'

        q = Regexp(field="user", value="k.*y", flags="ALL", max_determinized_states=10000, rewrite="constant_score")
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, tag)

        deserialized = Regexp.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, tag)

    def test_term_clause(self):
        body = {'user': {'value': 'Kimchy', 'boost': 1}}
        expected = {'term': body}

        q = Term(field='user', value='Kimchy', boost=1)
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, 'term, field=user, boost=1, value="Kimchy"')

        deserialized = Term.deserialize(**body)
        self.assertEqual(deserialized.body, body)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'term, field=user, boost=1, value="Kimchy"')

        # other format
        deserialized_2 = Term.deserialize(**{'user': 'Kimchy'})
        self.assertEqual(deserialized_2.body, {'user': {'value': 'Kimchy'}})
        self.assertEqual(deserialized_2.serialize(), {'term': {'user': {'value': 'Kimchy'}}})
        self.assertEqual(deserialized_2.tag, 'term, field=user, value="Kimchy"')

    def test_terms_clause(self):
        body = {'user': ['kimchy', 'elasticsearch'], 'boost': 1}
        expected = {'terms': body}

        q = Terms(field='user', terms=['kimchy', 'elasticsearch'], boost=1)
        self.assertEqual(q.body, body)
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q.tag, "terms, field=user, values=['kimchy', 'elasticsearch']")

        deserialized = q.deserialize(**body)
        self.assertEqual(deserialized, expected)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, "terms, field=user, values=['kimchy', 'elasticsearch']")

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
        self.assertEqual(q.tag, 'terms_set, field=programming_languages, minimum_should_match_field="required_matches", terms=["c++", "java", "php"]')

        deserialized = q.deserialize(**body)
        self.assertEqual(deserialized, expected)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'terms_set, field=programming_languages, minimum_should_match_field="required_matches", terms=["c++", "java", "php"]')

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
        self.assertEqual(q.tag, 'wildcard, field=user, boost=1.0, rewrite="constant_score", value="ki*y"')

        deserialized = q.deserialize(**body)
        self.assertEqual(deserialized, expected)
        self.assertEqual(deserialized.serialize(), expected)
        self.assertEqual(deserialized.tag, 'wildcard, field=user, boost=1.0, rewrite="constant_score", value="ki*y"')
