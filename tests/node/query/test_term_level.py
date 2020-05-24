from __future__ import unicode_literals

from unittest import TestCase

from pandagg.node.query import (
    Terms,
    Term,
    Fuzzy,
    Exists,
    Ids,
    Prefix,
    Range,
    Regexp,
    TermsSet,
    Wildcard,
)


class TermLevelQueriesTestCase(TestCase):
    def test_fuzzy_clause(self):
        body = {"user": {"value": "ki"}}
        expected = {"fuzzy": body}

        q1 = Fuzzy(field="user", value="ki")
        q2 = Fuzzy(user="ki")
        q3 = Fuzzy(user={"value": "ki"})
        for q in (q1, q2, q3):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(q.line_repr(depth=None), 'fuzzy, field=user, value="ki"')

    def test_exists_clause(self):
        body = {"field": "user"}
        expected = {"exists": body}

        q = Exists(field="user")
        self.assertEqual(q.body, body)
        self.assertEqual(q.to_dict(), expected)
        self.assertEqual(q.line_repr(depth=None), "exists, field=user")

    def test_ids_clause(self):
        body = {"values": [1, 4, 100]}
        expected = {"ids": body}

        q = Ids(values=[1, 4, 100])
        self.assertEqual(q.body, body)
        self.assertEqual(q.to_dict(), expected)
        self.assertEqual(q.line_repr(depth=None), "ids, values=[1, 4, 100]")

    def test_prefix_clause(self):
        body = {"user": {"value": "ki"}}
        expected = {"prefix": body}

        q = Prefix(field="user", value="ki")
        self.assertEqual(q.body, body)
        self.assertEqual(q.to_dict(), expected)
        self.assertEqual(q.line_repr(depth=None), 'prefix, field=user, value="ki"')

    def test_range_clause(self):
        body = {"age": {"gte": 10, "lte": 20, "boost": 2}}
        expected = {"range": body}

        q1 = Range(field="age", gte=10, lte=20, boost=2)
        q2 = Range(age={"gte": 10, "lte": 20, "boost": 2})
        for q in (q1, q2):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(
                q.line_repr(depth=None), "range, field=age, boost=2, gte=10, lte=20"
            )

    def test_regexp_clause(self):
        body = {
            "user": {
                "value": "k.*y",
                "flags": "ALL",
                "max_determinized_states": 10000,
                "rewrite": "constant_score",
            }
        }
        expected = {"regexp": body}
        tag = 'regexp, field=user, flags="ALL", max_determinized_states=10000, rewrite="constant_score", value="k.*y"'

        q1 = Regexp(
            field="user",
            value="k.*y",
            flags="ALL",
            max_determinized_states=10000,
            rewrite="constant_score",
        )
        q2 = Regexp(
            user={
                "value": "k.*y",
                "flags": "ALL",
                "max_determinized_states": 10000,
                "rewrite": "constant_score",
            }
        )
        for q in (q1, q2):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(q.line_repr(depth=None), tag)

    def test_term_clause(self):
        body = {"user": {"value": "Kimchy", "boost": 1}}
        expected = {"term": body}

        q1 = Term(field="user", value="Kimchy", boost=1)
        q2 = Term(user={"value": "Kimchy", "boost": 1})
        for q in (q1, q2):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(
                q.line_repr(depth=None), 'term, field=user, boost=1, value="Kimchy"'
            )

        # other format
        q3 = Term(user="Kimchy")
        self.assertEqual(q3.body, {"user": {"value": "Kimchy"}})
        self.assertEqual(q3.to_dict(), {"term": {"user": {"value": "Kimchy"}}})
        self.assertEqual(q3.line_repr(depth=None), 'term, field=user, value="Kimchy"')

    def test_terms_clause(self):
        # note: != syntax than term (...), the "boost" parameter is at same level that "user"
        body = {"user": ["kimchy", "elasticsearch"], "boost": 1}
        expected = {"terms": body}

        q = Terms(user=["kimchy", "elasticsearch"], boost=1)
        self.assertEqual(q.body, body)
        self.assertEqual(q.to_dict(), expected)
        self.assertEqual(
            q.line_repr(depth=None), 'terms, boost=1, user=["kimchy", "elasticsearch"]',
        )

    def test_terms_set_clause(self):
        body = {
            "programming_languages": {
                "terms": ["c++", "java", "php"],
                "minimum_should_match_field": "required_matches",
            }
        }
        expected = {"terms_set": body}

        q1 = TermsSet(
            field="programming_languages",
            terms=["c++", "java", "php"],
            minimum_should_match_field="required_matches",
        )
        q2 = TermsSet(
            programming_languages={
                "terms": ["c++", "java", "php"],
                "minimum_should_match_field": "required_matches",
            }
        )
        for q in (q1, q2):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(
                q.line_repr(depth=None),
                'terms_set, field=programming_languages, minimum_should_match_field="required_matches", terms=["c++", "java", "php"]',
            )

    def test_wildcard_clause(self):
        body = {"user": {"value": "ki*y", "boost": 1.0, "rewrite": "constant_score"}}
        expected = {"wildcard": body}

        q1 = Wildcard(field="user", value="ki*y", boost=1.0, rewrite="constant_score")
        q2 = Wildcard(user={"value": "ki*y", "boost": 1.0, "rewrite": "constant_score"})
        for q in (q1, q2):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(
                q.line_repr(depth=None),
                'wildcard, field=user, boost=1.0, rewrite="constant_score", value="ki*y"',
            )
