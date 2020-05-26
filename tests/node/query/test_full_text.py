from __future__ import unicode_literals

from unittest import TestCase

from pandagg.node.query import (
    Intervals,
    Match,
    MatchBoolPrefix,
    MatchPhrase,
    MatchPhrasePrefix,
    MultiMatch,
    QueryString,
    SimpleQueryString,
)


class FullTextQueriesTestCase(TestCase):
    def test_interval_clause(self):
        body = {
            "some_field": {
                "all_of": {
                    "intervals": [
                        {"match": {"query": "the"}},
                        {
                            "any_of": {
                                "intervals": [
                                    {"match": {"query": "big"}},
                                    {"match": {"query": "big bad"}},
                                ]
                            }
                        },
                        {"match": {"query": "wolf"}},
                    ],
                    "max_gaps": 0,
                    "ordered": True,
                }
            }
        }
        expected = {"intervals": body}

        q1 = Intervals(
            field="some_field",
            all_of={
                "intervals": [
                    {"match": {"query": "the"}},
                    {
                        "any_of": {
                            "intervals": [
                                {"match": {"query": "big"}},
                                {"match": {"query": "big bad"}},
                            ]
                        }
                    },
                    {"match": {"query": "wolf"}},
                ],
                "max_gaps": 0,
                "ordered": True,
            },
        )
        q2 = Intervals(
            some_field={
                "all_of": {
                    "intervals": [
                        {"match": {"query": "the"}},
                        {
                            "any_of": {
                                "intervals": [
                                    {"match": {"query": "big"}},
                                    {"match": {"query": "big bad"}},
                                ]
                            }
                        },
                        {"match": {"query": "wolf"}},
                    ],
                    "max_gaps": 0,
                    "ordered": True,
                }
            }
        )
        for q in (q1, q2):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(
                q.line_repr(depth=None),
                'intervals, field=some_field, all_of={"intervals": [{"match": {"query": "the"}}, {"any_of": {"intervals": [{"match": {"query": "big"}}, {"match": {"query": "big bad"}}]}}, {"match": {"query": "wolf"}}], "max_gaps": 0, "ordered": true}',
            )

    def test_match_clause(self):
        body = {"message": {"query": "this is a test", "operator": "and"}}
        expected = {"match": body}

        q1 = Match(field="message", query="this is a test", operator="and")
        q2 = Match(message={"query": "this is a test", "operator": "and"})
        for q in (q1, q2):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(
                q.line_repr(depth=None),
                'match, field=message, operator="and", query="this is a test"',
            )

        # short syntax
        q3 = Match(message="this is a test")
        self.assertEqual(q3.body, {"message": {"query": "this is a test"}})
        self.assertEqual(
            q3.to_dict(), {"match": {"message": {"query": "this is a test"}}}
        )
        self.assertEqual(
            q3.line_repr(depth=None), 'match, field=message, query="this is a test"'
        )

    def test_match_bool_prefix_clause(self):
        body = {"message": {"query": "quick brown f", "analyzer": "keyword"}}
        expected = {"match_bool_prefix": body}

        q1 = MatchBoolPrefix(field="message", query="quick brown f", analyzer="keyword")
        q2 = MatchBoolPrefix(message={"query": "quick brown f", "analyzer": "keyword"})
        for q in (q1, q2):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(
                q.line_repr(depth=None),
                'match_bool_prefix, field=message, analyzer="keyword", query="quick brown f"',
            )

        # short syntax
        q3 = MatchBoolPrefix(message="quick brown f")
        self.assertEqual(q3.body, {"message": {"query": "quick brown f"}})
        self.assertEqual(
            q3.to_dict(),
            {"match_bool_prefix": {"message": {"query": "quick brown f"}}},
        )
        self.assertEqual(
            q3.line_repr(depth=None),
            'match_bool_prefix, field=message, query="quick brown f"',
        )

    def test_match_phrase_clause(self):
        body = {"message": {"query": "this is a test", "analyzer": "my_analyzer"}}
        expected = {"match_phrase": body}

        q1 = MatchPhrase(
            field="message", query="this is a test", analyzer="my_analyzer"
        )
        q2 = MatchPhrase(message={"query": "this is a test", "analyzer": "my_analyzer"})
        for q in (q1, q2):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(
                q.line_repr(depth=None),
                'match_phrase, field=message, analyzer="my_analyzer", query="this is a test"',
            )

        # short syntax
        q3 = MatchPhrase(message="this is a test")
        self.assertEqual(q3.body, {"message": {"query": "this is a test"}})
        self.assertEqual(
            q3.to_dict(), {"match_phrase": {"message": {"query": "this is a test"}}},
        )
        self.assertEqual(
            q3.line_repr(depth=None),
            'match_phrase, field=message, query="this is a test"',
        )

    def test_match_phrase_prefix_clause(self):
        body = {"message": {"query": "this is a test", "analyzer": "my_analyzer"}}
        expected = {"match_phrase_prefix": body}

        q1 = MatchPhrasePrefix(
            field="message", query="this is a test", analyzer="my_analyzer"
        )
        q2 = MatchPhrasePrefix(
            message={"query": "this is a test", "analyzer": "my_analyzer"}
        )
        for q in (q1, q2):
            self.assertEqual(q.body, body)
            self.assertEqual(q.to_dict(), expected)
            self.assertEqual(
                q.line_repr(depth=None),
                'match_phrase_prefix, field=message, analyzer="my_analyzer", query="this is a test"',
            )

    def test_multi_match_clause(self):
        body = {
            "query": "this is a test",
            "fields": ["subject", "message"],
            "type": "best_fields",
        }
        expected = {"multi_match": body}

        q = MultiMatch(
            fields=["subject", "message"], query="this is a test", type="best_fields"
        )
        self.assertEqual(q.body, body)
        self.assertEqual(q.to_dict(), expected)
        self.assertEqual(
            q.line_repr(depth=None), "multi_match, fields=['subject', 'message']"
        )

    def test_query_string_clause(self):
        body = {"query": "(new york city) OR (big apple)", "default_field": "content"}
        expected = {"query_string": body}

        q = QueryString(query="(new york city) OR (big apple)", default_field="content")
        self.assertEqual(q.body, body)
        self.assertEqual(q.to_dict(), expected)
        self.assertEqual(
            q.line_repr(depth=None),
            'query_string, default_field="content", query="(new york city) OR (big apple)"',
        )

    def test_simple_string_clause(self):
        body = {"query": "(new york city) OR (big apple)", "default_field": "content"}
        expected = {"simple_string": body}

        q = SimpleQueryString(
            query="(new york city) OR (big apple)", default_field="content"
        )
        self.assertEqual(q.body, body)
        self.assertEqual(q.to_dict(), expected)
        self.assertEqual(
            q.line_repr(depth=None),
            'simple_string, default_field="content", query="(new york city) OR (big apple)"',
        )
