#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase

from pandagg.query import ScriptScore, PinnedQuery, Match


class SpecializedQueriesTestCase(TestCase):
    def test_script_score_clause(self):
        b1 = ScriptScore(
            query=Match(field="message", query="elasticsearch"),
            script={"source": "doc['likes'].value / 10 "},
        )

        b2 = ScriptScore(
            query={"match": {"message": "elasticsearch"}},
            script={"source": "doc['likes'].value / 10 "},
        )

        for b in (b1, b2):
            self.assertEqual(
                b.to_dict(),
                {
                    "script_score": {
                        "query": {"match": {"message": {"query": "elasticsearch"}}},
                        "script": {"source": "doc['likes'].value / 10 "},
                    }
                },
            )
            self.assertEqual(
                b.__repr__(),
                """<Query>
script_score, script={"source": "doc['likes'].value / 10 "}
└── query
    └── match, field=message, query="elasticsearch"
""",
            )

    def test_pinned_query_clause(self):
        b1 = PinnedQuery(
            ids=[1, 23], organic=Match(field="description", query="brown shoes")
        )

        b2 = PinnedQuery(ids=[1, 23], organic={"match": {"description": "brown shoes"}})

        for b in (b1, b2):
            self.assertEqual(
                b.to_dict(),
                {
                    "pinned": {
                        "ids": [1, 23],
                        "organic": {"match": {"description": {"query": "brown shoes"}}},
                    }
                },
            )
            self.assertEqual(
                b.__str__(),
                """<Query>
pinned, ids=[1, 23]
└── organic
    └── match, field=description, query="brown shoes"
""",
            )
