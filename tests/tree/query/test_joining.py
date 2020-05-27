#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase

from pandagg.query import Nested, Term


class JoiningQueriesTestCase(TestCase):
    def test_nested(self):
        n1 = Nested(
            query=Term(field="some_nested_path.id", value=2), path="some_nested_path"
        )
        n2 = Nested(query={"term": {"some_nested_path.id": 2}}, path="some_nested_path")

        for i, n in enumerate((n1, n2)):
            self.assertEqual(
                n.to_dict(),
                {
                    "nested": {
                        "path": "some_nested_path",
                        "query": {"term": {"some_nested_path.id": {"value": 2}}},
                    }
                },
            )
            self.assertEqual(
                n.__str__(),
                """<Query>
nested, path="some_nested_path"
└── query
    └── term, field=some_nested_path.id, value=2
""",
            )
