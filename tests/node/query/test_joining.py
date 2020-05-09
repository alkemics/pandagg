from __future__ import unicode_literals

from unittest import TestCase

from pandagg.node.query._parameter_clause import QueryP, Path
from pandagg.query import Nested, Term


class JoiningQueriesTestCase(TestCase):
    def test_nested(self):
        # test all possibles definitions
        n1 = Nested(
            query=Term(field="some_nested_path.id", value=2), path="some_nested_path"
        )
        n2 = Nested(query={"term": {"some_nested_path.id": 2}}, path="some_nested_path")
        n3 = Nested(
            {"query": {"term": {"some_nested_path.id": 2}}, "path": "some_nested_path"}
        )
        for i, n in enumerate((n1, n2, n3)):
            self.assertEqual(len(n._children), 2)
            self.assertEqual(n.line_repr(depth=None), "nested")
            self.assertEqual(n.path, "some_nested_path")

            q = next((c for c in n._children if isinstance(c, QueryP)))
            self.assertEqual(
                q.to_dict(),
                {"query": [{"term": {"some_nested_path.id": {"value": 2}}}]},
            )
            # ensure term query is present
            self.assertEqual(len(q._children), 1)
            self.assertIsInstance(q._children[0], Term, i)

            p = next((c for c in n._children if isinstance(c, Path)))
            self.assertEqual(p.body["value"], "some_nested_path")
