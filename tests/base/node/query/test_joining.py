
from __future__ import unicode_literals

from unittest import TestCase

from pandagg.node.query._parameter_clause import QueryP, Path
from pandagg.query import Nested, Term


class JoiningQueriesTestCase(TestCase):

    def test_nested(self):
        # test all possibles definitions
        n1 = Nested(
            query=Term(field='some_nested_path.id', value=2),
            path='some_nested_path'
        )
        n2 = Nested(
            query={'term': {'some_nested_path.id': 2}},
            path='some_nested_path'
        )
        n3 = Nested({
            'query': {'term': {'some_nested_path.id': 2}},
            'path': 'some_nested_path'
        })
        for n in (n1, n2, n3):
            self.assertEqual(len(n.children), 2)
            self.assertEqual(n.tag, 'nested')
            self.assertEqual(n.path, 'some_nested_path')

            q = next((c for c in n.children if isinstance(c, QueryP)))
            self.assertEqual(q.serialize(), {'query': {}})
            # ensure term query is present
            self.assertEqual(len(q.children), 1)
            self.assertIsInstance(q.children[0], Term)

            p = next((c for c in n.children if isinstance(c, Path)))
            self.assertEqual(p.body['value'], 'some_nested_path')
