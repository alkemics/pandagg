
from __future__ import unicode_literals

from pandagg.base.node.query import Terms, Bool, Term
from unittest import TestCase

from pandagg.base.node.query._parameter_clause import Boost, Filter


class QueryNodesTestCase(TestCase):

    def test_terms_clause(self):
        q = Terms(field='some_field', value=2, boost=1)
        body = {'some_field': {'value': 2, 'boost': 1}}
        expected = {'terms': body}
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q, expected)
        self.assertEqual(q.deserialize(**body), expected)

    def test_bool(self):
        # test all possibles definitions
        b1 = Bool(
            filter=Term(field='some_field', value=2),
            boost=1.2
        )
        b2 = Bool(
            filter=[Term(field='some_field', value=2)],
            boost=1.2
        )
        b3 = Bool(
            filter={'term': {'some_field': {'value': 2}}},
            boost=1.2
        )
        b4 = Bool({
            'filter': {'term': {'some_field': {'value': 2}}},
            'boost': 1.2
        })
        for b in (b1, b2, b3, b4):
            self.assertEqual(len(b.children), 2)
            boost = next((c for c in b.children if isinstance(c, Boost)))
            self.assertEqual(boost.serialize(), {'boost': 1.2})
            filter_ = next((c for c in b.children if isinstance(c, Filter)))
            self.assertEqual(len(filter_.children), 1)
            t = filter_.children[0]
            self.assertIsInstance(t, Term)
            self.assertEqual(t.field, 'some_field')
            self.assertEqual(t.body, {'some_field': {'value': 2}})
            self.assertEqual(t.serialize(), {'term': {'some_field': {'value': 2}}})
