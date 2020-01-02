
from __future__ import unicode_literals

from pandagg.base.node.query import Terms
from unittest import TestCase


class AggNodesTestCase(TestCase):

    def test_terms_clause(self):
        q = Terms(field='some_field', value=2, boost=1)
        body = {'some_field': {'value': 2, 'boost': 1}}
        expected = {'terms': body}
        self.assertEqual(q.serialize(), expected)
        self.assertEqual(q, expected)
        self.assertEqual(q.deserialize(body), expected)

    def test_bool(self):
        pass
