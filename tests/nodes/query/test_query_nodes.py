
from __future__ import unicode_literals

from pandagg.nodes.query.compound import Bool
from pandagg.nodes.query.term_level import Terms
from pandagg.nodes.query.abstract import QueryClause
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
