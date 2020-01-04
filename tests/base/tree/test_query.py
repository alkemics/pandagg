from pandagg.query import Query
from pandagg.query import Term, Bool


from unittest import TestCase


class AggNodesTestCase(TestCase):

    def test_term_query(self):
        q = Query(from_=Term(field='some_field', value=2))
        self.assertEqual(q.query_dict(), {'term': {'some_field': {'value': 2}}})

    def test_query(self):

        # q = Query(from_=Bool())
        pass
