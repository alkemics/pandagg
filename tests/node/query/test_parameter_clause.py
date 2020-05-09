from __future__ import unicode_literals

from unittest import TestCase

from pandagg.query import Term, Range
from pandagg.node.query._parameter_clause import Filter


class ParameterClausesTestCase(TestCase):
    def test_filter_parameter(self):
        # test all possibles definitions
        f1 = Filter(
            Term(field="some_field", value=1), Range(field="other_field", gte=2)
        )
        f2 = Filter(
            [Term(field="some_field", value=1), Range(field="other_field", gte=2)]
        )
        f3 = Filter(
            {"term": {"some_field": {"value": 1}}},
            {"range": {"other_field": {"gte": 2}}},
        )
        f4 = Filter(
            [
                {"term": {"some_field": {"value": 1}}},
                {"range": {"other_field": {"gte": 2}}},
            ]
        )
        for f in (f1, f2, f3, f4):
            self.assertEqual(len(f._children), 2)
            self.assertEqual(f.line_repr(depth=None), "filter")

            term = next((c for c in f._children if isinstance(c, Term)))
            self.assertEqual(term.to_dict(), {"term": {"some_field": {"value": 1}}})

            range_ = next((c for c in f._children if isinstance(c, Range)))
            self.assertEqual(range_.to_dict(), {"range": {"other_field": {"gte": 2}}})
