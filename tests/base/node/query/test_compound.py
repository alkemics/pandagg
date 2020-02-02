
from __future__ import unicode_literals

from unittest import TestCase

from pandagg.node.query._parameter_clause import Boost, Positive, Negative, NegativeBoost, Filter
from pandagg.query import Bool, Term, Boosting


class CompoundQueriesTestCase(TestCase):

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
            self.assertEqual(b.tag, 'bool')

            boost = next((c for c in b.children if isinstance(c, Boost)))
            self.assertEqual(boost.serialize(), {'boost': 1.2})
            self.assertEqual(boost.tag, 'boost=1.2')

            filter_ = next((c for c in b.children if isinstance(c, Filter)))
            self.assertEqual(len(filter_.children), 1)
            self.assertEqual(filter_.tag, 'filter')
            t = filter_.children[0]
            self.assertIsInstance(t, Term)
            self.assertEqual(t.field, 'some_field')
            self.assertEqual(t.body, {'some_field': {'value': 2}})
            self.assertEqual(t.serialize(), {'term': {'some_field': {'value': 2}}})
            self.assertEqual(t.tag, 'term, field=some_field, value=2')

    def test_boosting(self):
        b1 = Boosting(
            positive=Term(field='text', value='apple'),
            negative=Term(field='text', value='pie tart fruit crumble tree'),
            negative_boost=0.5
        )

        b2 = Boosting(
            positive={'term': {'text': 'apple'}},
            negative={'term': {'text': 'pie tart fruit crumble tree'}},
            negative_boost=0.5
        )

        b3 = Boosting({
            'positive': {'term': {'text': 'apple'}},
            'negative': {'term': {'text': 'pie tart fruit crumble tree'}},
            'negative_boost': 0.5
        })
        for b in (b1, b2, b3):
            self.assertEqual(len(b.children), 3)
            self.assertEqual(b.tag, 'boosting')

            negative_boosting = next((c for c in b.children if isinstance(c, NegativeBoost)))
            self.assertEqual(negative_boosting.tag, 'negative_boost=0.5')
            self.assertEqual(negative_boosting.body, {'value': 0.5})

            positive = next((c for c in b.children if isinstance(c, Positive)))
            self.assertEqual(len(positive.children), 1)
            self.assertEqual(positive.serialize(), {'positive': {}})
            self.assertEqual(positive.tag, 'positive')
            positive_term = positive.children[0]
            self.assertIsInstance(positive_term, Term)
            self.assertEqual(positive_term.field, 'text')
            self.assertEqual(positive_term.body, {'text': {'value': 'apple'}})
            self.assertEqual(positive_term.serialize(), {'term': {'text': {'value': 'apple'}}})
            self.assertEqual(positive_term.tag, 'term, field=text, value="apple"')

            negative = next((c for c in b.children if isinstance(c, Negative)))
            self.assertEqual(len(negative.children), 1)
            self.assertEqual(negative.serialize(), {'negative': {}})
            self.assertEqual(negative.tag, 'negative')
            negative_term = negative.children[0]
            self.assertIsInstance(negative_term, Term)
            self.assertEqual(negative_term.field, 'text')
            self.assertEqual(negative_term.body, {'text': {'value': 'pie tart fruit crumble tree'}})
            self.assertEqual(negative_term.serialize(), {'term': {'text': {'value': 'pie tart fruit crumble tree'}}})
            self.assertEqual(negative_term.tag, 'term, field=text, value="pie tart fruit crumble tree"')
