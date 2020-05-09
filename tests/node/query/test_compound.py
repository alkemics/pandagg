from __future__ import unicode_literals

from unittest import TestCase

from pandagg.node.query._parameter_clause import (
    Boost,
    Positive,
    Negative,
    NegativeBoost,
    Filter,
    Should,
)
from pandagg.node.query.term_level import Range
from pandagg.query import Bool, Term, Boosting


class CompoundQueriesTestCase(TestCase):
    def test_bool(self):
        # test all possibles definitions
        b1 = Bool(
            filter=Term(some_field=2),
            should=[Range(other={"gte": 1}), Term(some=3)],
            boost=1.2,
        )
        b2 = Bool(
            filter=[Term(some_field=2)],
            should=[Range(other={"gte": 1}), Term(some=3)],
            boost=1.2,
        )
        b3 = Bool(
            filter={"term": {"some_field": 2}},
            should=[{"range": {"other": {"gte": 1}}}, {"term": {"some": 3}}],
            boost=1.2,
        )
        b4 = Bool(
            filter=[{"term": {"some_field": 2}}],
            should=[{"range": {"other": {"gte": 1}}}, {"term": {"some": 3}}],
            boost=1.2,
        )
        b5 = Bool(
            {
                "filter": {"term": {"some_field": 2}},
                "should": [{"range": {"other": {"gte": 1}}}, {"term": {"some": 3}}],
                "boost": 1.2,
            }
        )
        b6 = Bool(
            {
                "filter": [{"term": {"some_field": 2}}],
                "should": [{"range": {"other": {"gte": 1}}}, {"term": {"some": 3}}],
                "boost": 1.2,
            }
        )
        b7 = Bool(
            {
                "should": [{"range": {"other": {"gte": 1}}}, {"term": {"some": 3}}],
                "boost": 1.2,
            },
            filter=[{"term": {"some_field": 2}}],
        )
        for b in (b1, b2, b3, b4, b5, b6, b7):
            self.assertEqual(
                b.to_dict(),
                {
                    "bool": {
                        "boost": 1.2,
                        "filter": [{"term": {"some_field": {"value": 2}}}],
                        "should": [
                            {"range": {"other": {"gte": 1}}},
                            {"term": {"some": {"value": 3}}},
                        ],
                    }
                },
            )
            self.assertEqual(len(b._children), 3)
            self.assertEqual(b.line_repr(depth=None), "bool")

            boost = next((c for c in b._children if isinstance(c, Boost)))
            self.assertEqual(boost.to_dict(), {"boost": 1.2})
            self.assertEqual(boost.line_repr(depth=None), "boost=1.2")

            filter_ = next((c for c in b._children if isinstance(c, Filter)))
            self.assertEqual(len(filter_._children), 1)
            self.assertEqual(filter_.line_repr(depth=None), "filter")
            t = filter_._children[0]
            self.assertIsInstance(t, Term)

            should = next((c for c in b._children if isinstance(c, Should)))
            self.assertEqual(len(should._children), 2)
            next((c for c in should._children if isinstance(c, Term)))
            next((c for c in should._children if isinstance(c, Range)))

    def test_boosting(self):
        b1 = Boosting(
            positive=Term(field="text", value="apple"),
            negative=Term(field="text", value="pie tart fruit crumble tree"),
            negative_boost=0.5,
        )

        b2 = Boosting(
            positive={"term": {"text": "apple"}},
            negative={"term": {"text": "pie tart fruit crumble tree"}},
            negative_boost=0.5,
        )

        b3 = Boosting(
            {
                "positive": {"term": {"text": "apple"}},
                "negative": {"term": {"text": "pie tart fruit crumble tree"}},
                "negative_boost": 0.5,
            }
        )
        for b in (b1, b2, b3):
            self.assertEqual(len(b._children), 3)
            self.assertEqual(b.line_repr(depth=None), "boosting")

            negative_boosting = next(
                (c for c in b._children if isinstance(c, NegativeBoost))
            )
            self.assertEqual(
                negative_boosting.line_repr(depth=None), "negative_boost=0.5"
            )
            self.assertEqual(negative_boosting.body, {"value": 0.5})

            positive = next((c for c in b._children if isinstance(c, Positive)))
            self.assertEqual(len(positive._children), 1)
            self.assertEqual(
                positive.to_dict(),
                {"positive": [{"term": {"text": {"value": "apple"}}}]},
            )
            self.assertEqual(positive.line_repr(depth=None), "positive")
            positive_term = positive._children[0]
            self.assertIsInstance(positive_term, Term)
            self.assertEqual(positive_term.field, "text")
            self.assertEqual(positive_term.body, {"text": {"value": "apple"}})
            self.assertEqual(
                positive_term.to_dict(), {"term": {"text": {"value": "apple"}}}
            )
            self.assertEqual(
                positive_term.line_repr(depth=None), 'term, field=text, value="apple"'
            )

            negative = next((c for c in b._children if isinstance(c, Negative)))
            self.assertEqual(len(negative._children), 1)
            self.assertEqual(
                negative.to_dict(),
                {
                    "negative": [
                        {"term": {"text": {"value": "pie tart fruit crumble tree"}}}
                    ]
                },
            )
            self.assertEqual(negative.line_repr(depth=None), "negative")
            negative_term = negative._children[0]
            self.assertIsInstance(negative_term, Term)
            self.assertEqual(negative_term.field, "text")
            self.assertEqual(
                negative_term.body, {"text": {"value": "pie tart fruit crumble tree"}}
            )
            self.assertEqual(
                negative_term.to_dict(),
                {"term": {"text": {"value": "pie tart fruit crumble tree"}}},
            )
            self.assertEqual(
                negative_term.line_repr(depth=None),
                'term, field=text, value="pie tart fruit crumble tree"',
            )
