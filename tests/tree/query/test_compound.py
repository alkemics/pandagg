from __future__ import unicode_literals

from pandagg.node.query.term_level import Range
from pandagg.query import Bool, Term, Boosting
from tests import PandaggTestCase


class CompoundQueriesTestCase(PandaggTestCase):
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

        for b in (b1, b2, b3, b4):
            self.assertQueryEqual(
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

        for b in (b1, b2):
            self.assertQueryEqual(
                b.to_dict(),
                {
                    "boosting": {
                        "negative": {
                            "term": {
                                "text": {"value": "pie tart fruit crumble " "tree"}
                            }
                        },
                        "negative_boost": 0.5,
                        "positive": {"term": {"text": {"value": "apple"}}},
                    }
                },
            )
