#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mock import patch

from pandagg.node.query._parameter_clause import Must
from pandagg.node.query.joining import Nested
from pandagg.query import Query, Range, Prefix, Ids, Term, Terms, Nested
from pandagg.node.query.term_level import Term as TermNode, Exists as ExistsNode
from pandagg.node.query.joining import Nested as NestedNode
from pandagg.tree.query.compound import Bool
from pandagg.node.query.compound import Bool as BoolNode

from pandagg.utils import equal_queries, ordered
from tests import PandaggTestCase


class QueryTestCase(PandaggTestCase):
    def setUp(self):
        patcher = patch("uuid.uuid4", side_effect=range(1000))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_term_query(self):
        q = Query(Term(field="some_field", value=2))
        self.assertEqual(q.to_dict(), {"term": {"some_field": {"value": 2}}})

    def test_query_compound(self):
        q = Query(Bool(filter=[Term(field="yolo", value=2)], boost=2))
        self.assertEqual(
            q.to_dict(),
            {"bool": {"boost": 2, "filter": [{"term": {"yolo": {"value": 2}}}]}},
        )
        self.assertEqual(
            q.__str__(),
            """<Query>
bool, boost=2
└── filter
    └── term, field=yolo, value=2
""",
        )

    def test_new_bool_must_above_node(self):
        initial_q = Query()
        result_q = initial_q.bool(_name="root_bool")
        self.assertEqual(result_q.to_dict(), None)

        # above element (in filter? in must? in should?)
        initial_q = Query(Term(field="some_field", value=2, _name="term_q"))
        result_q = initial_q.bool(_name="root_bool", child="term_q", child_param="must")
        self.assertEqual(
            result_q.to_dict(),
            {
                "bool": {
                    "_name": "root_bool",
                    "must": [{"term": {"_name": "term_q", "some_field": {"value": 2}}}],
                }
            },
        )

        # above element (without declaring above: default behavior is wrapping at root)
        initial_q = Query(Term(field="some_field", value=2, _name="term_q"))
        result_q = initial_q.bool(_name="root_bool", child_param="must")
        self.assertEqual(
            result_q.to_dict(),
            {
                "bool": {
                    "_name": "root_bool",
                    "must": [{"term": {"_name": "term_q", "some_field": {"value": 2}}}],
                }
            },
        )

        # above bool element
        initial_q = Query(
            Bool(
                _name="init_bool",
                should=[
                    Term(field="some_field", value="prod", _name="prod_term"),
                    Term(field="other_field", value="pizza", _name="pizza_term"),
                ],
            )
        )
        result_q = initial_q.bool(
            child="init_bool", _name="top_bool", filter=Range(field="price", gte=12)
        )
        self.assertEqual(
            ordered(result_q.to_dict()),
            ordered(
                {
                    "bool": {
                        "_name": "top_bool",
                        "filter": [{"range": {"price": {"gte": 12}}}],
                        "must": [
                            {
                                "bool": {
                                    "_name": "init_bool",
                                    "should": [
                                        {
                                            "term": {
                                                "_name": "prod_term",
                                                "some_field": {"value": "prod"},
                                            }
                                        },
                                        {
                                            "term": {
                                                "_name": "pizza_term",
                                                "other_field": {"value": "pizza"},
                                            }
                                        },
                                    ],
                                }
                            }
                        ],
                    }
                }
            ),
        )

        # above element in existing bool query
        initial_q = Query(
            Bool(
                _name="init_bool",
                should=[
                    Term(field="some_field", value="prod", _name="prod_term"),
                    Term(field="other_field", value="pizza", _name="pizza_term"),
                ],
            )
        )
        result_q = initial_q.bool(
            child="init_bool",
            _name="top_bool",
            child_param="must",
            filter=Range(field="price", gte=12),
        )
        self.assertTrue(
            ordered(result_q.to_dict()),
            ordered(
                {
                    "bool": {
                        "_name": "top_bool",
                        "filter": [{"range": {"price": {"gte": 12}}}],
                        "must": [
                            {
                                "bool": {
                                    "_name": "init_bool",
                                    "should": [
                                        {"term": {"some_field": {"value": "prod"}}},
                                        {"term": {"other_field": {"value": "pizza"}}},
                                    ],
                                }
                            }
                        ],
                    }
                }
            ),
        )

    def test_new_bool_below_node(self):
        # below single child parameter
        initial_q = Query(Nested(path="some_nested", _name="nested_id"))
        result_q = initial_q.bool(
            parent="nested_id", _name="bool_id", filter={"term": {"some_nested.id": 2}}
        )
        self.assertEqual(
            result_q.to_dict(),
            {
                "nested": {
                    "_name": "nested_id",
                    "path": "some_nested",
                    "query": {
                        "bool": {
                            "_name": "bool_id",
                            "filter": [{"term": {"some_nested.id": {"value": 2}}}],
                        }
                    },
                }
            },
        )

        initial_q = Query(
            Bool(
                _name="init_bool",
                should=[
                    Term(field="some_field", value="prod", _name="prod_term"),
                    Term(field="other_field", value="pizza", _name="pizza_term"),
                ],
            )
        )
        result_q = initial_q.bool(
            parent="init_bool",
            parent_param="filter",
            _name="below_bool",
            must={"term": {"new_field": 2}},
        )
        self.assertTrue(
            equal_queries(
                result_q.to_dict(),
                {
                    "bool": {
                        "_name": "init_bool",
                        "filter": [
                            {
                                "bool": {
                                    "_name": "below_bool",
                                    "must": [{"term": {"new_field": {"value": 2}}}],
                                }
                            }
                        ],
                        "should": [
                            {
                                "term": {
                                    "_name": "prod_term",
                                    "some_field": {"value": "prod"},
                                }
                            },
                            {
                                "term": {
                                    "_name": "pizza_term",
                                    "other_field": {"value": "pizza"},
                                }
                            },
                        ],
                    }
                },
            )
        )

    def test_not_possible_parent_child(self):
        initial_q = Query(
            Bool(
                _name="init_bool",
                should=[
                    Term(field="some_field", value="prod", _name="prod_term"),
                    Term(field="other_field", value="pizza", _name="pizza_term"),
                ],
            )
        )

        # UNDER non-existing
        with self.assertRaises(ValueError) as e:
            initial_q.must(
                {"term": {"new_field": 2}},
                parent="not_existing_node",
                _name="somewhere",
            )
        self.assertEqual(
            e.exception.args,
            ("Parent <not_existing_node> does not exist in current query.",),
        )

        with self.assertRaises(ValueError) as e:
            initial_q.must(
                {"term": {"new_field": 2}},
                parent="not_existing_node",
                _name="somewhere",
            )
        self.assertEqual(
            e.exception.args,
            ("Parent <not_existing_node> does not exist in current query.",),
        )

        # ABOVE non-existing
        with self.assertRaises(ValueError) as e:
            initial_q.must(
                {"term": {"new_field": 2}},
                child="not_existing_node",
                _name="somewhere",
            )
        self.assertEqual(
            e.exception.args,
            ("Child <not_existing_node> does not exist in current query.",),
        )

        with self.assertRaises(ValueError) as e:
            initial_q.must(
                {"term": {"new_field": 2}},
                child="not_existing_node",
                _name="somewhere",
            )
        self.assertEqual(
            e.exception.args,
            ("Child <not_existing_node> does not exist in current query.",),
        )

        # UNDER leaf
        with self.assertRaises(Exception) as e:
            initial_q.must(
                {"term": {"new_field": 2}}, parent="pizza_term", _name="somewhere"
            )
        self.assertEqual(
            e.exception.args,
            (
                "Cannot place clause under non-compound clause <pizza_term> of type <term>.",
            ),
        )

        with self.assertRaises(Exception) as e:
            initial_q.must(
                {"term": {"new_field": 2}}, parent="pizza_term", _name="somewhere"
            )
        self.assertEqual(
            e.exception.args,
            (
                "Cannot place clause under non-compound clause <pizza_term> of type <term>.",
            ),
        )

    def test_replace_all_existing_bool(self):
        initial_q = Query(
            Bool(
                _name="init_bool",
                should=[
                    Term(field="some_field", value="prod", _name="prod_term"),
                    Term(field="other_field", value="pizza", _name="pizza_term"),
                ],
            )
        )
        result_q = initial_q.bool(
            _name="init_bool",
            filter=Range(field="price", gte=12, _name="price_range"),
            mode="replace_all",
        )
        self.assertNotIn("prod_term", result_q)
        self.assertNotIn("pizza_term", result_q)
        self.assertIn("price_range", result_q)

    def test_add_existing_bool(self):
        initial_q = Query(
            Bool(
                _name="init_bool",
                should=[
                    Term(field="some_field", value="prod", _name="prod_term"),
                    Term(field="other_field", value="pizza", _name="pizza_term"),
                ],
            )
        )
        result_q = initial_q.bool(
            _name="init_bool",
            filter=Range(field="price", gte=12, _name="price_range"),
            should=Term(field="other_field", value="new_pizza", _name="new_pizza_term"),
            mode="add",
        )
        self.assertIn("prod_term", result_q)
        self.assertIn("pizza_term", result_q)
        self.assertIn("new_pizza_term", result_q)
        self.assertIn("price_range", result_q)
        self.assertQueryEqual(
            result_q.to_dict(),
            {
                "bool": {
                    "_name": "init_bool",
                    "filter": [
                        {"range": {"_name": "price_range", "price": {"gte": 12}}}
                    ],
                    "should": [
                        {
                            "term": {
                                "_name": "prod_term",
                                "some_field": {"value": "prod"},
                            }
                        },
                        {
                            "term": {
                                "_name": "pizza_term",
                                "other_field": {"value": "pizza"},
                            }
                        },
                        {
                            "term": {
                                "_name": "new_pizza_term",
                                "other_field": {"value": "new_pizza"},
                            }
                        },
                    ],
                }
            },
        )

    def test_replace_existing_bool(self):
        # replace != replace_all
        initial_q = Query(
            Bool(
                _name="init_bool",
                should=[
                    Term(field="some_field", value="prod", _name="prod_term"),
                    Term(field="other_field", value="pizza", _name="pizza_term"),
                ],
                filter=Range(field="price", gte=50, _name="very_high_price"),
                minimum_should_match=2,
            )
        )
        result_q = initial_q.bool(
            _name="init_bool",
            filter=Range(field="price", gte=20, _name="high_price"),
            mode="replace",
        )
        self.assertIn("prod_term", result_q)
        self.assertIn("pizza_term", result_q)
        self.assertIn("high_price", result_q)
        self.assertNotIn("very_high_price", result_q)
        self.assertTrue(
            equal_queries(
                result_q.to_dict(),
                {
                    "bool": {
                        "_name": "init_bool",
                        "filter": [
                            {"range": {"_name": "high_price", "price": {"gte": 20}}}
                        ],
                        "should": [
                            {
                                "term": {
                                    "_name": "prod_term",
                                    "some_field": {"value": "prod"},
                                }
                            },
                            {
                                "term": {
                                    "_name": "pizza_term",
                                    "other_field": {"value": "pizza"},
                                }
                            },
                        ],
                        "minimum_should_match": 2,
                    }
                },
            )
        )

    def test_must_at_root(self):
        q_i1 = Query()
        q1 = q_i1.must(
            Term(field="some_field", value=2, _name="term_nid"), _name="bool_nid",
        )
        self.assertEqual(len(q_i1.list()), 0)
        bool_ = next((c for c in q1.list() if isinstance(c, BoolNode)))
        self.assertEqual(bool_.name, "bool_nid")
        next((c for c in q1.list() if isinstance(c, Must)))
        term = next((c for c in q1.list() if isinstance(c, TermNode)))
        self.assertEqual(term.name, "term_nid")

        self.assertEqual(
            q1.__str__(),
            """<Query>
bool
└── must
    └── term, field=some_field, value=2
""",
        )

        q_i2 = Query()
        q2 = q_i2.must({"term": {"some_field": {"value": 2}}})
        self.assertEqual(len(q_i2.list()), 0)
        next((c for c in q2.list() if isinstance(c, BoolNode)))
        next((c for c in q2.list() if isinstance(c, Must)))
        next((c for c in q2.list() if isinstance(c, TermNode)))
        self.assertEqual(
            q2.__str__(),
            """<Query>
bool
└── must
    └── term, field=some_field, value=2
""",
        )

        # with multiple conditions (with different declarations)
        q_i3 = Query()
        q3 = q_i3.must(
            [
                {"exists": {"field": "some_field"}},
                {"term": {"other_field": {"value": 5}}},
            ]
        )
        next((c for c in q3.list() if isinstance(c, BoolNode)))
        next((c for c in q3.list() if isinstance(c, Must)))
        next((c for c in q3.list() if isinstance(c, TermNode)))
        next((c for c in q3.list() if isinstance(c, ExistsNode)))
        self.assertEqual(
            q3.__str__(),
            """<Query>
bool
└── must
    ├── exists, field=some_field
    └── term, field=other_field, value=5
""",
        )

    def test_must_method(self):
        q = (
            Query()
            .bool(filter=Term(field="field_a", value=2), _name="root_bool")
            .must({"exists": {"field": "field_b"}}, _name="root_bool")
            .must(Prefix(field="field_c", value="pre"), _name="root_bool")
            .must(
                Bool(
                    _name="level_1_bool",
                    should=[Range(field="field_d", gte=3), Ids(values=[1, 2, 3])],
                ),
                _name="root_bool",
            )
        )

        self.assertEqual(
            q.__str__(),
            """<Query>
bool
├── filter
│   └── term, field=field_a, value=2
└── must
    ├── exists, field=field_b
    ├── bool
    │   └── should
    │       ├── ids, values=[1, 2, 3]
    │       └── range, field=field_d, gte=3
    └── prefix, field=field_c, value="pre"
""",
        )

    def test_nested(self):
        q = Query()
        q1 = q.nested(
            query=Term(field="some_nested_field.other", value=2),
            path="some_nested_path",
            _name="nested_id",
        )
        self.assertEqual(
            q1.to_dict(),
            {
                "nested": {
                    "_name": "nested_id",
                    "path": "some_nested_path",
                    "query": {"term": {"some_nested_field.other": {"value": 2}}},
                }
            },
        )
        self.assertEqual(q1.root, "nested_id")

    def test_should(self):
        q = Query()
        q1 = q.should(
            Term(field="some_field", value=2),
            Term(field="other_field", value=3),
            _name="created_bool",
        )
        self.assertTrue(
            equal_queries(
                q1.to_dict(),
                {
                    "bool": {
                        "_name": "created_bool",
                        "should": [
                            {"term": {"some_field": {"value": 2}}},
                            {"term": {"other_field": {"value": 3}}},
                        ],
                    }
                },
            )
        )
        self.assertEqual(q1.root, "created_bool")

    def test_filter(self):
        q = Query()
        q1 = q.filter(
            Term(field="some_field", value=2),
            Term(field="other_field", value=3),
            _name="created_bool",
        )
        self.assertQueryEqual(
            q1.to_dict(),
            {
                "bool": {
                    "_name": "created_bool",
                    "filter": [
                        {"term": {"some_field": {"value": 2}}},
                        {"term": {"other_field": {"value": 3}}},
                    ],
                }
            },
        )
        self.assertEqual(q1.root, "created_bool")

    def test_must_not(self):
        q = Query()
        q1 = q.must_not(
            Term(field="some_field", value=2),
            Term(field="other_field", value=3),
            _name="created_bool",
        )
        self.assertTrue(
            equal_queries(
                q1.to_dict(),
                {
                    "bool": {
                        "_name": "created_bool",
                        "must_not": [
                            {"term": {"some_field": {"value": 2}}},
                            {"term": {"other_field": {"value": 3}}},
                        ],
                    }
                },
            )
        )
        self.assertEqual(q1.root, "created_bool")

    def test_deserialize_dict_query(self):
        # simple leaf
        q = Query({"term": {"some_field": {"value": 2}}})
        self.assertEqual(q.to_dict(), {"term": {"some_field": {"value": 2}}})
        self.assertEqual(len(q.list()), 1)
        n = q.get(q.root)
        self.assertIsInstance(n, TermNode)
        self.assertEqual(n.field, "some_field")

        # bool simple leaf
        q = Query({"bool": {"must_not": {"term": {"some_field": {"value": 2}}}}})
        self.assertEqual(len(q.list()), 3)
        n = q.get(q.root)
        self.assertIsInstance(n, BoolNode)
        self.assertEqual(
            q.to_dict(),
            {"bool": {"must_not": [{"term": {"some_field": {"value": 2}}}]}},
        )

        # bool multiple leaves
        q = Query(
            {
                "bool": {
                    "must_not": [
                        {"term": {"some_field": {"value": 2}}},
                        {"term": {"other_field": {"value": 3}}},
                    ]
                }
            }
        )
        self.assertEqual(len(q.list()), 4)
        n = q.get(q.root)
        self.assertIsInstance(n, BoolNode)
        self.assertQueryEqual(
            q.to_dict(),
            {
                "bool": {
                    "must_not": [
                        {"term": {"some_field": {"value": 2}}},
                        {"term": {"other_field": {"value": 3}}},
                    ]
                }
            },
        )

        # nested compound queries
        q = Query(
            {
                "nested": {
                    "path": "some_nested_path",
                    "query": {
                        "bool": {
                            "must_not": [
                                {"term": {"some_field": {"value": 2}}},
                                {"term": {"other_field": {"value": 3}}},
                            ]
                        }
                    },
                }
            }
        )
        self.assertEqual(len(q.list()), 6)
        n = q.get(q.root)
        self.assertIsInstance(n, NestedNode)
        self.assertQueryEqual(
            q.to_dict(),
            {
                "nested": {
                    "path": "some_nested_path",
                    "query": {
                        "bool": {
                            "must_not": [
                                {"term": {"some_field": {"value": 2}}},
                                {"term": {"other_field": {"value": 3}}},
                            ]
                        }
                    },
                }
            },
        )

    def test_query_method(self):
        # on empty query
        q = Query()
        q1 = q.query(Bool(_name="root_bool", must=Term(field="some_field", value=2)))
        q2 = q.query(
            {"bool": {"_name": "root_bool", "must": {"term": {"some_field": 2}}}}
        )
        for r in (q1, q2):
            self.assertEqual(
                r.__str__(),
                """<Query>
bool
└── must
    └── term, field=some_field, value=2
""",
            )

        # query WITHOUT defined parent -> must on top (existing bool)
        q = Query(Bool(_name="root_bool", must=Term(field="some_field", value=2)))
        q1 = q.query(Term(field="other_field", value=3))
        q2 = q.query({"term": {"other_field": 3}})
        for r in (q1, q2):
            self.assertEqual(
                r.__str__(),
                """<Query>
bool
└── must
    ├── term, field=some_field, value=2
    └── term, field=other_field, value=3
""",
            )

        # query WITHOUT defined parent -> must on top (non-existing bool)
        q = Query(Term(field="some_field", value=2))
        q1 = q.query(Term(field="other_field", value=3))
        q2 = q.query({"term": {"other_field": 3}})
        for r in (q1, q2):
            self.assertEqual(
                r.__str__(),
                """<Query>
bool
└── must
    ├── term, field=some_field, value=2
    └── term, field=other_field, value=3
""",
            )

        # query WITH defined parent (use default operator)
        q = Query(Nested(_name="root_nested", path="some_nested_path"))
        q1 = q.query(Term(field="some_nested_path.id", value=2), parent="root_nested")
        q2 = q.query({"term": {"some_nested_path.id": 2}}, parent="root_nested")
        for r in (q1, q2):
            self.assertEqual(
                r.__str__(),
                """<Query>
nested, path="some_nested_path"
└── query
    └── term, field=some_nested_path.id, value=2
""",
            )

        q = Query(Bool(_name="root_bool", filter=Term(field="some_field", value=2)))
        q1 = q.query(Term(field="some_other_field", value=2), parent="root_bool")
        q2 = q.query({"term": {"some_other_field": 2}}, parent="root_bool")
        for r in (q1, q2):
            self.assertEqual(
                r.__str__(),
                """<Query>
bool
├── filter
│   └── term, field=some_field, value=2
└── must
    └── term, field=some_other_field, value=2
""",
            )

        # query WITH defined parent and explicit operator
        q = Query(Bool(_name="root_bool", filter=Term(field="some_field", value=2)))
        q1 = q.query(
            Range(field="some_other_field", gte=3),
            parent="root_bool",
            parent_param="must_not",
        )
        q2 = q.query(
            {"range": {"some_other_field": {"gte": 3}}},
            parent="root_bool",
            parent_param="must_not",
        )
        for r in (q1, q2):
            self.assertEqual(
                r.__str__(),
                """<Query>
bool
├── filter
│   └── term, field=some_field, value=2
└── must_not
    └── range, field=some_other_field, gte=3
""",
            )

        # INVALID
        # query with leaf parent
        q = Query(Term(field="some_field", value=2, _name="leaf_node"))
        with self.assertRaises(ValueError) as e:
            q.query(Range(field="some_other_field", gte=3), parent="leaf_node")
        self.assertEqual(
            e.exception.args[0],
            "Cannot place clause under non-compound clause <leaf_node> of type <term>.",
        )
        with self.assertRaises(ValueError) as e:
            q.query({"range": {"some_other_field": {"gte": 3}}}, parent="leaf_node")
        self.assertEqual(
            e.exception.args[0],
            "Cannot place clause under non-compound clause <leaf_node> of type <term>.",
        )

        # query WITH wrong parent operator
        q = Query(Bool(_name="root_bool", filter=Term(field="some_field", value=2)))
        with self.assertRaises(ValueError) as e:
            q.query(
                Range(field="some_other_field", gte=3),
                parent="root_bool",
                parent_param="invalid_param",
            )
        self.assertEqual(
            e.exception.args[0],
            "Child operator <invalid_param> not permitted for compound query of type <Bool>",
        )
        with self.assertRaises(ValueError) as e:
            q.query(
                {"range": {"some_other_field": {"gte": 3}}},
                parent="root_bool",
                parent_param="invalid_param",
            )
        self.assertEqual(
            e.exception.args[0],
            "Child operator <invalid_param> not permitted for compound query of type <Bool>",
        )

        # query WITH non-existing parent
        q = Query(Bool(_name="root_bool", filter=Term(field="some_field", value=2)))
        with self.assertRaises(ValueError) as e:
            q.query(Range(field="some_other_field", gte=3), parent="yolo_id")
        self.assertEqual(
            e.exception.args[0], "Parent <yolo_id> does not exist in current query."
        )
        with self.assertRaises(ValueError) as e:
            q.query({"range": {"some_other_field": {"gte": 3}}}, parent="yolo_id")
        self.assertEqual(
            e.exception.args[0], "Parent <yolo_id> does not exist in current query."
        )

    def test_nested_query(self):
        q = (
            Query()
            .nested(path="some_nested", _name="nested_id")
            .query(Term(field="some_nested.type", value=2), parent="nested_id")
        )

        self.assertEqual(
            q.to_dict(),
            {
                "nested": {
                    "_name": "nested_id",
                    "path": "some_nested",
                    "query": {"term": {"some_nested.type": {"value": 2}}},
                }
            },
        )

    def test_must_below_nested_query(self):
        q = (
            Query()
            .nested(path="some_nested", _name="nested_id")
            .query(Term(field="some_nested.type", value=2), parent="nested_id")
            .query(
                Range(field="some_nested.creationYear", gte="2020"), parent="nested_id"
            )
        )

        self.assertTrue(
            equal_queries(
                q.to_dict(),
                {
                    "nested": {
                        "_name": "nested_id",
                        "path": "some_nested",
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "range": {
                                            "some_nested.creationYear": {"gte": "2020"}
                                        }
                                    },
                                    {"term": {"some_nested.type": {"value": 2}}},
                                ]
                            }
                        },
                    }
                },
            )
        )

    def test_multiple_compound_on_top(self):
        q = (
            Query()
            .nested(path="some_nested_path", query=Term("some_nested_path.id", value=3))
            .should([Term(field="type", value=2)], _name="top")
        )
        self.assertEqual(
            q.to_dict(),
            {
                "bool": {
                    "_name": "top",
                    "must": [
                        {
                            "nested": {
                                "path": "some_nested_path",
                                "query": {
                                    "term": {"some_nested_path.id": {"value": 3}}
                                },
                            }
                        }
                    ],
                    "should": [{"term": {"type": {"value": 2}}}],
                }
            },
        )

    def test_multiple_must_below_nested_query(self):
        q1 = (
            Query()
            .query(Nested(path="some_nested", _name="nested_id"))
            .query(Range(field="some_nested.price", lte=100), parent="nested_id")
            .query(
                Range(field="some_nested.creationYear", gte="2020"), parent="nested_id"
            )
            .query(Term(field="some_nested.type", value=2), parent="nested_id")
        )

        q2 = (
            Query()
            .nested(path="some_nested", _name="nested_id")
            .query(Range(field="some_nested.price", lte=100), parent="nested_id")
            .query(
                Range(field="some_nested.creationYear", gte="2020"), parent="nested_id"
            )
            .query(Term(field="some_nested.type", value=2), parent="nested_id")
        )

        q3 = (
            Query()
            .nested(path="some_nested", _name="nested_id")
            .bool(
                parent="nested_id",
                must=[
                    Range(field="some_nested.creationYear", gte="2020"),
                    Range(field="some_nested.price", lte=100),
                    Term(field="some_nested.type", value=2),
                ],
            )
        )

        q4 = (
            Query()
            .query({"nested": {"path": "some_nested", "_name": "nested_id"}})
            .query(
                {
                    "bool": {
                        "must": [
                            {"range": {"some_nested.creationYear": {"gte": "2020"}}},
                            {"range": {"some_nested.price": {"lte": 100}}},
                            {"term": {"some_nested.type": {"value": 2}}},
                        ]
                    }
                },
                parent="nested_id",
            )
        )

        expected = {
            "nested": {
                "_name": "nested_id",
                "path": "some_nested",
                "query": {
                    "bool": {
                        "must": [
                            {"range": {"some_nested.creationYear": {"gte": "2020"}}},
                            {"range": {"some_nested.price": {"lte": 100}}},
                            {"term": {"some_nested.type": {"value": 2}}},
                        ]
                    }
                },
            }
        }
        for i, q in enumerate((q1, q2, q3, q4)):
            self.assertTrue(
                equal_queries(q.to_dict(), expected), "failed on %d" % (i + 1)
            )

        q5 = (
            Query()
            .nested(path="some_nested", _name="nested_id")
            .must(
                Range(field="some_nested.creationYear", gte="2020"),
                parent="nested_id",
                _name="bool_id",
            )
            .must(Range(field="some_nested.price", lte=100), _name="bool_id")
            .must(Term(field="some_nested.type", value=2), _name="bool_id")
        )
        self.assertEqual(
            ordered(q5.to_dict()),
            ordered(
                {
                    "nested": {
                        "_name": "nested_id",
                        "path": "some_nested",
                        "query": {
                            "bool": {
                                "_name": "bool_id",
                                "must": [
                                    {
                                        "range": {
                                            "some_nested.creationYear": {"gte": "2020"}
                                        }
                                    },
                                    {"term": {"some_nested.type": {"value": 2}}},
                                    {"range": {"some_nested.price": {"lte": 100}}},
                                ],
                            }
                        },
                    }
                }
            ),
        )

    def test_autonested(self):
        q = Query(
            mapping={
                "properties": {
                    "actors": {
                        "type": "nested",
                        "properties": {"id": {"type": "keyword"}},
                    }
                }
            },
            nested_autocorrect=True,
        )
        self.assertEqual(
            q.query("term", actors__id=2).to_dict(),
            {
                "nested": {
                    "path": "actors",
                    "query": {"term": {"actors.id": {"value": 2}}},
                }
            },
        )

    def test_query_unnamed_inserts(self):

        q = (
            Query()
            .must(Terms(genres=["Action", "Thriller"]))
            .must(Range("rank", gte=7))
            .query(Nested(_name="nested_roles", path="roles"))
        )

        # we name the nested query that we would potentially use
        # but a compound clause (bool, nested etc..) without any children clauses is not serialized
        self.assertEqual(
            ordered(q.to_dict()),
            ordered(
                {
                    "bool": {
                        "must": [
                            {"terms": {"genres": ["Action", "Thriller"]}},
                            {"range": {"rank": {"gte": 7}}},
                        ]
                    }
                }
            ),
        )

        # we declare that those clauses must be placed below 'nested_roles' condition
        q = q.query(Term("roles.gender", value="F"), parent="nested_roles").query(
            Term("roles.role", value="Reporter"), parent="nested_roles"
        )

        self.assertTrue(
            equal_queries(
                q.to_dict(),
                {
                    "bool": {
                        "must": [
                            {"terms": {"genres": ["Action", "Thriller"]}},
                            {"range": {"rank": {"gte": 7}}},
                            {
                                "nested": {
                                    "_name": "nested_roles",
                                    "path": "roles",
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {
                                                    "term": {
                                                        "roles.gender": {"value": "F"}
                                                    }
                                                },
                                                {
                                                    "term": {
                                                        "roles.role": {
                                                            "value": "Reporter"
                                                        }
                                                    }
                                                },
                                            ]
                                        }
                                    },
                                }
                            },
                        ]
                    }
                },
            )
        )
