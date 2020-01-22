#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from pandagg.base.node.query.joining import Nested
from pandagg.query import Query, Exists, Range, Prefix, Ids, Filter, Must, Term, Bool
from pandagg.base.node.query.full_text import QueryString


from unittest import TestCase


class QueryTestCase(TestCase):

    def test_term_query(self):
        q = Query(from_=Term(field='some_field', value=2))
        self.assertEqual(q.query_dict(), {'term': {'some_field': {'value': 2}}})

    def test_query_compound(self):
        q = Query(Bool(filter=[Term(field='yolo', value=2)], boost=2))
        self.assertEqual(
            q.query_dict(),
            {
                'bool': {
                    'boost': 2,
                    'filter': [
                        {'term': {'yolo': {'value': 2}}}
                    ]
                }
            }
        )
        self.assertEqual(
            q.__str__().decode('utf-8'),
            '''<Query>
bool
├── boost=2
└── filter
    └── term, field=yolo
'''
        )

    def test_add_node(self):
        # under leafclause
        q = Query(Term(identifier='term_id', field='some_field', value=2))
        with self.assertRaises(ValueError) as e:
            q.add_node(Filter(QueryString(field='other_field', value='salut')), pid='term_id')
        self.assertEqual(e.exception.message, 'Cannot add clause under leaf query clause <term>')

        # under compound clause
        q = Query(Bool(identifier='bool'))
        q.add_node(Filter(Term(field='some_field', value=2)), pid='bool')

        q = Query(Bool(identifier='bool'))
        with self.assertRaises(ValueError):
            q.add_node(Term(field='some_field', value=2), pid='bool')
        self.assertEqual(e.exception.message, 'Cannot add clause under leaf query clause <term>')

    def test_new_bool_must_above_node(self):
        initial_q = Query()
        result_q = initial_q.filter(identifier='root_bool')
        self.assertEqual(result_q.query_dict(), None)

        # above element (in filter? in must? in should?)
        initial_q = Query(Term(field='some_field', value=2, identifier='term_q'))
        result_q = initial_q.bool(identifier='root_bool', child='term_q', child_param='must')
        self.assertEqual(
            result_q.query_dict(),
            {'bool': {'must': [{'term': {'some_field': {'value': 2}}}]}}
        )

        # above element (without declaring above: default behavior is wrapping at root)
        initial_q = Query(Term(field='some_field', value=2, identifier='term_q'))
        result_q = initial_q.bool(identifier='root_bool', child_param='must')
        self.assertEqual(
            result_q.query_dict(),
            {'bool': {'must': [{'term': {'some_field': {'value': 2}}}]}}
        )

        # above bool element
        initial_q = Query(Bool(identifier='init_bool', should=[
            Term(field='some_field', value='prod', identifier='prod_term'),
            Term(field='other_field', value='pizza', identifier='pizza_term'),
        ]))
        result_q = initial_q.bool(
            child='init_bool', identifier='top_bool',
            filter=Range(field='price', gte=12)
        )
        self.assertEqual(
            result_q.query_dict(),
            {'bool': {
                'filter': [{'range': {'price': {'gte': 12}}}],
                'must': [
                    {'bool': {
                        'should': [
                            {'term': {'some_field': {'value': 'prod'}}},
                            {'term': {'other_field': {'value': 'pizza'}}}
                        ]
                    }}
                ]
            }}
        )

        # above element in existing bool query
        initial_q = Query(Bool(identifier='init_bool', should=[
            Term(field='some_field', value='prod', identifier='prod_term'),
            Term(field='other_field', value='pizza', identifier='pizza_term'),
        ]))
        result_q = initial_q.bool(
            child='init_bool', identifier='top_bool', child_param='must',
            filter=Range(field='price', gte=12)
        )
        self.assertEqual(
            result_q.query_dict(),
            {'bool': {
                'filter': [{'range': {'price': {'gte': 12}}}],
                'must': [
                    {'bool': {
                        'should': [
                            {'term': {'some_field': {'value': 'prod'}}},
                            {'term': {'other_field': {'value': 'pizza'}}}
                        ]
                    }}
                ]
            }}
        )

    def test_new_bool_below_node(self):
        # below single child parameter
        initial_q = Query(Nested(path='some_nested', identifier='nested'))
        result_q = initial_q.bool(parent='nested', identifier='bool', filter={'term': {'some_nested.id': 2}})
        self.assertEqual(
            result_q.query_dict(),
            {
                'nested': {
                    'path': 'some_nested',
                    'query': {'bool': {'filter': [{'term': {'some_nested.id': {'value': 2}}}]}}
                }
            }
        )

        initial_q = Query(Bool(identifier='init_bool', should=[
            Term(field='some_field', value='prod', identifier='prod_term'),
            Term(field='other_field', value='pizza', identifier='pizza_term'),
        ]))
        result_q = initial_q.bool(
            parent='init_bool',
            parent_param='filter',
            identifier='below_bool',
            must={'term': {'new_field': 2}}
        )
        self.assertEqual(
            result_q.query_dict(),
            {'bool': {
                'filter': [
                    {'bool': {'must': [{'term': {'new_field': {'value': 2}}}]}}
                ],
                'should': [
                    {'term': {'some_field': {'value': 'prod'}}},
                    {'term': {'other_field': {'value': 'pizza'}}}
                ]
            }}
        )

    def test_not_possible_parent_child(self):
        initial_q = Query(Bool(identifier='init_bool', should=[
            Term(field='some_field', value='prod', identifier='prod_term'),
            Term(field='other_field', value='pizza', identifier='pizza_term'),
        ]))

        # UNDER non-existing
        with self.assertRaises(ValueError) as e:
            initial_q.must(
                {'term': {'new_field': 2}},
                parent='not_existing_node',
                identifier='somewhere'
            )
        self.assertEqual(e.exception.message, 'Parent <not_existing_node> does not exist in current query.')

        with self.assertRaises(ValueError) as e:
            initial_q.must(
                {'term': {'new_field': 2}},
                parent='not_existing_node',
                identifier='somewhere',
            )
        self.assertEqual(e.exception.message, 'Parent <not_existing_node> does not exist in current query.')

        # ABOVE non-existing
        with self.assertRaises(ValueError) as e:
            initial_q.must(
                {'term': {'new_field': 2}},
                child='not_existing_node',
                identifier='somewhere',
            )
        self.assertEqual(e.exception.message, 'Child <not_existing_node> does not exist in current query.')

        with self.assertRaises(ValueError) as e:
            initial_q.must(
                {'term': {'new_field': 2}},
                child='not_existing_node',
                identifier='somewhere',
            )
        self.assertEqual(e.exception.message, 'Child <not_existing_node> does not exist in current query.')

        # UNDER leaf
        with self.assertRaises(Exception) as e:
            initial_q.must(
                {'term': {'new_field': 2}},
                parent='pizza_term',
                identifier='somewhere'
            )
        self.assertEqual(
            e.exception.message,
            'Cannot place clause under non-compound clause <pizza_term> of type <term>.'
        )

        with self.assertRaises(Exception) as e:
            initial_q.must(
                {'term': {'new_field': 2}},
                parent='pizza_term',
                identifier='somewhere'
            )
        self.assertEqual(
            e.exception.message,
            'Cannot place clause under non-compound clause <pizza_term> of type <term>.'
        )

    def test_replace_all_existing_bool(self):
        initial_q = Query(Bool(identifier='init_bool', should=[
            Term(field='some_field', value='prod', identifier='prod_term'),
            Term(field='other_field', value='pizza', identifier='pizza_term'),
        ]))
        result_q = initial_q.bool(
            identifier='init_bool',
            filter=Range(field='price', gte=12, identifier='price_range'),
            mode='replace_all'
        )
        self.assertNotIn('prod_term', result_q)
        self.assertNotIn('pizza_term', result_q)
        self.assertIn('price_range', result_q)

    def test_add_existing_bool(self):
        initial_q = Query(Bool(identifier='init_bool', should=[
            Term(field='some_field', value='prod', identifier='prod_term'),
            Term(field='other_field', value='pizza', identifier='pizza_term'),
        ]))
        result_q = initial_q.bool(
            identifier='init_bool',
            filter=Range(field='price', gte=12, identifier='price_range'),
            should=Term(field='other_field', value='new_pizza', identifier='new_pizza_term'),
            mode='add'
        )
        self.assertIn('prod_term', result_q)
        self.assertIn('pizza_term', result_q)
        self.assertIn('new_pizza_term', result_q)
        self.assertIn('price_range', result_q)

    def test_replace_existing_bool(self):
        # replace != replace_all
        initial_q = Query(Bool(
            identifier='init_bool',
            should=[
                Term(field='some_field', value='prod', identifier='prod_term'),
                Term(field='other_field', value='pizza', identifier='pizza_term'),
            ],
            filter=Range(field='price', gte=50, identifier='very_high_price')
        ))
        result_q = initial_q.bool(
            identifier='init_bool',
            filter=Range(field='price', gte=20, identifier='high_price'),
            mode='replace'
        )
        self.assertIn('prod_term', result_q)
        self.assertIn('pizza_term', result_q)
        self.assertIn('high_price', result_q)
        self.assertNotIn('very_high_price', result_q)

    def test_must_at_root(self):
        q_i1 = Query()
        q1 = q_i1.must(
            Term(field='some_field', value=2, identifier='term_nid'),
            identifier='bool_nid',
        )
        self.assertEqual(len(q_i1.nodes.values()), 0)
        bool_ = next((c for c in q1.nodes.values() if isinstance(c, Bool)))
        self.assertEqual(bool_.identifier, 'bool_nid')
        next((c for c in q1.nodes.values() if isinstance(c, Must)))
        term = next((c for c in q1.nodes.values() if isinstance(c, Term)))
        self.assertEqual(term.identifier, 'term_nid')

        self.assertEqual(q1.__str__().decode('utf-8'), '''<Query>
bool
└── must
    └── term, field=some_field
''')

        q_i2 = Query()
        q2 = q_i2.must({'term': {'some_field': {'value': 2}}})
        self.assertEqual(len(q_i2.nodes.values()), 0)
        next((c for c in q2.nodes.values() if isinstance(c, Bool)))
        next((c for c in q2.nodes.values() if isinstance(c, Must)))
        next((c for c in q2.nodes.values() if isinstance(c, Term)))
        self.assertEqual(q2.__str__().decode('utf-8'), '''<Query>
bool
└── must
    └── term, field=some_field
''')

        # with multiple conditions (with different declarations)
        q_i3 = Query()
        q3 = q_i3.must([
            {'exists': {'field': 'some_field'}},
            {'term': {'other_field': {'value': 5}}}
        ])
        next((c for c in q3.nodes.values() if isinstance(c, Bool)))
        next((c for c in q3.nodes.values() if isinstance(c, Must)))
        next((c for c in q3.nodes.values() if isinstance(c, Term)))
        next((c for c in q3.nodes.values() if isinstance(c, Exists)))
        self.assertEqual(q3.__str__().decode('utf-8'), '''<Query>
bool
└── must
    ├── exists, field=some_field
    └── term, field=other_field
''')

    def test_must_method(self):
        q = Query()\
            .bool(filter=Term(field='field_a', value=2), identifier='root_bool')\
            .must({'exists': {'field': 'field_b'}}, identifier='root_bool')\
            .must(Prefix(field='field_c', value='pre'), identifier='root_bool')\
            .must(
                Bool(
                    identifier='level_1_bool',
                    should=[
                        Range(field='field_d', gte=3),
                        Ids(values=[1, 2, 3])
                    ]
                ),
                identifier='root_bool'
            )

        self.assertEqual(q.__str__().decode('utf-8'), '''<Query>
bool
├── filter
│   └── term, field=field_a
└── must
    ├── bool
    │   └── should
    │       ├── ids, values=[1, 2, 3]
    │       └── range, field=field_d
    ├── exists, field=field_b
    └── prefix, field=field_c
''')

    def test_nested(self):
        q = Query()
        q1 = q.nested(
            query=Term(field='some_nested_field.other', value=2),
            path='some_nested_path',
            identifier='nested_id'
        )
        self.assertEqual(q1.query_dict(), {
            "nested": {
                "path": "some_nested_path",
                "query": {
                    "term": {
                        "some_nested_field.other": {
                            "value": 2
                        }
                    }
                }
            }
        })
        self.assertEqual(q1.root, 'nested_id')

    def test_should(self):
        q = Query()
        q1 = q.should(
            Term(field='some_field', value=2),
            Term(field='other_field', value=3),
            identifier='created_bool'
        )
        self.assertEqual(q1.query_dict(), {
            "bool": {
                "should": [
                    {'term': {'some_field': {'value': 2}}},
                    {'term': {'other_field': {'value': 3}}}
                ]
            }
        })
        self.assertEqual(q1.root, 'created_bool')

    def test_filter(self):
        q = Query()
        q1 = q.filter(
            Term(field='some_field', value=2),
            Term(field='other_field', value=3),
            identifier='created_bool'
        )
        self.assertEqual(q1.query_dict(), {
            "bool": {
                "filter": [
                    {'term': {'some_field': {'value': 2}}},
                    {'term': {'other_field': {'value': 3}}}
                ]
            }
        })
        self.assertEqual(q1.root, 'created_bool')

    def test_must_not(self):
        q = Query()
        q1 = q.must_not(
            Term(field='some_field', value=2),
            Term(field='other_field', value=3),
            identifier='created_bool'
        )
        self.assertEqual(q1.query_dict(), {
            "bool": {
                "must_not": [
                    {'term': {'some_field': {'value': 2}}},
                    {'term': {'other_field': {'value': 3}}}
                ]
            }
        })
        self.assertEqual(q1.root, 'created_bool')

    def test_deserialize_dict_query(self):
        # simple leaf
        q = Query(from_={'term': {'some_field': {'value': 2}}})
        self.assertEqual(q.show(), '''term, field=some_field
''')
        self.assertEqual(len(q.nodes.values()), 1)
        n = q[q.root]
        self.assertIsInstance(n, Term)
        self.assertEqual(n.field, 'some_field')

        # bool simple leaf
        q = Query(from_={
            "bool": {
                "must_not": {
                    'term': {'some_field': {'value': 2}}
                }
            }
        })
        self.assertEqual(q.show(), '''bool
└── must_not
    └── term, field=some_field
''')
        self.assertEqual(len(q.nodes.values()), 3)
        n = q[q.root]
        self.assertIsInstance(n, Bool)
        self.assertEqual(q.query_dict(), {'bool': {
            'must_not': [
                {'term': {'some_field': {'value': 2}}}
            ]
        }})

        # bool multiple leaves
        q = Query(from_={
            "bool": {
                "must_not": [
                    {'term': {'some_field': {'value': 2}}},
                    {'term': {'other_field': {'value': 3}}}
                ]
            }
        })
        self.assertEqual(q.show(), '''bool
└── must_not
    ├── term, field=other_field
    └── term, field=some_field
''')
        self.assertEqual(len(q.nodes.values()), 4)
        n = q[q.root]
        self.assertIsInstance(n, Bool)
        self.assertEqual(q.query_dict(), {'bool': {
            'must_not': [
                {'term': {'some_field': {'value': 2}}},
                {'term': {'other_field': {'value': 3}}}
            ]
        }})

        # nested compound queries
        q = Query(from_={
            "nested": {
                "path": "some_nested_path",
                "query": {
                    "bool": {
                        "must_not": [
                            {'term': {'some_field': {'value': 2}}},
                            {'term': {'other_field': {'value': 3}}}
                        ]
                    }
                }
            }
        })
        self.assertEqual(q.show(), '''nested
├── path="some_nested_path"
└── query
    └── bool
        └── must_not
            ├── term, field=other_field
            └── term, field=some_field
''')
        self.assertEqual(len(q.nodes.values()), 7)
        n = q[q.root]
        self.assertIsInstance(n, Nested)
        self.assertEqual(q.query_dict(), {
            "nested": {
                "path": "some_nested_path",
                "query": {
                    "bool": {
                        "must_not": [
                            {'term': {'some_field': {'value': 2}}},
                            {'term': {'other_field': {'value': 3}}}
                        ]
                    }
                }
            }
        })

    def test_query_method(self):
        # on empty query
        q = Query()
        q1 = q.query(Bool(identifier='root_bool', must=Term(field='some_field', value=2)))
        q2 = q.query({'bool': {'identifier': 'root_bool', 'must': {'term': {'some_field':  2}}}})
        for r in (q1, q2):
            self.assertEqual(r.__str__().decode('utf-8'), '''<Query>
bool
└── must
    └── term, field=some_field
''')

        # query WITHOUT defined parent -> must on top (existing bool)
        q = Query(Bool(identifier='root_bool', must=Term(field='some_field', value=2)))
        q1 = q.query(Term(field='other_field', value=3))
        q2 = q.query({'term': {'other_field':  3}})
        for r in (q1, q2):
            self.assertEqual(r.__str__().decode('utf-8'), '''<Query>
bool
└── must
    ├── term, field=other_field
    └── term, field=some_field
''')

        # query WITHOUT defined parent -> must on top (non-existing bool)
        q = Query(Term(field='some_field', value=2))
        q1 = q.query(Term(field='other_field', value=3))
        q2 = q.query({'term': {'other_field':  3}})
        for r in (q1, q2):
            self.assertEqual(r.__str__().decode('utf-8'), '''<Query>
bool
└── must
    ├── term, field=other_field
    └── term, field=some_field
''')

        # query WITH defined parent (use default operator)
        q = Query(Nested(identifier='root_nested', path='some_nested_path'))
        q1 = q.query(Term(field='some_nested_path.id', value=2), parent='root_nested')
        q2 = q.query({'term': {'some_nested_path.id': 2}}, parent='root_nested')
        for r in (q1, q2):
            self.assertEqual(r.__str__().decode('utf-8'), '''<Query>
nested
├── path="some_nested_path"
└── query
    └── term, field=some_nested_path.id
''')

        q = Query(Bool(identifier='root_bool', filter=Term(field='some_field', value=2)))
        q1 = q.query(Term(field='some_other_field', value=2), parent='root_bool')
        q2 = q.query({'term': {'some_other_field': 2}}, parent='root_bool')
        for r in (q1, q2):
            self.assertEqual(r.__str__().decode('utf-8'), '''<Query>
bool
├── filter
│   └── term, field=some_field
└── must
    └── term, field=some_other_field
''')

        # query WITH defined parent and explicit operator
        q = Query(Bool(identifier='root_bool', filter=Term(field='some_field', value=2)))
        q1 = q.query(Range(field='some_other_field', gte=3), parent='root_bool', parent_param='must_not')
        q2 = q.query({'range': {'some_other_field': {'gte': 3}}}, parent='root_bool', parent_param='must_not')
        for r in (q1, q2):
            self.assertEqual(r.__str__().decode('utf-8'), '''<Query>
bool
├── filter
│   └── term, field=some_field
└── must_not
    └── range, field=some_other_field
''')

        # INVALID
        # query with leaf parent
        q = Query(Term(field='some_field', value=2, identifier='leaf_node'))
        with self.assertRaises(ValueError) as e:
            q.query(Range(field='some_other_field', gte=3), parent='leaf_node')
        self.assertEqual(e.exception.args[0], 'Cannot place clause under non-compound clause <leaf_node> of type <term>.')
        with self.assertRaises(ValueError) as e:
            q.query({'range': {'some_other_field': {'gte': 3}}}, parent='leaf_node')
        self.assertEqual(e.exception.args[0], 'Cannot place clause under non-compound clause <leaf_node> of type <term>.')

        # query WITH wrong parent operator
        q = Query(Bool(identifier='root_bool', filter=Term(field='some_field', value=2)))
        with self.assertRaises(ValueError) as e:
            q.query(Range(field='some_other_field', gte=3), parent='root_bool', parent_param='invalid_param')
        self.assertEqual(e.exception.args[0], 'Child operator <invalid_param> not permitted for compound query of type <Bool>')
        with self.assertRaises(ValueError) as e:
            q.query({'range': {'some_other_field': {'gte': 3}}}, parent='root_bool', parent_param='invalid_param')
        self.assertEqual(e.exception.args[0], 'Child operator <invalid_param> not permitted for compound query of type <Bool>')

        # query WITH non-existing parent
        q = Query(Bool(identifier='root_bool', filter=Term(field='some_field', value=2)))
        with self.assertRaises(ValueError) as e:
            q.query(Range(field='some_other_field', gte=3), parent='yolo_id')
        self.assertEqual(e.exception.args[0], 'Parent <yolo_id> does not exist in current query.')
        with self.assertRaises(ValueError) as e:
            q.query({'range': {'some_other_field': {'gte': 3}}}, parent='yolo_id')
        self.assertEqual(e.exception.args[0], 'Parent <yolo_id> does not exist in current query.')
