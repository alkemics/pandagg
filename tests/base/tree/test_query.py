#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.base.node.query import Filter, Must, Exists, Range, Prefix, Ids
from pandagg.base.node.query.full_text import QueryString
from pandagg.query import Query
from pandagg.query import Term, Bool


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
            q.__str__(),
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
        with self.assertRaises(AssertionError):
            q.add_node(Filter(QueryString(field='other_field', value='salut')), pid='term_id')

        # under compound clause
        q = Query(Bool(identifier='bool'))
        q.add_node(Filter(Term(field='some_field', value=2)), pid='bool')

        q = Query(Bool(identifier='bool'))
        with self.assertRaises(AssertionError):
            q.add_node(Term(field='some_field', value=2), pid='bool')

    def test_bool_at_root(self):
        # at root
        q = Query()
        q1 = q.bool(identifier='root_bool')
        self.assertEqual(q.nodes.keys(), [])
        self.assertEqual(q1.nodes.keys(), ['root_bool'])

    def test_must_at_root(self):
        q_i1 = Query()
        q1 = q_i1.must(
            Term(field='some_field', value=2, identifier='term_nid'),
            identifier='must_nid',
            bool_identifier='bool_nid'
        )
        self.assertEqual(len(q_i1.nodes.values()), 0)
        bool_ = next((c for c in q1.nodes.values() if isinstance(c, Bool)))
        self.assertEqual(bool_.identifier, 'bool_nid')
        must = next((c for c in q1.nodes.values() if isinstance(c, Must)))
        self.assertEqual(must.identifier, 'must_nid')
        term = next((c for c in q1.nodes.values() if isinstance(c, Term)))
        self.assertEqual(term.identifier, 'term_nid')

        self.assertEqual(q1.__str__(), '''<Query>
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
        self.assertEqual(q2.__str__(), '''<Query>
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
        self.assertEqual(q3.__str__(), '''<Query>
bool
└── must
    ├── exists, field=some_field
    └── term, field=other_field
''')

    def test_query_method(self):
        q = Query()\
            .bool(filter=Term(field='field_a', value=2), identifier='root_bool')\
            .must({'exists': {'field': 'field_b'}}, pid='root_bool')\
            .must(Prefix(field='field_c', value='pre'), pid='root_bool')\
            .must(
                Bool(
                    identifier='level_1_bool',
                    should=[
                        Range(field='field_d', gte=3),
                        Ids(values=[1, 2, 3])
                    ]
                ),
                pid='root_bool'
            )

        self.assertEqual(q.__str__(), '''<Query>
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
            path='some_nested_path'
        )
        self.assertEqual(q1.query_dict(), {
            "nested": {
                "path": "some_nested_path",
                "query": [
                    {
                        "term": {
                            "some_nested_field.other": {
                                "value": 2
                            }
                        }
                    }
                ]
            }
        })