#!/usr/bin/env python
# -*- coding: utf-8 -*-


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

    def test_bool_query(self):
        q = Query()
