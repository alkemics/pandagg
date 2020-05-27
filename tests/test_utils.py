#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase
from pandagg.utils import equal_queries, equal_search


class UtilsTestCase(TestCase):
    def test_equal(self):
        q1 = {"bool": {"must": [{"term": {"field_A": 1}}, {"term": {"field_B": 2}}]}}
        q2 = {"bool": {"must": [{"term": {"field_B": 2}}, {"term": {"field_A": 1}}]}}
        non_equal_q = {
            "bool": {"must": [{"term": {"field_B": 2}}, {"term": {"field_A": 123}}]}
        }
        self.assertTrue(equal_queries(q1, q2))
        self.assertFalse(equal_queries(q1, non_equal_q))

        self.assertTrue(
            equal_search(
                {
                    "query": q1,
                    "sort": ["title", {"category": {"order": "desc"}}, "_score"],
                },
                {
                    "query": q2,
                    "sort": ["title", {"category": {"order": "desc"}}, "_score"],
                },
            )
        )
        self.assertFalse(
            equal_search(
                {
                    "query": q1,
                    "sort": ["title", {"category": {"order": "desc"}}, "_score"],
                },
                {
                    "query": q2,
                    "sort": ["title", "_score", {"category": {"order": "desc"}}],
                },
            )
        )
