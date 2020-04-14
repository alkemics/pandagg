#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase
from pandagg.utils import equal_queries


class UtilsTestCase(TestCase):
    def test_equal_queries(self):
        q1 = {"bool": {"must": [{"term": {"field_A": 1}}, {"term": {"field_B": 2}}]}}
        q2 = {"bool": {"must": [{"term": {"field_B": 2}}, {"term": {"field_A": 1}}]}}
        non_equal_q = {
            "bool": {"must": [{"term": {"field_B": 2}}, {"term": {"field_A": 123}}]}
        }
        self.assertTrue(equal_queries(q1, q2))
        self.assertFalse(equal_queries(q1, non_equal_q))
