#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from pandagg.utils import bool_if_required

from unittest import TestCase


class BoolIfRequiredTestCase(TestCase):
    def test_simple(self):
        self.assertEqual(
            bool_if_required([{"term": {"some_field": 2}}], operator="must"),
            {"term": {"some_field": 2}},
        )

        self.assertEqual(
            bool_if_required(
                [{"term": {"some_field": 2}}, {"range": {"some_field": {"gt": 1}}},],
                operator="must",
            ),
            {
                "bool": {
                    "must": [
                        {"term": {"some_field": 2}},
                        {"range": {"some_field": {"gt": 1}}},
                    ]
                }
            },
        )

    def test_flatten_sub_condition(self):
        self.assertEqual(
            bool_if_required(
                [
                    {"term": {"some_field": 2}},
                    {"bool": {"should": [{"term": {"some_other_field": 3}}]}},
                ],
                operator="should",
            ),
            {
                "bool": {
                    "should": [
                        {"term": {"some_field": 2}},
                        {"term": {"some_other_field": 3}},
                    ]
                }
            },
        )

        # with multiple sub-conditions
        self.assertEqual(
            bool_if_required(
                [
                    {"term": {"some_field": 2}},
                    {
                        "bool": {
                            "should": [
                                {"term": {"some_other_field": 3}},
                                {"term": {"some_other_field": 4}},
                            ]
                        }
                    },
                ],
                operator="should",
            ),
            {
                "bool": {
                    "should": [
                        {"term": {"some_field": 2}},
                        {"term": {"some_other_field": 3}},
                        {"term": {"some_other_field": 4}},
                    ]
                }
            },
        )

        # not flattened because different conditions
        self.assertEqual(
            bool_if_required(
                [
                    {"term": {"some_field": 2}},
                    {
                        "bool": {
                            "must": [
                                {"term": {"some_other_field": 3}},
                                {"term": {"some_other_field": 4}},
                            ]
                        }
                    },
                ],
                operator="should",
            ),
            {
                "bool": {
                    "should": [
                        {"term": {"some_field": 2}},
                        {
                            "bool": {
                                "must": [
                                    {"term": {"some_other_field": 3}},
                                    {"term": {"some_other_field": 4}},
                                ]
                            }
                        },
                    ]
                }
            },
        )
