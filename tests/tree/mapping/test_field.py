#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from pandagg.mapping import Object, Keyword, Text, Integer
from unittest import TestCase


class FieldTestCase(TestCase):
    def test_field(self):
        field1 = Object(
            "organization",
            properties={
                "orga_type": {"type": "integer"},
                "name": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
            },
        )
        field2 = Object(
            "organization",
            properties=[Integer("orga_type"), Text("name", fields=Keyword("raw"))],
        )
        for field in (field1, field2):
            self.assertEqual(
                field.to_dict(),
                {
                    "dynamic": False,
                    "properties": {
                        "name": {
                            "fields": {"raw": {"type": "keyword"}},
                            "type": "text",
                        },
                        "orga_type": {"type": "integer"},
                    },
                },
            )
            self.assertEqual(
                field.__str__(),
                """<Object>
_
├── name                                                      Text
│   └── raw                                                 ~ Keyword
└── orga_type                                                 Integer
""",
            )
