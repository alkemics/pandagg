from pandagg.node.mapping.abstract import Field
from pandagg.node.mapping.field_datatypes import Object, Keyword, Text, Integer
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
            self.assertIsInstance(field, Field)
            self.assertEqual(len(field._children), 2)
            orga_type = next((c for c in field._children if isinstance(c, Integer)))
            self.assertEqual(orga_type.name, "orga_type")
            self.assertFalse(orga_type.is_subfield)
            name = next((c for c in field._children if isinstance(c, Text)))
            self.assertEqual(name.name, "name")
            self.assertFalse(name.is_subfield)
            self.assertTrue(len(name._children), 1)
            raw = name._children[0]
            self.assertIsInstance(raw, Keyword)
            self.assertEqual(raw.name, "raw")
            self.assertTrue(raw.is_subfield)
