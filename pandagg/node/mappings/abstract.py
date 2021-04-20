#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from builtins import str as text


from pandagg.node._node import Node


class Field(Node):
    _type_name = "field"
    KEY = None

    def __init__(self, **body):
        super(Node, self).__init__()
        self._subfield = body.pop("_subfield", False)
        self._body = body.copy()

    def line_repr(self, depth, **kwargs):
        if self.KEY is None:
            return "_", ""
        return "", self._display_pattern % self.KEY.capitalize()

    @property
    def body(self):
        b = self._body.copy()
        if self.KEY in ("object", None):
            return b
        b["type"] = self.KEY
        return b

    @property
    def _display_pattern(self):
        if self.KEY == "object":
            return " {%s}"
        if self.KEY == "nested":
            return " [%s]"
        if self._subfield:
            return "~ %s"
        return "  %s"

    def __str__(self):
        return "<%s field>:\n%s" % (
            text(self.KEY).capitalize(),
            text(json.dumps(self.body, indent=4)),
        )


class ComplexField(Field):
    KEY = None

    def __init__(self, **body):
        properties = body.pop("properties", None)
        if properties and not isinstance(properties, dict):
            raise ValueError("Invalid properties %s" % properties)
        self.properties = properties or {}
        super(ComplexField, self).__init__(**body)


class RegularField(Field):
    KEY = None

    def __init__(self, **body):
        fields = body.pop("fields", None)
        if fields and not isinstance(fields, dict):
            raise ValueError("Invalid fields %s" % fields)
        self.fields = fields
        super(RegularField, self).__init__(**body)
