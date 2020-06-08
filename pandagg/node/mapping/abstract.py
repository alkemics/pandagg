#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from future.utils import python_2_unicode_compatible
from builtins import str as text

from six import add_metaclass

from pandagg.node._node import Node
from pandagg.utils import DslMeta, get_dsl_class


@add_metaclass(DslMeta)
class UnnamedField:
    _type_name = "field"
    KEY = None

    def __init__(self, **body):
        self.body = body.copy()

    def to_named_field(self, name, _subfield=False):
        return Field(name, self.KEY, _subfield=_subfield, **self.body)

    get_dsl_class = classmethod(get_dsl_class)


class UnnamedComplexField(UnnamedField):
    KEY = None

    def __init__(self, **body):
        properties = body.pop("properties", None)
        if properties and not isinstance(properties, dict):
            raise ValueError("Invalid properties %s" % properties)
        self.properties = properties or {}
        super(UnnamedComplexField, self).__init__(**body)


class ShadowRoot(UnnamedComplexField):
    KEY = "_"


class UnnamedRegularField(UnnamedField):
    KEY = None

    def __init__(self, **body):
        fields = body.pop("fields", None)
        if fields and not isinstance(fields, dict):
            raise ValueError("Invalid fields %s" % fields)
        self.fields = fields
        super(UnnamedRegularField, self).__init__(**body)


@python_2_unicode_compatible
class Field(Node):
    def __init__(self, name, key, **body):
        # name will be used for dynamic attribute access in mapping interactive tree
        self.name = name
        self.KEY = key

        self._subfield = body.pop("_subfield", False)

        # rest of body
        self._body = body
        super(Field, self).__init__()

    @property
    def _identifier_prefix(self):
        return self.name

    @property
    def body(self):
        b = self._body.copy()
        if self.KEY in ("object", "_"):
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

    def line_repr(self, depth, **kwargs):
        if self.KEY == "_":
            return "_"
        max_size = 60
        pad = max(max_size - 4 * depth - len(self.name), 4)
        return (
            "%s%s%s"
            % (self.name, " " * pad, self._display_pattern)
            % self.KEY.capitalize()
        )

    def __str__(self):
        return "<Mapping Field %s> of type %s:\n%s" % (
            text(self.name),
            text(self.KEY),
            text(json.dumps(self.body, indent=4)),
        )
