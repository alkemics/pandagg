#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from future.utils import python_2_unicode_compatible
from builtins import str as text
from pandagg.node._node import Node


@python_2_unicode_compatible
class Field(Node):
    _type_name = "field"
    KEY = None
    _display_pattern = "  %s"

    def __init__(self, name, **body):
        # name will be used for dynamic attribute access in tree
        self.name = name
        # rest of body
        self._body = body
        self.is_subfield = False
        super(Field, self).__init__()

    @property
    def _identifier_prefix(self):
        return self.name

    @property
    def body(self):
        b = self._body.copy()
        if self.KEY in ("object", " "):
            return b
        b["type"] = self.KEY
        return b

    def line_repr(self, depth, **kwargs):
        if depth == 0:
            return "_"
        max_size = 60
        pad = max(max_size - 4 * depth - len(self.name), 4)
        field_pattern = "~ %s" if self.is_subfield else self._display_pattern
        return "%s%s%s" % (self.name, " " * pad, field_pattern) % self.KEY.capitalize()

    def __str__(self):
        return "<Mapping Field %s> of type %s:\n%s" % (
            text(self.name),
            text(self.KEY),
            text(json.dumps(self.body, indent=4)),
        )


class NumericField(Field):
    pass


class ComplexField(Field):
    def __init__(self, name, dynamic=False, **body):
        # properties can be a Field instance, a sequence of Field instances, or a dict
        super(ComplexField, self).__init__(name, dynamic=dynamic, **body)
