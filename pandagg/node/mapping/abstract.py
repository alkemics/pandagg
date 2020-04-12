#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import json
from six import python_2_unicode_compatible, iteritems
from builtins import str as text
from pandagg.node._node import Node


@python_2_unicode_compatible
class Field(Node):
    KEY = NotImplementedError()
    DISPLAY_PATTERN = "  %s"

    def __init__(self, name, is_subfield=False, **body):
        # name will be used for dynamic attribute access in tree
        self.name = name
        self.is_subfield = is_subfield
        # fields and properties can be a Field instance, a sequence of Field instances, or a dict
        self.fields = self._atomize(body.pop("fields", None))
        self.properties = self._atomize(body.pop("properties", None))
        # rest of body
        self._body = body
        super(Field, self).__init__()

    @staticmethod
    def _atomize(children):
        if children is None:
            return []
        if isinstance(children, dict):
            return [{k: v} for k, v in iteritems(children)]
        if isinstance(children, Field):
            return [children]
        return children

    @staticmethod
    def _serialize_atomized(children):
        d = {}
        for child in children:
            if isinstance(child, dict):
                d.update(child)
            if isinstance(child, Field):
                d[child.name] = child.body(with_children=True)
        return d

    @property
    def _identifier_prefix(self):
        return self.name

    @classmethod
    def deserialize(cls, name, body, is_subfield=False):
        if "type" in body and body["type"] != cls.KEY:
            raise ValueError(
                "Deserialization error for field <%s>: <%s>" % (cls.KEY, body)
            )
        return cls(name=name, is_subfield=is_subfield, **body)

    def body(self, with_children=False):
        b = copy.deepcopy(self._body)
        if with_children and self.properties:
            b["properties"] = self._serialize_atomized(self.properties)
        if with_children and self.fields:
            b["fields"] = self._serialize_atomized(self.fields)
        if self.KEY in ("object", ""):
            return b
        b["type"] = self.KEY
        return b

    def line_repr(self, depth, **kwargs):
        max_size = 60
        pad = max(max_size - 4 * depth - len(self.name), 4)
        field_pattern = "~ %s" if self.is_subfield else self.DISPLAY_PATTERN
        return "%s%s%s" % (self.name, " " * pad, field_pattern) % self.KEY.capitalize()

    def __str__(self):
        return "<Mapping Field %s> of type %s:\n%s" % (
            text(self.name),
            text(self.KEY),
            text(json.dumps(self.body(with_children=True), indent=4)),
        )
