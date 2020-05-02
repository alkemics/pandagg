#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from future.utils import python_2_unicode_compatible, iteritems
from builtins import str as text
from pandagg.node._node import Node

try:
    import collections.abc as collections_abc  # only works on python 3.3+
except ImportError:
    import collections as collections_abc


@python_2_unicode_compatible
class Field(Node):
    _type_name = "field"
    KEY = None
    _display_pattern = "  %s"

    def __init__(self, name, _children=None, **body):
        # name will be used for dynamic attribute access in tree
        self.name = name
        # rest of body
        self._body = body
        self.is_subfield = False
        super(Field, self).__init__(_children=_children)

    @classmethod
    def _type_deserializer(cls, name_or_field, **params):
        is_subfield = params.pop("is_subfield", False)
        # Keyword()
        if isinstance(name_or_field, Field):
            if params:
                raise ValueError()
            if is_subfield:
                name_or_field.is_subfield = is_subfield
            return name_or_field
        # {"some_field": {"type": "keyword"}}
        if isinstance(name_or_field, collections_abc.Mapping):
            if params:
                raise ValueError()
            name, body = name_or_field.copy().popitem()
            type_ = body.get("type", "object")
            field = cls.get_dsl_class(type_)(name=name, **body)
            if is_subfield:
                field.is_subfield = is_subfield
            return field
        # 'nested', properties={)
        field = cls.get_dsl_class(name_or_field)(**params)
        if is_subfield:
            field.is_subfield = is_subfield
        return field

    @staticmethod
    def _atomize(children):
        if children is None:
            return []
        if isinstance(children, dict):
            return [{k: v} for k, v in iteritems(children)]
        if isinstance(children, Field):
            return [children]
        return children

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

    def serialize(self, *args, **kwargs):
        return self.body

    def line_repr(self, depth, **kwargs):
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


class StringField(Field):
    def __init__(self, name, fields=None, **body):
        # fields can be a Field instance, a sequence of Field instances, or a dict
        fields = [
            self._type_deserializer(child, is_subfield=True)
            for child in self._atomize(fields)
        ]
        super(StringField, self).__init__(name, _children=fields, **body)


class NumericField(Field):
    pass


class ComplexField(Field):
    def __init__(self, name, properties=None, dynamic=False, **body):
        # properties can be a Field instance, a sequence of Field instances, or a dict
        properties = [
            self._type_deserializer(child) for child in self._atomize(properties)
        ]
        super(ComplexField, self).__init__(
            name, dynamic=dynamic, _children=properties, **body
        )


class ShadowRoot(ComplexField):

    KEY = " "

    def __init__(self, properties=None, dynamic=False, **body):
        super(ShadowRoot, self).__init__(
            "_", dynamic=dynamic, properties=properties, **body
        )
