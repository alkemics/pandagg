#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import json
from six import python_2_unicode_compatible
from builtins import str as text
from pandagg.node._node import Node
from pandagg.utils import PrettyNode


@python_2_unicode_compatible
class Field(Node):
    KEY = NotImplementedError()
    DISPLAY_PATTERN = '  %s'

    def __init__(self, name, depth, is_subfield=False, **body):
        # name will be used for dynamic attribute access in tree
        self.name = name
        # TODO - remove knowledge of depth here -> PR in treelib to update `show` method
        self.depth = depth
        self.is_subfield = is_subfield

        self.fields = body.pop('fields', None)
        self.properties = body.pop('properties', None)
        self._body = body
        super(Field, self).__init__(data=PrettyNode(pretty=self.tree_repr))

    @property
    def _identifier_prefix(self):
        return self.name

    @classmethod
    def deserialize(cls, name, body, depth=0, is_subfield=False):
        if 'type' in body and body['type'] != cls.KEY:
            raise ValueError('Deserialization error for field <%s>: <%s>' % (cls.KEY, body))
        return cls(name=name, depth=depth, is_subfield=is_subfield, **body)

    @property
    def body(self):
        b = copy.deepcopy(self._body)
        if self.properties:
            b['properties'] = self.properties
        if self.fields:
            b['fields'] = self.fields
        if self.KEY in ('object', ''):
            return b
        b['type'] = self.KEY
        return b

    @property
    def tree_repr(self):
        max_size = 60
        pad = max(max_size - 4 * self.depth - len(self.name), 4)
        field_pattern = '~ %s' if self.is_subfield else self.DISPLAY_PATTERN
        return "%s%s%s" % (self.name, ' ' * pad, field_pattern) % self.KEY.capitalize()

    def __str__(self):
        return '<Mapping Field %s> of type %s:\n%s' % (
            text(self.name),
            text(self.KEY),
            text(json.dumps(self.body, indent=4))
        )
