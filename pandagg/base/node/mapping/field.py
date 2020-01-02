#!/usr/bin/env python
# -*- coding: utf-8 -*-

from six import python_2_unicode_compatible
from builtins import str as text
import json
from pandagg.base.exceptions import MappingError
from pandagg.base.node.types import MAPPING_TYPES
from pandagg.base._tree import Node
from pandagg.base.utils import PrettyNode


@python_2_unicode_compatible
class MappingNode(Node):

    REPR_SIZE = 60

    def __init__(self, path, body, depth, is_root=False, is_subfield=False):
        self.is_root = is_root
        self.path = path
        # name will be used for dynamic attribute access in tree
        self.name = path.split('.')[-1]
        self.depth = depth
        self.body = body
        self.is_subfield = is_subfield
        if is_root:
            self.type = ''
        else:
            type_ = body.get('type', 'object')
            if type_ not in MAPPING_TYPES:
                raise MappingError(u'Unkown <%s> field type on path <%s>' % (type_, path))
            self.type = type_
        super(MappingNode, self).__init__(identifier=path, data=PrettyNode(pretty=self.tree_repr))

    @property
    def tree_repr(self):
        pad = max(self.REPR_SIZE - 4 * self.depth - len(self.name), 4)
        s = 'root' if self.is_root else self.name
        if self.type == 'object':
            return s + ' ' * (pad - 1) + '{%s}' % self.type.capitalize()
        elif self.type == 'nested':
            return s + ' ' * (pad - 1) + '[%s]' % self.type.capitalize()
        elif self.is_subfield:
            return s + ' ' * (pad - 2) + '~ %s' % self.type.capitalize()
        return s + ' ' * pad + '%s' % self.type.capitalize()

    def __str__(self):
        return '<Mapping Field %s> of type %s:\n%s' % (
            text(self.path),
            text(self.type),
            text(json.dumps(self.body, indent=4))
        )
