#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from pandagg.node._node import Node
from pandagg.utils import PrettyNode


class Bucket(Node):

    ROOT_NAME = 'root'

    def __init__(self, depth, value, key=None, level=None):
        self.value = value
        self.level = level if level is not None else self.ROOT_NAME
        self.depth = depth
        self.key = key

        super(Bucket, self).__init__(
            tag=self.display_name,
            data=PrettyNode(pretty=self.display_name_with_value)
        )

    @property
    def attr_name(self):
        """Determine under which attribute name the bucket will be available in response tree.
        Dots are replaced by `_` characters so that they don't prevent from accessing as attribute.

        Resulting attribute unfit for python attribute name syntax is still possible and will be accessible through
        item access (dict like), see more in 'utils.Obj' for more details.
        """
        if self.key is not None:
            return '%s_%s' % (self.level.replace('.', '_'), self.key)
        return self.level.replace('.', '_')

    @property
    def display_name(self):
        if self.key is not None:
            return '%s=%s' % (self.level, self.key)
        return self.level

    @property
    def display_name_with_value(self):
        REPR_SIZE = 60
        # Determine how this node will be represented in tree representation.
        s = self.display_name
        if self.value is not None:
            pad = max(REPR_SIZE - 4 * self.depth - len(s) - len(str(self.value)), 4)
            s = s + ' ' * pad + str(self.value)
        return s
