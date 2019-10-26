#!/usr/bin/env python
# -*- coding: utf-8 -*-

from builtins import str as text
from pandagg.tree import Node
from pandagg.utils import PrettyNode


class Bucket(Node):

    REPR_SIZE = 60
    ROOT_NAME = 'root'

    def __init__(self, depth, value, key=None, aggregation_node=None):
        self.aggregation_node = aggregation_node
        self.value = value
        self.level = aggregation_node.agg_name if aggregation_node is not None else self.ROOT_NAME
        self.depth = depth
        self.key = key
        # level=key
        if self.key is not None:
            self.name = '%s_%s' % (self.level.replace('.', '_'), self.key)
        else:
            self.name = self.level.replace('.', '_')
        pretty = self._str_current_level(
            level=self.level,
            key=self.key,
            depth=self.depth, sep='=',
            value=self.value
        )
        super(Bucket, self).__init__(tag=self.name, data=PrettyNode(pretty=pretty))

    @classmethod
    def _str_current_level(cls, level, key, depth, sep=':', value=None):
        s = level
        if key is not None:
            s = '%s%s%s' % (s, sep, key)
        if value is not None:
            pad = max(cls.REPR_SIZE - 4 * depth - len(s) - len(str(value)), 4)
            s = s + ' ' * pad + str(value)
        return s

    def __str__(self):
        return '<Bucket, identifier={identifier}>\n{pretty}'.format(
            identifier=text(self.identifier),
            pretty=text(self.data.pretty)
        )
