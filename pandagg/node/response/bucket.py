#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from pandagg.node._node import Node


class Bucket(Node):

    ROOT_NAME = "root"

    def __init__(self, value, key=None, level=None):
        self.value = value
        self.level = level if level is not None else self.ROOT_NAME
        self.key = key
        super(Bucket, self).__init__()

    @property
    def attr_name(self):
        """Determine under which attribute name the bucket will be available in response tree.
        Dots are replaced by `_` characters so that they don't prevent from accessing as attribute.

        Resulting attribute unfit for python attribute name syntax is still possible and will be accessible through
        item access (dict like), see more in 'utils.Obj' for more details.
        """
        if self.key is not None:
            return "%s_%s" % (self.level.replace(".", "_"), self._coerced_key)
        return self.level.replace(".", "_")

    def line_repr(self, **kwargs):
        REPR_SIZE = 60
        s = self.level
        if self.key is not None:
            s += "=%s" % self._coerced_key
        if self.value is not None:
            pad = max(
                REPR_SIZE - 4 * kwargs.get("depth") - len(s) - len(str(self.value)), 4
            )
            s = s + " " * pad + str(self.value)
        return s

    @property
    def _coerced_key(self):
        key = self.key
        try:
            # order matters, will
            key = float(key)
            key = int(key)
        except (ValueError, TypeError):
            pass
        return key
