#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.node._node import Node


class BucketNode(Node):
    def __init__(self):
        self.level = None
        super(BucketNode, self).__init__(keyed=False)


class Bucket(BucketNode):
    def __init__(self, value, key=None, level=None):
        super(Bucket, self).__init__()
        self.value = value
        self.level = level
        self.key = key

    @property
    def attr_name(self):
        """
        Determine under which attribute name the bucket will be available in response tree.
        Dots are replaced by `_` characters so that they don't prevent from accessing as attribute.

        Resulting attribute unfit for python attribute name syntax is still possible and will be accessible through
        item access (dict like), see more in 'utils.Obj' for more details.
        """
        if self.key is not None:
            return "%s_%s" % (self.level.replace(".", "_"), self._coerced_key)
        return self.level.replace(".", "_")

    def line_repr(self, **kwargs):
        s = self.level or ""
        if self.key is not None:
            s += "=%s" % self._coerced_key
        return s, str(self.value) if self.value else ""

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
