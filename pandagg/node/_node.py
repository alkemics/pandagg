#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text

import uuid

from six import python_2_unicode_compatible
from treelib import Node as OriginalNode


@python_2_unicode_compatible
class Node(OriginalNode):

    NID_SIZE = 8

    def __str__(self):
        name = text(self.__class__.__name__)
        kwargs = [
            "tag={0}".format(text(self.tag)),
            "identifier={0}".format(text(self.identifier)),
        ]
        return "%s(%s)" % (name, ", ".join(kwargs))

    @property
    def _identifier_prefix(self):
        return ''

    def _set_identifier(self, nid):
        if nid is not None:
            self._identifier = nid
            return
        self._identifier = '%s%s' % (self._identifier_prefix, text(uuid.uuid4())[:self.NID_SIZE])

    def __repr__(self):
        return self.__str__()
