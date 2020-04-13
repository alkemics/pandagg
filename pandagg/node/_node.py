#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text

import uuid

from lighttree import Node as OriginalNode


class Node(OriginalNode):

    NID_SIZE = 8

    def __init__(self, identifier=None):
        if identifier is None:
            identifier = "%s%s" % (
                self._identifier_prefix,
                text(uuid.uuid4())[: self.NID_SIZE],
            )
        super(Node, self).__init__(identifier=identifier)

    def __str__(self):
        return "%s, identifier=%s" % (
            text(self.__class__.__name__),
            text(self.identifier),
        )

    @property
    def _identifier_prefix(self):
        return ""
