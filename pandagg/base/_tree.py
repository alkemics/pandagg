#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text

import uuid

from six import python_2_unicode_compatible
from treelib import Tree as OriginalTree
from treelib import Node as OriginalNode


from treelib.exceptions import NodeIDAbsentError


# slighly modified version of treelib.Tree
@python_2_unicode_compatible
class Tree(OriginalTree):

    def show(self, nid=None, level=OriginalTree.ROOT, idhidden=True, filter=None,
             key=None, reverse=False, line_type='ascii-ex', data_property=None):
        self._reader = ''

        def write(line):
            self._reader += line.decode('utf-8') + "\n"

        try:
            self.__print_backend(nid, level, idhidden, filter,
                                 key, reverse, line_type, data_property, func=write)
        except NodeIDAbsentError:
            self._reader = 'Empty'
        return self._reader

    def __str__(self):
        self.show()
        return '<{class_}>\n{tree}'.format(
            class_=text(self.__class__.__name__),
            tree=text(self._reader)
        )

    def __repr__(self):
        return self.__str__()


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
        self._identifier = '%s%s' % (self._identifier_prefix, str(uuid.uuid4())[:self.NID_SIZE])

    def __repr__(self):
        return self.__str__()
