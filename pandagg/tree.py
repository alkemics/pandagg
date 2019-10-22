#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text
from treelib import Tree as OriginalTree


from treelib.exceptions import NodeIDAbsentError


# slighly modified version of treelib.Tree
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

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        self.show()
        return '<{class_}>\n{tree}'.format(
            class_=text(self.__class__.__name__),
            tree=self._reader
        )
