#!/usr/bin/env python
# -*- coding: utf-8 -*-

from treelib import Tree as OriginalTree
from treelib import Node as OriginalNode


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
        self.show()
        return (u'<{class_}>\n{tree}'.format(class_=self.__class__.__name__, tree=self._reader)).encode('utf-8')

    def __str__(self):
        return self.__repr__()


class Node(OriginalNode):

    def __str__(self):
        return self.__repr__()
