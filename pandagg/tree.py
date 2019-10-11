#!/usr/bin/env python
# -*- coding: utf-8 -*-

from treelib import Tree as OriginalTree
from treelib import Node as OriginalNode


from treelib.exceptions import NodeIDAbsentError


# slighly modifier version of treelib.Tree
class Tree(OriginalTree):

    # copy are done with deep=True by default, because with shallow copies of nodes, nodes' parent/children pointers
    # would be mixed up
    DEEP = True

    def __init__(self, tree=None, deep=DEEP, node_class=None):
        super(Tree, self).__init__(tree=tree, deep=deep, node_class=node_class)
        self._reader = ''

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
        self.__repr__()


class Node(OriginalNode):

    def __str__(self):
        return self.__repr__()
