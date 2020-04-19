#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text

from future.utils import python_2_unicode_compatible
from lighttree import Tree as OriginalTree


@python_2_unicode_compatible
class Tree(OriginalTree):
    def _insert_from_node_hierarchy(self, node, parent_id=None):
        """Insert in tree a node and all of its potential children (stored in .children)."""
        self.insert_node(node, parent_id)
        if hasattr(node, "_children"):
            for child_node in node._children or []:
                self._insert(child_node, pid=node.identifier)

    def __str__(self):
        return "<{class_}>\n{tree}".format(
            class_=text(self.__class__.__name__), tree=self.show(limit=40)
        )

    def __repr__(self):
        return self.__str__()
