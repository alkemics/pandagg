#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text

from six import python_2_unicode_compatible
from treelib import Tree as OriginalTree


# slighly modified version of treelib.Tree
@python_2_unicode_compatible
class Tree(OriginalTree):

    def show(self, nid=None, level=OriginalTree.ROOT, idhidden=True, filter=None,
             key=None, reverse=False, line_type='ascii-ex', data_property=None, stdout=False):
        return super(Tree, self).show(
            nid=nid, level=level, idhidden=idhidden, filter=filter,
            key=key, reverse=reverse, line_type=line_type, data_property=data_property,
            stdout=stdout)

    def __str__(self):
        return '<{class_}>\n{tree}'.format(
            class_=text(self.__class__.__name__),
            tree=text(self.show())
        )

    def __repr__(self):
        return self.__str__()
