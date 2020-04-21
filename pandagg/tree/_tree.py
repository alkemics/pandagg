#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from future.utils import python_2_unicode_compatible

from builtins import str as text

from lighttree import Tree as OriginalTree


@python_2_unicode_compatible
class Tree(OriginalTree):
    def __str__(self):
        return "<{class_}>\n{tree}".format(
            class_=text(self.__class__.__name__), tree=self.show(limit=40)
        )

    def __repr__(self):
        return self.__str__()
