#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from future.utils import python_2_unicode_compatible

from builtins import str as text

from lighttree import Tree as OriginalTree
from six import add_metaclass

from pandagg.utils import DslMeta


@python_2_unicode_compatible
@add_metaclass(DslMeta)
class Tree(OriginalTree):

    KEY = None
    _type_name = None

    @classmethod
    def get_dsl_class(cls, name):
        try:
            return cls._classes[name]
        except KeyError:
            raise NotImplementedError(
                "DSL class `{}` does not exist in {}.".format(name, cls._type_name)
            )

    @classmethod
    def get_node_dsl_class(cls, name):
        return cls.node_class.get_dsl_class(name)

    def __str__(self):
        return "<{class_}>\n{tree}".format(
            class_=text(self.__class__.__name__), tree=self.show(limit=40)
        )

    def __repr__(self):
        return self.__str__()
