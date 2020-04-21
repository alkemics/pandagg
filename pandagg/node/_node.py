#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text

import uuid
from future.utils import iteritems

from lighttree import Node as OriginalNode
from six import add_metaclass

from pandagg.utils import DslMeta


@add_metaclass(DslMeta)
class Node(OriginalNode):

    KEY = None
    _type_name = None
    _children_prefix = None

    NID_SIZE = 8

    def __init__(self, identifier=None, _children=None):
        if identifier is None:
            identifier = self._craft_identifier()
        super(Node, self).__init__(
            identifier=identifier, _children=self._deserialize_children(_children)
        )

    @property
    def _identifier_prefix(self):
        return ""

    def _craft_identifier(self):
        return "%s%s" % (self._identifier_prefix, text(uuid.uuid4())[: self.NID_SIZE])

    @classmethod
    def get_dsl_class(cls, name, prefix=None):
        if prefix:
            name = prefix + name
        try:
            return cls._classes[name]
        except KeyError:
            raise NotImplementedError(
                "DSL class `{}` does not exist in {}.".format(name, cls._type_name)
            )

    @staticmethod
    def expand__to_dot(params):
        nparams = {}
        for pname, pvalue in iteritems(params):
            if "__" in pname:
                pname = pname.replace("__", ".")
            nparams[pname] = pvalue
        return nparams

    @classmethod
    def _validate_children(cls, children):
        pass

    @classmethod
    def _deserialize_children(cls, children):
        children = [
            cls._type_deserializer(child) for child in cls._atomize_children(children)
        ]
        cls._validate_children(children)
        return children

    @staticmethod
    def _atomize_children(children):
        if children is None:
            return []
        if isinstance(children, (tuple, list)):
            return children
        if isinstance(children, dict):
            return [{k: v} for k, v in iteritems(children)]
        return [children]

    def __str__(self):
        return "%s, identifier=%s" % (
            text(self.__class__.__name__),
            text(self.identifier),
        )
