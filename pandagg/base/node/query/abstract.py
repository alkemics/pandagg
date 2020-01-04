#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text
from six import iteritems
import json

from pandagg.base._tree import Node


class QueryClause(Node):
    Q_TYPE = NotImplementedError()

    def __init__(self, identifier=None, **body):
        super(QueryClause, self).__init__(identifier=identifier)
        assert isinstance(body, dict)
        self.body = body

    @classmethod
    def deserialize(cls, **body):
        return cls(**body)

    def serialize(self):
        return {self.Q_TYPE: self.body}

    def __str__(self):
        return "<{class_}, id={id}, type={type}, body={body}>".format(
            class_=text(self.__class__.__name__),
            type=text(self.Q_TYPE),
            id=text(self.identifier), body=json.dumps(self.body)
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return other.serialize() == self.serialize()
        # make sure we still equal to a dict with the same data
        return other == self.serialize()


class LeafQueryClause(QueryClause):

    def __init__(self, field, identifier=None, **body):
        self.field = field
        super(LeafQueryClause, self).__init__(identifier=identifier, **{field: body})

    @classmethod
    def deserialize(cls, **body):
        assert len(body.keys()) == 1
        k, v = next(iteritems(body))
        return cls(field=k, **v)


class ParameterClause(QueryClause):
    P_KEY = NotImplementedError()
    HASHABLE = False
    MULTIPLE = False

    def __init__(self, *args, **kwargs):
        if kwargs and kwargs.keys() != ['identifier']:
            raise ValueError('Invalid keywords arguments: <%s>.' % kwargs.keys())
        if not isinstance(args, (tuple, list)):
            args = (args,)
        if not self.MULTIPLE and len(args) > 1:
            raise ValueError('%s clause does not accept multiple query clauses.' % self.P_KEY)
        self.children = args
        super(ParameterClause, self).__init__(identifier=kwargs.get('identifier'))
