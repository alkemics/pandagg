#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text
from six import iteritems
import json

from pandagg.base._tree import Node


class QueryClause(Node):
    KEY = NotImplementedError()

    def __init__(self, identifier=None, tag=None, **body):
        if tag is None and identifier is None:
            tag = self.KEY
        super(QueryClause, self).__init__(identifier=identifier, tag=tag)
        assert isinstance(body, dict)
        self.body = body

    @classmethod
    def deserialize(cls, **body):
        return cls(**body)

    def serialize(self):
        return {self.KEY: self.body}

    def __str__(self):
        return "<{class_}, id={id}, type={type}, body={body}>".format(
            class_=text(self.__class__.__name__),
            type=text(self.KEY),
            id=text(self.identifier), body=json.dumps(self.body)
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return other.serialize() == self.serialize()
        # make sure we still equal to a dict with the same data
        return other == self.serialize()


class LeafQueryClause(QueryClause):

    def __init__(self, field, identifier=None, tag=None, **body):
        self.field = field
        if tag is None and identifier is None:
            tag = '%s, field=%s' % (self.KEY, field)
        super(LeafQueryClause, self).__init__(identifier=identifier, tag=tag, **{field: body})

    @classmethod
    def deserialize(cls, **body):
        assert len(body.keys()) == 1
        k, v = next(iteritems(body))
        return cls(field=k, **v)


class ParameterClause(QueryClause):
    MULTIPLE = False

    def __init__(self, *args, **kwargs):
        identifier = kwargs.pop('identifier', None)
        if kwargs:
            raise ValueError('Invalid keywords arguments: <%s>.' % kwargs.keys())
        if not isinstance(args, (tuple, list)):
            args = (args,)
        if not self.MULTIPLE and len(args) > 1:
            raise ValueError('%s clause does not accept multiple query clauses.' % self.KEY)
        self.children = args
        super(ParameterClause, self).__init__(identifier=identifier)
