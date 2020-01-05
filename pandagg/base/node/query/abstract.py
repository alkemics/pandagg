#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text
from six import iteritems
import json

from pandagg.base._tree import Node


class QueryClause(Node):
    NID_SIZE = 6
    KEY = NotImplementedError()

    def __init__(self, identifier=None, **body):
        super(QueryClause, self).__init__(identifier=identifier)
        assert isinstance(body, dict)
        self.body = body

    @property
    def tag(self):
        return self.KEY

    @property
    def _identifier_prefix(self):
        return '%s_' % self.KEY

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

    ALLOW_SIMPLE_VALUE = False

    def __init__(self, field, identifier=None, **body):
        self.field = field
        super(LeafQueryClause, self).__init__(identifier=identifier, **{field: body})

    @property
    def tag(self):
        return '%s, field=%s' % (self.KEY, self.field)

    @classmethod
    def deserialize(cls, **body):
        assert len(body.keys()) == 1
        k, v = next(iteritems(body))
        if cls.ALLOW_SIMPLE_VALUE and not isinstance(v, dict):
            assert k == 'field'
            return cls(field=v)
        return cls(field=k, **v)
