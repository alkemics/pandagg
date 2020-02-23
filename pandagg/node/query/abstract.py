#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import copy

from builtins import str as text
from six import iteritems
import json

from pandagg.node._node import Node


class QueryClause(Node):
    NID_SIZE = 6
    KEY = NotImplementedError()

    def __init__(self, _name=None, **body):
        super(QueryClause, self).__init__(identifier=_name)
        self.body = body

    @property
    def tag(self):
        return self.KEY

    @property
    def name(self):
        return self.identifier

    @property
    def _identifier_prefix(self):
        return '%s_' % self.KEY

    @classmethod
    def deserialize(cls, **body):
        return cls(**body)

    def serialize(self, named=False):
        b = copy.deepcopy(self.body)
        if named:
            b['_name'] = self.name
        return {self.KEY: b}

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
    pass


class SingleFieldQueryClause(LeafQueryClause):
    SHORT_TAG = None
    FLAT = False

    def __init__(self, field, _name=None, **body):
        self.field = field
        if self.FLAT:
            self.inner_body = body
            super(LeafQueryClause, self).__init__(field=field, _name=_name, **body)
        else:
            if isinstance(body, dict):
                self.inner_body = body
            else:
                self.inner_body = {self.SHORT_TAG: body}
            super(LeafQueryClause, self).__init__(_name=_name, **{field: body})

    @property
    def tag(self):
        base = '%s, field=%s' % (text(self.KEY), text(self.field))
        if self.inner_body:
            base += ', %s' % ', '.join(
                '%s=%s' % (text(k), text(json.dumps(self.inner_body[k], sort_keys=True)))
                for k in sorted(self.inner_body.keys())
            )
        return base

    @classmethod
    def deserialize(cls, **body):
        if cls.FLAT:
            return cls(**body)
        _name = body.pop('_name', None)
        assert len(body.keys()) == 1
        k, v = next(iteritems(body))
        if cls.SHORT_TAG and not isinstance(v, dict):
            return cls(field=k, _name=_name, **{cls.SHORT_TAG: v})
        return cls(field=k, _name=_name, **v)


class MultiFieldsQueryClause(LeafQueryClause):
    def __init__(self, fields, _name=None, **body):
        self.fields = fields
        super(LeafQueryClause, self).__init__(_name=_name, fields=fields, **body)

    @property
    def tag(self):
        return '%s, fields=%s' % (self.KEY, list(map(text, self.fields)))
