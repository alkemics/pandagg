#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from six import iteritems

from pandagg.base.node.query._leaf_clause import deserialize_leaf_clause
from pandagg.base.node.query.abstract import QueryClause, LeafQueryClause


class ParameterClause(QueryClause):
    KEY = NotImplementedError()
    SIMPLE = NotImplementedError()


class SimpleParameter(ParameterClause):
    KEY = NotImplementedError()
    SIMPLE = True

    def __init__(self, value):
        super(SimpleParameter, self).__init__(value=value)

    @property
    def tag(self):
        return '%s=%s' % (self.KEY, self.body['value'])

    @classmethod
    def deserialize(cls, value):
        return cls(value)

    def serialize(self):
        return {self.KEY: self.body['value']}


class Boost(SimpleParameter):
    KEY = 'boost'


class ParentClause(ParameterClause):
    KEY = NotImplementedError()
    SIMPLE = False
    MULTIPLE = False

    def __init__(self, *args, **kwargs):
        children = kwargs.pop('children', [])
        identifier = kwargs.pop('identifier', None)
        if kwargs:
            children.append(kwargs)
        if args:
            if isinstance(args, (tuple, list)):
                children.extend(args)
            else:
                children.append(args)
        if not self.MULTIPLE and len(children) > 1:
            raise ValueError('%s clause does not accept multiple query clauses.' % self.KEY)
        serialized_children = []
        for child in children:
            if isinstance(child, LeafQueryClause):
                serialized_children.append(child)
            else:
                assert isinstance(child, dict)
                k, v = next(iteritems(child))
                serialized_children.append(deserialize_leaf_clause(k, v))

        self.children = serialized_children
        super(ParentClause, self).__init__(identifier=identifier)


class Filter(ParentClause):
    KEY = 'filter'
    MULTIPLE = True


class Must(ParentClause):
    KEY = 'must'
    MULTIPLE = True


class Should(ParentClause):
    KEY = 'should'
    MULTIPLE = True


class MustNot(ParentClause):
    KEY = 'must_not'
    MULTIPLE = True


PARAMETERS = {
    p.KEY: p for p in [
        Boost,
        Filter,
        MustNot,
        Must,
        Should
    ]
}


def deserialize_parameter(key, body):
    if key not in PARAMETERS.keys():
        raise NotImplementedError('Unknown parameter type <%s>' % key)
    klass = PARAMETERS[key]
    if klass.SIMPLE:
        return klass.deserialize(body)
    if isinstance(body, (tuple, list)) and all((isinstance(b, QueryClause) for b in body)):
        return klass.deserialize(children=body)
    if isinstance(body, QueryClause):
        return klass.deserialize(children=[body])
    return klass.deserialize(**body)
