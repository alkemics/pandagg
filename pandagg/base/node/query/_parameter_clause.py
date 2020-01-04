#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from six import iteritems

from pandagg.base.node.query._leaf_clause import deserialize_leaf_clause
from pandagg.base.node.query.abstract import QueryClause, LeafQueryClause


class ParameterClause(QueryClause):
    P_KEY = NotImplementedError()
    SIMPLE = NotImplementedError()


class SimpleParameter(ParameterClause):
    P_KEY = NotImplementedError()
    SIMPLE = True

    def __init__(self, value):
        super(SimpleParameter, self).__init__(value=value)

    @classmethod
    def deserialize(cls, value):
        return cls(value)

    def serialize(self):
        return {self.P_KEY: self.body['value']}


class Boost(SimpleParameter):
    P_KEY = 'boost'


class ParentClause(QueryClause):
    P_KEY = NotImplementedError()
    SIMPLE = False
    HASHABLE = False
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
            raise ValueError('%s clause does not accept multiple query clauses.' % self.P_KEY)
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
    P_KEY = 'filter'
    MULTIPLE = True


class Must(ParentClause):
    P_KEY = 'must'
    MULTIPLE = True


class Should(ParentClause):
    P_KEY = 'should'
    MULTIPLE = True


class MustNot(ParentClause):
    P_KEY = 'must_not'
    MULTIPLE = True


PARAMETERS = {
    p.P_KEY: p for p in [
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
