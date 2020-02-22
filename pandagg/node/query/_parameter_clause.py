#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import json
from six import iteritems

from pandagg.node.query._leaf_clause import deserialize_leaf_clause
from pandagg.node.query.abstract import QueryClause, LeafQueryClause


class ParameterClause(QueryClause):
    KEY = NotImplementedError()


class SimpleParameter(ParameterClause):
    KEY = NotImplementedError()

    def __init__(self, value):
        super(SimpleParameter, self).__init__(value=value)

    @property
    def tag(self):
        return '%s=%s' % (self.KEY, json.dumps(self.body['value']))

    @classmethod
    def deserialize(cls, value):
        return cls(value)

    def serialize(self, named=False):
        return {self.KEY: self.body['value']}


class Boost(SimpleParameter):
    KEY = 'boost'


class BoostMode(SimpleParameter):
    KEY = 'boost_mode'


class FieldValueFactor(SimpleParameter):
    KEY = 'field_value_factor'


class Functions(SimpleParameter):
    KEY = 'functions'


class IdsP(SimpleParameter):
    KEY = 'ids'


class IgnoreUnmapped(SimpleParameter):
    KEY = 'ignore_unmapped'


class MaxBoost(SimpleParameter):
    KEY = 'max_boost'


class MaxChildren(SimpleParameter):
    KEY = 'max_children'


class MinChildren(SimpleParameter):
    KEY = 'min_children'


class MinimumShouldMatch(SimpleParameter):
    KEY = 'minimum_should_match'


class MinScore(SimpleParameter):
    KEY = 'min_score'


class NegativeBoost(SimpleParameter):
    KEY = 'negative_boost'


class Path(SimpleParameter):
    KEY = 'path'


class ParentType(SimpleParameter):
    KEY = 'parent_type'


class RandomScore(SimpleParameter):
    KEY = 'random_score'


class Rewrite(SimpleParameter):
    KEY = 'rewrite'


class ScoreMode(SimpleParameter):
    KEY = 'score_mode'


class ScriptP(SimpleParameter):
    KEY = 'script'


class ScriptScore(SimpleParameter):
    KEY = 'script_score'


class TieBreaker(SimpleParameter):
    KEY = 'tie_breaker'


class Type(SimpleParameter):
    KEY = 'type'


SIMPLE_PARAMETERS = [
    Boost,
    BoostMode,
    FieldValueFactor,
    Functions,
    IdsP,
    IgnoreUnmapped,
    MaxBoost,
    MaxChildren,
    MinChildren,
    MinimumShouldMatch,
    MinScore,
    NegativeBoost,
    Path,
    ParentType,
    RandomScore,
    Rewrite,
    ScoreMode,
    ScriptP,
    ScriptScore,
    TieBreaker,
    Type,
]


class ParentParameterClause(ParameterClause):
    KEY = NotImplementedError()
    MULTIPLE = False

    def __init__(self, *args, **kwargs):
        children = []
        _name = kwargs.pop('_name', None)
        if kwargs:
            children.append(kwargs)
        for arg in args:
            if isinstance(arg, (tuple, list)):
                children.extend(arg)
            else:
                children.append(arg)
        if not self.MULTIPLE and len(children) > 1:
            raise ValueError('%s clause does not accept multiple query clauses.' % self.KEY)
        serialized_children = []
        for child in children:
            if isinstance(child, dict):
                k, v = next(iteritems(child))
                try:
                    serialized_children.append(deserialize_leaf_clause(k, v))
                except Exception:
                    # until metaclass is implemented
                    serialized_children.append({k: v})
            elif isinstance(child, LeafQueryClause):
                serialized_children.append(child)
            else:
                # Compound - will be validated in query tree
                serialized_children.append(child)

        self.children = serialized_children
        super(ParentParameterClause, self).__init__(_name=_name)

    @classmethod
    def deserialize(cls, *args, **body):
        return cls(*args, **body)


class Filter(ParentParameterClause):
    KEY = 'filter'
    MULTIPLE = True


class Must(ParentParameterClause):
    KEY = 'must'
    MULTIPLE = True


class MustNot(ParentParameterClause):
    KEY = 'must_not'
    MULTIPLE = True


class Negative(ParentParameterClause):
    KEY = 'negative'
    MULTIPLE = False


class Organic(ParentParameterClause):
    KEY = 'organic'
    MULTIPLE = False


class Positive(ParentParameterClause):
    KEY = 'positive'
    MULTIPLE = False


class Queries(ParentParameterClause):
    KEY = 'queries'
    MULTIPLE = True


class QueryP(ParentParameterClause):
    # different name to avoid confusion with Query "tree" class
    KEY = 'query'
    MULTIPLE = False


class Should(ParentParameterClause):
    KEY = 'should'
    MULTIPLE = True


PARENT_PARAMETERS = [
    Filter,
    Must,
    MustNot,
    Negative,
    Organic,
    Positive,
    Queries,
    QueryP,
    Should,
]


PARAMETERS = {p.KEY: p for p in PARENT_PARAMETERS + SIMPLE_PARAMETERS}


def deserialize_parameter(key, body):
    if key not in PARAMETERS.keys():
        raise NotImplementedError('Unknown parameter type <%s>' % key)
    klass = PARAMETERS[key]
    if issubclass(klass, SimpleParameter):
        return klass.deserialize(body)
    if isinstance(body, (tuple, list)):
        return klass.deserialize(*body)
    if isinstance(body, QueryClause):
        return klass.deserialize(body)
    return klass.deserialize(**body)
