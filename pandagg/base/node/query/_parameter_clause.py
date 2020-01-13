#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import json
from six import iteritems

from pandagg.base.node.query._leaf_clause import deserialize_leaf_clause
from pandagg.base.node.query.abstract import QueryClause, LeafQueryClause


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

    def serialize(self):
        return {self.KEY: self.body['value']}


class Boost(SimpleParameter):
    KEY = 'boost'


class MinimumShouldMatch(SimpleParameter):
    KEY = 'minimum_should_match'


class Rewrite(SimpleParameter):
    KEY = 'rewrite'


class TieBreaker(SimpleParameter):
    KEY = 'tie_breaker'


class NegativeBoost(SimpleParameter):
    KEY = 'negative_boost'


class Functions(SimpleParameter):
    KEY = 'functions'


class MaxBoost(SimpleParameter):
    KEY = 'max_boost'


class ScoreMode(SimpleParameter):
    KEY = 'score_mode'


class BoostMode(SimpleParameter):
    KEY = 'boost_mode'


class MinScore(SimpleParameter):
    KEY = 'min_score'


class ScriptScore(SimpleParameter):
    KEY = 'script_score'


class RandomScore(SimpleParameter):
    KEY = 'random_score'


class FieldValueFactor(SimpleParameter):
    KEY = 'field_value_factor'


class Path(SimpleParameter):
    KEY = 'path'


class IdsP(SimpleParameter):
    KEY = 'ids'


class ScriptP(SimpleParameter):
    KEY = 'script'


class IgnoreUnmapped(SimpleParameter):
    KEY = 'ignore_unmapped'


class ParentClause(ParameterClause):
    KEY = NotImplementedError()
    MULTIPLE = False

    def __init__(self, *args, **kwargs):
        children = kwargs.pop('children', [])
        identifier = kwargs.pop('identifier', None)
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
                serialized_children.append(deserialize_leaf_clause(k, v))
            elif isinstance(child, LeafQueryClause):
                serialized_children.append(child)
            else:
                # Compound - will be validated in query tree
                serialized_children.append(child)

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


class QueryP(ParentClause):
    # different name to avoid confusion with Query "tree" class
    KEY = 'query'
    MULTIPLE = False


class Queries(ParentClause):
    KEY = 'queries'
    MULTIPLE = True


class Positive(ParentClause):
    KEY = 'positive'
    MULTIPLE = False


class Negative(ParentClause):
    KEY = 'negative'
    MULTIPLE = False


class Organic(ParentClause):
    KEY = 'organic'
    MULTIPLE = False


PARENT_PARAMETERS = [
    QueryP,
    Queries,
    Filter,
    MustNot,
    Must,
    Should,
    Positive,
    Negative,
    Organic,
]

SIMPLE_PARAMETERS = [
    Boost,
    MinimumShouldMatch,
    TieBreaker,
    Rewrite,
    NegativeBoost,
    Functions,
    MaxBoost,
    ScoreMode,
    BoostMode,
    MinScore,
    ScriptScore,
    RandomScore,
    FieldValueFactor,
    Path,
    IdsP,
    ScriptP
]

PARAMETERS = {p.KEY: p for p in PARENT_PARAMETERS + SIMPLE_PARAMETERS}


def deserialize_parameter(key, body):
    if key not in PARAMETERS.keys():
        raise NotImplementedError('Unknown parameter type <%s>' % key)
    klass = PARAMETERS[key]
    if issubclass(klass, SimpleParameter):
        return klass.deserialize(body)
    if isinstance(body, (tuple, list)) and all((isinstance(b, QueryClause) for b in body)):
        return klass.deserialize(children=body)
    if isinstance(body, QueryClause):
        return klass.deserialize(children=[body])
    return klass.deserialize(**body)
