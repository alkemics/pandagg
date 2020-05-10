#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from future.utils import string_types

import json

from pandagg.node.query.abstract import QueryClause


class ParameterClause(QueryClause):
    _prefix = "_param_"


class SimpleParameter(ParameterClause):
    _variant = "param"

    def __init__(self, value):
        super(SimpleParameter, self).__init__(value=value)

    def line_repr(self, depth, **kwargs):
        return "%s=%s" % (self.KEY, json.dumps(self.body["value"]))

    def to_dict(self, with_name=True):
        return {self.KEY: self.body["value"]}


class Boost(SimpleParameter):
    KEY = "boost"


class BoostMode(SimpleParameter):
    KEY = "boost_mode"


class FieldValueFactor(SimpleParameter):
    KEY = "field_value_factor"


class Functions(SimpleParameter):
    KEY = "functions"


class IdsP(SimpleParameter):
    KEY = "ids"


class IgnoreUnmapped(SimpleParameter):
    KEY = "ignore_unmapped"


class MaxBoost(SimpleParameter):
    KEY = "max_boost"


class MaxChildren(SimpleParameter):
    KEY = "max_children"


class MinChildren(SimpleParameter):
    KEY = "min_children"


class MinimumShouldMatch(SimpleParameter):
    KEY = "minimum_should_match"


class MinScore(SimpleParameter):
    KEY = "min_score"


class NegativeBoost(SimpleParameter):
    KEY = "negative_boost"


class Path(SimpleParameter):
    KEY = "path"


class ParentType(SimpleParameter):
    KEY = "parent_type"


class RandomScore(SimpleParameter):
    KEY = "random_score"


class Rewrite(SimpleParameter):
    KEY = "rewrite"


class ScoreMode(SimpleParameter):
    KEY = "score_mode"


class ScriptP(SimpleParameter):
    KEY = "script"


class ScriptScore(SimpleParameter):
    KEY = "script_score"


class TieBreaker(SimpleParameter):
    KEY = "tie_breaker"


class Type(SimpleParameter):
    KEY = "type"


class ParentParameterClause(ParameterClause):
    MULTIPLE = False

    def __init__(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], string_types):
            children = [self._type_deserializer(*args, **kwargs)]
        else:
            children = []
            if kwargs:
                children.append(kwargs)
            for arg in args:
                if isinstance(arg, (tuple, list)):
                    children.extend(arg)
                else:
                    children.append(arg)
            if not self.MULTIPLE and len(children) > 1:
                raise ValueError(
                    "%s clause does not accept multiple query clauses." % self.KEY
                )
        super(ParentParameterClause, self).__init__(_children=children)

    def to_dict(self, with_name=True):
        return {self.KEY: [n.to_dict() for n in self._children]}


class Filter(ParentParameterClause):
    KEY = "filter"
    MULTIPLE = True


class Must(ParentParameterClause):
    KEY = "must"
    MULTIPLE = True


class MustNot(ParentParameterClause):
    KEY = "must_not"
    MULTIPLE = True


class Negative(ParentParameterClause):
    KEY = "negative"
    MULTIPLE = False


class Organic(ParentParameterClause):
    KEY = "organic"
    MULTIPLE = False


class Positive(ParentParameterClause):
    KEY = "positive"
    MULTIPLE = False


class Queries(ParentParameterClause):
    KEY = "queries"
    MULTIPLE = True


class QueryP(ParentParameterClause):
    # different name to avoid confusion with Query "tree" class
    KEY = "query"
    MULTIPLE = False


class Should(ParentParameterClause):
    KEY = "should"
    MULTIPLE = True
