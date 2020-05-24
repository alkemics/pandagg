#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals


from pandagg.node.query.abstract import ParentParameterClause


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
