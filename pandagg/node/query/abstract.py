#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

from pandagg.node._node import Node


def Q(type_or_query=None, **body):
    """
    Accept multiple syntaxes, return a QueryClause node.

    :param type_or_query:
    :param body:
    :return: QueryClause
    """
    if isinstance(type_or_query, QueryClause):
        if body:
            raise ValueError(
                'Body cannot be added using "QueryClause" declaration, got %s.' % body
            )
        return type_or_query

    if isinstance(type_or_query, dict):
        if body:
            raise ValueError(
                'Body cannot be added using "dict" query clause declaration, got %s.'
                % body
            )
        type_or_query = type_or_query.copy()
        # {"term": {"some_field": 1}}
        # {"bool": {"filter": [{"term": {"some_field": 1}}]}}
        if len(type_or_query) != 1:
            raise ValueError(
                "Invalid query clause declaration (two many keys): got <%s>"
                % type_or_query
            )
        type_, body_ = type_or_query.popitem()
        return QueryClause._get_dsl_class(type_)(**body_)
    if isinstance(type_or_query, str):
        return QueryClause._get_dsl_class(type_or_query)(**body)
    raise ValueError('"type_or_query" must be among "dict", "AggNode", "str"')


class QueryClause(Node):
    _type_name = "query"
    KEY = None

    def __init__(
        self, _name=None, accept_children=True, keyed=True, _children=None, **body
    ):
        self.body = body.copy()
        self._named = _name is not None
        super(QueryClause, self).__init__(
            identifier=_name, accept_children=accept_children, keyed=keyed
        )
        self._children = _children or {}

    def line_repr(self, depth, **kwargs):
        repr_args = []
        if self._named:
            repr_args.append("_name=%s" % str(self.identifier))
        if self.body:
            repr_args.append(self._params_repr(self.body))
        return self.KEY, ", ".join(repr_args)

    @staticmethod
    def _params_repr(params):
        params = params or {}
        return ", ".join(
            "%s=%s" % (str(k), str(json.dumps(params[k], sort_keys=True)))
            for k in sorted(params.keys())
        )

    @property
    def name(self):
        return self.identifier

    @property
    def _identifier_prefix(self):
        return "%s_" % self.KEY

    def to_dict(self):
        b = self.body.copy()
        if self._named:
            b["_name"] = self.name
        return {self.KEY: b}

    def __str__(self):
        return "<{class_}, id={id}, type={type}, body={body}>".format(
            class_=str(self.__class__.__name__),
            type=str(self.KEY),
            id=str(self.identifier),
            body=self.body,
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return other.to_dict() == self.to_dict()
        # make sure we still equal to a dict with the same data
        return other == self.to_dict()


class LeafQueryClause(QueryClause):
    def __init__(self, _name=None, **body):
        super(LeafQueryClause, self).__init__(
            _name=_name, accept_children=False, **body
        )


class AbstractSingleFieldQueryClause(LeafQueryClause):
    _FIELD_AT_BODY_ROOT = False

    def __init__(self, field, _name=None, **body):
        self.field = field
        if self._FIELD_AT_BODY_ROOT:
            super(LeafQueryClause, self).__init__(_name=_name, field=field, **body)
        else:
            super(LeafQueryClause, self).__init__(_name=_name, **body)


class FlatFieldQueryClause(AbstractSingleFieldQueryClause):
    """
    Query clause applied on one single field.
    Example:

    Exists:
    {"exists": {"field": "user"}}
    -> field = "user"
    -> body = {"field": "user"}
    >>> from pandagg.query import Exists
    >>> q = Exists(field="user")

    DistanceFeature:
    {"distance_feature": {"field": "production_date", "pivot": "7d", "origin": "now"}}
    -> field = "production_date"
    -> body = {"field": "production_date", "pivot": "7d", "origin": "now"}
    >>> from pandagg.query import DistanceFeature
    >>> q = DistanceFeature(field="production_date", pivot="7d", origin="now")
    """

    _FIELD_AT_BODY_ROOT = True

    def __init__(self, field, _name=None, **body):
        self.field = field
        super(FlatFieldQueryClause, self).__init__(_name=_name, field=field, **body)


class KeyFieldQueryClause(AbstractSingleFieldQueryClause):
    """
    Clause with field used as key in clause body:

    Term:
    {"term": {"user": {"value": "Kimchy", "boost": 1}}}
    -> field = "user"
    -> body = {"user": {"value": "Kimchy", "boost": 1}}
    >>> from pandagg.query import Term
    >>> q1 = Term(user={"value": "Kimchy", "boost": 1}})
    >>> q2 = Term(field="user", value="Kimchy", boost=1}})

    Can accept a "_implicit_param" attribute specifying which is the equivalent key when inner body isn't a dict but a
    raw value.
    For Term:
    _implicit_param = "value"
    >>> q = Term(user="Kimchy")
    {"term": {"user": {"value": "Kimchy"}}}
    -> field = "user"
    -> body = {"term": {"user": {"value": "Kimchy"}}}
    """

    _implicit_param = None

    def __init__(self, field=None, _name=None, _expand__to_dot=True, **params):
        if field is None:
            # Term(item__id=32) or Term(item__id={'value': 32, 'boost': 1})
            if len(params) != 1:
                raise ValueError(
                    "Invalid declaration for <%s> clause, got:\n%s"
                    % (self.__class__.__name__, params)
                )
            if _expand__to_dot:
                field, value = self.expand__to_dot(params).copy().popitem()
            else:
                field, value = params.copy().popitem()
            params = value if isinstance(value, dict) else {self._implicit_param: value}
        self.inner_body = params
        super(KeyFieldQueryClause, self).__init__(
            field=field, _name=_name, **{field: params}
        )

    def line_repr(self, depth, **kwargs):
        if not self.inner_body:
            return "", ", ".join([str(self.KEY), "field=%s" % str(self.field)])
        return (
            self.KEY,
            ", ".join(
                ["field=%s" % str(self.field), self._params_repr(self.inner_body)]
            ),
        )


class MultiFieldsQueryClause(LeafQueryClause):
    def __init__(self, fields, _name=None, **body):
        self.fields = fields
        super(LeafQueryClause, self).__init__(_name=_name, fields=fields, **body)

    def line_repr(self, depth, **kwargs):
        return self.KEY, "fields=%s" % (list(map(str, self.fields)))


class ParentParameterClause(QueryClause):
    def __init__(self):
        super(ParentParameterClause, self).__init__(accept_children=True, keyed=False)

    def line_repr(self, **kwargs):
        return "", ""
