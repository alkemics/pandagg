#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from builtins import str as text
import json

from pandagg.node._node import Node


class QueryClause(Node):
    _type_name = "query"
    KEY = None

    def __init__(self, **body):
        body = body.copy()
        _name = body.pop("_name", None)
        self.body = body
        self._named = _name is not None
        super(QueryClause, self).__init__(identifier=_name)

    def line_repr(self, depth, **kwargs):
        if not self.body:
            return self.KEY
        return ", ".join([text(self.KEY), self._params_repr(self.body)])

    @staticmethod
    def _params_repr(params):
        params = params or {}
        return ", ".join(
            "%s=%s" % (text(k), text(json.dumps(params[k], sort_keys=True)))
            for k in sorted(params.keys())
        )

    @property
    def name(self):
        return self.identifier

    @property
    def _identifier_prefix(self):
        return "%s_" % self.KEY

    def to_dict(self, with_name=True):
        b = self.body.copy()
        if with_name and self._named:
            b["_name"] = self.name
        return {self.KEY: b}

    def __str__(self):
        return "<{class_}, id={id}, type={type}, body={body}>".format(
            class_=text(self.__class__.__name__),
            type=text(self.KEY),
            id=text(self.identifier),
            body=json.dumps(self.body),
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return other.to_dict() == self.to_dict()
        # make sure we still equal to a dict with the same data
        return other == self.to_dict()


class LeafQueryClause(QueryClause):
    pass


class AbstractSingleFieldQueryClause(LeafQueryClause):
    _FIELD_AT_BODY_ROOT = False

    def __init__(self, field, _name=None, **body):
        self.field = field
        if self._FIELD_AT_BODY_ROOT:
            super(LeafQueryClause, self).__init__(_name=_name, field=field, **body)
        else:
            super(LeafQueryClause, self).__init__(_name=_name, **body)


class FlatFieldQueryClause(AbstractSingleFieldQueryClause):
    """Query clause applied on one single field.
    Example:

    Exists:
    {"exists": {"field": "user"}}
    -> field = "user"
    -> body = {"field": "user"}
    q = Exists(field="user")

    DistanceFeature:
    {"distance_feature": {"field": "production_date", "pivot": "7d", "origin": "now"}}
    -> field = "production_date"
    -> body = {"field": "production_date", "pivot": "7d", "origin": "now"}
    q = DistanceFeature(field="production_date", pivot="7d", origin="now")
    """

    _FIELD_AT_BODY_ROOT = True

    def __init__(self, field, _name=None, **body):
        self.field = field
        super(FlatFieldQueryClause, self).__init__(_name=_name, field=field, **body)


class KeyFieldQueryClause(AbstractSingleFieldQueryClause):
    """Clause with field used as key in clause body:

    Term:
    {"term": {"user": {"value": "Kimchy", "boost": 1}}}
    -> field = "user"
    -> body = {"user": {"value": "Kimchy", "boost": 1}}
    q1 = Term(user={"value": "Kimchy", "boost": 1}})
    q2 = Term(field="user", value="Kimchy", boost=1}})

    Can accept a "_implicit_param" attribute specifying which is the equivalent key when inner body isn't a dict but a
    raw value.
    For Term:
    _implicit_param = "value"
    q = Term(user="Kimchy")
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
            return ", ".join([text(self.KEY), "field=%s" % text(self.field)])
        return ", ".join(
            [
                text(self.KEY),
                "field=%s" % text(self.field),
                self._params_repr(self.inner_body),
            ]
        )


class MultiFieldsQueryClause(LeafQueryClause):
    def __init__(self, fields, _name=None, **body):
        self.fields = fields
        super(LeafQueryClause, self).__init__(_name=_name, fields=fields, **body)

    def line_repr(self, depth, **kwargs):
        return "%s, fields=%s" % (self.KEY, list(map(text, self.fields)))


class ParentParameterClause(QueryClause):
    MULTIPLE = False
