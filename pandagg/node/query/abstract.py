import json

from pandagg.node._node import Node
from typing import Optional, Union, Dict, Any, Tuple, List, Type

from pandagg.types import QueryType, QueryClauseDict


class QueryClause(Node):

    _classes: Dict[QueryType, Type["QueryClause"]]

    KEY: str
    _type_name = "query"

    def __init__(
        self,
        _name: Optional[str] = None,
        accept_children: bool = True,
        keyed: bool = True,
        _children: Any = None,
        **body: Any
    ) -> None:
        self.body = body.copy()
        self._named = _name is not None
        super(QueryClause, self).__init__(
            identifier=_name, accept_children=accept_children, keyed=keyed
        )
        self._children = _children or {}

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        repr_args = []
        if self._named:
            repr_args.append("_name=%s" % str(self.identifier))
        if self.body:
            repr_args.append(self._params_repr(self.body))
        return self.KEY, ", ".join(repr_args)

    @staticmethod
    def _params_repr(params: Dict) -> str:
        params = params or {}
        return ", ".join(
            "%s=%s" % (str(k), str(json.dumps(params[k], sort_keys=True)))
            for k in sorted(params.keys())
        )

    @property
    def name(self) -> str:
        return self.identifier

    @property
    def _identifier_prefix(self) -> str:
        return "%s_" % self.KEY

    def to_dict(self) -> Dict[str, Any]:
        b = self.body.copy()
        if self._named:
            b["_name"] = self.name
        return {self.KEY: b}

    def __str__(self) -> str:
        return "<{class_}, id={id}, type={type}, body={body}>".format(
            class_=str(self.__class__.__name__),
            type=str(self.KEY),
            id=str(self.identifier),
            body=self.body,
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return other.to_dict() == self.to_dict()
        # make sure we still equal to a dict with the same data
        return other == self.to_dict()


TypeOrQuery_ = Union[QueryType, QueryClauseDict, QueryClause]


def Q(type_or_query: Optional[TypeOrQuery_] = None, **body: Any) -> QueryClause:
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
        return QueryClause.get_dsl_class(type_)(**body_)
    if isinstance(type_or_query, str):
        return QueryClause.get_dsl_class(type_or_query)(**body)
    raise ValueError('"type_or_query" must be among "dict", "AggNode", "str"')


class LeafQueryClause(QueryClause):
    KEY: str

    def __init__(self, _name: Optional[str] = None, **body: Any):
        super(LeafQueryClause, self).__init__(
            _name=_name, accept_children=False, **body
        )


class AbstractSingleFieldQueryClause(LeafQueryClause):
    _FIELD_AT_BODY_ROOT: bool = False

    def __init__(self, field: str, _name: Optional[str] = None, **body: Any):
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

    def __init__(self, field: str, _name: Optional[str] = None, **body: Any) -> None:
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

    _implicit_param: Optional[str] = None
    KEY: str

    def __init__(
        self,
        field: Optional[str] = None,
        _name: Optional[str] = None,
        _expand__to_dot: bool = True,
        **params: Any
    ) -> None:
        field_: str
        if field is None:
            # Term(item__id=32) or Term(item__id={'value': 32, 'boost': 1})
            if len(params) != 1:
                raise ValueError(
                    "Invalid declaration for <%s> clause, got:\n%s"
                    % (self.__class__.__name__, params)
                )
            if _expand__to_dot:
                params = self.expand__to_dot(params)
            field_, value = params.copy().popitem()
            if self._implicit_param is None:
                # GeoBoundingBox(pin__location={"top_left": xxx, "bottom_right": xxx})
                # -> {"top_left": xxx, "bottom_right": xxx}
                params = value
            elif isinstance(value, dict):
                # Term(user={"value": "Kimchy", "boost": 1})  -> {"user": {"value": "Kimchy", "boost": 1}}
                params = value
            else:
                # Term(user="Kimchy")                         -> {"user": {"value": "Kimchy"}}
                # in this case we normalize query so that both syntax generate same query:
                # - `Term(user="Kimchy")`
                # - `Term(user={"value": "Kimchy"})`
                params = {self._implicit_param: value}
        else:
            # Term(field="user", value="Kimchy", boost=1)     -> {"user": {"value": "Kimchy", "boost": 1}}
            field_ = field
        self.inner_body: Dict[str, Any] = params
        super(KeyFieldQueryClause, self).__init__(
            field=field_, _name=_name, **{field_: params}
        )

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        if not self.inner_body:
            return "", ", ".join([str(self.KEY), "field=%s" % str(self.field)])
        return (
            self.KEY,
            ", ".join(
                ["field=%s" % str(self.field), self._params_repr(self.inner_body)]
            ),
        )


class MultiFieldsQueryClause(LeafQueryClause):

    KEY: str

    def __init__(self, fields: List[str], _name: Optional[str] = None, **body: Any):
        self.fields = fields
        super(LeafQueryClause, self).__init__(_name=_name, fields=fields, **body)

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        return self.KEY, "fields=%s" % (list(map(str, self.fields)))


class ParentParameterClause(QueryClause):
    KEY: str

    def __init__(self) -> None:
        super(ParentParameterClause, self).__init__(accept_children=True, keyed=False)

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        return "", ""
