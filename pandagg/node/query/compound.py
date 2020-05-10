from future.utils import iteritems

from pandagg.node.query._parameter_clause import (
    SimpleParameter,
    Must,
    Filter,
    Queries,
    QueryP,
    Positive,
)
from pandagg.node.query.abstract import QueryClause


class CompoundClause(QueryClause):
    """Compound clauses can encapsulate other query clauses::

        {
            "<query_type>" : {
                <query_body>
                <children_clauses>
            }
        }

    Note: the children attribute's only purpose is for initiation with the following syntax:

    >>> from pandagg.query import Bool, Term
    >>> query = Bool(
    >>>     filter=Term(field='some_path', value=3),
    >>>     _name='bool_id',
    >>> )
    """

    DEFAULT_OPERATOR = None
    PARAMS_WHITELIST = None
    _variant = "compound"

    def __init__(self, *args, **kwargs):
        _name = kwargs.pop("_name", None)
        children = []
        for key, value in iteritems(kwargs):
            children.append({key: value})
        for arg in args:
            if isinstance(arg, dict):
                children.extend([{k: v} for k, v in iteritems(arg)])
            elif isinstance(arg, (tuple, list)):
                children.extend(arg)
            else:
                children.append(arg)
        super(CompoundClause, self).__init__(_name=_name, _children=children)

    @classmethod
    def operator(cls, key):
        if key is None:
            return cls.DEFAULT_OPERATOR
        if key not in cls.PARAMS_WHITELIST:
            raise ValueError(
                "Child operator <%s> not permitted for compound query of type <%s>"
                % (key, cls.__name__)
            )
        return cls.get_dsl_class(key, "_param_")

    @classmethod
    def params(cls, parent_only=False):
        """Return map of key -> params that handle children leaves."""
        return {
            p: cls.get_dsl_class(p, "_param_")
            for p in cls.PARAMS_WHITELIST or []
            if not parent_only
            or not issubclass(cls.get_dsl_class(p, "_param_"), SimpleParameter)
        }

    def to_dict(self, with_name=True):
        d = {}
        for c in self._children:
            d.update(c.to_dict())
        return {self.KEY: d}


class Bool(CompoundClause):
    DEFAULT_OPERATOR = Must
    PARAMS_WHITELIST = [
        "should",
        "must",
        "must_not",
        "filter",
        "boost",
        "minimum_should_match",
    ]
    KEY = "bool"


class Boosting(CompoundClause):
    DEFAULT_OPERATOR = Positive
    PARAMS_WHITELIST = ["positive", "negative", "negative_boost"]
    KEY = "boosting"


class ConstantScore(CompoundClause):
    DEFAULT_OPERATOR = Filter
    PARAMS_WHITELIST = ["filter", "boost"]
    KEY = "constant_score"


class DisMax(CompoundClause):
    DEFAULT_OPERATOR = Queries
    PARAMS_WHITELIST = ["queries", "tie_breaker"]
    KEY = "dis_max"


class FunctionScore(CompoundClause):
    DEFAULT_OPERATOR = QueryP
    PARAMS_WHITELIST = [
        "query",
        "boost",
        "random_score",
        "boost_mode",
        "functions",
        "max_boost",
        "score_mode",
        "min_score",
    ]
    KEY = "function_score"
