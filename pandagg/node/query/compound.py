from six import iteritems

from pandagg.node.query._parameter_clause import deserialize_parameter, ParameterClause, PARAMETERS, \
    SimpleParameter, Must, Filter, Queries, QueryP, Positive
from pandagg.node.query.abstract import QueryClause


class CompoundClause(QueryClause):
    """Compound clauses can encapsulate other query clauses.

    Note: the children attribute's only purpose is for initiation with the following syntax:
    >>> from pandagg.query import Bool, Term
    >>> query = Bool(
    >>>     filter=Term(field='some_path', value=3),
    >>>     _name='term_agg',
    >>> )
    Yet, the children attribute will then be reset to None to avoid confusion since the real hierarchy is stored in the
    bpointer/fpointer attributes inherited from treelib.Tree class.
    """

    """
    {
        "<query_type>" : {
            <query_body>
            <children_clauses>
        }
    }
    >>>{
    >>>    "bool" : {
    >>>         # query body
    >>>         "minimum_should_match": 1,
    >>>         # children clauses
    >>>         "should": [<q1>, <q2>],
    >>>         "filter": [<q3>]
    >>>    }
    >>>}
    """
    DEFAULT_OPERATOR = NotImplementedError()
    PARAMS_WHITELIST = None

    def __init__(self, *args, **kwargs):
        _name = kwargs.pop('_name', None)
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
        serialized_children = []
        for child in children:
            if isinstance(child, dict):
                assert len(child.keys()) == 1
                key, value = next(iteritems(child))
                if self.PARAMS_WHITELIST is not None and key not in self.PARAMS_WHITELIST:
                    raise ValueError('Unauthorized parameter <%s> under <%s> clause' % (key, self.KEY))
                serialized_child = deserialize_parameter(key, value)
            else:
                if not isinstance(child, ParameterClause):
                    raise ValueError('Unsupported <%s> clause type under compound clause of type <%s>' % (
                        type(child), self.KEY))
                key = child.KEY
                serialized_child = child
            if self.PARAMS_WHITELIST is not None and key not in self.PARAMS_WHITELIST:
                raise ValueError('Unauthorized parameter <%s> under <%s> clause' % (key, self.KEY))
            serialized_children.append(serialized_child)
        self.children = serialized_children
        super(CompoundClause, self).__init__(_name=_name)

    @classmethod
    def operator(cls, key):
        if key is None:
            return cls.DEFAULT_OPERATOR
        if key not in cls.PARAMS_WHITELIST:
            raise ValueError('Child operator <%s> not permitted for compound query of type <%s>' % (
                key, cls.__name__
            ))
        return PARAMETERS[key]

    @classmethod
    def params(cls, parent_only=False):
        """Return map of key -> params that handle children leaves."""
        return {
            p: PARAMETERS[p] for p in cls.PARAMS_WHITELIST or []
            if not parent_only or not issubclass(PARAMETERS[p], SimpleParameter)
        }

    @classmethod
    def deserialize(cls, *args, **body):
        return cls(*args, **body)


class Bool(CompoundClause):
    DEFAULT_OPERATOR = Must
    PARAMS_WHITELIST = ['should', 'must', 'must_not', 'filter', 'boost', 'minimum_should_match']
    KEY = 'bool'


class Boosting(CompoundClause):
    DEFAULT_OPERATOR = Positive
    PARAMS_WHITELIST = ['positive', 'negative', 'negative_boost']
    KEY = 'boosting'


class ConstantScore(CompoundClause):
    DEFAULT_OPERATOR = Filter
    PARAMS_WHITELIST = ['filter', 'boost']
    KEY = 'constant_score'


class DisMax(CompoundClause):
    DEFAULT_OPERATOR = Queries
    PARAMS_WHITELIST = ['queries', 'tie_breaker']
    KEY = 'dis_max'


class FunctionScore(CompoundClause):
    DEFAULT_OPERATOR = QueryP
    PARAMS_WHITELIST = [
        'query', 'boost', 'random_score', 'boost_mode', 'functions', 'max_boost', 'score_mode', 'min_score'
    ]
    KEY = 'function_score'


COMPOUND_QUERIES = [
    Bool,
    Boosting,
    ConstantScore,
    FunctionScore,
    DisMax
]
