from six import iteritems

from pandagg.base.node.query._parameter_clause import deserialize_parameter
from pandagg.base.node.query.abstract import QueryClause


class CompoundClause(QueryClause):
    """Compound clauses can encapsulate other query clauses.

    Note: the children attribute's only purpose is for initiation with the following syntax:
    >>> from pandagg.query import Bool, Term
    >>> query = Bool(
    >>>     filter=Term(field='some_path', value=3),
    >>>     identifier='term_agg',
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
    PARAMS_WHITELIST = None

    def __init__(self, *args, **kwargs):
        identifier = kwargs.pop('identifier', None)
        self.children = []
        if args:
            for arg in args:
                kwargs.update(arg)
        for key, value in iteritems(kwargs):
            if self.PARAMS_WHITELIST is not None and key not in self.PARAMS_WHITELIST:
                raise ValueError('Unauthorized parameter <%s>' % key)
            self.children.append(deserialize_parameter(key, value))
        super(CompoundClause, self).__init__(identifier=identifier)


class Bool(CompoundClause):
    Q_TYPE = 'bool'


class Boosting(CompoundClause):
    Q_TYPE = 'boosting'


class ConstantScore(CompoundClause):
    Q_TYPE = 'constant_score'


class DisMax(CompoundClause):
    Q_TYPE = 'dis_max'


class FunctionScore(CompoundClause):
    Q_TYPE = 'function_score'
