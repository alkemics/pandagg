from six import iteritems

from pandagg.base.node.query._parameter_clause import deserialize_parameter, ParameterClause
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
        children = kwargs.pop('children', None) or []
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
                    raise ValueError('Unauthorized parameter <%s>' % key)
                serialized_child = deserialize_parameter(key, value)
            else:
                assert isinstance(child, ParameterClause)
                key = child.KEY
                serialized_child = child
            if self.PARAMS_WHITELIST is not None and key not in self.PARAMS_WHITELIST:
                raise ValueError('Unauthorized parameter <%s>' % key)
            serialized_children.append(serialized_child)
        self.children = serialized_children
        super(CompoundClause, self).__init__(identifier=identifier)


class Bool(CompoundClause):
    PARAMS_WHITELIST = ['should', 'must', 'must_not', 'filter', 'boost', 'minimum_should_match']
    KEY = 'bool'


class Boosting(CompoundClause):
    KEY = 'boosting'


class ConstantScore(CompoundClause):
    KEY = 'constant_score'


class DisMax(CompoundClause):
    KEY = 'dis_max'


class FunctionScore(CompoundClause):
    KEY = 'function_score'
