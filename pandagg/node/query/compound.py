from pandagg.node.query.abstract import QueryClause


class CompoundClause(QueryClause):
    """Compound clauses can encapsulate other query clauses::

        {
            "<query_type>" : {
                <query_body>
                <children_clauses>
            }
        }
    """

    _default_operator = None
    _parent_params = None

    @classmethod
    def operator(cls, key):
        if key is None:
            return cls.get_dsl_class(cls._default_operator)
        if key not in cls._parent_params:
            raise ValueError(
                "Child operator <%s> not permitted for compound query of type <%s>"
                % (key, cls.__name__)
            )
        return cls.get_dsl_class(key)


class Bool(CompoundClause):
    _default_operator = "must"
    _parent_params = ["should", "must", "must_not", "filter"]
    KEY = "bool"


class Boosting(CompoundClause):
    _default_operator = "positive"
    _parent_params = ["positive", "negative"]
    KEY = "boosting"


class ConstantScore(CompoundClause):
    _default_operator = "filter"
    _parent_params = ["filter", "boost"]
    KEY = "constant_score"


class DisMax(CompoundClause):
    _default_operator = "queries"
    _parent_params = ["queries"]
    KEY = "dis_max"


class FunctionScore(CompoundClause):
    _default_operator = "query"
    _parent_params = ["query"]
    KEY = "function_score"
