from typing import List, Optional, Any

from pandagg.types import QueryName
from pandagg.node.query.abstract import QueryClause


class CompoundClause(QueryClause):
    """
    Compound clauses can encapsulate other query clauses:

    .. code-block::

        {
            "<query_type>" : {
                <query_body>
                <children_clauses>
            }
        }

    """

    KEY: str
    _default_operator: str = ""
    _parent_params: List[str] = []

    def __init__(self, _name: Optional[QueryName] = None, **body: Any) -> None:
        b = body.copy()
        children = {}
        for param in self._parent_params:
            v = b.pop(param, None)
            if not v:
                continue
            children[param] = v if isinstance(v, list) else [v]
        super(CompoundClause, self).__init__(
            _name=_name, accept_children=True, keyed=True, _children=children, **b
        )


class Bool(CompoundClause):
    """
    >>> Bool(must=[], should=[], filter=[], must_not=[], boost=1.2)
    """

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
