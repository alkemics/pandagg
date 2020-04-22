from pandagg.node.query.compound import CompoundClause
from pandagg.node.query._parameter_clause import Organic, QueryP


class ScriptScore(CompoundClause):
    DEFAULT_OPERATOR = QueryP
    PARAMS_WHITELIST = ["query", "script", "min_score"]
    KEY = "script_score"


class PinnedQuery(CompoundClause):
    DEFAULT_OPERATOR = Organic
    PARAMS_WHITELIST = ["ids", "organic"]
    KEY = "pinned"
