from pandagg.node.query.compound import CompoundClause


class ScriptScore(CompoundClause):
    _default_operator = "query"
    _parent_params = ["query"]
    KEY = "script_score"


class PinnedQuery(CompoundClause):
    _default_operator = "organic"
    _parent_params = ["organic"]
    KEY = "pinned"
