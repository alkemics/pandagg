from pandagg.base.node.query import CompoundClause


class ScriptScore(CompoundClause):
    PARAMS_WHITELIST = ['query', 'script', 'min_score']
    KEY = 'script_score'


class PinnedQuery(CompoundClause):
    PARAMS_WHITELIST = ['ids', 'organic']
    KEY = 'pinned'


SPECIALIZED_COMPOUND_QUERIES = [
    ScriptScore,
    PinnedQuery
]
