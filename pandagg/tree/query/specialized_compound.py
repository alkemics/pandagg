from pandagg.tree.query.abstract import Compound


class ScriptScore(Compound):
    KEY = "script_score"


class PinnedQuery(Compound):
    KEY = "pinned"
