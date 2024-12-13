from typing import Any

from pandagg.node.query import LeafQueryClause


class MatchAll(LeafQueryClause):

    KEY = "match_all"

    def __init__(self, **body: Any) -> None:
        super(MatchAll, self).__init__(**body)


class MatchNone(LeafQueryClause):
    KEY = "match_none"

    def __init__(self, **body: Any) -> None:
        super(MatchNone, self).__init__(**body)
