from pandagg.node.query.compound import CompoundClause
from pandagg.node.query._parameter_clause import Path, QueryP


class Nested(CompoundClause):
    DEFAULT_OPERATOR = QueryP
    PARAMS_WHITELIST = ["path", "query", "score_mode", "ignore_unmapped"]
    KEY = "nested"

    def __init__(self, *args, **kwargs):
        super(Nested, self).__init__(*args, **kwargs)
        self.path = next(
            (c.body["value"] for c in self._children if isinstance(c, Path))
        )


class HasChild(CompoundClause):
    DEFAULT_OPERATOR = QueryP
    PARAMS_WHITELIST = [
        "query",
        "type",
        "max_children",
        "min_children",
        "score_mode",
        "ignore_unmapped",
    ]
    KEY = "has_child"


class HasParent(CompoundClause):
    DEFAULT_OPERATOR = QueryP
    PARAMS_WHITELIST = ["query", "parent_type", "score", "ignore_unmapped"]
    KEY = "has_parent"


class ParentId(CompoundClause):
    KEY = "parent_id"
