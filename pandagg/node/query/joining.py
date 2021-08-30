from typing import Any

from pandagg.node.query.abstract import LeafQueryClause
from pandagg.node.query.compound import CompoundClause


class Nested(CompoundClause):
    _default_operator = "query"
    _parent_params = ["query"]
    KEY = "nested"

    def __init__(self, path: str, **body: Any) -> None:
        super(Nested, self).__init__(path=path, **body)
        self.path: str = path


class HasChild(CompoundClause):
    _default_operator = "query"
    _parent_params = ["query"]
    KEY = "has_child"


class HasParent(CompoundClause):
    _default_operator = "query"
    _parent_params = ["query"]
    KEY = "has_parent"


class ParentId(LeafQueryClause):
    KEY = "parent_id"
