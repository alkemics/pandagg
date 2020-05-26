from .abstract import Compound


class Nested(Compound):
    KEY = "nested"


class HasChild(Compound):
    KEY = "has_child"


class HasParent(Compound):
    KEY = "has_parent"


class ParentId(Compound):
    KEY = "parent_id"
