from pandagg.base.node.query import CompoundClause


class Nested(CompoundClause):
    KEY = 'nested'


class HasChild(CompoundClause):
    KEY = 'has_child'


class HasParent(CompoundClause):
    KEY = 'has_parent'


class ParentId(CompoundClause):
    KEY = 'parent_id'
