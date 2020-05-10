#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from builtins import str as text

from future.utils import python_2_unicode_compatible

from pandagg.tree._tree import Tree

from pandagg.node.query._parameter_clause import (
    SimpleParameter,
    ParameterClause,
    ParentParameterClause,
)
from pandagg.node.query.abstract import QueryClause, LeafQueryClause
from pandagg.node.query.compound import (
    CompoundClause,
    Bool,
    Boosting,
    ConstantScore,
    DisMax,
    FunctionScore,
)
from pandagg.node.query.joining import Nested, HasChild, HasParent, ParentId
from pandagg.node.query.specialized_compound import ScriptScore, PinnedQuery

# necessary imports to ensure all clauses are loaded
import pandagg.node.query.full_text as full_text  # noqa
import pandagg.node.query.geo as geo  # noqa
import pandagg.node.query.shape as shape  # noqa
import pandagg.node.query.span as span  # noqa
import pandagg.node.query.specialized as specialized  # noqa
import pandagg.node.query.term_level as term_level  # noqa
from pandagg.tree.mapping import Mapping

ADD = "add"
REPLACE = "replace"
REPLACE_ALL = "replace_all"


@python_2_unicode_compatible
class Query(Tree):
    r"""Combination of query clauses.

    Mapping declaration is optional, but doing so validates query validity and automatically inserts nested clauses
    when necessary.

    :Keyword Arguments:
        * *mapping* (``dict`` or ``pandagg.tree.mapping.Mapping``) --
          Mapping of requested indice(s). Providing it will add validation features, and add required nested
          clauses if missing.

        * *nested_autocorrect* (``bool``) --
          In case of missing nested clauses in query, if True, automatically add missing nested clauses, else raise
          error.

        * remaining kwargs:
          Used as body in query clauses.
    """

    node_class = QueryClause

    def __init__(self, *args, **kwargs):
        self.mapping = Mapping(kwargs.pop("mapping", None))
        self.nested_autocorrect = kwargs.pop("nested_autocorrect", False)
        super(Query, self).__init__()
        if args or kwargs:
            self._fill(*args, **kwargs)

    def __nonzero__(self):
        return bool(self.to_dict())

    __bool__ = __nonzero__

    def _clone_init(self, deep=False):
        return Query(
            mapping=self.mapping.clone(with_tree=True, deep=deep),
            nested_autocorrect=self.nested_autocorrect,
        )

    @classmethod
    def deserialize(cls, *args, **kwargs):
        mapping = kwargs.pop("mapping", None)
        if len(args) == 1 and isinstance(args[0], Query):
            return args[0]

        new = cls(mapping=mapping)
        return new._fill(*args, **kwargs)

    def _fill(self, *args, **kwargs):
        if args:
            if not kwargs and len(args) == 1 and args[0] is None:
                return self
            node_hierarchy = self.node_class._type_deserializer(*args, **kwargs)
        elif kwargs:
            node_hierarchy = self.node_class._type_deserializer(kwargs)
        else:
            return self
        self.insert(node_hierarchy)
        return self

    def _insert_node_below(self, node, parent_id=None, with_children=True):
        """Override lighttree.Tree._insert_node_below method to ensure inserted query clause is consistent."""
        if parent_id is not None:
            pnode = self.get(parent_id)
            if isinstance(pnode, LeafQueryClause):
                raise ValueError(
                    "Cannot add clause under leaf query clause <%s>" % pnode.KEY
                )
            if isinstance(pnode, ParentParameterClause):
                if isinstance(node, ParameterClause):
                    raise ValueError(
                        "Cannot add parameter clause <%s> under another paramter clause <%s>"
                        % (pnode.KEY, node.KEY)
                    )
            if isinstance(pnode, CompoundClause):
                if (
                    not isinstance(node, ParameterClause)
                    or node.KEY not in pnode.PARAMS_WHITELIST
                ):
                    raise ValueError(
                        "Expect a parameter clause of type %s under <%s> compound clause, got <%s>"
                        % (pnode.PARAMS_WHITELIST, pnode.KEY, node.KEY)
                    )

        # automatic handling of nested clauses
        if isinstance(node, Nested) or not self.mapping or not hasattr(node, "field"):
            return super(Query, self)._insert_node_below(
                node=node, parent_id=parent_id, with_children=with_children
            )
        required_nested_level = self.mapping.nested_at_field(node.field)
        if self.is_empty():
            current_nested_level = None
        else:
            current_nested_level = self.applied_nested_path_at_node(parent_id)
        if not self.nested_autocorrect:
            raise ValueError(
                "Invalid %s query clause on %s field. Invalid nested: expected %s, current %s."
                % (node.KEY, node.field, required_nested_level, current_nested_level)
            )
        if current_nested_level == required_nested_level:
            return super(Query, self)._insert_node_below(
                node=node, parent_id=parent_id, with_children=with_children
            )
        # requires nested - apply all required nested fields
        for nested_lvl in self.mapping.list_nesteds_at_field(node.field):
            if current_nested_level != nested_lvl:
                node = Nested(path=nested_lvl, query=node)
        super(Query, self)._insert_node_below(node, parent_id, with_children=True)

    def applied_nested_path_at_node(self, nid):
        # from current node to root
        for id_ in [nid] + self.ancestors(nid):
            node = self.get(id_)
            if isinstance(node, Nested):
                return node.path
        return None

    def to_dict(self, from_=None, with_name=True):
        """Return None if no query clause.
        """
        if self.root is None:
            return None
        from_ = self.root if from_ is None else from_
        node = self.get(from_)
        if isinstance(node, (LeafQueryClause, SimpleParameter)):
            return node.to_dict(with_name=True)
        serialized_children = []
        should_yield = False
        for child_node in self.children(node.identifier, id_only=False):
            serialized_child = self.to_dict(
                from_=child_node.identifier, with_name=with_name
            )
            if serialized_child is not None:
                serialized_children.append(serialized_child)
                if not isinstance(child_node, SimpleParameter):
                    should_yield = True
        if not should_yield:
            return None
        if isinstance(node, CompoundClause):
            # {bool: {filter: ..., must: ...}
            body = {k: v for d in serialized_children for k, v in d.items()}
            if with_name and node._named:
                body["_name"] = node.name
            return {node.KEY: body}
        # parent parameter clause
        # {filter: [{...}, {...}]}
        assert isinstance(node, ParentParameterClause)
        if node.MULTIPLE:
            return {node.KEY: serialized_children}
        return {node.KEY: serialized_children[0]}

    def query(self, *args, **kwargs):
        mode = kwargs.pop("mode", ADD)
        parent = kwargs.pop("parent", None)
        parent_param = kwargs.pop("parent_param", None)
        child = kwargs.pop("child", None)
        child_param = kwargs.pop("child_param", None)
        return self._insert_into(
            self.node_class._type_deserializer(*args, **kwargs),
            parent=parent,
            child=child,
            mode=mode,
            child_param=child_param,
            parent_param=parent_param,
        )

    def _insert_into(
        self,
        inserted,
        mode,
        parent=None,
        parent_param=None,
        child=None,
        child_param=None,
    ):
        """Insert element (node or tree) in query.

        If compound query with existing identifier: merge according to mode (place in-between parent and child).
        If no parent nor child is provided, place on top (wrapped in bool-must if necessary).
        If a child is provided (only possible if inserted node is compound): place on top using child_param.
        If a parent is provided (only under compound query): place under it.
        """
        if not isinstance(inserted, Query):
            inserted = Query(inserted)
        if inserted.is_empty():
            return self
        # ensure we don't modify passed query
        inserted = inserted.clone(with_tree=True)
        inserted_root = inserted.get(inserted.root)
        q = self.clone(with_tree=True)

        # If compound query with existing name: merge according to mode (place in-between parent and child).
        if isinstance(inserted_root, CompoundClause) and inserted_root.name in q:
            if child is not None or parent is not None:
                raise ValueError(
                    "Child or parent cannot be provided when inserting compound clause with existing "
                    "_name <%s> in query. Got child <%s> and parent <%s>."
                    % (inserted_root.name, child, parent)
                )
            return q._compound_update(
                name=inserted.root, new_compound=inserted, mode=mode
            )

        # If no parent nor child is provided, place on top (wrapped in bool-must if necessary).
        if parent is None and child is None:
            # if inital query is empty, just insert new one
            if q.root is None:
                return q.insert(inserted)
            # if both initial root query and inserted one are bool, merge
            if isinstance(q.get(q.root), Bool) and isinstance(inserted_root, Bool):
                return q._compound_update(name=q.root, new_compound=inserted, mode=mode)
            # if only inserted node is bool, insert initial query in it
            if isinstance(inserted_root, Bool):
                child_operator = inserted_root.operator(child_param)
                child_operator_node = next(
                    (
                        c
                        for c in inserted.children(inserted.root, id_only=False)
                        if isinstance(c, child_operator)
                    ),
                    None,
                )
                if child_operator_node is None:
                    child_operator_node = child_operator()
                    inserted.insert_node(child_operator_node, parent_id=inserted.root)
                return inserted.insert(item=q, parent_id=child_operator_node.name)
            if isinstance(q.get(q.root), Bool):
                return q.must(
                    inserted,
                    _name=q.root,
                    mode=mode,
                    parent_param=parent_param,
                    child_param=child_param,
                )
            parent_param_key = Bool.operator(parent_param).KEY
            return q.bool(
                parent_param=parent_param,
                child_param=child_param,
                mode=mode,
                **{parent_param_key: inserted}
            )

        # If a child is provided (only possible if inserted node is compound): place on top using child_param.
        if child is not None:
            if not isinstance(inserted_root, CompoundClause):
                raise ValueError(
                    "Cannot place non-compound clause <%s> above other clause <%s>."
                    % (inserted_root.KEY, child)
                )
            if child not in q:
                raise ValueError("Child <%s> does not exist in current query." % child)
            child_operator = inserted_root.operator(child_param)
            if parent is not None:
                raise ValueError(
                    "Cannot declare both parent <%s> and child <%s> (only one accepted)."
                    % (parent, child)
                )

            # suppose we are under a nested clause, the parent is the "query" param clause
            existing_parent_param_node = q.parent(child, id_only=False)
            direct_pid = (
                existing_parent_param_node.name if existing_parent_param_node else None
            )
            child_tree = q.drop_subtree(child)

            q.insert(inserted, parent_id=direct_pid)
            child_operator_node = next(
                (
                    c
                    for c in q.children(inserted.root, id_only=False)
                    if isinstance(c, child_operator)
                ),
                None,
            )
            if child_operator_node is None:
                child_operator_node = child_operator()
                q.insert_node(child_operator_node, parent_id=inserted.root)
            q.insert(item=child_tree, parent_id=child_operator_node.name)
            return q

        # If a parent is provided (only under compound query): place under it.
        if parent not in q:
            raise ValueError("Parent <%s> does not exist in current query." % parent)
        parent_node = q.get(parent)
        if not isinstance(parent_node, CompoundClause):
            raise ValueError(
                "Cannot place clause under non-compound clause <%s> of type <%s>."
                % (parent, parent_node.KEY)
            )
        parent_operator = parent_node.operator(parent_param)
        parent_operator_node = next(
            (
                c
                for c in q.children(parent, id_only=False)
                if isinstance(c, parent_operator)
            ),
            None,
        )
        if parent_operator_node is not None and not parent_operator_node.MULTIPLE:
            if isinstance(parent_node, Bool):
                return q.bool(must=inserted, _name=parent)
            child_node = q.children(parent_operator_node.name, id_only=False)[0]
            child = child_node.name
            if isinstance(child_node, Bool):
                return q.bool(must=inserted, _name=child, mode=mode)
            return q.bool(must=inserted, child=child, mode=mode)
        if parent_operator_node is None:
            parent_operator_node = parent_operator()
            q.insert_node(parent_operator_node, parent_id=parent)
        return q.insert(inserted, parent_id=parent_operator_node.name)

    # compound
    def bool(self, *args, **kwargs):
        return self._compound_insert(Bool, *args, **kwargs)

    def boost(self, *args, **kwargs):
        return self._compound_insert(Boosting, *args, **kwargs)

    def constant_score(self, *args, **kwargs):
        return self._compound_insert(ConstantScore, *args, **kwargs)

    def dis_max(self, *args, **kwargs):
        return self._compound_insert(DisMax, *args, **kwargs)

    def function_score(self, *args, **kwargs):
        return self._compound_insert(FunctionScore, *args, **kwargs)

    def nested(self, *args, **kwargs):
        return self._compound_insert(Nested, *args, **kwargs)

    def has_child(self, *args, **kwargs):
        return self._compound_insert(HasChild, *args, **kwargs)

    def has_parent(self, *args, **kwargs):
        return self._compound_insert(HasParent, *args, **kwargs)

    def parent_id(self, *args, **kwargs):
        return self._compound_insert(ParentId, *args, **kwargs)

    def script_score(self, *args, **kwargs):
        return self._compound_insert(ScriptScore, *args, **kwargs)

    def pinned_query(self, *args, **kwargs):
        return self._compound_insert(PinnedQuery, *args, **kwargs)

    # compound parameters
    def must(self, *args, **kwargs):
        return self._compound_param_insert("bool", "must", *args, **kwargs)

    def should(self, *args, **kwargs):
        return self._compound_param_insert("bool", "should", *args, **kwargs)

    def must_not(self, *args, **kwargs):
        return self._compound_param_insert("bool", "must_not", *args, **kwargs)

    def filter(self, *args, **kwargs):
        return self._compound_param_insert("bool", "filter", *args, **kwargs)

    def _compound_insert(self, compound_klass, *args, **kwargs):
        _name = kwargs.pop("_name", None)
        mode = kwargs.pop("mode", ADD)
        # provided parent is compound, real one is parameter
        parent = kwargs.pop("parent", None)
        parent_param = kwargs.pop("parent_param", None)
        child = kwargs.pop("child", None)
        child_param = kwargs.pop("child_param", None)
        compound_node = compound_klass(_name=_name, *args, **kwargs)
        return self._insert_into(
            compound_node,
            mode=mode,
            parent=parent,
            parent_param=parent_param,
            child=child,
            child_param=child_param,
        )

    def _compound_param_insert(self, method_name, param_key, *args, **kwargs):
        mode = kwargs.pop("mode", ADD)
        param_klass = self.node_class.get_dsl_class(param_key, "_param_")
        _name = kwargs.pop("_name", None)
        parent = kwargs.pop("parent", None)
        parent_param = kwargs.pop("parent_param", None)
        child = kwargs.pop("child", None)
        child_param = kwargs.pop("child_param", None)
        return getattr(self, method_name)(
            param_klass(*args, **kwargs),
            mode=mode,
            _name=_name,
            parent=parent,
            parent_param=parent_param,
            child=child,
            child_param=child_param,
        )

    def _compound_update(self, name, new_compound, mode):
        """Update existing compound clause <name> inplace in query by merging it with provided <new_compound> query.

        Three modes are available:
        Mode 'add':
        >>> initial_compound = Query({'bool': {'must': [A, B]}, '_name': 'some_bool_id'})
        >>> new_compound = Query({'bool': {'must': [C], 'filter': [D]}, '_name': 'some_bool_id'})

        :name: bool name in current query
        :param new_compound:
        :param mode: 'add', 'replace' or 'replace_all'.
        :return: self
        """
        if mode not in (ADD, REPLACE, REPLACE_ALL):
            raise ValueError("Unsupported mode <%s> to update compound clause" % mode)
        parent_node = self.parent(name, id_only=False)
        if parent_node is None:
            parent = None
        else:
            parent = parent_node.identifier

        if mode == REPLACE_ALL:
            self.drop_subtree(name)
            self.insert(new_compound, parent_id=parent)
            return self

        for param_node in new_compound.children(new_compound.root, id_only=False):
            existing_param = next(
                (
                    p
                    for p in self.children(name, id_only=False)
                    if p.KEY == param_node.KEY
                ),
                None,
            )
            if not existing_param:
                self.insert(
                    item=new_compound.subtree(param_node.identifier), parent_id=name,
                )
                continue
            if mode == REPLACE:
                self.drop_node(existing_param.identifier)
                self.insert(
                    item=new_compound.subtree(param_node.identifier), parent_id=name,
                )
                continue
            if mode == ADD:
                for clause_node in new_compound.children(
                    param_node.identifier, id_only=False
                ):
                    self.insert(
                        item=new_compound.subtree(clause_node.identifier),
                        parent_id=existing_param.identifier,
                    )
                continue
        return self

    def __str__(self):
        return "<Query>\n%s" % text(self.show())
