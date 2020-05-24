#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from builtins import str as text

from future.utils import python_2_unicode_compatible, iteritems, string_types

from pandagg.node.query._parameter_clause import ParentParameterClause
from pandagg.node.query.abstract import QueryClause, LeafQueryClause
from pandagg.node.query.compound import CompoundClause, Bool as BoolNode
from pandagg.node.query.joining import Nested

from pandagg.tree._tree import Tree
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

    _type_name = "query_tree"
    KEY = None
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
    def _from_dict(cls, d):
        """return Query, from dict"""
        # {"nested": {"path": "xxx", "query": {"term": {"some_field": "234"}}}}
        if not len(d.keys()) == 1:
            raise ValueError("Wrong declaration, got %s" % d.keys())
        vk, vv = d.copy().popitem()
        return cls._get_dsl_class_from_tree_or_node(vk, **vv)

    @classmethod
    def _get_dsl_class_from_tree_or_node(cls, key, **body):
        # either a compound query clause -> search among trees
        if key in cls._classes:
            return cls.get_dsl_class(key)(**body)
        # either a simple query clause -> search among nodes
        elif key in cls.node_class._classes:
            return Query(cls.node_class.get_dsl_class(key)(**body))
        else:
            raise ValueError("Unkown clause %s" % key)

    def _fill(self, *args, **kwargs):
        if kwargs:
            # only allowed when using special syntax: Query("term", my__field=23)
            if len(args) != 1:
                raise ValueError("Unkown syntax %s %s" % (args, kwargs))
            arg = args[0]
            self.insert(self._get_dsl_class_from_tree_or_node(arg, **kwargs))
            return

        if not args:
            return

        if len(args) > 1:
            raise ValueError("Wrong declaration")

        arg = args[0]
        # None
        if arg is None:
            return self
        # Term(), Query
        if isinstance(arg, (Query, QueryClause)):
            return self.insert(arg)
        # {"bool": {"filter: xxx"}}
        if isinstance(arg, dict):
            self.insert(self._from_dict(arg))
            return
        raise ValueError("Unkown declaration %s" % arg)

    def _insert_node_below(self, node, parent_id=None):
        """Override lighttree.Tree._insert_node_below method to ensure inserted query clause is consistent."""
        if parent_id is not None:
            pnode = self.get(parent_id)
            if isinstance(pnode, LeafQueryClause):
                raise ValueError(
                    "Cannot add clause under leaf query clause <%s>" % pnode.KEY
                )
            if isinstance(pnode, ParentParameterClause):
                if isinstance(node, ParentParameterClause):
                    raise ValueError(
                        "Cannot add parameter clause <%s> under another parameter clause <%s>"
                        % (pnode.KEY, node.KEY)
                    )
            if isinstance(pnode, CompoundClause):
                if (
                    not isinstance(node, ParentParameterClause)
                    or node.KEY not in pnode._parent_params
                ):
                    raise ValueError(
                        "Expect a parameter clause of type %s under <%s> compound clause, got <%s>"
                        % (pnode._parent_params, pnode.KEY, node.KEY)
                    )

        # automatic handling of nested clauses
        if isinstance(node, Nested) or not self.mapping or not hasattr(node, "field"):
            return super(Query, self)._insert_node_below(node=node, parent_id=parent_id)
        required_nested_level = self.mapping.nested_at_field(node.field)
        if self.is_empty():
            current_nested_level = None
        else:
            current_nested_level = self.applied_nested_path_at_node(parent_id)
        if current_nested_level == required_nested_level:
            return super(Query, self)._insert_node_below(node=node, parent_id=parent_id)
        if not self.nested_autocorrect:
            raise ValueError(
                "Invalid %s query clause on %s field. Invalid nested: expected %s, current %s."
                % (node.KEY, node.field, required_nested_level, current_nested_level)
            )
        # requires nested - apply all required nested fields
        to_insert = node
        for nested_lvl in self.mapping.list_nesteds_at_field(node.field):
            if current_nested_level != nested_lvl:
                to_insert = self.get_dsl_class("nested")(
                    path=nested_lvl, query=to_insert
                )
        super(Query, self).insert(to_insert, parent_id)

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
        if isinstance(node, LeafQueryClause):
            return node.to_dict(with_name=True)
        serialized_children = []
        should_yield = False
        for child_node in self.children(node.identifier, id_only=False):
            serialized_child = self.to_dict(
                from_=child_node.identifier, with_name=with_name
            )
            if serialized_child is not None:
                serialized_children.append(serialized_child)
                should_yield = True
        if not should_yield:
            return None
        if isinstance(node, CompoundClause):
            # {bool: {filter: ..., must: ...}
            q = node.to_dict(with_name=with_name)
            extra_body = {k: v for d in serialized_children for k, v in d.items()}
            q[node.KEY].update(extra_body)
            return q
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
            Query(*args, **kwargs),
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
            if isinstance(q.get(q.root), BoolNode) and isinstance(
                inserted_root, BoolNode
            ):
                return q._compound_update(name=q.root, new_compound=inserted, mode=mode)
            # if only inserted node is bool, insert initial query in it
            if isinstance(inserted_root, BoolNode):
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
            if isinstance(q.get(q.root), BoolNode):
                return q.must(
                    inserted,
                    _name=q.root,
                    mode=mode,
                    parent_param=parent_param,
                    child_param=child_param,
                )
            parent_param_key = BoolNode.operator(parent_param).KEY
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
            if isinstance(parent_node, BoolNode):
                return q.bool(must=inserted, _name=parent)
            child_node = q.children(parent_operator_node.name, id_only=False)[0]
            child = child_node.name
            if isinstance(child_node, BoolNode):
                return q.bool(must=inserted, _name=child, mode=mode)
            return q.bool(must=inserted, child=child, mode=mode)
        if parent_operator_node is None:
            parent_operator_node = parent_operator()
            q.insert_node(parent_operator_node, parent_id=parent)
        return q.insert(inserted, parent_id=parent_operator_node.name)

    # compound
    def bool(self, *args, **kwargs):
        return self._compound_insert("bool", *args, **kwargs)

    def boost(self, *args, **kwargs):
        return self._compound_insert("boosting", *args, **kwargs)

    def constant_score(self, *args, **kwargs):
        return self._compound_insert("constant_score", *args, **kwargs)

    def dis_max(self, *args, **kwargs):
        return self._compound_insert("dis_max", *args, **kwargs)

    def function_score(self, *args, **kwargs):
        return self._compound_insert("function_score", *args, **kwargs)

    def nested(self, *args, **kwargs):
        return self._compound_insert("nested", *args, **kwargs)

    def has_child(self, *args, **kwargs):
        return self._compound_insert("has_child", *args, **kwargs)

    def has_parent(self, *args, **kwargs):
        return self._compound_insert("has_parent", *args, **kwargs)

    def parent_id(self, *args, **kwargs):
        return self._compound_insert("parent_id", *args, **kwargs)

    def script_score(self, *args, **kwargs):
        return self._compound_insert("script_score", *args, **kwargs)

    def pinned_query(self, *args, **kwargs):
        return self._compound_insert("pinned_query", *args, **kwargs)

    # compound parameters
    def must(self, *args, **kwargs):
        return self._compound_param_insert("bool", "must", *args, **kwargs)

    def should(self, *args, **kwargs):
        return self._compound_param_insert("bool", "should", *args, **kwargs)

    def must_not(self, *args, **kwargs):
        return self._compound_param_insert("bool", "must_not", *args, **kwargs)

    def filter(self, *args, **kwargs):
        return self._compound_param_insert("bool", "filter", *args, **kwargs)

    def _compound_insert(self, compound_key, *args, **kwargs):
        mode = kwargs.pop("mode", ADD)
        # provided parent is compound, real one is parameter
        parent = kwargs.pop("parent", None)
        parent_param = kwargs.pop("parent_param", None)
        child = kwargs.pop("child", None)
        child_param = kwargs.pop("child_param", None)
        compound_q = self.get_dsl_class(compound_key)(*args, **kwargs)
        return self._insert_into(
            compound_q,
            mode=mode,
            parent=parent,
            parent_param=parent_param,
            child=child,
            child_param=child_param,
        )

    def _compound_param_insert(self, method_name, param_key, *args, **kwargs):
        """Must accept:
        bool, must, terms, some_field=2
        bool, must, Terms()
        bool, must, [Terms()]
        bool, must, {"terms": {"some_field": 2}}
        bool, must, [{"terms": {"some_field": 2}}]
        """
        mode = kwargs.pop("mode", ADD)
        _name = kwargs.pop("_name", None)
        parent = kwargs.pop("parent", None)
        parent_param = kwargs.pop("parent_param", None)
        child = kwargs.pop("child", None)
        child_param = kwargs.pop("child_param", None)

        if len(args) < 1:
            raise ValueError("Invalid: %s %s" % (args, kwargs))

        # special syntax
        if kwargs:
            # only allowed when using special syntax:
            # q.must("term", my__field=23)
            if len(args) > 1:
                raise ValueError("Invalid: %s %s" % (args, kwargs))
            arg = args[0]
            if not isinstance(arg, string_types):
                raise ValueError()
            children = [self._get_dsl_class_from_tree_or_node(arg, **kwargs)]
        else:
            children = []
            for arg in args:
                # .must([{}, {}])
                # .must({}, {})
                children.extend(arg if isinstance(arg, list) else [arg])

        return getattr(self, method_name)(
            mode=mode,
            _name=_name,
            parent=parent,
            parent_param=parent_param,
            child=child,
            child_param=child_param,
            **{param_key: children}
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

        # update simple body parameters
        new_compound_node = new_compound.get(new_compound.root)
        current_root_node = self.get(self.root)
        current_root_node.body.update(new_compound_node.body)
        return self

    def __str__(self):
        return "<Query>\n%s" % text(self.show())


class Leaf(Query):
    KEY = None

    def __init__(self, *args, **kwargs):
        mapping = kwargs.pop("mapping", None)
        nested_autocorrect = kwargs.pop("nested_autocorrect", None)
        super(Leaf, self).__init__(
            mapping=mapping, nested_autocorrect=nested_autocorrect
        )
        node = self.get_node_dsl_class(self.KEY)(*args, **kwargs)
        if not isinstance(node, LeafQueryClause):
            raise ValueError("Error: not a leaf %s" % self.KEY)
        self.insert(node)


class Compound(Query):

    KEY = None
    _params_keys = None

    def __init__(self, **kwargs):
        mapping = kwargs.pop("mapping", None)
        nested_autocorrect = kwargs.pop("nested_autocorrect", None)
        super(Compound, self).__init__(
            mapping=mapping, nested_autocorrect=nested_autocorrect
        )
        body = kwargs.copy()
        extra_body = dict()

        # separate body parameters with children clauses (should), from regular body parameters (minimum_should_match=1)
        node_klass = self.get_node_dsl_class(self.KEY)
        if not issubclass(node_klass, CompoundClause):
            raise ValueError("Error: not a compound clause %s" % self.KEY)
        for p_key in node_klass._parent_params:
            p_value = body.pop(p_key, None)
            if p_value is None:
                continue
            extra_body[p_key] = p_value

        # insert compound node (for instance bool)
        compound_node = node_klass(**body)
        self.insert_node(compound_node)

        for p_key, p_value in iteritems(extra_body):
            # insert parameter (for instance must)
            p_klass = self.get_node_dsl_class(p_key)
            p_node = p_klass()
            self.insert_node(p_node, parent_id=compound_node.identifier)

            # atomize
            # filter=[{}, {}], filter={}, filter=Bool(), filter=[Bool(), Term()]
            p_values = p_value if isinstance(p_value, (list, tuple)) else (p_value,)
            if len(p_values) > 1 and not p_klass.MULTIPLE:
                raise ValueError(
                    "Not possible to have multiple clauses under %s parameter."
                    % p_klass.KEY
                )

            # deserialize
            for v in p_values:
                if isinstance(v, (Query, QueryClause)):
                    self.insert(v, parent_id=p_node.identifier)
                    continue
                if isinstance(v, dict):
                    t = self._from_dict(v)
                    self.insert(t, parent_id=p_node.identifier)
                    continue
                raise ValueError("Unkown clause %s" % v)
