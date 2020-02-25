#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import copy

from six import iteritems, python_2_unicode_compatible
from builtins import str as text

from pandagg.tree._tree import Tree
from pandagg.interactive.mapping import as_mapping
from pandagg.node.query._parameter_clause import SimpleParameter, ParameterClause, ParentParameterClause, PARAMETERS
from pandagg.node.query.abstract import QueryClause, LeafQueryClause
from pandagg.node.query.compound import CompoundClause, Bool, Boosting, ConstantScore, DisMax, FunctionScore
from pandagg.node.query.deserializer import deserialize_node
from pandagg.node.query.joining import Nested, HasChild, HasParent, ParentId
from pandagg.node.query.specialized_compound import ScriptScore, PinnedQuery

ADD = 'add'
REPLACE = 'replace'
REPLACE_ALL = 'replace_all'


@python_2_unicode_compatible
class Query(Tree):
    """Tree combination of query nodes.

    Mapping declaration is optional, but doing so validates query validity.
    """

    node_class = QueryClause

    def __init__(self, from_=None, mapping=None, identifier=None, client=None, index_name=None):
        self.index_name = index_name
        self.client = client
        self.tree_mapping = None
        if mapping is not None:
            self.set_mapping(mapping)
        super(Query, self).__init__(identifier=identifier)
        if from_ is not None:
            self._insert(from_)

    def _clone(self, identifier=None, with_tree=False, deep=False):
        return Query(
            client=self.client,
            index_name=self.index_name,
            mapping=self.tree_mapping,
            identifier=identifier,
            from_=self if with_tree else None
        )

    def bind(self, client, index_name=None):
        self.client = client
        if index_name is not None:
            self.index_name = index_name
        return self

    def set_mapping(self, mapping):
        self.tree_mapping = as_mapping(mapping)
        return self

    @classmethod
    def deserialize(cls, from_):
        if isinstance(from_, Query):
            return from_
        if isinstance(from_, QueryClause):
            new = cls()
            new._insert_from_node(query_node=from_)
            return new
        if isinstance(from_, dict):
            from_ = copy.deepcopy(from_)
            new = cls()
            new._insert_from_dict(from_)
            return new
        else:
            raise ValueError('Unsupported type <%s>.' % type(from_))

    def _insert(self, from_, pid=None):
        inserted_tree = self.deserialize(from_=from_)
        if self.root is None:
            self.merge(nid=pid, new_tree=inserted_tree)
            return self
        self.paste(nid=pid, new_tree=inserted_tree)
        return self

    def _insert_from_dict(self, body, pid=None):
        if len(body.keys()) > 1:
            raise ValueError('Invalid query format, got multiple keys, expected a single one: %s' % (body.keys()))
        q_type, q_body = next(iteritems(body))
        node = deserialize_node(q_type, q_body, accept_param=False)
        self._insert_from_node(node, pid)

    def _insert_from_node(self, query_node, pid=None):
        """Insert in tree a node and all of its potential children (stored in .children)."""
        self.add_node(query_node, pid)
        if hasattr(query_node, 'children'):
            for child_node in query_node.children or []:
                self._insert(child_node, pid=query_node.identifier)
            # reset children to None to avoid confusion since this serves only __init__ syntax.
            query_node.children = None

    def add_node(self, node, pid=None):
        if pid is None:
            return super(Query, self).add_node(node, pid)

        pnode = self[pid]
        if isinstance(pnode, LeafQueryClause):
            raise ValueError('Cannot add clause under leaf query clause <%s>' % pnode.KEY)
        if isinstance(pnode, ParentParameterClause):
            if isinstance(node, ParameterClause):
                raise ValueError('Cannot add parameter clause <%s> under another paramter clause <%s>' % (
                    pnode.KEY, node.KEY))
        if isinstance(pnode, CompoundClause):
            if not isinstance(node, ParameterClause) or node.KEY not in pnode.PARAMS_WHITELIST:
                raise ValueError('Expect a parameter clause of type %s under <%s> compound clause, got <%s>' % (
                    pnode.PARAMS_WHITELIST, pnode.KEY, node.KEY))
        super(Query, self).add_node(node, pid)

    def query_dict(self, from_=None, named=False):
        """Return None if no query clause.
        """
        if self.root is None:
            return None
        from_ = self.root if from_ is None else from_
        node = self[from_]
        if isinstance(node, (LeafQueryClause, SimpleParameter)):
            return node.serialize(named=named)
        serialized_children = []
        should_yield = False
        for child_node in self.children(node.identifier):
            serialized_child = self.query_dict(from_=child_node.identifier, named=named)
            if serialized_child is not None:
                serialized_children.append(serialized_child)
                if not isinstance(child_node, SimpleParameter):
                    should_yield = True
        if not should_yield:
            return None
        if isinstance(node, CompoundClause):
            # {bool: {filter: ..., must: ...}
            body = {k: v for d in serialized_children for k, v in d.items()}
            if named:
                body['_name'] = node.name
            return {node.KEY: body}
        # parent parameter clause
        # {filter: [{...}, {...}]}
        assert isinstance(node, ParentParameterClause)
        if node.MULTIPLE:
            return {node.KEY: serialized_children}
        return {node.KEY: serialized_children[0]}

    def query(self, q, parent=None, child=None, parent_param=None, child_param=None, mode=ADD):
        """Place query below a given parent.
        """
        # TODO accept query tree
        if isinstance(q, dict):
            q_type, q_body = next(iteritems(q))
            node = deserialize_node(q_type, q_body, accept_param=False)
        elif isinstance(q, QueryClause):
            node = q
        else:
            raise ValueError('Unsupported type <%s>, must be either dict or QueryClause.' % type(q))
        return self._insert_into(
            node,
            parent=parent,
            child=child,
            mode=mode,
            child_param=child_param,
            parent_param=parent_param
        )

    def _update_compound(self, new_compound, mode):
        if mode not in (ADD, REPLACE, REPLACE_ALL):
            raise ValueError('Unsupported mode <%s> to update compound clause' % mode)
        existing_query = self._clone(with_tree=True)
        parent_node = existing_query.parent(new_compound.identifier)
        if parent_node is None:
            parent = None
        else:
            parent = parent_node.identifier

        if mode == REPLACE_ALL:
            existing_query.remove_subtree(new_compound.identifier)
            existing_query._insert_from_node(new_compound, pid=parent)
            return existing_query

        new_compound_tree = Query(new_compound)
        for param_node in new_compound_tree.children(new_compound.identifier):
            existing_param = next((
                p for p in existing_query.children(new_compound.identifier) if p.KEY == param_node.KEY), None)
            if not existing_param:
                existing_query.paste(
                    new_tree=new_compound_tree.subtree(param_node.identifier),
                    nid=new_compound.identifier
                )
                continue
            if mode == REPLACE:
                existing_query.remove_node(existing_param.identifier)
                existing_query.paste(
                    new_tree=new_compound_tree.subtree(param_node.identifier),
                    nid=new_compound.identifier
                )
                continue
            if mode == ADD:
                for clause_node in new_compound_tree.children(param_node.identifier):
                    existing_query.paste(
                        new_tree=new_compound_tree.subtree(clause_node.identifier),
                        nid=existing_param.identifier
                    )
                continue
        return existing_query

    def _compound_insert(self, compound_klass, *args, **kwargs):
        _name = kwargs.pop('_name', None)
        mode = kwargs.pop('mode', ADD)
        # provided parent is compound, real one is parameter
        parent = kwargs.pop('parent', None)
        parent_param = kwargs.pop('parent_param', None)
        child = kwargs.pop('child', None)
        child_param = kwargs.pop('child_param', None)
        compound_node = compound_klass(_name=_name, *args, **kwargs)
        return self._insert_into(
            compound_node,
            mode=mode,
            parent=parent,
            parent_param=parent_param,
            child=child,
            child_param=child_param
        )

    def _insert_into(self, inserted_node, mode=None, parent=None, parent_param=None, child=None, child_param=None):
        """Insert node in query.
        :param inserted_node:
        :param mode:
        :param parent:
        :param parent_param:
        :param child:
        :param child_param:

        If compound query with existing identifier: merge according to mode (place in-between parent and child).
        If no parent nor child is provided, place on top (wrapped in bool-must if necessary).
        If a child is provided (only possible if inserted node is compound): place on top using child_param.
        If a parent is provided (only under compound query): place under it.
        """

        q = self._clone(with_tree=True)

        # If compound query with existing name: merge according to mode (place in-between parent and child).
        if isinstance(inserted_node, CompoundClause) and inserted_node.name in q:
            if child is not None or parent is not None:
                raise ValueError(
                    'Child or parent cannot be provided when inserting compound clause with existing '
                    '_name <%s> in query. Got child <%s> and parent <%s>.' % (inserted_node.name, child, parent))
            return q._update_compound(new_compound=inserted_node, mode=mode)

        # If no parent nor child is provided, place on top (wrapped in bool-must if necessary).
        if parent is None and child is None:
            # if inital query is empty, just insert new one
            if q.root is None:
                q._insert_from_node(inserted_node)
                return q
            # if both initial root query and inserted one are bool, merge
            if isinstance(q[q.root], Bool) and isinstance(inserted_node, Bool):
                inserted_node.identifier = q.root
                return q._insert_into(inserted_node, mode=mode)
            # if only inserted node is bool, insert initial query in it
            if isinstance(inserted_node, Bool):
                inserted_q = Query(inserted_node)
                child_operator = inserted_node.operator(child_param)
                child_operator_node = next((
                    c for c in inserted_q.children(inserted_node.name) if isinstance(c, child_operator)), None)
                if child_operator_node is None:
                    child_operator_node = child_operator()
                    inserted_q.add_node(child_operator_node, pid=inserted_node.name)
                inserted_q.paste(new_tree=q, nid=child_operator_node.name)
                return inserted_q
            if isinstance(q[q.root], Bool):
                return q.must(inserted_node, _name=q.root, mode=mode,
                              parent_param=parent_param, child_param=child_param)
            parent_param_key = Bool.operator(parent_param).KEY
            return q.bool(
                parent_param=parent_param,
                child_param=child_param,
                mode=mode,
                **{parent_param_key: inserted_node}
            )

        # If a child is provided (only possible if inserted node is compound): place on top using child_param.
        if child is not None:
            if not isinstance(inserted_node, CompoundClause):
                raise ValueError('Cannot place non-compound clause <%s> above other clause <%s>.' % (
                    inserted_node.KEY, child
                ))
            if child not in q:
                raise ValueError('Child <%s> does not exist in current query.' % child)
            child_operator = inserted_node.operator(child_param)
            if parent is not None:
                raise ValueError('Cannot declare both parent <%s> and child <%s> (only one accepted).' % (
                    parent, child
                ))

            # suppose we are under a nested clause, the parent is the "query" param clause
            existing_parent_param_node = q.parent(child)
            direct_pid = existing_parent_param_node.name if existing_parent_param_node else None
            child_tree = q.remove_subtree(child)

            q._insert_from_node(inserted_node, pid=direct_pid)
            child_operator_node = next((
                c for c in q.children(inserted_node.name) if isinstance(c, child_operator)), None)
            if child_operator_node is None:
                child_operator_node = child_operator()
                q.add_node(child_operator_node, pid=inserted_node.name)
            q.paste(new_tree=child_tree, nid=child_operator_node.name)
            return q

        # If a parent is provided (only under compound query): place under it.
        if parent not in q:
            raise ValueError('Parent <%s> does not exist in current query.' % parent)
        parent_node = q[parent]
        if not isinstance(parent_node, CompoundClause):
            raise ValueError(
                'Cannot place clause under non-compound clause <%s> of type <%s>.' % (parent, parent_node.KEY))
        parent_operator = parent_node.operator(parent_param)
        parent_operator_node = next((c for c in q.children(parent) if isinstance(c, parent_operator)), None)
        if parent_operator_node is not None and not parent_operator_node.MULTIPLE:
            if isinstance(parent_node, Bool):
                return q.bool(must=inserted_node, _name=parent)
            child_node = q.children(parent_operator_node.name)[0]
            child = child_node.name
            if isinstance(child_node, Bool):
                return q.bool(must=inserted_node, _name=child, mode=mode)
            return q.bool(must=inserted_node, child=child, mode=mode)
        if parent_operator_node is None:
            parent_operator_node = parent_operator()
            q.add_node(parent_operator_node, pid=parent)
        q._insert_from_node(inserted_node, pid=parent_operator_node.name)
        return q

    def _compound_param(self, method_name, param_key, *args, **kwargs):
        mode = kwargs.pop('mode', ADD)
        param_klass = PARAMETERS[param_key]
        _name = kwargs.pop('_name', None)
        parent = kwargs.pop('parent', None)
        parent_param = kwargs.pop('parent_param', None)
        child = kwargs.pop('child', None)
        child_param = kwargs.pop('child_param', None)
        return getattr(self, method_name)(
            param_klass(*args, **kwargs),
            mode=mode,
            _name=_name,
            parent=parent,
            parent_param=parent_param,
            child=child,
            child_param=child_param,
        )

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
        return self._compound_param('bool', 'must', *args, **kwargs)

    def should(self, *args, **kwargs):
        return self._compound_param('bool', 'should', *args, **kwargs)

    def must_not(self, *args, **kwargs):
        return self._compound_param('bool', 'must_not', *args, **kwargs)

    def filter(self, *args, **kwargs):
        return self._compound_param('bool', 'filter', *args, **kwargs)

    def __str__(self):
        return '<Query>\n%s' % text(self.show())

    def execute(self, index=None, **kwargs):
        if self.client is None:
            raise ValueError('Execution requires to specify "client" at __init__.')
        body = {'query': self.query_dict()}
        body.update(kwargs)
        return self.client.search(index=index or self.index_name, body=body)
