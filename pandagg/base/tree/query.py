#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from six import iteritems, python_2_unicode_compatible
from builtins import str as text

from pandagg.base._tree import Tree
from pandagg.base.interactive.mapping import as_mapping
from pandagg.base.node.query._parameter_clause import SimpleParameter, ParameterClause, ParentClause, PARAMETERS
from pandagg.base.node.query.abstract import QueryClause, LeafQueryClause
from pandagg.base.node.query.compound import CompoundClause, Bool
from pandagg.base.node.query.joining import Nested


@python_2_unicode_compatible
class Query(Tree):
    """Tree combination of query nodes.

    Mapping declaration is optional, but doing so validates query validity.
    """

    node_class = QueryClause

    def __init__(self, from_=None, mapping=None, identifier=None):
        # set mapping
        self.tree_mapping = None
        if mapping is not None:
            self.set_mapping(mapping)

        if isinstance(from_, Query):
            super(Query, self).__init__(tree=from_, identifier=identifier)
            return
        super(Query, self).__init__(identifier=identifier)
        if from_ is not None:
            if isinstance(from_, QueryClause):
                self._deserialize_from_node(from_)
            elif isinstance(from_, dict):
                self._deserialize_tree_from_dict(from_)
            else:
                raise ValueError('Unsupported <%s> type.' % type(from_))

    def _clone(self, identifier=None, with_tree=False, deep=False):
        return Query(
            mapping=self.tree_mapping,
            identifier=identifier,
            from_=self if with_tree else None
        )

    def set_mapping(self, mapping):
        self.tree_mapping = as_mapping(mapping)
        return self

    def _deserialize_tree_from_dict(self, body, pid=None):
        if not isinstance(body, dict):
            raise ValueError()
        assert len(body.keys()) == 1
        q_type, q_body = next(iteritems(body))
        node = self._node_from_dict(q_type=q_type, q_body=q_body)
        self.add_node(node, pid)
        # extract children
        for child in []:
            self._deserialize_tree_from_dict(child, node.identifier)

    def _deserialize_from_node(self, query_node, pid=None):
        self.add_node(query_node, pid)
        if hasattr(query_node, 'children'):
            for child_node in query_node.children or []:
                if isinstance(child_node, QueryClause):
                    self._deserialize_from_node(child_node, pid=query_node.identifier)
                elif isinstance(child_node, Query):
                    self.paste(nid=pid, new_tree=child_node)
                elif isinstance(child_node, dict):
                    self._deserialize_tree_from_dict(body=child_node, pid=pid)
                else:
                    raise ValueError('Unsupported type <%s>' % type(child_node))
            # reset children to None to avoid confusion since this serves only __init__ syntax.
            query_node.children = None

    def add_node(self, node, pid=None):
        # TODO, validate consistency
        assert isinstance(node, QueryClause)
        if pid is None:
            assert not isinstance(node, ParameterClause)
        else:
            pnode = self[pid]
            assert isinstance(pnode, (ParentClause, CompoundClause))
            if isinstance(pnode, ParentClause):
                assert not isinstance(node, ParameterClause)
            if isinstance(pnode, CompoundClause):
                assert isinstance(node, ParameterClause)
                if pnode.PARAMS_WHITELIST is not None:
                    assert node.KEY in (pnode.PARAMS_WHITELIST or [])
        super(Query, self).add_node(node, pid)

    def query_dict(self, from_=None):
        if self.root is None:
            return {}
        from_ = self.root if from_ is None else from_
        node = self[from_]
        if isinstance(node, (LeafQueryClause, SimpleParameter)):
            return node.serialize()
        serialized_children = []
        should_yield = False
        for child_node in self.children(node.identifier):
            serialized_child = self.query_dict(from_=child_node.identifier)
            if serialized_child is not None:
                serialized_children.append(serialized_child)
                if not isinstance(child_node, SimpleParameter):
                    should_yield = True
        if not should_yield:
            return None
        if isinstance(node, CompoundClause):
            # {bool: {filter: ..., must: ...}
            return {node.KEY: {k: v for d in serialized_children for k, v in d.items()}}
        # parameter clause
        # {filter: [{...}, {...}]}
        return {node.KEY: serialized_children}

    def __str__(self):
        self.show()
        return '<Query>\n%s' % text(self._reader)

    def query(self, arg, pid=None):
        new_query = self._clone(with_tree=True)
        if isinstance(arg, Query):
            new_query.paste(nid=pid, new_tree=arg)
        elif isinstance(arg, QueryClause):
            new_query._deserialize_from_node(pid=pid, query_node=arg)
        elif isinstance(arg, dict):
            new_query._deserialize_tree_from_dict(pid=pid, body=arg)
        else:
            raise ValueError('Unsupported type <%s>.' % type(arg))
        return new_query

    def nested(self, query, path, pid=None, identifier=None):
        new_query = self._clone(with_tree=True)
        new_query._deserialize_from_node(Nested(path=path, identifier=identifier, query=query), pid=pid)
        return new_query

    def bool(self, *args, **kwargs):
        new_query = self._clone(with_tree=True)

        child = kwargs.pop('child', None)
        child_operator = kwargs.pop('child_operator', None)
        if child is not None:
            assert child in self
        if child_operator is not None:
            assert child_operator in Bool.params(parent_only=True).keys()
            child_operator_klass = Bool.params(parent_only=True)[child_operator]
        else:
            child_operator_klass = Bool.DEFAULT_OPERATOR

        # provided parent is compound, real one is parameter
        parent = kwargs.pop('parent', None)
        parent_operator = kwargs.pop('parent_operator', None)
        if parent is not None:
            assert parent in new_query
            parent_node = new_query[parent]
            if parent_operator is not None:
                assert parent_operator in parent_node.params(parent_only=True).keys()
                parent_operator_klass = parent_node.params(parent_only=True)[parent_operator]
            else:
                parent_operator_klass = parent_node.DEFAULT_OPERATOR
            parent_operator_id = next((c.identifier for c in new_query.children(parent) if isinstance(c, parent_operator_klass)), None)
            if parent_operator_id is None:
                parent_operator = parent_operator_klass()
                new_query.add_node(parent_operator, pid=parent)
                parent = parent_operator.identifier
            else:
                parent = parent_operator_id

        if parent is not None and child is not None:
            raise ValueError('Only "child" or "parent" must be declared.')
        # either parent is declared, either child is declared

        if child is None and parent is None:
            if self.root is None:
                new_query = self._clone()
                new_query._deserialize_from_node(Bool(*args, **kwargs))
                return new_query
            # if none is declared, we consider that bool is added on top of existing query
            child = self.root

        # either child, either parent is declared

        # based on child (parent is None)
        if parent is None and child is not None:
            # we insert bool in-between child and its nearest parent (parent is a parameter clause)
            parent_node = new_query.parent(child)
            if parent_node is not None:
                # if child is not already root
                parent = parent_node.identifier

        # insert bool below parent
        child_tree = None
        if child is not None:
            child_tree = new_query.remove_subtree(child)
        b_query = Bool(*args, **kwargs)
        new_query._deserialize_from_node(b_query, pid=parent)
        if child is not None:
            operator_id = next((c.identifier for c in new_query.children(b_query.identifier) if isinstance(c, child_operator_klass)), None)
            if operator_id is None:
                operator = child_operator_klass()
                new_query.add_node(operator, pid=b_query.identifier)
                operator_id = operator.identifier
            new_query.paste(nid=operator_id, new_tree=child_tree)
        return new_query

    def _bool_param(self, param_key, *args, **kwargs):
        param_klass = PARAMETERS[param_key]
        pid = kwargs.pop('pid', None)
        param_identifier = kwargs.pop('identifier', None)
        bool_identifier = kwargs.pop('bool_identifier', None)
        new_query = self._clone(with_tree=True)

        # not providing a parent is only allowed when tree is empty
        if pid is None:
            assert new_query.root is None
            return Query(Bool(param_klass(identifier=param_identifier, *args, **kwargs), identifier=bool_identifier))

        pnode = new_query[pid]

        # if pid is a leaf query, wrap it in bool-param
        if isinstance(pnode, LeafQueryClause):
            gpid = new_query.parent(pid).identifier
            new_query.remove_node(pid)
            new_query._deserialize_from_node(
                query_node=Bool(
                    param_klass(
                        pnode,
                        identifier=param_identifier,
                        *args,
                        **kwargs
                    ),
                    identifier=bool_identifier
                ),
                pid=gpid
            )
            return new_query

        if isinstance(pnode, Bool):
            existing_param = next((c for c in new_query.children(pid) if isinstance(c, param_klass)), None)
            if existing_param is None:
                new_param = param_klass(identifier=param_identifier)
                new_query.add_node(node=new_param, pid=pid)
                pnode = new_param
                pid = new_param.identifier
            else:
                pnode = existing_param
                pid = existing_param.identifier

        if isinstance(pnode, param_klass):
            if param_identifier is not None and param_identifier != pnode.identifier:
                raise ValueError('Param identifier can be provided only if not already existing: provided <%s>, '
                                 'existing <%s>' % (param_identifier, pnode.identifier))
            existing_clauses = new_query.children(pid)
            gp = new_query.parent(pid)
            new_query.remove_node(pid)
            assert isinstance(gp, Bool)
            new_query._deserialize_from_node(
                query_node=param_klass(
                    identifier=param_identifier,
                    children=existing_clauses,
                    *args,
                    **kwargs
                ),
                pid=gp.identifier
            )
            return new_query
        raise ValueError('Unsupported type <%s> as parent for <%s> clause.' % (type(pnode), param_key))

    def must(self, *args, **kwargs):
        return self._bool_param('must', *args, **kwargs)

    def should(self, *args, **kwargs):
        return self._bool_param('should', *args, **kwargs)

    def must_not(self, *args, **kwargs):
        return self._bool_param('must_not', *args, **kwargs)

    def filter(self, *args, **kwargs):
        return self._bool_param('filter', *args, **kwargs)
