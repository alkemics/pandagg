#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from six import iteritems, python_2_unicode_compatible
from builtins import str as text

from pandagg.base.interactive.mapping import as_mapping
from pandagg.base.node.query import Nested
from pandagg.base.node.query._parameter_clause import SimpleParameter, ParameterClause, ParentClause, PARAMETERS
from pandagg.base.node.query.abstract import QueryClause, LeafQueryClause
from pandagg.base.node.query.compound import CompoundClause, Bool
from pandagg.base._tree import Tree


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
                self._deserialize_from_node(child_node, pid=query_node.identifier)
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
        for child_node in self.children(node.identifier):
            serialized_children.append(self.query_dict(from_=child_node.identifier))
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
        pid = kwargs.pop('pid', None)
        new_query = self._clone(with_tree=True)
        new_query._deserialize_from_node(Bool(*args, **kwargs), pid=pid)
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
