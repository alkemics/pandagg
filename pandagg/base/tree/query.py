#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from six import iteritems, python_2_unicode_compatible
from builtins import str as text

from pandagg.base.interactive.mapping import as_mapping
from pandagg.base.node.query._parameter_clause import SimpleParameter, ParameterClause, ParentClause
from pandagg.base.node.query.abstract import QueryClause, LeafQueryClause
from pandagg.base.node.query.compound import CompoundClause
from pandagg.base._tree import Tree


@python_2_unicode_compatible
class Query(Tree):
    """Tree combination of query nodes.

    Mapping declaration is optional, but doing so validates query validity.
    """

    node_class = QueryClause
    _crafted_root_name = 'root'

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
