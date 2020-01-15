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


ADD = 'add'
REPLACE = 'replace'
REPLACE_ALL = 'replace_all'


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
        # TODO, validate mapping consistency when provided
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

    def _compound(self, compound_klass, *args, **kwargs):
        """Insert compound class in query.
        :param compound_klass:
        :param args:
        :param kwargs:
        :return:
        """
        identifier = kwargs.pop('identifier', None)
        mode = kwargs.pop('mode', ADD)
        if mode not in (ADD, REPLACE, REPLACE_ALL):
            raise ValueError('Unsupported mode <%s>' % mode)
        # provided parent is compound, real one is parameter
        parent = kwargs.pop('parent', None)
        parent_operator = kwargs.pop('parent_operator', None)
        child = kwargs.pop('child', None)
        child_operator = kwargs.pop('child_operator', None)

        if parent is not None and child is not None:
            raise ValueError('Only "child" or "parent" must be declared.')

        if child is not None:
            if child not in self:
                raise ValueError('Child <%s> does not exist in current query.' % child)
        if parent is not None:
            if parent not in self:
                raise ValueError('Parent <%s> does not exist in current query.' % parent)
            parent_node = self[parent]
            if not isinstance(parent_node, CompoundClause):
                raise ValueError('Declared parent <%s> of type <%s> is not a compound query and thus cannot '
                                 'have children queries.' % (parent, parent_node.KEY))
        if child_operator is not None:
            if child_operator not in compound_klass.params(parent_only=True).keys():
                raise ValueError('Child operator <%s> not permitted for compound query of type <%s>' % (
                    child_operator, compound_klass.__name__
                ))
            child_operator_klass = compound_klass.params(parent_only=True)[child_operator]
        else:
            child_operator_klass = compound_klass.DEFAULT_OPERATOR

        existing_query = self._clone(with_tree=True)

        # on an existing bool: three modes: ADD or REPLACE, REPLACE_ALL
        if identifier is not None and identifier in existing_query:
            parent_node = existing_query.parent(identifier)
            if parent_node is None:
                parent = None
            else:
                parent = parent_node.identifier

            if mode == REPLACE_ALL:
                existing_query.remove_subtree(identifier)
                existing_query._deserialize_from_node(compound_klass(identifier=identifier, *args, **kwargs), pid=parent)
                return existing_query

            new_compound_tree = Query(compound_klass(identifier=identifier, *args, **kwargs))
            for param_node in new_compound_tree.children(identifier):
                # TODO - simple parameters
                existing_param = next((p for p in existing_query.children(identifier) if p.KEY == param_node.KEY), None)
                if not existing_param:
                    existing_query.paste(new_tree=new_compound_tree.subtree(param_node.identifier), nid=identifier)
                    continue
                if mode == REPLACE:
                    existing_query.remove_node(existing_param.identifier)
                    existing_query.paste(new_tree=new_compound_tree.subtree(param_node.identifier), nid=identifier)
                    continue
                if mode == ADD:
                    for clause_node in new_compound_tree.children(param_node.identifier):
                        existing_query.paste(new_tree=new_compound_tree.subtree(clause_node.identifier), nid=existing_param.identifier)
                    continue
            return existing_query

        # non existing compound clause
        if parent is not None:
            parent_node = existing_query[parent]
            if parent_operator is not None:
                if parent_operator not in parent_node.params(parent_only=True).keys():
                    raise ValueError('Parent operator <%s> not permitted for compound query of type <%s>' % (
                        parent_operator, compound_klass.__name__
                    ))
                parent_operator_klass = parent_node.params(parent_only=True)[parent_operator]
            else:
                parent_operator_klass = parent_node.DEFAULT_OPERATOR
            parent_operator_id = next((c.identifier for c in existing_query.children(parent) if isinstance(c, parent_operator_klass)), None)
            if parent_operator_id is None:
                parent_operator = parent_operator_klass()
                existing_query.add_node(parent_operator, pid=parent)
                parent = parent_operator.identifier
            else:
                parent = parent_operator_id

        if child is None and parent is None:
            if existing_query.root is None:
                existing_query._deserialize_from_node(compound_klass(identifier=identifier, *args, **kwargs))
                return existing_query
            # if none is declared, we consider that compound clause is added on top of existing query
            child = existing_query.root

        # based on child (parent is None)
        if parent is None and child is not None:
            # we insert compound clause in-between child and its nearest parent (parent is a parameter clause)
            parent_node = existing_query.parent(child)
            if parent_node is not None:
                # if child is not already root
                parent = parent_node.identifier

        # insert compound clause below parent
        child_tree = None
        if child is not None:
            child_tree = existing_query.remove_subtree(child)
        b_query = compound_klass(identifier=identifier, *args, **kwargs)
        existing_query._deserialize_from_node(b_query, pid=parent)
        if child is not None:
            operator_id = next((c.identifier for c in existing_query.children(b_query.identifier) if isinstance(c, child_operator_klass)), None)
            if operator_id is None:
                operator = child_operator_klass()
                existing_query.add_node(operator, pid=b_query.identifier)
                operator_id = operator.identifier
            existing_query.paste(nid=operator_id, new_tree=child_tree)
        return existing_query

    def _compound_param(self, method_name, param_key, *args, **kwargs):
        mode = kwargs.pop('mode', ADD)
        param_klass = PARAMETERS[param_key]
        identifier = kwargs.pop('identifier', None)
        parent = kwargs.pop('parent', None)
        parent_operator = kwargs.pop('parent_operator', None)
        child = kwargs.pop('child', None)
        child_operator = kwargs.pop('child_operator', None)
        return getattr(self, method_name)(
            param_klass(*args, **kwargs),
            mode=mode,
            identifier=identifier,
            parent=parent,
            parent_operator=parent_operator,
            child=child,
            child_operator=child_operator,
        )

    # compound
    def bool(self, *args, **kwargs):
        return self._compound(Bool, *args, **kwargs)

    def nested(self, *args, **kwargs):
        return self._compound(Nested, *args, **kwargs)

    # compound parameters
    def must(self, *args, **kwargs):
        return self._compound_param('bool', 'must', *args, **kwargs)

    def should(self, *args, **kwargs):
        return self._compound_param('bool', 'should', *args, **kwargs)

    def must_not(self, *args, **kwargs):
        return self._compound_param('bool', 'must_not', *args, **kwargs)

    def filter(self, *args, **kwargs):
        return self._compound_param('bool', 'filter', *args, **kwargs)
