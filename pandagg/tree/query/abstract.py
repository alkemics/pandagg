#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json

from future.utils import python_2_unicode_compatible, text_type
from builtins import str as text

from pandagg._decorators import Substitution
from pandagg.node.query._parameter_clause import ParentParameterClause
from pandagg.node.query.abstract import QueryClause, LeafQueryClause
from pandagg.node.query.compound import CompoundClause, Bool
from pandagg.node.query.joining import Nested

from pandagg.tree._tree import Tree
from pandagg.tree.mapping import _mapping

ADD = "add"
REPLACE = "replace"
REPLACE_ALL = "replace_all"

sub_insertion = Substitution(
    insertion_doc="""
    * *parent* (``str``) --
      named query clause under which the inserted clauses should be placed.

    * *parent_param* (``str`` optional parameter when using *parent* param) --
      parameter under which inserted clauses will be placed. For instance if *parent* clause is a boolean, can be
      'must', 'filter', 'should', 'must_not'.

    * *child* (``str``) --
      named query clause above which the inserted clauses should be placed.

    * *child_param* (``str`` optional parameter when using *parent* param) --
      parameter of inserted boolean clause under which child clauses will be placed. For instance if inserted clause
      is a boolean, can be 'must', 'filter', 'should', 'must_not'.

    * *mode* (``str`` one of 'add', 'replace', 'replace_all') --
      merging strategy when inserting clauses on a existing compound clause.

      - 'add' (default) : adds new clauses keeping initial ones
      - 'replace' : for each parameter (for instance in 'bool' case : 'filter', 'must', 'must_not', 'should'),
        replace existing clauses under this parameter, by new ones only if declared in inserted compound query
      - 'replace_all' : existing compound clause is completely replaced by the new one
"""
)


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

    KEY = None
    node_class = QueryClause

    def __init__(self, q=None, mapping=None, nested_autocorrect=False):
        self.mapping = _mapping(mapping)
        self.nested_autocorrect = nested_autocorrect
        super(Query, self).__init__()
        if q:
            self._insert_query(q)

    def __nonzero__(self):
        return bool(self.to_dict())

    __bool__ = __nonzero__

    def _clone_init(self, deep=False):
        return Query(
            mapping=None
            if self.mapping is None
            else self.mapping.clone(with_nodes=True, deep=deep),
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

    def _has_bool_root(self):
        if not self.root:
            return False
        _, r = self.get(self.root)
        return isinstance(r, Bool)

    def _compound_param_id(self, nid, key, create_if_not_exists=True):
        """
        :param nid: id of compound node
        :param key: param key, for instance if compound if bool, can be 'must', 'should', 'must_not' etc
        :return: param node id
        """
        try:
            return self.child_id(nid, key)
        except ValueError:
            if not create_if_not_exists:
                raise
        # add key
        param_node = self.get_node_dsl_class(key)()
        self._insert_node_below(param_node, parent_id=nid, key=key, by_path=False)
        return param_node.identifier

    @classmethod
    def _translate_query(cls, type_or_query=None, **body):
        """Accept multiple syntaxes, return a QueryClause node.
        :param type_or_query:
        :param body:
        :return: QueryClause
        """
        if isinstance(type_or_query, QueryClause):
            if body:
                raise ValueError(
                    'Body cannot be added using "QueryClause" declaration, got %s.'
                    % body
                )
            return type_or_query
        if isinstance(type_or_query, Query):
            type_or_query = type_or_query.to_dict()
        if isinstance(type_or_query, dict):
            if body:
                raise ValueError(
                    'Body cannot be added using "dict" query clause declaration, got %s.'
                    % body
                )
            type_or_query = type_or_query.copy()
            # {"term": {"some_field": 1}}
            # {"bool": {"filter": [{"term": {"some_field": 1}}]}}
            if len(type_or_query) != 1:
                raise ValueError(
                    "Invalid query clause declaration (two many keys): got <%s>"
                    % type_or_query
                )
            type_, body_ = type_or_query.popitem()
            return cls.get_node_dsl_class(type_)(**body_)
        if isinstance(type_or_query, text_type):
            return cls.get_node_dsl_class(type_or_query)(**body)
        raise ValueError('"type_or_query" must be among "dict", "AggNode", "str"')

    def _insert_query_at(
        self, node, mode, on=None, insert_below=None, compound_param=None
    ):
        """Insert clause (and its children) in query.

        If compound query with on specified: merge according to mode.
        If insert_below is not specified, place on top (wrapped in bool-must if necessary).
        If insert_below is provided (only under compound query): place under it.

        :param node: node to insert, can contain children clauses (in case of compound query).
        :param mode: how compound queries merge should be treated
        :param on: id of compound query on which node should be merged
        :param insert_below:
        :return:
        """
        if mode not in (ADD, REPLACE, REPLACE_ALL):
            raise ValueError("Invalid mode %s" % mode)

        if (
            isinstance(node, Bool)
            and not on
            and not insert_below
            and self._has_bool_root()
        ):
            on = self.root

        if isinstance(node, CompoundClause) and on:
            # ensure we try to merge on same type of clause
            _, existing = self.get(on)
            if existing.KEY != node.KEY:
                raise ValueError(
                    "Cannot merge compound clause %s on %s. Must be the same."
                    % (node.KEY, existing.KEY)
                )
            if mode == REPLACE_ALL:
                pid = self.parent_id(on)
                existing_k, _ = self.drop_subtree(on)
                self._insert_query(node, insert_below=pid)
                existing.body = node.body
                return

            # merge
            existing.body.update(node.body)
            for param_key, children in node._children.items():
                if not children:
                    continue
                param_id = self._compound_param_id(on, param_key)
                # here, possible modes are either ADD, or REPLACE
                if mode == REPLACE:
                    existing_clauses_ids = self.children_ids(param_id)
                    for eid in existing_clauses_ids:
                        self.drop_node(eid)
                for child in children:
                    self._insert_query(child, insert_below=param_id)
            return

        if insert_below:
            # below node, with compound_param
            _, pnode = self.get(insert_below)
            if not isinstance(pnode, CompoundClause):
                raise ValueError(
                    "Cannot insert clause below %s clause (only compound clauses can have children clauses)."
                    % pnode.KEY
                )
            compound_param = compound_param or pnode._default_operator
            if compound_param not in pnode._parent_params.keys():
                raise ValueError(
                    "<%s> parameter for <%s> compound clause does not accept children clauses."
                    % (compound_param, pnode.KEY)
                )
            param_id = self._compound_param_id(insert_below, compound_param)
            _, p = self.get(param_id)
            if not p.MULTIPLE:
                # inserting a clause, under a parameter allowing a single clause (for instance below nested query)
                cids = self.children_ids(param_id)
                if cids:
                    # can be at most one
                    cid = cids[0]
                    # must place existing clause, and new one under a bool -> must
                    _, existing_child = self.get(cid)
                    if isinstance(existing_child, Bool):
                        child_param_id = self._compound_param_id(
                            existing_child.identifier, "must"
                        )
                        self._insert_query(node, insert_below=child_param_id)
                        return
                    _, existing_child = self.drop_node(cid)
                    self._insert_query(
                        Bool(must=[existing_child, node]), insert_below=param_id
                    )
                    return
            self._insert_query(node, insert_below=param_id)
            return

        # from now on: position was not provided:
        if self.is_empty():
            # currently empty (-> on top)
            self._insert_query(node)
            return

        if self._has_bool_root():
            # top query is bool
            must_id = self._compound_param_id(self.root, "must")
            self._insert_query(node, insert_below=must_id)
            return

        # top query is not bool
        _, initial_query = self.drop_subtree(self.root)
        if isinstance(node, Bool):
            # if inserted node is bool, placed on top, and previous query is added under "must" parameter
            self._insert_query(node)
            self._insert_query_at(
                initial_query.to_dict(), insert_below=node.identifier, mode=ADD
            )
        else:
            # we place initial + added clauses under a bool>must
            self._insert_query(Bool(must=[node, initial_query.to_dict()]))
        return

    def _insert_query(self, query=None, insert_below=None):
        """
        Accept Node, or dict syntaxes, convert to nodes.
        Insert query clause and its children.
        Wraps in bool->must if parent param is not multiple.

        Does not handle:
        - syntax conversion (Tree -> Node, or flat syntax)
        - logic about where to insert it, should be handled before (dumb insert below insert_below)

        >>> Query()._insert_query({"terms": {"field": "user"}})
        >>> Query()._insert_query(Query({"terms": {"field": "user"}}))
        >>> Query()._insert_query({"bool": {"must": {"term": {"some_field": "yolo"}}, "should": {}}})
        >>> Query()._insert_query({"term": {"some_field": "yolo"})
        """
        if isinstance(query, QueryClause):
            node = query
        elif isinstance(query, dict):
            query = query.copy()
            # {"term": {"some_field": 1}}
            # {"bool": {"filter": [{"term": {"some_field": 1}}]}}
            if len(query.keys()) != 1:
                raise ValueError(
                    "Invalid query clause declaration (two many keys): got <%s>"
                    % query.keys()
                )
            type_, body_ = query.popitem()
            node_klass = self.get_node_dsl_class(type_)
            if issubclass(node_klass, ParentParameterClause):
                raise ValueError()
            node = node_klass(**body_)
        else:
            raise ValueError('"query" must be of type "dict" or "AggNode"')

        self.insert_node(node, parent_id=insert_below)

        if not isinstance(node, CompoundClause):
            return

        _children_clauses = node._children.copy()
        for param_name, child_nodes in _children_clauses.items():
            param_node = self.get_node_dsl_class(param_name)()
            if not param_node.MULTIPLE and len(child_nodes) > 1:
                raise ValueError(
                    "Cannot insert multiple query clauses under %s parameter"
                    % param_name
                )
            self.insert_node(param_node, parent_id=node.identifier, key=param_name)
            for child in child_nodes:
                self._insert_query(query=child, insert_below=param_node.identifier)

    def _insert_node_below(self, node, parent_id, key, by_path):
        """Override lighttree.Tree._insert_node_below method to ensure inserted query clause is consistent."""
        if parent_id is not None:
            _, pnode = self.get(parent_id)
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
                if key not in pnode._parent_params:
                    raise ValueError(
                        "Expect a parameter clause of type %s under <%s> compound clause, got <%s>"
                        % (pnode._parent_params.keys(), pnode.KEY, key)
                    )

        # automatic handling of nested clauses
        if isinstance(node, Nested) or not self.mapping or not hasattr(node, "field"):
            return super(Query, self)._insert_node_below(
                node=node, parent_id=parent_id, key=key, by_path=by_path
            )
        required_nested_level = self.mapping.nested_at_field(node.field)
        if len(self.list()) <= 1:
            # empty
            current_nested_level = None
        else:
            current_nested_level = self.applied_nested_path_at_node(parent_id)
        if current_nested_level == required_nested_level:
            return super(Query, self)._insert_node_below(
                node=node, parent_id=parent_id, key=key, by_path=by_path
            )
        if not self.nested_autocorrect:
            raise ValueError(
                "Invalid %s query clause on %s field. Invalid nested: expected %s, current %s."
                % (node.KEY, node.field, required_nested_level, current_nested_level)
            )
        # requires nested - apply all required nested fields
        to_insert = node
        for nested_lvl in self.mapping.list_nesteds_at_field(node.field):
            if current_nested_level != nested_lvl:
                to_insert = self.get_node_dsl_class("nested")(
                    path=nested_lvl, query=to_insert
                )
        self._insert_query(to_insert, parent_id)

    def applied_nested_path_at_node(self, nid):
        # from current node to root
        for id_ in self.ancestors_ids(nid, include_current=True):
            _, node = self.get(id_)
            if isinstance(node, Nested):
                return node.path
        return None

    def to_dict(self, from_=None):
        """Serialize query as dict."""
        if self.root is None:
            return None
        from_ = self.root if from_ is None else from_
        key, node = self.get(from_)
        if isinstance(node, LeafQueryClause):
            return node.to_dict()

        if not isinstance(node, CompoundClause):
            raise ValueError("Unexpected %s" % node.__class__)

        d = {}
        is_empty = True
        for param_key, param_node in self.children(node.identifier):
            children_serialized = [
                self.to_dict(child_node.identifier)
                for _, child_node in self.children(param_node.identifier)
            ]
            children_serialized = [c for c in children_serialized if c]
            if not children_serialized:
                continue
            is_empty = False
            if not param_node.MULTIPLE:
                d[param_key] = children_serialized[0]
                continue
            d[param_key] = children_serialized
        # a compound query clause can exist, without children clauses, in that case, just ignore it
        if is_empty:
            return None
        q = node.to_dict()
        q[node.KEY].update(d)
        return q

    @sub_insertion
    def query(
        self,
        type_or_query,
        insert_below=None,
        on=None,
        mode=ADD,
        compound_param=None,
        **body
    ):
        r"""Insert new clause(s) in current query.

        Inserted clause can accepts following syntaxes.

        Given an empty query:

        >>> from pandagg.query import Query
        >>> q = Query()

        flat syntax: clause type, followed by query clause body as keyword arguments:

        >>> q.query('term', some_field=23)
        {'term': {'some_field': 23}}

        using pandagg DSL:

        >>> from pandagg.query import Term
        >>> q.query(Term(field=23))
        {'term': {'some_field': 23}}

        >>> q.query({'bool': {'must': [{'term': {'some_field': 1}}]}})
        {'bool': {'must': [{'term': {'some_field': 1}}]}}

        >>> from pandagg.query import Bool
        >>> q.query(Bool(must=[{'term': {'some_field': 1}}], boost=1))

        :Keyword Arguments:
        %(insertion_doc)s

        """
        q = self.clone(with_nodes=True)
        node = self._translate_query(type_or_query, **body)
        q._insert_query_at(
            node,
            mode=mode,
            on=on,
            insert_below=insert_below,
            compound_param=compound_param,
        )
        return q

    # compound
    def bool(
        self,
        must=None,
        should=None,
        must_not=None,
        filter=None,
        insert_below=None,
        on=None,
        mode=ADD,
        **body
    ):
        """
        >>> Query().bool(must={"term": {"some_field": "yolo"}})
        """
        return self.query(
            "bool",
            must=must,
            should=should,
            must_not=must_not,
            filter=filter,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def boosting(
        self, positive=None, negative=None, insert_below=None, on=None, mode=ADD, **body
    ):
        if not positive and not negative:
            raise ValueError('Expect at least one of "positive", "negative"')
        return self.query(
            "boosting",
            positive=positive,
            negative=negative,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def constant_score(
        self, filter=None, boost=None, insert_below=None, on=None, mode=ADD, **body
    ):
        if not filter and not boost:
            raise ValueError('Expect at least one of "filter", "boost"')
        return self.query(
            "constant_score",
            filter=filter,
            boost=boost,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def dis_max(self, queries, insert_below=None, on=None, mode=ADD, **body):
        return self.query(
            "dis_max",
            queries=queries,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def function_score(self, query, insert_below=None, on=None, mode=ADD, **body):
        return self.query(
            "function_score",
            query=query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def nested(self, path, query=None, insert_below=None, on=None, mode=ADD, **body):
        return self.query(
            "nested",
            query=query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            path=path,
            **body
        )

    def has_child(self, query, insert_below=None, on=None, mode=ADD, **body):
        return self.query(
            "has_child",
            query=query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def has_parent(self, query, insert_below=None, on=None, mode=ADD, **body):
        return self.query(
            "has_parent",
            query=query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def script_score(self, query, insert_below=None, on=None, mode=ADD, **body):
        return self.query(
            "script_score",
            query=query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    def pinned_query(self, organic, insert_below=None, on=None, mode=ADD, **body):
        return self.query(
            "pinned_query",
            organic=organic,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )

    # compound parameters
    def _compound_param_insert(
        self,
        compound_key,
        compound_param_key,
        mode,
        type_or_query,
        insert_below=None,
        on=None,
        compound_body=None,
        **body
    ):
        q = self.clone(with_nodes=True)
        node = self._translate_query(type_or_query, **body)
        compound_body = compound_body or {}
        compound_body[compound_param_key] = node
        compound_node = self.get_node_dsl_class(compound_key)(**compound_body)
        q._insert_query_at(compound_node, on=on, insert_below=insert_below, mode=mode)
        return q

    def must(
        self,
        type_or_query,
        insert_below=None,
        on=None,
        mode=ADD,
        bool_body=None,
        **body
    ):
        """
        >>> Query().must('term', some_field=1)
        >>> Query().must({'term': {'some_field': 1}})
        """
        return self._compound_param_insert(
            "bool", "must", mode, type_or_query, insert_below, on, bool_body, **body
        )

    def should(
        self,
        type_or_query,
        insert_below=None,
        on=None,
        mode=ADD,
        bool_body=None,
        **body
    ):
        return self._compound_param_insert(
            "bool", "should", mode, type_or_query, insert_below, on, bool_body, **body
        )

    def must_not(
        self,
        type_or_query,
        insert_below=None,
        on=None,
        mode=ADD,
        bool_body=None,
        **body
    ):
        return self._compound_param_insert(
            "bool", "must_not", mode, type_or_query, insert_below, on, bool_body, **body
        )

    def filter(
        self,
        type_or_query,
        insert_below=None,
        on=None,
        mode=ADD,
        bool_body=None,
        **body
    ):
        return self._compound_param_insert(
            "bool", "filter", mode, type_or_query, insert_below, on, bool_body, **body
        )

    def show(self, *args, line_max_length=80, **kwargs):
        return "<Query>\n%s" % text(
            super(Tree, self).show(*args, line_max_length=line_max_length, **kwargs)
        )

    def __str__(self):
        return json.dumps(self.to_dict(), indent=2)
