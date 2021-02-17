#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text
from six import text_type

import json

from future.utils import python_2_unicode_compatible

from pandagg.tree._tree import Tree
from pandagg.tree.mapping import _mapping

from pandagg.node.aggs.abstract import BucketAggNode, AggNode, Root
from pandagg.node.aggs.bucket import Nested, ReverseNested
from pandagg.node.aggs.pipeline import BucketSelector, BucketSort


@python_2_unicode_compatible
class Aggs(Tree):
    r"""
    Combination of aggregation clauses. This class provides handful methods to build an aggregation (see
    :func:`~pandagg.tree.aggs.Aggs.aggs` and :func:`~pandagg.tree.aggs.Aggs.groupby`), and is used as well
    to parse aggregations response in handy formats.

    Mapping declaration is optional, but doing so validates aggregation validity and automatically handles missing
    nested clauses.

    All following syntaxes are identical:

    From a dict:
    >>> Aggs({"per_user": {"terms": {"field": "user"}}})

    from an other Aggregation (simply a copy):
    >>> Aggs(Aggs({"per_user": {"terms": {"field": "user"}}}))

    :Keyword Arguments:
        * *mapping* (``dict`` or ``pandagg.tree.mapping.Mapping``) --
          Mapping of requested indice(s). Providing it will validate aggregations validity, and add required nested
          clauses if missing.

        * *nested_autocorrect* (``bool``) --
          In case of missing nested clauses in aggregation, if True, automatically add missing nested clauses, else
          raise error.

        * remaining kwargs:
          Used as body in aggregation
    """

    node_class = AggNode

    def __init__(
        self, aggs=None, mapping=None, nested_autocorrect=None, _groupby_ptr=None
    ):
        self.mapping = _mapping(mapping)
        self.nested_autocorrect = nested_autocorrect
        super(Aggs, self).__init__()

        # the root node of an aggregation is just the initial empty dict
        root_node = Root()
        self.insert_node(root_node)
        self._groupby_ptr = self.root if _groupby_ptr is None else _groupby_ptr

        if aggs is not None:
            self._insert_aggs(aggs, at_root=True)

    def __nonzero__(self):
        return bool(self.to_dict())

    __bool__ = __nonzero__

    def _insert_agg(self, name, node, insert_below=None, at_root=None, groupby=False):
        """
        Using flat declaration:
        >>> Aggs().agg("per_user", "terms", field="user")
        >>> Aggs().agg("per_user", {"terms": {"field": "user"}})

        Using DSL class:
        >>> from pandagg.aggs import Terms
        >>> Aggs().agg("per_user", Terms(field='user'))

        Agg node insertion, accepts following syntax:
        - name="per_user", type_or_agg="terms", field="user"
        - name="per_user", type_or_agg=Terms(field='user')
        - name="per_user", type_or_agg={"terms": {"field": "user"}}

        insert children aggs as well:
        - name="per_user", type_or_agg="terms", field="user", aggs={"avg_spent_time": {"avg": {"field": "spent_time"}}}
        - name="per_user", type_or_agg=Terms(field='user', aggs={"avg_spent_time": Avg(field="spent_time")})
        - name="per_user", type_or_agg={"field": "user", aggs: {"avg_spent_time": {"avg": {"field": "spent_time"}}}}
        """
        if insert_below and at_root:
            raise ValueError('Must define at most one of "insert_below" or "at_root".')
        if at_root:
            insert_below = self.root
        # based on last groupby pointer
        if insert_below is None:
            insert_below = self._groupby_ptr

        if not isinstance(name, text_type):
            raise ValueError('Agg "name" must be a str.')

        _children_aggs = node._children or {}

        if groupby:
            if _children_aggs:
                raise ValueError("Cannot group by multiple aggs at once.")
            subs = [self.drop_subtree(cid) for cid in self.children_ids(insert_below)]
            self.insert(node, key=name, parent_id=insert_below)
            for sub_key, sub_tree in subs:
                self.insert(sub_tree, key=sub_key, parent_id=node.identifier)
            # moving pointer when using groupby
            self._groupby_ptr = node.identifier
            return

        # in aggs mode, do not move pointer
        self.insert_node(node=node, key=name, parent_id=insert_below)
        for child_name, child in _children_aggs.items():
            child_node = self._translate_agg(child_name, child)
            self._insert_agg(
                name=child_name, node=child_node, insert_below=node.identifier
            )

    def _clone_init(self, deep=False):
        return Aggs(
            mapping=self.mapping.clone(deep=deep) if self.mapping is not None else None,
            nested_autocorrect=self.nested_autocorrect,
            _groupby_ptr=self._groupby_ptr,
        )

    def _is_eligible_grouping_node(self, nid):
        """Return whether node can be used as grouping node."""
        _, node = self.get(nid)
        if not isinstance(node, BucketAggNode):
            return False
        # special aggregations not returning anything
        if isinstance(node, (BucketSelector, BucketSort)):
            return False
        return True

    @property
    def deepest_linear_bucket_agg(self):
        """
        Return deepest bucket aggregation node (pandagg.nodes.abstract.BucketAggNode) of that aggregation that
        neither has siblings, nor has an ancestor with siblings.
        """
        if len(self._nodes_map) <= 1:
            return self.root
        last_bucket_agg_id = self.root
        children = [
            c
            for k, c in self.children(last_bucket_agg_id)
            if self._is_eligible_grouping_node(c.identifier)
        ]
        while len(children) == 1:
            last_agg_node = children[0]
            if not self._is_eligible_grouping_node(last_agg_node.identifier):
                break
            last_bucket_agg_id = last_agg_node.identifier
            children = [
                c
                for k, c in self.children(last_bucket_agg_id)
                if self._is_eligible_grouping_node(c.identifier)
            ]
        return last_bucket_agg_id

    def _validate_aggs_parent_id(self, pid):
        """
        If pid is not None, ensure that pid belongs to tree, and that it refers to a bucket aggregation.

        If pid not provided:

        - if previous groupby call, return deepest inserted node by last groupby call
        - else return deepest bucket aggregation if there is no ambiguity (linear aggregations).

        KO: non-ambiguous::
            A──> B──> C1
                 └──> C2
            raise error

        OK: non-ambiguous (linear)::

            A──> B──> C1
            return C1

        """
        if pid is not None:
            if not self._is_eligible_grouping_node(pid):
                raise ValueError("Node id <%s> is not a bucket aggregation." % pid)
            return pid
        if len(self._nodes_map) <= 1:
            return self.root

        # previous groupby calls defined grouping pointer
        if self._groupby_ptr is not None:
            return self._groupby_ptr

        eligible_nids = [
            n.identifier
            for _, n in self.list()
            if self._is_eligible_grouping_node(n.identifier)
        ]
        if len(eligible_nids) == 0:
            return self.root
        if len(eligible_nids) > 1:
            raise ValueError(
                "Declaration is ambiguous, you must declare the node id under which these "
                "aggregations should be placed."
            )
        return eligible_nids[0]

    def groupby(self, name, type_or_agg=None, insert_below=None, at_root=None, **body):
        r"""
        Arrange passed aggregations in vertical/nested manner, above or below another agg clause.

        Given the initial aggregation::

            A──> B
            └──> C

        If `insert_below` = 'A'::

            A──> new──> B
                  └──> C

        If `insert_above` = 'B'::

            A──> new──> B
            └──> C

        >>> Aggs()\
        >>> .groupby('per_user_id', 'terms', field='user_id')
        {"per_user_id":{"terms":{"field":"user_id"}}}

        >>> Aggs()\
        >>> .groupby('per_user_id', {'terms': {"field": "user_id"}})
        {"per_user_id":{"terms":{"field":"user_id"}}}

        >>> from pandagg.aggs import Terms
        >>> Aggs()\
        >>> .groupby('per_user_id', Terms(field="user_id"))
        {"per_user_id":{"terms":{"field":"user_id"}}}

        Accepted declarations for multiple aggregations:

        * *insert_below* (``string``) --
          Parent aggregation name under which these aggregations should be placed
        * *at_root* (``string``) --
          Insert aggregations at root of aggregation query

        * remaining kwargs:
          Used as body in aggregation

        :rtype: pandagg.aggs.Aggs
        """
        new_agg = self.clone(with_nodes=True)
        if insert_below is not None:
            insert_below = new_agg.id_from_key(insert_below)
        node = self._translate_agg(name, type_or_agg, **body)
        new_agg._insert_agg(
            name=name,
            node=node,
            insert_below=insert_below,
            at_root=at_root,
            groupby=True,
        )
        return new_agg

    def aggs(self, aggs, insert_below=None, at_root=False):
        r"""
        Arrange passed aggregations "horizontally".

        Given the initial aggregation::

            A──> B
            └──> C

        If passing multiple aggregations with `insert_below` = 'A'::

            A──> B
            └──> C
            └──> new1
            └──> new2

        Note: those will be placed under the `insert_below` aggregation clause id if provided, else under the deepest
        linear bucket aggregation if there is no ambiguity:

        OK::

            A──> B ─> C ─> new

        KO::

            A──> B
            └──> C

        `args` accepts single occurrence or sequence of following formats:

        * string (for terms agg concise declaration)
        * regular Elasticsearch dict syntax
        * AggNode instance (for instance Terms, Filters etc)


        :Keyword Arguments:
            * *insert_below* (``string``) --
              Parent aggregation name under which these aggregations should be placed
            * *at_root* (``string``) --
              Insert aggregations at root of aggregation query

            * remaining kwargs:
              Used as body in aggregation

        :rtype: pandagg.aggs.Aggs
        """
        new_agg = self.clone(with_nodes=True)
        if insert_below is not None:
            insert_below = new_agg.id_from_key(insert_below)
        new_agg._insert_aggs(aggs=aggs, insert_below=insert_below, at_root=at_root)
        return new_agg

    def _insert_aggs(self, aggs, insert_below=None, at_root=False):
        if at_root:
            insert_below = self.root
        elif not insert_below:
            # parent based on last groupby pointer
            if self._groupby_ptr:
                insert_below = self._groupby_ptr
            else:
                insert_below = self.deepest_linear_bucket_agg
        if isinstance(aggs, Aggs):
            self.merge(aggs, nid=insert_below)
            self._groupby_ptr = self.root
        elif isinstance(aggs, dict):
            for agg_name, agg_body in aggs.items():
                node = self._translate_agg(agg_name, agg_body)
                self._insert_agg(name=agg_name, node=node, insert_below=insert_below)
        elif aggs is not None:
            raise TypeError("Unsupported aggs type %s for Aggs" % type(aggs))

    @classmethod
    def _translate_agg(cls, name, type_or_agg=None, **body):
        """Accept multiple syntaxes, return a AggNode.
        :param type_or_agg:
        :param body:
        :return: AggNode
        """
        if isinstance(type_or_agg, text_type):
            # _translate_agg("per_user", "terms", field="user")
            return cls.get_node_dsl_class(type_or_agg)(**body)
        if isinstance(type_or_agg, AggNode):
            # _translate_agg("per_user", Terms(field='user'))
            if body:
                raise ValueError(
                    'Body cannot be added using "AggNode" declaration, got %s.' % body
                )
            return type_or_agg
        if isinstance(type_or_agg, dict):
            # _translate_agg("per_user", {"terms": {"field": "user"}})
            if body:
                raise ValueError(
                    'Body cannot be added using "dict" agg declaration, got %s.' % body
                )
            type_or_agg = type_or_agg.copy()
            children_aggs = (
                type_or_agg.pop("aggs", None)
                or type_or_agg.pop("aggregations", None)
                or {}
            )
            if len(type_or_agg) != 1:
                raise ValueError(
                    "Invalid aggregation declaration (two many keys): got <%s>"
                    % type_or_agg
                )
            type_, body_ = type_or_agg.popitem()
            body_ = body_.copy()
            if children_aggs:
                body_["aggs"] = children_aggs
            return cls.get_node_dsl_class(type_)(**body_)
        if type_or_agg is None:
            # if type_or_agg is not provided, by default execute a terms aggregation
            # _translate_agg("per_user")
            return cls.get_node_dsl_class("terms")(field=name, **body)
        raise ValueError('"type_or_agg" must be among "dict", "AggNode", "str"')

    def agg(self, name, type_or_agg=None, insert_below=None, at_root=False, **body):
        """

        :param name:
        :param type_or_agg:
        :param insert_below:
        :param at_root:
        :param body:
        :return:
        """
        new_agg = self.clone(with_nodes=True)
        if insert_below is not None:
            insert_below = new_agg.id_from_key(insert_below)
        node = self._translate_agg(name, type_or_agg, **body)
        new_agg._insert_agg(
            name=name, node=node, insert_below=insert_below, at_root=at_root
        )
        return new_agg

    def to_dict(self, from_=None, depth=None):
        if self.root is None:
            return None
        from_ = self.root if from_ is None else from_
        _, node = self.get(from_)
        children_queries = {}
        if depth is None or depth > 0:
            if depth is not None:
                depth -= 1
            for child_name, child_node in self.children(node.identifier):
                children_queries[child_name] = self.to_dict(
                    from_=child_node.identifier, depth=depth
                )
        if node.identifier == self.root:
            return children_queries
        node_query_dict = node.to_dict()
        if children_queries:
            node_query_dict["aggs"] = children_queries
        return node_query_dict

    def applied_nested_path_at_node(self, nid):
        # from current node to root
        for id_ in self.ancestors_ids(nid, include_current=True):
            _, node = self.get(id_)
            if isinstance(node, (Nested, ReverseNested)):
                return node.path
        return None

    def _insert_node_below(self, node, parent_id, key, by_path):
        """If mapping is provided, nested aggregations are automatically applied."""
        # if aggregation node is explicitely nested or reverse nested aggregation, do not override, but validate
        if (
            isinstance(node, Nested)
            or isinstance(node, ReverseNested)
            or not self.mapping
            or not hasattr(node, "field")
            or self.root is None
        ):
            return super(Aggs, self)._insert_node_below(node, parent_id, key, by_path)

        self.mapping.validate_agg_node(node)

        # from deepest to highest
        required_nested_level = self.mapping.nested_at_field(node.field)

        current_nested_level = self.applied_nested_path_at_node(parent_id)
        if current_nested_level == required_nested_level:
            return super(Aggs, self)._insert_node_below(node, parent_id, key, by_path)
        if not self.nested_autocorrect:
            raise ValueError(
                "Invalid %s agg on %s field. Invalid nested: expected %s, current %s."
                % (node.KEY, node.field, required_nested_level, current_nested_level)
            )
        if current_nested_level and (
            required_nested_level or "" in current_nested_level
        ):
            # requires reverse-nested
            # check if already exists in direct children, else create it
            child_reverse_nested = next(
                (
                    n
                    for k, n in self.children(parent_id)
                    if isinstance(n, ReverseNested) and n.path == required_nested_level
                ),
                None,
            )
            if child_reverse_nested:
                return super(Aggs, self)._insert_node_below(
                    node, child_reverse_nested.identifier, key, by_path
                )
            else:
                rv_node = ReverseNested()
                super(Aggs, self).insert_node(
                    rv_node,
                    parent_id=parent_id,
                    key="reverse_nested_below_%s" % self.get_key(parent_id),
                )
                return super(Aggs, self)._insert_node_below(
                    node, rv_node.identifier, key, by_path
                )

        # requires nested - apply all required nested fields
        for nested_lvl in reversed(self.mapping.list_nesteds_at_field(node.field)):
            if current_nested_level != nested_lvl:
                # check if already exists in direct children, else create it
                child_nested = next(
                    (
                        n
                        for k, n in (
                            self.children(parent_id) if parent_id is not None else []
                        )
                        if isinstance(n, Nested) and n.path == nested_lvl
                    ),
                    None,
                )
                if child_nested:
                    parent_id = child_nested.identifier
                    continue
                nested_node_name = (
                    "nested_below_root"
                    if parent_id is None
                    else "nested_below_%s" % self.get_key(parent_id)
                )
                nested_node = Nested(path=nested_lvl)
                super(Aggs, self)._insert_node_below(
                    nested_node, parent_id, nested_node_name, by_path
                )
                parent_id = nested_node.identifier
        super(Aggs, self)._insert_node_below(node, parent_id, key, by_path)

    def __str__(self):
        return json.dumps(self.to_dict(), indent=2)

    def show(self, *args, line_max_length=80, **kwargs):
        root_children = self.children(self.root)
        if len(root_children) == 0:
            return "<Aggregations> empty"
        if len(root_children) == 1:
            child_id = root_children[0][1].identifier
            return "<Aggregations>\n%s" % text(
                super(Tree, self).show(
                    child_id, *args, line_max_length=line_max_length, **kwargs
                )
            )

        return "<Aggregations>\n%s" % text(
            super(Tree, self).show(*args, line_max_length=line_max_length, **kwargs)
        )
