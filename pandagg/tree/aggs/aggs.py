#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from builtins import str as text
from six import text_type

import json

from future.utils import python_2_unicode_compatible, string_types

from pandagg.tree._tree import Tree
from pandagg.tree.mapping import Mapping

from pandagg.node.aggs.abstract import BucketAggNode, AggNode
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

    def __init__(self, aggs=None, mapping=None, nested_autocorrect=None):
        self.mapping = Mapping(mapping)
        self.nested_autocorrect = nested_autocorrect
        super(Aggs, self).__init__()

        if aggs is None:
            return

        # the root node of an aggregation is just the initial empty dict
        self._insert_root_node_if_needed()

        if isinstance(aggs, Aggs):
            self.merge(aggs, nid=self.root)
            return
        if isinstance(aggs, dict):
            for agg_name, agg_body in aggs.items():
                self._insert_agg(
                    name=agg_name,
                    type_or_agg=agg_body,
                    parent_id=self.root,
                    _with_children=True,
                )

    def _insert_root_node_if_needed(self):
        # the root node of an aggregation is just the initial empty dict
        if self.root:
            return
        root_node = AggNode()
        self.insert_node(root_node)

    def _insert_agg(
        self, name, type_or_agg, parent_id=None, _with_children=False, **body
    ):
        """
        Using flat declaration:
        >>> Aggs("per_user", "terms", field="user")

        Using DSL class:
        >>> from pandagg.aggs import Terms
        >>> Aggs("per_user", Terms(field='user'))

        Agg node insertion, accepts following syntax:
        - name="per_user", type_or_agg="terms", field="user"
        - name="per_user", type_or_agg=Terms(field='user')
        - name="per_user", type_or_agg={"field": "user"}

        if _with_children=True, insert children aggs as well:
        - name="per_user", type_or_agg="terms", field="user", aggs={"avg_spent_time": {"avg": {"field": "spent_time"}}}
        - name="per_user", type_or_agg=Terms(field='user', aggs={"avg_spent_time": Avg(field="spent_time")})
        - name="per_user", type_or_agg={"field": "user", aggs: {"avg_spent_time": {"avg": {"field": "spent_time"}}}}
        """
        self._insert_root_node_if_needed()
        if parent_id is None:
            parent_id = self.root
        if not type_or_agg:
            if not name:
                raise ValueError(
                    "Invalid declaration: name must be provided in case of agg insertion."
                )
            return

        if not isinstance(name, text_type):
            raise ValueError('Agg "name" must be a str.')

        if isinstance(type_or_agg, text_type):
            # Aggs("per_user", "terms", field="user")
            _children_aggs = body.pop("aggs", None) or body.pop("aggregations", None)
            node = self.get_node_dsl_class(type_or_agg)(**body)
        elif isinstance(type_or_agg, AggNode):
            # Aggs("per_user", Terms(field='user'))
            node = type_or_agg
            if body:
                raise ValueError(
                    'Body cannot be added using "AggNode" declaration, got %s.' % body
                )
            _children_aggs = node._children
        elif isinstance(type_or_agg, dict):
            if len(type_or_agg) != 1:
                raise ValueError(
                    "Invalid aggregation declaration: got <%s>" % type_or_agg
                )
            if body:
                raise ValueError(
                    'Body cannot be added using "dict" agg declaration, got %s.' % body
                )
            type_or_agg = type_or_agg.copy()
            type_, body_ = type_or_agg.copy().popitem()
            _children_aggs = body_.pop("aggs", None) or body_.pop("aggregations", None)
            node = self.get_node_dsl_class(type_)(**body_)
        else:
            raise ValueError('"type_or_agg" must be among "dict", "AggNode", "str"')

        self.insert_node(node=node, key=name, parent_id=parent_id)
        if _with_children:
            for child_name, child_node in _children_aggs.items():
                self._insert_agg(
                    name=child_name,
                    type_or_agg=child_node,
                    parent_id=node.identifier,
                    _with_children=True,
                )

    def __nonzero__(self):
        return len(self.list()) > 1

    __bool__ = __nonzero__

    def _clone_init(self, deep=False):
        return Aggs(
            mapping=self.mapping.clone(deep=deep),
            nested_autocorrect=self.nested_autocorrect,
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
        if not self.root or not self._is_eligible_grouping_node(self.root):
            return None
        last_bucket_agg_name = self.root
        children = [
            c
            for k, c in self.children(last_bucket_agg_name)
            if self._is_eligible_grouping_node(c.identifier)
        ]
        while len(children) == 1:
            last_agg = children[0]
            if not self._is_eligible_grouping_node(last_agg.identifier):
                break
            last_bucket_agg_name = last_agg.name
            children = [
                c
                for k, c in self.children(last_bucket_agg_name)
                if self._is_eligible_grouping_node(c.identifier)
            ]
        return last_bucket_agg_name

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
        leaves = self.leaves()
        # root
        if len(leaves) == 0:
            return None

        # previous groupby calls defined grouping pointer
        if self._groupby_ptr is not None:
            return self._groupby_ptr

        if len(leaves) > 1 or not isinstance(leaves[0], BucketAggNode):
            raise ValueError(
                "Declaration is ambiguous, you must declare the node id under which these "
                "aggregations should be placed."
            )
        return leaves[0].identifier

    def groupby(self, *args, **kwargs):
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

        `by` argument accepts single occurrence or sequence of following formats:

        * string (for terms agg concise declaration)
        * regular Elasticsearch dict syntax
        * AggNode instance (for instance Terms, Filters etc)

        If `insert_below` nor `insert_above` is provided by will be placed between the the deepest linear
        bucket aggregation if there is no ambiguity, and its children::

            A──> B      : OK generates     A──> B ─> C ─> by

            A──> B      : KO, ambiguous, must precise either A, B or C
            └──> C


        Accepted all Aggs.__init__ syntaxes

        >>> Aggs()\
        >>> .groupby('terms', name='per_user_id', field='user_id')
        {"terms_on_my_field":{"terms":{"field":"some_field"}}}

        Passing a dict:

        >>> Aggs().groupby({"terms_on_my_field":{"terms":{"field":"some_field"}}})
        {"terms_on_my_field":{"terms":{"field":"some_field"}}}


        Using DSL class:

        >>> from pandagg.aggs import Terms
        >>> Aggs().groupby(Terms('terms_on_my_field', field='some_field'))
        {"terms_on_my_field":{"terms":{"field":"some_field"}}}

        Shortcut syntax for terms aggregation: creates a terms aggregation, using field as aggregation name

        >>> Aggs().groupby('some_field')
        {"some_field":{"terms":{"field":"some_field"}}}

        Using a Aggs object:

        >>> Aggs().groupby(Aggs('per_user_id', 'terms', field='user_id'))
        {"terms_on_my_field":{"terms":{"field":"some_field"}}}

        Accepted declarations for multiple aggregations:


        :Keyword Arguments:
            * *insert_below* (``string``) --
              Parent aggregation name under which these aggregations should be placed
            * *insert_above* (``string``) --
              Aggregation name above which these aggregations should be placed
            * *at_root* (``string``) --
              Insert aggregations at root of aggregation query

            * remaining kwargs:
              Used as body in aggregation

        :rtype: pandagg.aggs.Aggs
        """
        insert_below = kwargs.pop("insert_below", None)
        insert_above = kwargs.pop("insert_above", None)
        at_root = kwargs.pop("at_root", None)
        if (
            sum(
                (
                    insert_below is not None,
                    insert_above is not None,
                    at_root is not None,
                )
            )
            > 1
        ):
            raise ValueError(
                'Must define at most one of "insert_above" or "insert_below" or "at_root".'
            )

        new_agg = self.clone()
        if at_root is not None:
            if not new_agg.is_empty():
                existing_root = new_agg.get(new_agg.root)
                if isinstance(existing_root, ShadowRoot):
                    insert_below = existing_root.identifier
                else:
                    insert_above = existing_root.identifier

        # groupby({}, {}), but not groupby("some_field", "terms", field="yolo")
        if len(args) > 2 or (len(args) > 1 and not isinstance(args[0], string_types)):
            if kwargs:
                raise ValueError(
                    "Kwargs not allowed when passing multiple aggregations in args."
                )
            inserted_aggs = [Aggs(arg) for arg in args]
        # groupby([{}, {}])
        elif len(args) == 1 and isinstance(args[0], (list, tuple)):
            # kwargs applied on all
            inserted_aggs = [Aggs(arg, **kwargs) for arg in args[0]]
        # groupby({})
        # groupby(Terms())
        # groupby('terms', name='per_tag', field='tag')
        else:
            inserted_aggs = [Aggs(*args, **kwargs)]

        if insert_above is not None:
            parent = new_agg.parent(insert_above, id_only=False)
            # None if insert_above was root
            insert_below = parent.identifier if parent is not None else None
            insert_above_subtree = new_agg.drop_subtree(insert_above)
            # if isinstance(by, (list, tuple)):
            for inserted_agg in inserted_aggs:
                new_agg.insert(inserted_agg, parent_id=insert_below)
                insert_below = inserted_agg.deepest_linear_bucket_agg
            new_agg.insert_tree(parent_id=insert_below, new_tree=insert_above_subtree)
            return new_agg

        insert_below = self._validate_aggs_parent_id(insert_below)

        if insert_below is None:
            # empty initial tree
            insert_below_subtrees = []
        else:
            # elements above which the inserted clauses are placed
            insert_below_subtrees = [
                new_agg.drop_subtree(c.identifier)
                for c in new_agg.children(insert_below, id_only=False)
            ]
        for inserted_agg in inserted_aggs:
            new_agg.insert(inserted_agg, parent_id=insert_below)
            insert_below = inserted_agg.deepest_linear_bucket_agg
            if not insert_below_subtrees:
                new_agg._groupby_ptr = insert_below
        for st in insert_below_subtrees:
            new_agg.insert_tree(parent_id=insert_below, new_tree=st)
        return new_agg

    def aggs(self, name, type_or_agg, insert_below=None, at_root=False, **body):
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
        if at_root and insert_below is not None:
            raise ValueError(
                "Should provide at most one of 'at_root' and 'insert_below'."
            )
        if at_root:
            insert_below = self.deepest_linear_bucket_agg
        else:
            insert_below = self._validate_aggs_parent_id(insert_below)
        new_agg._insert_agg(
            name=name,
            type_or_agg=type_or_agg,
            parent_id=insert_below,
            _with_children=True,
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
        for id_ in [nid] + self.ancestors(nid):
            node = self.get(id_)
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
        ):
            return super(Aggs, self)._insert_node_below(node, parent_id, key, by_path)

        self.mapping.validate_agg_node(node)

        # from deepest to highest
        required_nested_level = self.mapping.nested_at_field(node.field)

        if self.is_empty():
            current_nested_level = None
        else:
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
                rv_node = ReverseNested(name="reverse_nested_below_%s" % parent_id)
                super(Aggs, self).insert_node(rv_node, parent_id)
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
                    else "nested_below_%s" % parent_id
                )
                nested_node = Nested(path=nested_lvl)
                super(Aggs, self)._insert_node_below(
                    nested_node, parent_id, nested_node_name, by_path
                )
                parent_id = nested_node.identifier
        super(Aggs, self)._insert_node_below(node, parent_id, key, by_path)

    def __str__(self):
        return json.dumps(self.to_dict(), indent=2)

    def show(self, *args, **kwargs):
        stdout = kwargs.pop("stdout", True)

        result = "<Aggregations>\n%s" % text(super(Tree, self).show(*args, **kwargs))
        if not stdout:
            return result
        print(result)
