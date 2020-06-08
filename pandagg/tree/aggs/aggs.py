#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import json

from future.utils import python_2_unicode_compatible, iteritems, string_types

from pandagg.tree._tree import Tree
from pandagg.tree.mapping import Mapping

from pandagg.node.aggs.abstract import (
    BucketAggNode,
    AggNode,
    ShadowRoot,
)
from pandagg.node.aggs.bucket import Nested, ReverseNested, Terms
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

    >>> Aggs({"per_user":{"terms":{"field":"user"}}})

    Using shortcut declaration: first argument is the aggregation type, other arguments are aggregation body parameters:

    >>> Aggs('terms', name='per_user', field='user')

    Using DSL class:

    >>> from pandagg.aggs import Terms
    >>> Aggs(Terms('per_user', field='user'))

    Dict and DSL class syntaxes allow to provide multiple clauses aggregations:

    >>> Aggs({"per_user":{"terms":{"field":"user"}, "aggs": {"avg_age": {"avg": {"field": "age"}}}}})

    With is similar to:

    >>> from pandagg.aggs import Terms, Avg
    >>> Aggs(Terms('per_user', field='user', aggs=Avg('avg_age', field='age')))

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
    _crafted_root_name = "root"

    def __init__(self, *args, **kwargs):
        self.mapping = Mapping(kwargs.pop("mapping", None))
        self.nested_autocorrect = kwargs.pop("nested_autocorrect", False)
        super(Aggs, self).__init__()
        if args or kwargs:
            self._fill(*args, **kwargs)

    def __nonzero__(self):
        return bool(self.to_dict())

    __bool__ = __nonzero__

    def _fill(self, *args, **kwargs):
        if not args or len(args) > 2:
            raise ValueError()
        if len(args) == 2:
            # Aggs("my_term_agg", "terms", field="some_field")
            agg_name, agg_type = args
            sub_aggs = kwargs.pop("aggs", None) or kwargs.pop("aggregations", None)
            agg_node = self.get_node_dsl_class(agg_type)(agg_name, **kwargs)
            self.insert_node(agg_node)
            if sub_aggs:
                self.insert(Aggs(sub_aggs), parent_id=agg_node.identifier)
            return

        arg = args[0]

        if isinstance(arg, string_types):
            # Aggs("classification_type") -> {"classification_type": {"terms": {"field": "classification_type"}}}
            self.insert(Terms(arg, field=arg, **kwargs))
            return

        if kwargs:
            raise ValueError("Unexpected kwargs %s" % kwargs)

        if isinstance(arg, dict):
            # Aggs({"my_term_agg": {"terms": {"field": "some_field"}}, "my_filters_agg": {"filters": {...}}})
            pid = None
            if len(arg.keys()) > 1:
                shadow_root = ShadowRoot()
                self.insert_node(shadow_root)
                pid = shadow_root.identifier
            for agg_name, v in iteritems(arg):
                v = v.copy()
                sub_aggs = v.pop("aggs", None) or v.pop("aggregations", None)
                if len(v.keys()) != 1:
                    raise ValueError("Invalid aggregation: %s" % v)
                agg_type, agg_body = v.copy().popitem()
                agg_node = self.get_node_dsl_class(agg_type)(agg_name, **agg_body)
                self.insert_node(agg_node, parent_id=pid)
                if sub_aggs:
                    self.insert(Aggs(sub_aggs), parent_id=agg_node.identifier)
            return

        # Aggs([Terms("my_term_agg", field="some_field"), Filters(...)])
        # Aggs(Terms("my_term_agg", field="some_field"))
        if not isinstance(arg, (tuple, list)):
            arg = (arg,)
        pid = None
        if len(arg) > 1:
            shadow_root = ShadowRoot()
            self.insert_node(shadow_root)
            pid = shadow_root.identifier
        for ar in arg:
            if not isinstance(ar, (AggNode, Aggs)):
                raise ValueError("Invalid type %s, %s" % (type(ar), ar))
            self.insert(ar, parent_id=pid)

    def _clone_init(self, deep=False):
        return Aggs(
            mapping=self.mapping.clone(deep=deep),
            nested_autocorrect=self.nested_autocorrect,
        )

    def _is_eligible_grouping_node(self, nid):
        """Return whether node can be used as grouping node."""
        node = self.get(nid)
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
            for c in self.children(last_bucket_agg_name, id_only=False)
            if self._is_eligible_grouping_node(c.identifier)
        ]
        while len(children) == 1:
            last_agg = children[0]
            if not self._is_eligible_grouping_node(last_agg.identifier):
                break
            last_bucket_agg_name = last_agg.name
            children = [
                c
                for c in self.children(last_bucket_agg_name, id_only=False)
                if self._is_eligible_grouping_node(c.identifier)
            ]
        return last_bucket_agg_name

    def _validate_aggs_parent_id(self, pid):
        """
        If pid is not None, ensure that pid belongs to tree, and that it refers to a bucket aggregation.

        Else, if not provided, return deepest bucket aggregation if there is no ambiguity (linear aggregations).
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
        leaves = self.leaves(id_only=False)
        # root
        if len(leaves) == 0:
            return None

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

        new_agg = self.clone(with_tree=True)
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
            if kwargs:
                raise ValueError(
                    "Kwargs not allowed when passing multiple aggregations in args."
                )
            inserted_aggs = [Aggs(arg) for arg in args[0]]
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

        # empty initial tree
        if insert_below is None:
            insert_below_subtrees = []
        else:
            insert_below_subtrees = [
                new_agg.drop_subtree(c.identifier)
                for c in new_agg.children(insert_below, id_only=False)
            ]
        for inserted_agg in inserted_aggs:
            new_agg.insert(inserted_agg, parent_id=insert_below)
            insert_below = inserted_agg.deepest_linear_bucket_agg
        for st in insert_below_subtrees:
            new_agg.insert_tree(parent_id=insert_below, new_tree=st)
        return new_agg

    def aggs(self, *args, **kwargs):
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
        insert_below = kwargs.pop("insert_below", None)
        at_root = kwargs.pop("at_root", None)
        new_agg = self.clone(with_tree=True)
        if at_root is None:
            insert_below = self._validate_aggs_parent_id(insert_below)
        else:
            if insert_below is not None:
                raise ValueError(
                    "Should provide at most one of 'at_root' and 'insert_below'."
                )
            if not new_agg.is_empty():
                existing_root = new_agg.get(new_agg.root)
                if isinstance(existing_root, ShadowRoot):
                    insert_below = existing_root.identifier
                else:
                    new_root = ShadowRoot()
                    new_agg._insert_node_above(new_root, existing_root.identifier)
                    insert_below = new_root.identifier

        deserialized = Aggs(*args, **kwargs)
        deserialized_root = deserialized.get(deserialized.root)
        if isinstance(deserialized_root, ShadowRoot):
            new_agg.merge(deserialized, nid=insert_below)
        else:
            new_agg.insert(deserialized, parent_id=insert_below)
        return new_agg

    def to_dict(self, from_=None, depth=None, with_name=True):
        if self.root is None:
            return {}
        from_ = self.root if from_ is None else from_
        node = self.get(from_)
        children_queries = {}
        if depth is None or depth > 0:
            if depth is not None:
                depth -= 1
            for child_node in self.children(node.name, id_only=False):
                children_queries[child_node.name] = self.to_dict(
                    from_=child_node.name, depth=depth, with_name=False
                )
        if isinstance(node, ShadowRoot):
            return children_queries
        else:
            node_query_dict = node.to_dict()
            if children_queries:
                node_query_dict["aggs"] = children_queries
        if with_name:
            return {node.name: node_query_dict}
        return node_query_dict

    def applied_nested_path_at_node(self, nid):
        # from current node to root
        for id_ in [nid] + self.ancestors(nid):
            node = self.get(id_)
            if isinstance(node, (Nested, ReverseNested)):
                return node.path
        return None

    def _insert_tree_below(self, new_tree, parent_id, deep):
        nroot = new_tree.get(new_tree.root)
        if isinstance(nroot, ShadowRoot) and not self.is_empty():
            return super(Aggs, self).merge(new_tree, nid=parent_id, deep=deep)
        return super(Aggs, self)._insert_tree_below(
            new_tree, parent_id=parent_id, deep=deep
        )

    def _insert_node_below(self, node, parent_id):
        """If mapping is provided, nested aggregations are automatically applied.
        """
        # if aggregation node is explicitely nested or reverse nested aggregation, do not override, but validate
        if (
            isinstance(node, Nested)
            or isinstance(node, ReverseNested)
            or not self.mapping
            or not hasattr(node, "field")
        ):
            return super(Aggs, self)._insert_node_below(node, parent_id)

        self.mapping.validate_agg_node(node)

        # from deepest to highest
        required_nested_level = self.mapping.nested_at_field(node.field)

        if self.is_empty():
            current_nested_level = None
        else:
            current_nested_level = self.applied_nested_path_at_node(parent_id)
        if current_nested_level == required_nested_level:
            return super(Aggs, self)._insert_node_below(node, parent_id)
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
                    for n in self.children(parent_id, id_only=False)
                    if isinstance(n, ReverseNested) and n.path == required_nested_level
                ),
                None,
            )
            if child_reverse_nested:
                return super(Aggs, self)._insert_node_below(
                    node, child_reverse_nested.identifier
                )
            else:
                rv_node = ReverseNested(name="reverse_nested_below_%s" % parent_id)
                super(Aggs, self).insert_node(rv_node, parent_id)
                return super(Aggs, self)._insert_node_below(node, rv_node.identifier)

        # requires nested - apply all required nested fields
        for nested_lvl in reversed(self.mapping.list_nesteds_at_field(node.field)):
            if current_nested_level != nested_lvl:
                # check if already exists in direct children, else create it
                child_nested = next(
                    (
                        n
                        for n in (
                            self.children(parent_id, id_only=False)
                            if parent_id is not None
                            else []
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
                nested_node = Nested(name=nested_node_name, path=nested_lvl)
                super(Aggs, self)._insert_node_below(nested_node, parent_id)
                parent_id = nested_node.identifier
        super(Aggs, self)._insert_node_below(node, parent_id)

    def __str__(self):
        return json.dumps(self.to_dict(), indent=2)


class AbstractLeafAgg(Aggs):
    KEY = None

    """Allow following syntax::

    >>> a = Avg("my_terms_agg", field="yolo")
    """

    def __init__(self, *args, **kwargs):
        mapping = kwargs.pop("mapping", None)
        nested_autocorrect = kwargs.pop("nested_autocorrect", None)
        super(AbstractLeafAgg, self).__init__(
            mapping=mapping, nested_autocorrect=nested_autocorrect
        )
        node = self.get_node_dsl_class(self.KEY)(*args, **kwargs)
        self.insert_node(node)


class AbstractParentAgg(Aggs):
    KEY = None

    """Allow following syntax::

    >>> a = Terms("my_terms_agg", field="yolo", aggs={...})
    """

    def __init__(self, *args, **kwargs):
        mapping = kwargs.pop("mapping", None)
        nested_autocorrect = kwargs.pop("nested_autocorrect", None)
        aggs = kwargs.pop("aggs", None)
        super(AbstractParentAgg, self).__init__(
            mapping=mapping, nested_autocorrect=nested_autocorrect
        )
        node = self.get_node_dsl_class(self.KEY)(*args, **kwargs)
        self.insert_node(node)

        if aggs:
            if isinstance(aggs, dict):
                self.insert(Aggs(aggs), parent_id=node.identifier)
                return
            if not isinstance(aggs, (list, tuple)):
                aggs = (aggs,)
            for agg in aggs:
                if not isinstance(agg, (AggNode, Aggs)):
                    raise ValueError("Unsupported type %s" % type(agg))
                self.insert(agg, node.identifier)
