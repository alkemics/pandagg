#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals


from builtins import str as text
from future.utils import python_2_unicode_compatible

from pandagg.tree._tree import Tree
from pandagg.tree.mapping import Mapping

from pandagg.node.aggs.abstract import (
    BucketAggNode,
    AggNode,
    ShadowRoot,
)
from pandagg.node.aggs.bucket import Nested, ReverseNested
from pandagg.node.aggs.pipeline import BucketSelector, BucketSort

# necessary to ensure all agg nodes are registered in meta class
import pandagg.node.aggs.metric as metric  # noqa


@python_2_unicode_compatible
class Aggs(Tree):
    """Tree combination of aggregation nodes.

    Mapping declaration is optional, but doing so validates aggregation validity.
    """

    node_class = AggNode
    _crafted_root_name = "root"

    def __init__(self, *args, **kwargs):
        self.tree_mapping = Mapping(kwargs.pop("mapping", None))
        super(Aggs, self).__init__()
        if args or kwargs:
            self._fill(*args, **kwargs)

    def __nonzero__(self):
        return bool(self.to_dict())

    __bool__ = __nonzero__

    @classmethod
    def deserialize(cls, *args, **kwargs):
        mapping = kwargs.pop("mapping", None)
        if len(args) == 1 and isinstance(args[0], Aggs):
            return args[0]

        new = cls(mapping=mapping)
        return new._fill(*args, **kwargs)

    def _fill(self, *args, **kwargs):
        if args:
            node_hierarchy = self.node_class._type_deserializer(*args, **kwargs)
        elif kwargs:
            node_hierarchy = self.node_class._type_deserializer(kwargs)
        else:
            return self
        self.insert(node_hierarchy)
        return self

    def _clone_init(self, deep=False):
        return Aggs(mapping=self.tree_mapping.clone(deep=deep))

    def _is_eligible_grouping_node(self, nid):
        node = self.get(nid)
        if not isinstance(node, BucketAggNode):
            return False
        # special aggregations not returning anything
        if isinstance(node, (BucketSelector, BucketSort)):
            return False
        return True

    @property
    def deepest_linear_bucket_agg(self):
        """Return deepest bucket aggregation node (pandagg.nodes.abstract.BucketAggNode) of that aggregation that
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
        """If pid is not None, ensure that pid belongs to tree, and that it refers to a bucket aggregation.

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
        # TODO
        if len(leaves) == 0:
            return None

        if len(leaves) > 1 or not isinstance(leaves[0], BucketAggNode):
            raise ValueError(
                "Declaration is ambiguous, you must declare the node id under which these "
                "aggregations should be placed."
            )
        return leaves[0].identifier

    def groupby(self, *args, **kwargs):
        """Arrange passed aggregations in `by` arguments "vertically" (nested manner), above or below another agg
        clause.

        Given the initial aggregation::

            A──> B
            └──> C

        If `insert_below` = 'A'::

            A──> by──> B
                  └──> C

        If `insert_above` = 'B'::

            A──> by──> B
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

        :param by: aggregation(s) clauses to insert "vertically"
        :param insert_below: parent aggregation id under which these aggregations should be placed
        :param insert_above: aggregation id above which these aggregations should be placed
        :param kwargs: agg body arguments when using "string" syntax for terms aggregation
        :rtype: pandagg.aggs.Aggs
        """
        insert_below = kwargs.pop("insert_below", None)
        insert_above = kwargs.pop("insert_above", None)
        if insert_below is not None and insert_above is not None:
            raise ValueError(
                'Must define at most one of "insert_above" and "insert_below", got both.'
            )

        new_agg = self.clone(with_tree=True)

        # groupby({}, {})
        if len(args) > 1:
            if kwargs:
                raise ValueError()
            inserted_aggs = [self.deserialize(arg) for arg in args]
        # groupby([{}, {}])
        elif len(args) == 1 and isinstance(args[0], (list, tuple)):
            if kwargs:
                raise ValueError()
            inserted_aggs = [self.deserialize(arg) for arg in args[0]]
        # groupby({})
        # groupby(Terms())
        # groupby('terms', name='per_tag', field='tag')
        else:
            inserted_aggs = [self.deserialize(*args, **kwargs)]

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
        """Arrange passed aggregations in `arg` arguments "horizontally".

        Those will be placed under the `insert_below` aggregation clause id if provided, else under the deepest linear
        bucket aggregation if there is no ambiguity:

        OK::

            A──> B ─> C ─> arg

        KO::

            A──> B
            └──> C

        `arg` argument accepts single occurrence or sequence of following formats:

        * string (for terms agg concise declaration)
        * regular Elasticsearch dict syntax
        * AggNode instance (for instance Terms, Filters etc)


        :param arg: aggregation(s) clauses to insert "horizontally"
        :param insert_below: parent aggregation id under which these aggregations should be placed
        :param kwargs: agg body arguments when using "string" syntax for terms aggregation
        :rtype: pandagg.aggs.Aggs
        """
        insert_below = self._validate_aggs_parent_id(kwargs.pop("insert_below", None))
        new_agg = self.clone(with_tree=True)
        deserialized = self.deserialize(*args, mapping=self.tree_mapping, **kwargs)
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

    def _insert_node_below(self, node, parent_id, with_children=True):
        """If mapping is provided, nested aggregations are automatically applied.
        """
        if isinstance(node, ShadowRoot):
            for child in node._children or []:
                super(Aggs, self)._insert_node_below(
                    child, parent_id=parent_id, with_children=with_children
                )
            return
        # if aggregation node is explicitely nested or reverse nested aggregation, do not override, but validate
        if (
            isinstance(node, Nested)
            or isinstance(node, ReverseNested)
            or not self.tree_mapping
            or parent_id is None
            or not hasattr(node, "field")
        ):
            return super(Aggs, self)._insert_node_below(
                node, parent_id, with_children=with_children
            )

        self.tree_mapping.validate_agg_node(node)

        # from deepest to highest
        required_nested_level = self.tree_mapping.nested_at_field(node.field)
        current_nested_level = self.applied_nested_path_at_node(parent_id)
        if current_nested_level == required_nested_level:
            return super(Aggs, self)._insert_node_below(
                node, parent_id, with_children=with_children
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
                    node, child_reverse_nested.identifier, with_children=with_children
                )
            else:
                rv_node = ReverseNested(name="reverse_nested_below_%s" % parent_id)
                super(Aggs, self).insert_node(rv_node, parent_id)
                return super(Aggs, self)._insert_node_below(
                    node, rv_node.identifier, with_children=with_children
                )

        # requires nested - apply all required nested fields
        for nested_lvl in reversed(self.tree_mapping.list_nesteds_at_field(node.field)):
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
        super(Aggs, self)._insert_node_below(
            node, parent_id, with_children=with_children
        )

    def validate_tree(self, exc=False):
        """Validate tree definition against defined mapping.
        :param exc: if set to True, will raise exception if tree is invalid
        :return: boolean
        """
        if self.tree_mapping is None:
            return True
        for agg_node in self.list():
            # path for 'nested'/'reverse-nested', field for metric aggregations
            if not self.tree_mapping.validate_agg_node(agg_node, exc=exc):
                return False
        return True

    def __str__(self):
        return "<Aggregation>\n%s" % text(self.show())
