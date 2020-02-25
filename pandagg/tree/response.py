#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict, defaultdict

from six import iteritems

from pandagg.node.response.bucket import Bucket
from pandagg.node.agg.abstract import UniqueBucketAgg
from pandagg.tree._tree import Tree
from pandagg.utils import bool_if_required


class ResponseTree(Tree):
    """Tree representation of an ES response. ES response format is determined by the aggregation query.
    """

    def __init__(self, agg_tree, identifier=None):
        """
        :param agg_tree: instance of pandagg.agg.Agg from which this ES response originates
        :param identifier: optional, tree identifier
        """
        super(ResponseTree, self).__init__(identifier=identifier)
        self.agg_tree = agg_tree

    def _clone(self, identifier, with_tree=False, deep=False):
        return ResponseTree(
            agg_tree=self.agg_tree,
            identifier=identifier
        )

    def parse_aggregation(self, raw_response):
        """Build response tree from ES response
        :param raw_response: ES aggregation response
        :return: self

        Note: if the root aggregation node can generate multiple buckets, a response root is crafted to avoid having
        multiple roots.
        """
        root_node = self.agg_tree[self.agg_tree.root]
        if not isinstance(root_node, UniqueBucketAgg):
            bucket = Bucket(value=None, depth=0)
            self.add_node(bucket, None)
            self._parse_node_with_children(root_node, raw_response, pid=bucket.identifier)
        else:
            self._parse_node_with_children(root_node, raw_response)
        return self

    def _parse_node_with_children(self, agg_node, raw_response, pid=None, depth=0):
        """Recursive method to parse ES raw response.
        :param agg_node: current aggregation, pandagg.nodes.AggNode instance
        :param raw_response: ES response at current level, dict
        :param pid: parent node identifier
        :param depth: depth in tree
        """
        agg_raw_response = raw_response.get(agg_node.name)
        for key, raw_value in agg_node.extract_buckets(agg_raw_response):
            bucket = Bucket(
                level=agg_node.name,
                key=key,
                value=agg_node.extract_bucket_value(raw_value),
                depth=depth + 1
            )
            self.add_node(bucket, pid)
            for child in self.agg_tree.children(agg_node.name):
                self._parse_node_with_children(
                    agg_node=child,
                    raw_response=raw_value,
                    depth=depth + 1,
                    pid=bucket.identifier
                )

    def bucket_properties(self, bucket, properties=None, end_level=None, depth=None):
        """Recursive method returning a given bucket's properties in the form of an ordered dictionnary.
        Travel from current bucket through all ancestors until reaching root.
        :param bucket: instance of pandagg.buckets.buckets.Bucket
        :param properties: OrderedDict accumulator of 'level' -> 'key'
        :param end_level: optional parameter to specify until which level properties are fetched
        :param depth: optional parameter to specify a limit number of levels which are fetched
        :return: OrderedDict of structure 'level' -> 'key'
        """
        if properties is None:
            properties = OrderedDict()
        if bucket.level != Bucket.ROOT_NAME:
            properties[bucket.level] = bucket.key
        if depth is not None:
            depth -= 1
        parent = self.parent(bucket.identifier)
        if bucket.level == end_level or depth == 0 or parent is None:
            return properties
        return self.bucket_properties(parent, properties, end_level, depth)

    @classmethod
    def _build_filter(cls, nid_to_children, filters_per_nested_level, current_nested_path=None):
        """Recursive function to build bucket filters from highest to deepest nested conditions.
        """
        # TODO - use Query DSL
        current_conditions = filters_per_nested_level.get(current_nested_path, [])
        nested_children = nid_to_children[current_nested_path]
        for nested_child in nested_children:
            nested_child_conditions = cls._build_filter(
                nid_to_children=nid_to_children,
                filters_per_nested_level=filters_per_nested_level,
                current_nested_path=nested_child
            )
            if nested_child_conditions:
                current_conditions.append({'nested': {'path': nested_child, 'query': nested_child_conditions}})
        return bool_if_required(current_conditions)

    def get_bucket_filter(self, nid):
        """Build query filtering documents belonging to that bucket.
        Suppose the following configuration:

        Base                        <- filter on base
          |── Nested_A                 no filter on A (nested still must be applied for children)
          |     |── SubNested A1
          |     └── SubNested A2    <- filter on A2
          └── Nested_B              <- filter on B

        """
        tree_mapping = self.agg_tree.tree_mapping

        selected_bucket = self[nid]
        bucket_properties = self.bucket_properties(selected_bucket)
        agg_node_key_tuples = [
            (self.agg_tree[level], key) for
            level, key in iteritems(bucket_properties)
        ]

        filters_per_nested_level = defaultdict(list)

        for agg_node, key in agg_node_key_tuples:
            level_agg_filter = agg_node.get_filter(key)
            # remove unnecessary match_all filters
            if level_agg_filter is not None and 'match_all' not in level_agg_filter:
                current_nested = self.agg_tree.applied_nested_path_at_node(agg_node.identifier)
                filters_per_nested_level[current_nested].append(level_agg_filter)

        nested_with_conditions = [n for n in filters_per_nested_level.keys() if n]

        all_nesteds = [
            n.identifier
            for n in tree_mapping.filter_nodes(
                lambda x: (x.KEY == 'nested') and any((i in x.identifier or '' for i in nested_with_conditions))
            )
        ]

        nid_to_children = defaultdict(set)
        for nested in all_nesteds:
            nested_with_parents = list(tree_mapping.rsearch(nid=nested, filter=lambda x: x.KEY == 'nested'))
            nearest_nested_parent = next(iter(nested_with_parents[1:]), None)
            nid_to_children[nearest_nested_parent].add(nested)
        return self._build_filter(nid_to_children, filters_per_nested_level)

    def show(self, data_property='pretty', **kwargs):
        return super(ResponseTree, self).show(data_property=data_property)
