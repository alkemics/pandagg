#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict

from pandagg.node.response.bucket import Bucket
from pandagg.node.agg.abstract import UniqueBucketAgg
from pandagg._tree import Tree


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
        """Recursive method returning a bucket properties in the form of an ordered dictionnary.
        Travel from current bucket to all parents until reaching root.
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

    def show(self, data_property='pretty', **kwargs):
        return super(ResponseTree, self).show(data_property=data_property)
