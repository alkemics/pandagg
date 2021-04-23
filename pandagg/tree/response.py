#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict, defaultdict

from pandagg.node.query.joining import Nested
from pandagg.tree._tree import Tree

from pandagg.node.response.bucket import Bucket, BucketNode
from pandagg.tree.query import Query


class AggsResponseTree(Tree):
    """
    Tree shaped representation of an ElasticSearch aggregations response.
    """

    node_class = BucketNode

    def __init__(self, aggs, raw_response=None):
        """
        :param aggs: instance of pandagg.agg.Aggs from which this Elasticsearch response originates.
        """
        super(AggsResponseTree, self).__init__()
        self.__aggs = aggs
        root_node = BucketNode()
        self.insert_node(root_node)
        if raw_response:
            self.parse(raw_response)

    def parse(self, raw_response):
        """
        Build response tree from ElasticSearch aggregation response

        :param raw_response: ElasticSearch aggregation response
        :return: self
        """
        _, agg_root_node = self.__aggs.get(self.__aggs.root)
        for child_name, child in self.__aggs.children(agg_root_node.identifier):
            self._parse_node_with_children(
                child_name, agg_node=child, raw_response=raw_response, pid=self.root
            )
        return self

    def bucket_properties(self, bucket, properties=None, end_level=None, depth=None):
        """
        Recursive method returning a given bucket's properties in the form of an ordered dictionnary.
        Travel from current bucket through all ancestors until reaching root.

        :param bucket: instance of pandagg.buckets.buckets.Bucket
        :param properties: OrderedDict accumulator of 'level' -> 'key'
        :param end_level: optional parameter to specify until which level properties are fetched
        :param depth: optional parameter to specify a limit number of levels which are fetched
        :return: OrderedDict of structure 'level' -> 'key'
        """
        if properties is None:
            properties = OrderedDict()
        if bucket.level is not None:
            properties[bucket.level] = bucket.key
        if depth is not None:
            depth -= 1
        _, parent = self.parent(bucket.identifier)
        if bucket.level == end_level or depth == 0 or parent is None:
            return properties
        return self.bucket_properties(parent, properties, end_level, depth)

    def get_bucket_filter(self, nid):
        """
        Build query filtering documents belonging to that bucket.
        Suppose the following configuration::

            Base                        <- filter on base
              |── Nested_A                 no filter on A (nested still must be applied for children)
              |     |── SubNested A1
              |     └── SubNested A2    <- filter on A2
              └── Nested_B              <- filter on B

        """
        tree_mapping = self.__aggs.mappings

        b_key, selected_bucket = self.get(nid)
        bucket_properties = self.bucket_properties(selected_bucket)
        agg_node_key_tuples = [
            (self.__aggs.get(self.__aggs.id_from_key(level))[1], key)
            for level, key in bucket_properties.items()
        ]

        filters_per_nested_level = defaultdict(list)

        for agg_node, key in agg_node_key_tuples:
            level_agg_filter = agg_node.get_filter(key)
            # remove unnecessary match_all filters
            if level_agg_filter is not None and "match_all" not in level_agg_filter:
                current_nested = self.__aggs.applied_nested_path_at_node(
                    agg_node.identifier
                )
                filters_per_nested_level[current_nested].append(level_agg_filter)

        nested_with_conditions = [n for n in filters_per_nested_level.keys() if n]

        all_nesteds = [
            n.identifier
            for n in tree_mapping.list(
                filter_=lambda x: (x.KEY == "nested")
                and any((i in x.identifier or "" for i in nested_with_conditions))
            )
        ]

        nid_to_children = defaultdict(set)
        for nested in all_nesteds:
            nested_with_parents = [
                n
                for n in tree_mapping.ancestors(nid=nested, id_only=False)
                if n.KEY == "nested"
            ]
            nearest_nested_parent = next(iter(nested_with_parents[1:]), None)
            nid_to_children[nearest_nested_parent].add(nested)
        return self._build_filter(nid_to_children, filters_per_nested_level).to_dict()

    def show(self, **kwargs):
        kwargs["key"] = kwargs.get("key", lambda x: x.line_repr(depth=0))
        return super(AggsResponseTree, self).show(**kwargs)

    def _clone_init(self, deep=False):
        return AggsResponseTree(aggs=self.__aggs.clone(deep=deep))

    def _parse_node_with_children(self, agg_name, agg_node, raw_response, pid):
        """
        Recursive method to parse ES raw response.

        :param agg_node: current aggregation, pandagg.nodes.AggNode instance
        :param raw_response: ES response at current level, dict
        :param pid: parent node identifier
        """
        agg_raw_response = raw_response.get(agg_name)
        for key, raw_value in agg_node.extract_buckets(agg_raw_response):
            bucket = Bucket(
                level=agg_name, key=key, value=agg_node.extract_bucket_value(raw_value)
            )
            self.insert_node(bucket, parent_id=pid)
            for child_name, child in self.__aggs.children(agg_node.identifier):
                self._parse_node_with_children(
                    child_name,
                    agg_node=child,
                    raw_response=raw_value,
                    pid=bucket.identifier,
                )

    @classmethod
    def _build_filter(
        cls, nid_to_children, filters_per_nested_level, current_nested_path=None
    ):
        """
        Recursive function to build bucket filters from highest to deepest nested conditions."""
        current_conditions = filters_per_nested_level.get(current_nested_path, [])
        nested_children = nid_to_children[current_nested_path]
        for nested_child in nested_children:
            nested_child_conditions = cls._build_filter(
                nid_to_children=nid_to_children,
                filters_per_nested_level=filters_per_nested_level,
                current_nested_path=nested_child,
            )
            if nested_child_conditions:
                current_conditions.append(
                    Nested(path=nested_child, query=nested_child_conditions)
                )
        q = Query()
        for clause in current_conditions:
            q = q.query(clause)
        return q
