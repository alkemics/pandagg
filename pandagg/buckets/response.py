#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict, defaultdict

from six import iteritems
from pandagg.buckets.buckets import Bucket
from pandagg.nodes.abstract import UniqueBucketAgg
from pandagg.tree import Tree
from pandagg.utils import TreeBasedObj, bool_if_required


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


class Response(TreeBasedObj):

    _NODE_PATH_ATTR = 'attr_name'
    _COERCE_ATTR = True

    @classmethod
    def _build_filter(cls, nid_to_children, filters_per_nested_level, current_nested_path=None):
        """Recursive function to build bucket filters from highest to deepest nested conditions.
        """
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

    def _documents_query(self):
        """Build query filtering documents belonging to that bucket.
        Suppose the following configuration:

        Base                        <- filter on base
          |── Nested_A                 no filter on A (nested still must be applied for children)
          |     |── SubNested A1
          |     └── SubNested A2    <- filter on A2
          └── Nested_B              <- filter on B

        """
        current_bucket = self._tree[self._tree.root]
        agg_tree = self._initial_tree.agg_tree
        tree_mapping = self._initial_tree.agg_tree.tree_mapping

        bucket_properties = self._initial_tree.bucket_properties(current_bucket)
        agg_node_key_tuples = [
            (agg_tree[level], key) for
            level, key in iteritems(bucket_properties)
        ]

        filters_per_nested_level = defaultdict(list)

        for agg_node, key in agg_node_key_tuples:
            level_agg_filter = agg_node.get_filter(key)
            # remove unnecessary match_all filters
            if level_agg_filter is not None and 'match_all' not in level_agg_filter:
                current_nested = agg_tree.applied_nested_path_at_node(agg_node.identifier)
                filters_per_nested_level[current_nested].append(level_agg_filter)

        nested_with_conditions = [n for n in filters_per_nested_level.keys() if n]

        all_nesteds = [
            n.identifier
            for n in tree_mapping.filter_nodes(
                lambda x: (x.type == 'nested') and any((i in x.identifier or '' for i in nested_with_conditions))
            )
        ]

        nid_to_children = defaultdict(set)
        for nested in all_nesteds:
            nested_with_parents = list(tree_mapping.rsearch(nid=nested, filter=lambda x: x.type == 'nested'))
            nearest_nested_parent = next(iter(nested_with_parents[1:]), None)
            nid_to_children[nearest_nested_parent].add(nested)
        return self._build_filter(nid_to_children, filters_per_nested_level)

    def list_documents(self, **kwargs):
        """Return ES aggregation query to list documents belonging to given bucket."""
        return self._documents_query()


class ClientBoundResponse(Response):

    def __init__(self, client, index_name, tree, root_path=None, depth=None, initial_tree=None, query=None):
        self._client = client
        self._index_name = index_name
        self._query = query
        super(Response, self).__init__(tree=tree, root_path=root_path, depth=depth, initial_tree=initial_tree)

    def _clone(self, nid, root_path, depth):
        return ClientBoundResponse(
            client=self._client,
            index_name=self._index_name,
            tree=self._tree.subtree(nid),
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree,
            query=self._query
        )

    def list_documents(self, size=None, execute=True, _source=None, compact=True, **kwargs):
        """Return ES aggregation query to list documents belonging to given bucket.
        :param size: number of returned documents (ES default: 20)
        :param execute: if set to False, return aggregation query
        :param _source: list of desired documents attributes
        :param compact: provide more compact ES response
        :param kwargs: query arguments passed to aggregation body
        :return:
        """
        filter_query = self._documents_query()
        if self._query is not None:
            filter_query = bool_if_required([filter_query, self._query])
        if not execute:
            return filter_query
        body = {}
        if filter_query:
            body["query"] = filter_query
        if size is not None:
            body["size"] = size
        if _source is not None:
            body["_source"] = _source
        body.update(kwargs)
        response = self._client.search(index=self._index_name, body=body)['hits']
        if not compact:
            return response
        return {
            'total': response['total'],
            'hits': list(map(lambda x: x['_source'], response['hits']))
        }
