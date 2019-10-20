#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pandagg.buckets.buckets import Bucket
from pandagg.tree import Tree
from pandagg.utils import TreeBasedObj
from collections import OrderedDict


class ResponseTree(Tree):

    def __init__(self, agg_tree, identifier=None):
        super(ResponseTree, self).__init__(identifier=identifier)
        self.agg_tree = agg_tree

    def _get_instance(self, identifier, **kwargs):
        return ResponseTree(agg_tree=self.agg_tree, identifier=identifier)

    def parse_aggregation(self, raw_response):
        # init tree with fist node called 'aggs'
        agg_node = self.agg_tree[self.agg_tree.root]
        response_node = Bucket(
            aggregation_node=agg_node,
            value=raw_response,
            override_current_level='aggs',
            lvl=0,
            identifier='crafted_root'
        )
        self.add_node(response_node)
        self._parse_node_with_children(agg_node, response_node)
        return self

    def _parse_node_with_children(self, agg_node, parent_node, lvl=1):
        agg_value = parent_node.value.get(agg_node.agg_name)
        if agg_value:
            # if no data is present, elasticsearch doesn't return any bucket, for instance for TermAggregations
            for key, value in agg_node.extract_buckets(agg_value):
                bucket = Bucket(aggregation_node=agg_node, key=key, value=value, lvl=lvl + 1)
                self.add_node(bucket, parent_node.identifier)
                for child in self.agg_tree.children(agg_node.agg_name):
                    self._parse_node_with_children(agg_node=child, parent_node=bucket, lvl=lvl + 1)

    def bucket_properties(self, bucket, properties=None, end_level=None, depth=None):
        if properties is None:
            properties = OrderedDict()
        properties[bucket.current_level] = bucket.current_key
        if depth is not None:
            depth -= 1
        parent = self.parent(bucket.identifier)
        if bucket.current_level == end_level or depth == 0 or parent is None or parent.identifier == 'crafted_root':
            return properties
        return self.bucket_properties(parent, properties, end_level, depth)

    def show(self, data_property='pretty', **kwargs):
        return super(ResponseTree, self).show(data_property=data_property)

    def __repr__(self):
        self.show()
        return (u'<{class_}>\n{tree}'.format(class_=self.__class__.__name__, tree=self._reader)).encode('utf-8')


class Response(TreeBasedObj):

    _NODE_PATH_ATTR = 'path'

    def __call__(self, *args, **kwargs):
        initial_tree = self._tree if self._initial_tree is None else self._initial_tree
        root_bucket = self._tree[self._tree.root]
        return root_bucket.bind(tree=initial_tree)


class ClientBoundResponse(Response):

    def __init__(self, client, index_name, tree, root_path=None, depth=None, initial_tree=None):
        self._client = client
        self._index_name = index_name
        super(Response, self).__init__(tree=tree, root_path=root_path, depth=depth, initial_tree=initial_tree)

    def _get_instance(self, nid, root_path, depth, **kwargs):
        return ClientBoundResponse(
            client=self._client,
            index_name=self._index_name,
            tree=self._tree.subtree(nid),
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree
        )

    def __call__(self, *args, **kwargs):
        initial_tree = self._tree if self._initial_tree is None else self._initial_tree
        root_bucket = self._tree[self._tree.root]
        return root_bucket.bind(tree=initial_tree, client=self._client, index_name=self._index_name)
