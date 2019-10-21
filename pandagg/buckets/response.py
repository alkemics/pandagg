#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict, defaultdict

from pandagg.buckets.buckets import Bucket
from pandagg.tree import Tree
from pandagg.utils import TreeBasedObj, bool_if_required


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

    def list_documents(self, **kwargs):
        initial_tree = self._tree if self._initial_tree is None else self._initial_tree
        return initial_tree.list_documents()


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

        Note: on Filter and Filters query, if some filter condition is nested, it might require some propagation in
        children conditions on that same nested. This feature is not implemented yet. The main difficulty being to
        disambiguate if OR or AND operator must be applied on conditions of same nested level in some cases.
        """
        current_bucket = self._tree[self._tree.root]
        agg_tree = self._initial_tree.agg_tree
        tree_mapping = self._initial_tree.agg_tree.tree_mapping

        aggs_keys = [
            (agg_tree[level], key) for
            level, key in self._tree.bucket_properties(current_bucket).items()
        ]

        filters_per_nested_level = defaultdict(list)

        for level_agg, key in aggs_keys:
            level_agg_filter = level_agg.get_filter(key)
            # remove unnecessary match_all filters
            if level_agg_filter is not None and 'match_all' not in level_agg_filter:
                current_nested = agg_tree.applied_nested_path_at_node(level_agg.identifier)
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

    def list_documents(self, size=None, execute=True, _source=None, **kwargs):
        filter_query = self._documents_query()
        if not execute:
            return filter_query
        body = {"query": filter_query}
        if size is not None:
            body["size"] = size
        if _source is not None:
            body["_source"] = _source
        body.update(kwargs)
        return self._client.search(index=self._index_name, body=body)['hits']
