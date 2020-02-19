#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import defaultdict

from six import iteritems

from pandagg.tree.query import Query
from pandagg.utils import bool_if_required
from pandagg.interactive.abstract import TreeBasedObj


class IResponse(TreeBasedObj):

    """Interactive aggregation response.
    """

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
                lambda x: (x.KEY == 'nested') and any((i in x.identifier or '' for i in nested_with_conditions))
            )
        ]

        nid_to_children = defaultdict(set)
        for nested in all_nesteds:
            nested_with_parents = list(tree_mapping.rsearch(nid=nested, filter=lambda x: x.KEY == 'nested'))
            nearest_nested_parent = next(iter(nested_with_parents[1:]), None)
            nid_to_children[nearest_nested_parent].add(nested)
        return self._build_filter(nid_to_children, filters_per_nested_level)

    def list_documents(self, **kwargs):
        """Return ES aggregation query to list documents belonging to given bucket."""
        return self._documents_query()


class ClientBoundResponse(IResponse):

    def __init__(self, client, index_name, tree, root_path=None, depth=None, initial_tree=None, query=None):
        self._client = client
        self._index_name = index_name
        self._query = Query(query)
        super(IResponse, self).__init__(tree=tree, root_path=root_path, depth=depth, initial_tree=initial_tree)

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
        filter_query = self._query.query(self._documents_query())
        if not execute:
            return filter_query
        body = {}
        if filter_query:
            body["query"] = filter_query.query_dict()
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
