#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.tree.query import Query
from pandagg.interactive.abstract import TreeBasedObj


class IResponse(TreeBasedObj):

    """Interactive aggregation response.
    """

    _NODE_PATH_ATTR = 'attr_name'
    _COERCE_ATTR = True

    def __init__(self, tree, client=None, index_name=None, root_path=None, depth=None, initial_tree=None, query=None):
        self._client = client
        self._index_name = index_name
        self._query = Query(query)
        super(IResponse, self).__init__(tree=tree, root_path=root_path, depth=depth, initial_tree=initial_tree)

    def _clone(self, nid, root_path, depth):
        return IResponse(
            client=self._client,
            index_name=self._index_name,
            tree=self._tree.subtree(nid),
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree,
            query=self._query
        )

    def get_bucket_filter(self):
        """Build filters to select documents belonging to that bucket"""
        return self._initial_tree.get_bucket_filter(self._tree.root)

    def list_documents(self, size=None, execute=True, _source=None, compact=False, **kwargs):
        """Return ES aggregation query to list documents belonging to given bucket.
        :param size: number of returned documents (ES default: 20)
        :param execute: if set to False, return aggregation query
        :param _source: list of desired documents attributes
        :param compact: provide more compact ES response
        :param kwargs: query arguments passed to aggregation body
        :return:
        """
        filter_query = self._query.query(self.get_bucket_filter())
        if not execute:
            return filter_query.query_dict()
        body = {}
        if filter_query:
            body["query"] = filter_query.query_dict()
        if size is not None:
            body["size"] = size
        if _source is not None:
            body["_source"] = _source
        body.update(kwargs)
        response = self._client.search(index=self._index_name, body=body)
        if not compact:
            return response
        return {
            'total': response['hits']['total'],
            'hits': list(map(lambda x: x['_source'], response['hits']['hits']))
        }
