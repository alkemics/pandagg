#!/usr/bin/env python
# -*- coding: utf-8 -*-

from lighttree import TreeBasedObj
from pandagg.tree.query.abstract import Query


class IResponse(TreeBasedObj):

    """Interactive aggregation response.
    """

    _NODE_PATH_ATTR = "attr_name"
    _COERCE_ATTR = True

    def __init__(
        self,
        tree,
        client=None,
        index_name=None,
        root_path=None,
        depth=None,
        initial_tree=None,
        query=None,
    ):
        self._client = client
        self._index_name = index_name
        self._query = Query(query)
        super(IResponse, self).__init__(
            tree=tree, root_path=root_path, depth=depth, initial_tree=initial_tree
        )

    def _clone(self, nid, root_path, depth):
        return IResponse(
            client=self._client,
            index_name=self._index_name,
            tree=self._tree.subtree(nid),
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree,
            query=self._query,
        )

    def get_bucket_filter(self):
        """Build filters to select documents belonging to that bucket"""
        bucket_filter = self._initial_tree.get_bucket_filter(self._tree.root)
        return self._query.query(bucket_filter).to_dict()

    def list_documents(self, **body):
        """Return ES aggregation query to list documents belonging to given bucket.
        :return:
        """
        filter_query = self.get_bucket_filter()
        if filter_query:
            body["query"] = filter_query
        return self._client.search(index=self._index_name, body=body)
