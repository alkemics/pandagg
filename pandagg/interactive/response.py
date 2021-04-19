#!/usr/bin/env python
# -*- coding: utf-8 -*-

from lighttree import TreeBasedObj

from pandagg.tree.aggs import Aggs


class IResponse(TreeBasedObj):

    """Interactive aggregation response."""

    _ATTR = "attr_name"
    _COERCE_ATTR = True

    def __init__(self, tree, search, depth, root_path=None, initial_tree=None):
        self.__search = search
        super(IResponse, self).__init__(
            tree=tree, root_path=root_path, depth=depth, initial_tree=initial_tree
        )

    @property
    def _client(self):
        return self.__search._using

    @property
    def _index(self):
        return self.__search._index

    def _clone(self, nid, root_path, depth):
        return IResponse(
            tree=self._tree.subtree(nid)[1],
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree,
            search=self.__search,
        )

    def get_bucket_filter(self):
        """Build filters to select documents belonging to that bucket, independently from initial search query
        clauses."""
        return self._initial_tree.get_bucket_filter(self._tree.root)

    def search(self):
        # add bucket filter to initial query clauses
        s = self.__search.query(self.get_bucket_filter())
        # remove no-more necessary aggregations
        s._aggs = Aggs()
        return s
