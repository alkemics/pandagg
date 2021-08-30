from __future__ import annotations

from typing import Optional, TYPE_CHECKING, List

from elasticsearch import Elasticsearch

from lighttree import TreeBasedObj
from lighttree.node import NodeId

from pandagg.tree.aggs import Aggs
from pandagg.tree.response import AggsResponseTree
from pandagg.types import QueryClauseDict

if TYPE_CHECKING:
    from pandagg.search import Search


class IResponse(TreeBasedObj[AggsResponseTree]):

    """Interactive aggregation response."""

    _ATTR = "attr_name"
    _COERCE_ATTR = True

    def __init__(
        self,
        tree: AggsResponseTree,
        search: Search,
        depth: int,
        root_path: Optional[str] = None,
        initial_tree: Optional[AggsResponseTree] = None,
    ) -> None:
        self.__search: Search = search
        super(IResponse, self).__init__(
            tree=tree, root_path=root_path, depth=depth, initial_tree=initial_tree
        )

    @property
    def _client(self) -> Optional[Elasticsearch]:
        return self.__search._using

    @property
    def _index(self) -> Optional[List[str]]:
        return self.__search._index

    def _clone(self, nid: NodeId, root_path: Optional[str], depth: int) -> "IResponse":
        return IResponse(
            tree=self._tree.subtree(nid)[1],
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree,
            search=self.__search,
        )

    def get_bucket_filter(self) -> Optional[QueryClauseDict]:
        """Build filters to select documents belonging to that bucket, independently from initial search query
        clauses."""
        return self._initial_tree.get_bucket_filter(self._tree.root)

    def search(self) -> Search:
        s = self.__search._clone()
        # remove no-more necessary aggregations
        s._aggs = Aggs()

        q = self.get_bucket_filter()
        if q is None:
            return s
        # add bucket filter to initial query clauses
        return s.query(q)
