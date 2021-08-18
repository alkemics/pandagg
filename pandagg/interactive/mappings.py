import json
from typing import Optional, List

from elasticsearch import Elasticsearch

from lighttree import TreeBasedObj
from lighttree.node import NodeId

from pandagg.tree.mappings import Mappings
from pandagg.interactive._field_agg_factory import field_classes_per_name
from pandagg.utils import DSLMixin


class IMappings(DSLMixin, TreeBasedObj[Mappings]):
    """Interactive wrapper upon mappings tree, allowing field navigation and quick access to single clause aggregations
    computation.
    """

    _REPR_NAME = "Mappings"
    _NODE_PATH_ATTR = "name"

    def __init__(
        self,
        mappings: Mappings,
        client: Optional[Elasticsearch] = None,
        index: Optional[List[str]] = None,
        depth: int = 1,
        root_path: Optional[str] = None,
        initial_tree: Optional[Mappings] = None,
    ) -> None:
        self._client: Optional[Elasticsearch] = client
        self._index: Optional[List[str]] = index
        super(IMappings, self).__init__(
            tree=mappings, root_path=root_path, depth=depth, initial_tree=initial_tree
        )
        # if we reached a leave, add aggregation capabilities based on reached mappings type
        self._set_agg_property_if_required()

    def _clone(self, nid: NodeId, root_path: Optional[str], depth: int) -> "IMappings":
        return IMappings(
            self._tree.subtree(nid)[1],
            client=self._client,
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree,
            index=self._index,
        )

    def _set_agg_property_if_required(self) -> None:
        if (
            self._client is not None
            and self._root_path is not None
            and not self._tree.children(self._tree.root)
        ):
            _, field_node = self._tree.get(self._tree.root)
            if field_node.KEY in field_classes_per_name:
                search_class = self.get_dsl_type("search")
                self.a = field_classes_per_name[field_node.KEY](
                    _search=search_class(
                        using=self._client,
                        index=self._index,
                        mappings=self._initial_tree,
                        repr_auto_execute=True,
                        nested_autocorrect=True,
                    ),
                    _field=self._root_path,
                )

    def __call__(self, *args, **kwargs) -> None:  # type: ignore
        print(
            json.dumps(
                self._tree.to_dict(), indent=2, sort_keys=True, separators=(",", ": ")
            )
        )
