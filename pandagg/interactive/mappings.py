#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

from lighttree import TreeBasedObj

from pandagg.tree.mappings import _mappings
from pandagg.interactive._field_agg_factory import field_classes_per_name
from pandagg.utils import DSLMixin


class IMappings(DSLMixin, TreeBasedObj):
    """Interactive wrapper upon mappings tree, allowing field navigation and quick access to single clause aggregations
    computation.
    """

    _REPR_NAME = "Mappings"
    _NODE_PATH_ATTR = "name"

    def __init__(
        self,
        mappings,
        client=None,
        index=None,
        depth=1,
        root_path=None,
        initial_tree=None,
    ):
        if mappings is None:
            raise ValueError("mappings cannot be None")
        self._client = client
        self._index = index
        super(IMappings, self).__init__(
            tree=_mappings(mappings),
            root_path=root_path,
            depth=depth,
            initial_tree=initial_tree,
        )
        # if we reached a leave, add aggregation capabilities based on reached mappings type
        self._set_agg_property_if_required()

    def _clone(self, nid, root_path, depth):
        return IMappings(
            self._tree.subtree(nid)[1],
            client=self._client,
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree,
            index=self._index,
        )

    def _set_agg_property_if_required(self):
        if self._client is not None and not self._tree.children(self._tree.root):
            _, field_node = self._tree.get(self._tree.root)
            if field_node.KEY in field_classes_per_name:
                search_class = self._get_dsl_type("search")
                self.a = field_classes_per_name[field_node.KEY](
                    search=search_class(
                        using=self._client,
                        index=self._index,
                        mappings=self._initial_tree,
                        repr_auto_execute=True,
                        nested_autocorrect=True,
                    ),
                    field=self._root_path,
                )

    def __call__(self, *args, **kwargs):
        print(
            json.dumps(
                self._tree.to_dict(), indent=2, sort_keys=True, separators=(",", ": ")
            )
        )
