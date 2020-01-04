#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.base.interactive._field_agg_factory import field_classes_per_name
from pandagg.base.interactive.abstract import TreeBasedObj
from pandagg.base.tree.mapping import Mapping


def as_mapping(mapping):
    if isinstance(mapping, Mapping):
        return mapping
    elif isinstance(mapping, IMapping):
        return mapping._tree
    elif isinstance(mapping, dict):
        return Mapping(mapping)
    else:
        raise NotImplementedError()


class IMapping(TreeBasedObj):
    """Interactive wrapper upon mapping tree.
    """
    _NODE_PATH_ATTR = 'name'

    def __call__(self, *args, **kwargs):
        return self._tree[self._tree.root]


class ClientBoundMapping(IMapping):

    def __init__(self, client, tree, root_path=None, depth=None, initial_tree=None, index_name=None):
        self._client = client
        self._index_name = index_name
        super(ClientBoundMapping, self).__init__(
            tree=tree,
            root_path=root_path,
            depth=depth,
            initial_tree=initial_tree,
        )
        # if we reached a leave, add aggregation capabilities based on reached mapping type
        if not self._tree.children(self._tree.root):
            field_node = self._tree[self._tree.root]
            if field_node.type in field_classes_per_name:
                self.a = field_classes_per_name[field_node.type](
                    mapping_tree=self._initial_tree,
                    client=self._client,
                    field=field_node.path,
                    index_name=self._index_name
                )

    def _clone(self, nid, root_path, depth):
        return ClientBoundMapping(
            client=self._client,
            tree=self._tree.subtree(nid),
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree,
            index_name=self._index_name
        )
