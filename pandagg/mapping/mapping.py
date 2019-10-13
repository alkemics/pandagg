#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

from pandagg.mapping.types import field_classes_per_name
from pandagg.tree import Tree, Node
from pandagg.utils import Obj, PrettyNode


class MappingNode(Node):

    REPR_SIZE = 60

    def __init__(self, field_path, field_name, detail, depth):
        self.field_path = field_path
        self.field_name = field_name
        self.type = detail.get('type', 'object')
        self.dynamic = detail.get('dynamic', False)
        self.depth = depth
        self.extra = detail
        super(MappingNode, self).__init__(identifier=field_path, data=PrettyNode(pretty=self.pretty))

    @property
    def pretty(self):
        pad = max(self.REPR_SIZE - 4 * self.depth - len(self.field_name), 4)
        s = self.field_name
        if self.type == 'object':
            s += ' ' * (pad - 1) + '{%s}' % self.type.capitalize()
        elif self.type == 'nested':
            s += ' ' * (pad - 1) + '[%s]' % self.type.capitalize()
        else:
            s += ' ' * pad + '%s' % self.type.capitalize()
        return s

    def __repr__(self):
        return '<Mapping Field %s> of type %s:\n%s' % (
            self.field_path,
            self.type,
            json.dumps(self.extra, indent=4, encoding='utf-8')
        )


class TreeMapping(Tree):
    """
    Tree
    """
    node_class = MappingNode

    def __init__(self, mapping_name, mapping_detail=None, identifier=None):
        super(TreeMapping, self).__init__(identifier=identifier)
        self.mapping_name = mapping_name
        self.mapping_detail = mapping_detail
        if mapping_detail:
            self.build_mapping_from_dict(mapping_name, mapping_detail)

    def build_mapping_from_dict(self, name, detail, pid=None, depth=0, path=None):
        path = path or ''
        node = MappingNode(field_path=path, field_name=name, detail=detail, depth=depth)
        self.add_node(node, parent=pid)
        if detail:
            depth += 1
            for sub_name, sub_detail in (detail.get('properties') or {}).iteritems():
                sub_path = '%s.%s' % (path, sub_name) if path else sub_name
                self.build_mapping_from_dict(sub_name, sub_detail, pid=node.identifier, depth=depth, path=sub_path)

    def get_instance(self, identifier):
        return TreeMapping(mapping_name=self.mapping_name, mapping_detail=self.mapping_detail, identifier=identifier)

    def subtree(self, nid):
        st = TreeMapping(mapping_name=self.mapping_name)
        st.root = nid
        for node_n in self.expand_tree(nid):
            st._nodes.update({self[node_n].identifier: self[node_n]})
            st[node_n].clone_pointers(self.identifier, st.identifier)
        return st

    def show(self, data_property='pretty', **kwargs):
        return super(TreeMapping, self).show(data_property=data_property, **kwargs)


class Mapping(Obj):
    """
    Autocomplete attributes
    """

    def __init__(self, tree, root_path=None, depth=None):
        super(Mapping, self).__init__()
        self._tree = tree
        self._root_path = root_path
        self._expand_attrs(depth)

    def _get_instance(self, nid, root_path, depth):
        return Mapping(tree=self._tree.subtree(nid), root_path=root_path, depth=depth)

    def _expand_attrs(self, depth):
        if depth:
            for child in self._tree.children(nid=self._tree.root):
                if hasattr(self, child.field_name):
                    continue
                if self._root_path is None:
                    child_root = '%s.%s' % (self._root_path, child.field_name)
                else:
                    child_root = child.field_name
                self[child.field_name] = self._get_instance(child.identifier, root_path=child_root, depth=depth-1)

    def __getattribute__(self, item):
        r = super(Mapping, self).__getattribute__(item)
        if isinstance(r, Mapping):
            r._expand_attrs(depth=1)
        return r

    def __repr__(self):
        tree_repr = self._tree.show()
        mapping_name = self._tree.mapping_name
        if self._root_path is None:
            return (u'\n<Mapping %s>\n%s' % (mapping_name, tree_repr)).encode('utf-8')
        current_path = self._root_path
        return (u'\n<Mapping %s subpart: %s>\n%s' % (mapping_name, current_path, tree_repr)).encode('utf-8')

    def __call__(self, *args, **kwargs):
        return self._tree[self._tree.root]


class ClientBoundMapping(Mapping):

    def __init__(self, client, tree, root_path=None, depth=None):
        self._client = client
        super(ClientBoundMapping, self).__init__(tree, root_path, depth)
        # if we reached a leave, add aggregation capabilities based on reached mapping type
        if not self._tree.children(self._tree.root):
            field_type = self._tree[self._tree.root].type
            if field_type in field_classes_per_name:
                self.a = field_classes_per_name[field_type](self._client)

    def _get_instance(self, nid, root_path, depth):
        return ClientBoundMapping(tree=self._tree.subtree(nid), root_path=root_path, client=self._client, depth=depth)
