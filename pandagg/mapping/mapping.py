#!/usr/bin/env python
# -*- coding: utf-8 -*-

from six import iteritems, python_2_unicode_compatible
from builtins import str as text
import json
from pandagg.exceptions import AbsentMappingFieldError, InvalidOperationMappingFieldError, MappingError
from pandagg.mapping.types import MAPPING_TYPES
from pandagg.mapping.field_agg_factory import field_classes_per_name
from pandagg.tree import Tree, Node
from pandagg.utils import PrettyNode, TreeBasedObj


@python_2_unicode_compatible
class MappingNode(Node):

    REPR_SIZE = 60

    def __init__(self, path, body, depth, is_root=False, is_subfield=False):
        self.is_root = is_root
        self.path = path
        # name will be used for dynamic attribute access in tree
        self.name = path.split('.')[-1]
        self.depth = depth
        self.body = body
        self.is_subfield = is_subfield
        if is_root:
            self.type = ''
        else:
            type_ = body.get('type', 'object')
            if type_ not in MAPPING_TYPES:
                raise MappingError(u'Unkown <%s> field type on path <%s>' % (type_, path))
            self.type = type_
        super(MappingNode, self).__init__(identifier=path, data=PrettyNode(pretty=self.tree_repr))

    @property
    def tree_repr(self):
        pad = max(self.REPR_SIZE - 4 * self.depth - len(self.name), 4)
        s = 'root' if self.is_root else self.name
        if self.type == 'object':
            return s + ' ' * (pad - 1) + '{%s}' % self.type.capitalize()
        elif self.type == 'nested':
            return s + ' ' * (pad - 1) + '[%s]' % self.type.capitalize()
        elif self.is_subfield:
            return s + ' ' * (pad - 2) + '~ %s' % self.type.capitalize()
        return s + ' ' * pad + '%s' % self.type.capitalize()

    def __str__(self):
        return '<Mapping Field %s> of type %s:\n%s' % (
            text(self.path),
            text(self.type),
            text(json.dumps(self.body, indent=4))
        )


class MappingTree(Tree):
    """Mapping hierarchy represented as a tree.
    """
    node_class = MappingNode

    def __init__(self, mapping_detail=None, identifier=None):
        super(MappingTree, self).__init__(identifier=identifier)
        self.mapping_detail = mapping_detail
        if mapping_detail:
            self.build_mapping_from_dict(mapping_detail)

    def build_mapping_from_dict(self, body, pid=None, depth=0, path=None, is_subfield=False):
        path = path or ''
        node = MappingNode(path=path, body=body, depth=depth, is_root=depth == 0, is_subfield=is_subfield)
        self.add_node(node, parent=pid)
        if not body:
            return
        depth += 1
        for sub_name, sub_body in iteritems(body.get('properties') or {}):
            sub_path = '%s.%s' % (path, sub_name) if path else sub_name
            self.build_mapping_from_dict(sub_body, pid=node.path, depth=depth, path=sub_path)
        for sub_name, sub_body in iteritems(body.get('fields') or {}):
            sub_path = '%s.%s' % (path, sub_name) if path else sub_name
            self.build_mapping_from_dict(
                sub_body, pid=node.identifier, depth=depth, path=sub_path, is_subfield=True)

    def _clone(self, identifier, with_tree=False, deep=False):
        return MappingTree(
            identifier=identifier,
            mapping_detail=self.mapping_detail if with_tree else None
        )

    def show(self, data_property='pretty', **kwargs):
        return super(MappingTree, self).show(data_property=data_property, **kwargs)

    def validate_agg_node(self, agg_node, exc=True):
        """Ensure if node has field or path that it exists in mapping, and that required aggregation type
        if allowed on this kind of field.
        :param agg_node: AggNode you want to validate on this mapping
        :param exc: boolean, if set to True raise exception if invalid
        :rtype: boolean
        """
        if hasattr(agg_node, 'path'):
            if agg_node.path is None:
                # reverse nested
                return True
            return agg_node.path in self

        if not hasattr(agg_node, 'field'):
            return True

        if agg_node.field not in self:
            if not exc:
                return False
            raise AbsentMappingFieldError(u'Agg of type <%s> on non-existing field <%s>.' % (
                agg_node.AGG_TYPE, agg_node.field))

        field_type = self.mapping_type_of_field(agg_node.field)
        if not agg_node.valid_on_field_type(field_type):
            if not exc:
                return False
            raise InvalidOperationMappingFieldError(u'Agg of type <%s> not possible on field of type <%s>.'
                                                    % (agg_node.AGG_TYPE, field_type))
        return True

    def mapping_type_of_field(self, field_path):
        if field_path not in self:
            raise AbsentMappingFieldError(u'<%s field is not present in mapping>' % field_path)
        return self[field_path].type

    def nested_at_field(self, field_path):
        return next(iter(self.list_nesteds_at_field(field_path)), None)

    def list_nesteds_at_field(self, field_path):
        # from deepest to highest
        return list(self.rsearch(field_path, filter=lambda n: n.type == 'nested'))


class Mapping(TreeBasedObj):
    """Wrapper upon mapping tree, enabling interactive navigation in ipython.
    """
    _NODE_PATH_ATTR = 'name'

    def __call__(self, *args, **kwargs):
        return self._tree[self._tree.root]


class ClientBoundMapping(Mapping):

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
