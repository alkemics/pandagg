#!/usr/bin/env python
# -*- coding: utf-8 -*-

from six import iteritems, python_2_unicode_compatible
from builtins import str as text
import json
from pandagg.exceptions import AbsentMappingFieldError, InvalidOperationMappingFieldError, MappingError
from pandagg.mapping.types import MAPPING_TYPES
from pandagg.mapping.field_agg_factory import field_classes_per_name
from pandagg.tree import Tree, Node
from pandagg.utils import PrettyNode, TreeBasedObj, validate_client


@python_2_unicode_compatible
class MappingNode(Node):

    REPR_SIZE = 60

    def __init__(self, field_path, field_name, detail, depth, root=False, sub_field=False):
        self.field_path = field_path
        self.field_name = field_name
        self.type = '' if root else detail.get('type', 'object')
        if not root and self.type not in MAPPING_TYPES:
            raise MappingError(u'Unkown <%s> field type on path <%s>' % (self.type, field_path))
        self.sub_field = sub_field
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
        elif self.sub_field:
            s += ' ' * (pad - 2) + '~ %s' % self.type.capitalize()
        else:
            s += ' ' * pad + '%s' % self.type.capitalize()
        return s

    def __str__(self):
        return '<Mapping Field %s> of type %s:\n%s' % (
            text(self.field_path),
            text(self.type),
            text(json.dumps(self.extra, indent=4))
        )


class MappingTree(Tree):
    """Mapping hierarchy represented as a tree.
    """
    node_class = MappingNode

    def __init__(self, mapping_name, mapping_detail=None, identifier=None):
        super(MappingTree, self).__init__(identifier=identifier)
        self.mapping_name = mapping_name
        self.mapping_detail = mapping_detail
        if mapping_detail:
            self.build_mapping_from_dict(mapping_name, mapping_detail, root=True)

    def build_mapping_from_dict(self, name, detail, pid=None, depth=0, path=None, root=False, sub_field=False):
        path = path or ''
        node = MappingNode(field_path=path, field_name=name, detail=detail, depth=depth, root=root, sub_field=sub_field)
        self.add_node(node, parent=pid)
        if not detail:
            return
        depth += 1
        for sub_name, sub_detail in iteritems(detail.get('properties') or {}):
            sub_path = '%s.%s' % (path, sub_name) if path else sub_name
            self.build_mapping_from_dict(sub_name, sub_detail, pid=node.identifier, depth=depth, path=sub_path)
        for sub_name, sub_detail in iteritems(detail.get('fields') or {}):
            sub_path = '%s.%s' % (path, sub_name) if path else sub_name
            self.build_mapping_from_dict(
                sub_name, sub_detail, pid=node.identifier, depth=depth, path=sub_path, sub_field=True)

    def _get_instance(self, identifier, **kwargs):
        return MappingTree(mapping_name=self.mapping_name, identifier=identifier)

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
    _NODE_PATH_ATTR = 'field_name'

    def __call__(self, *args, **kwargs):
        return self._tree[self._tree.root]


class ClientBoundMapping(Mapping):

    def __init__(self, client, tree, root_path=None, depth=None, initial_tree=None, index_name=None):
        validate_client(client)
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
                    field=field_node.field_path,
                    index_name=self._index_name
                )

    def _get_instance(self, nid, root_path, depth, **kwargs):
        return ClientBoundMapping(
            client=self._client,
            tree=self._tree.subtree(nid),
            root_path=root_path,
            depth=depth,
            initial_tree=self._initial_tree,
            index_name=self._index_name
        )
