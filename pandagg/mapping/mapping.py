#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import re

from pandagg.exceptions import AbsentMappingFieldError, InvalidOperationMappingFieldError
from pandagg.mapping.types import field_classes_per_name
from pandagg.tree import Tree, Node
from pandagg.utils import PrettyNode, TreeBasedObj


class MappingNode(Node):

    REPR_SIZE = 60

    def __init__(self, field_path, field_name, detail, depth, root=False):
        self.field_path = field_path
        self.field_name = field_name
        self.type = '' if root else detail.get('type', 'object')
        self.dynamic = detail.get('dynamic', False)
        self.depth = depth
        self.extra = detail
        super(MappingNode, self).__init__(identifier=field_path, data=PrettyNode(pretty=self.pretty))

    def has_subfield(self, subfield):
        return 'fields' in (self.extra or {}) and subfield in self.extra['fields']

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


class MappingTree(Tree):
    """
    Tree
    """
    node_class = MappingNode

    def __init__(self, mapping_name, mapping_detail=None, identifier=None):
        super(MappingTree, self).__init__(identifier=identifier)
        self.mapping_name = mapping_name
        self.mapping_detail = mapping_detail
        if mapping_detail:
            self.build_mapping_from_dict(mapping_name, mapping_detail, root=True)

    def build_mapping_from_dict(self, name, detail, pid=None, depth=0, path=None, root=False):
        path = path or ''
        node = MappingNode(field_path=path, field_name=name, detail=detail, depth=depth, root=root)
        self.add_node(node, parent=pid)
        if detail:
            depth += 1
            for sub_name, sub_detail in (detail.get('properties') or {}).iteritems():
                sub_path = '%s.%s' % (path, sub_name) if path else sub_name
                self.build_mapping_from_dict(sub_name, sub_detail, pid=node.identifier, depth=depth, path=sub_path)

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

        if not self.is_field_in_mapping(agg_node.field):
            if not exc:
                return False
            raise AbsentMappingFieldError('Agg of type <%s> on non-existing field <%s>.' % (
                agg_node.AGG_TYPE, agg_node.field))

        field_type = self.mapping_type_of_field(agg_node.field)
        if not agg_node.valid_on_field_type(field_type):
            if not exc:
                return False
            raise InvalidOperationMappingFieldError('Agg of type <%s> not possible on field of type <%s>.'
                                                    % (agg_node.AGG_TYPE, field_type))
        return True

    def is_field_in_mapping(self, field_path):
        if field_path in self:
            return True
        m = re.match(string=field_path, pattern=r'(.+)\.([a-zA-Z0-9_]+)$')
        if not m:
            return False
        field, sub_field = m.groups()
        if field not in self or not self[field].has_subfield(sub_field):
            return False
        return True

    def mapping_type_of_field(self, field_path):
        if field_path in self:
            return self[field_path].type
        m = re.match(string=field_path, pattern=r'(.+)\.([a-zA-Z0-9_]+)$')
        if not m:
            raise AbsentMappingFieldError('<%s field is not present in mapping>' % field_path)
        field, sub_field = m.groups()
        if field not in self or not self[field].has_subfield(sub_field):
            raise AbsentMappingFieldError('<%s field is not present in mapping>' % field_path)
        return self[field].extra['fields'][sub_field]['type']

    def nested_at_field(self, field_path):
        return next(iter(self.list_nesteds_at_field(field_path)), None)

    def list_nesteds_at_field(self, field_path):
        # from deepest to highest
        if field_path.endswith('.raw'):
            field_path = re.sub(string=field_path, repl='', pattern=r'(\.raw)$')
        return list(self.rsearch(field_path, filter=lambda n: n.type == 'nested'))


class Mapping(TreeBasedObj):
    """
    Autocomplete attributes
    """
    _NODE_PATH_ATTR = 'field_name'

    def __call__(self, *args, **kwargs):
        return self._tree[self._tree.root]


class ClientBoundMapping(Mapping):

    def __init__(self, client, tree, root_path=None, depth=None):
        self._client = client
        super(ClientBoundMapping, self).__init__(tree, root_path, depth)
        # if we reached a leave, add aggregation capabilities based on reached mapping type
        if not self._tree.children(self._tree.root):
            field_node = self._tree[self._tree.root]
            if field_node.type in field_classes_per_name:
                self.a = field_classes_per_name[field_node.type](client=self._client, field=field_node.field_path)

    def _get_instance(self, nid, root_path, depth, **kwargs):
        return ClientBoundMapping(tree=self._tree.subtree(nid), root_path=root_path, client=self._client, depth=depth)
