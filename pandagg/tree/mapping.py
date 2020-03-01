#!/usr/bin/env python
# -*- coding: utf-8 -*-

from six import iteritems
from treelib.exceptions import NodeIDAbsentError

from pandagg.node.mapping.abstract import Field
from pandagg.node.mapping.deserializer import deserialize_field
from pandagg.exceptions import AbsentMappingFieldError, InvalidOperationMappingFieldError
from pandagg.tree._tree import Tree


class Mapping(Tree):

    node_class = Field

    def __init__(self, body=None, identifier=None):
        super(Mapping, self).__init__(identifier=identifier)
        self.body = body
        if body:
            self.deserialize(name='', body=body)

    def deserialize(self, name, body, pid=None, depth=0, is_subfield=False):
        node = deserialize_field(name=name, depth=depth, is_subfield=is_subfield, body=body)
        self.add_node(node, parent=pid)
        depth += 1
        for sub_name, sub_body in iteritems(node.properties or {}):
            self.deserialize(name=sub_name, body=sub_body, pid=node.identifier, depth=depth)
        for sub_name, sub_body in iteritems(node.fields or {}):
            self.deserialize(name=sub_name, body=sub_body, pid=node.identifier, depth=depth, is_subfield=True)

    def __getitem__(self, key):
        """Tries to fetch node by identifier, else by succession of names."""
        try:
            return self._nodes[key]
        except KeyError:
            pass
        pid = self.root
        names = key.split('.')
        for name in names:
            matching_children = [c for c in self.children(pid) if c.name == name]
            if len(matching_children) != 1:
                raise NodeIDAbsentError()
            pid = matching_children[0].identifier
        return self[pid]

    def __contains__(self, identifier):
        try:
            return self[identifier] is not None
        except NodeIDAbsentError:
            return False

    def node_path(self, nid):
        path = self[nid].name
        node = self.parent(nid)
        while node is not None and node.identifier is not self.root:
            path = '%s.%s' % (node.name, path)
            node = self.parent(node.identifier)
        return path

    def contains(self, nid):
        # remove after https://github.com/caesar0301/treelib/issues/155
        return nid in self

    def _clone(self, identifier, with_tree=False, deep=False):
        return Mapping(
            identifier=identifier,
            body=self.body if with_tree else None
        )

    def show(self, data_property='pretty', **kwargs):
        return super(Mapping, self).show(data_property=data_property, **kwargs)

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

        # TODO take into account flattened data type
        if agg_node.field not in self:
            if not exc:
                return False
            raise AbsentMappingFieldError(u'Agg of type <%s> on non-existing field <%s>.' % (
                agg_node.KEY, agg_node.field))

        field_type = self.mapping_type_of_field(agg_node.field)
        if not agg_node.valid_on_field_type(field_type):
            if not exc:
                return False
            raise InvalidOperationMappingFieldError(u'Agg of type <%s> not possible on field of type <%s>.'
                                                    % (agg_node.KEY, field_type))
        return True

    def mapping_type_of_field(self, field_path):
        if field_path not in self:
            raise AbsentMappingFieldError(u'<%s field is not present in mapping>' % field_path)
        return self[field_path].KEY

    def nested_at_field(self, field_path):
        nesteds = self.list_nesteds_at_field(field_path)
        if nesteds:
            return self.node_path(nesteds[0])
        return None

    def list_nesteds_at_field(self, field_path):
        # from deepest to highest
        return [self.node_path(nid) for nid in self.rsearch(field_path, filter=lambda n: n.KEY == 'nested')]
