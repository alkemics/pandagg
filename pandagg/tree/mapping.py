#!/usr/bin/env python
# -*- coding: utf-8 -*-

from six import iteritems

from pandagg.node.mapping.abstract import Field
from pandagg.node.mapping.deserializer import deserialize_field
from pandagg.exceptions import AbsentMappingFieldError, InvalidOperationMappingFieldError
from pandagg._tree import Tree


class Mapping(Tree):

    node_class = Field

    def __init__(self, body=None, identifier=None):
        super(Mapping, self).__init__(identifier=identifier)
        self.body = body
        if body:
            self.deserialize(path='', body=body)

    def deserialize(self, path, body, pid=None, depth=0, is_subfield=False):
        node = deserialize_field(path=path, depth=depth, is_subfield=is_subfield, body=body)
        self.add_node(node, parent=pid)
        depth += 1
        for sub_name, sub_body in iteritems(node.properties or {}):
            sub_path = '%s.%s' % (path, sub_name) if path else sub_name
            self.deserialize(path=sub_path, body=sub_body, pid=node.path, depth=depth)
        for sub_name, sub_body in iteritems(node.fields or {}):
            sub_path = '%s.%s' % (path, sub_name) if path else sub_name
            self.deserialize(path=sub_path, body=sub_body, pid=node.identifier, depth=depth, is_subfield=True)

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
        return next(iter(self.list_nesteds_at_field(field_path)), None)

    def list_nesteds_at_field(self, field_path):
        # from deepest to highest
        return list(self.rsearch(field_path, filter=lambda n: n.KEY == 'nested'))
