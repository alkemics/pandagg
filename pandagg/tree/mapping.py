#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy

from six import iteritems
from treelib.exceptions import NodeIDAbsentError

from pandagg.node.mapping.abstract import Field
from pandagg.node.mapping.deserializer import deserialize_field
from pandagg.exceptions import AbsentMappingFieldError, InvalidOperationMappingFieldError
from pandagg.node.mapping.field_datatypes import Object
from pandagg.tree._tree import Tree


class Mapping(Tree):

    node_class = Field

    def __init__(self, from_=None, identifier=None, properties=None, dynamic=False):
        if from_ is not None and properties is not None:
            raise ValueError('Can provide at most one of "from_" and "properties"')
        if properties is not None:
            from_ = Object(name='', properties=properties, dynamic=dynamic)
        super(Mapping, self).__init__(identifier=identifier)
        if from_ is not None:
            self._insert(from_, depth=0)

    @classmethod
    def deserialize(cls, from_, depth=0):
        if isinstance(from_, Mapping):
            return from_
        if isinstance(from_, Field):
            new = Mapping()
            new._insert_from_node(field=from_, depth=depth, is_subfield=False)
            return new
        if isinstance(from_, dict):
            from_ = copy.deepcopy(from_)
            new = Mapping()
            new._insert_from_dict(name='', body=from_, is_subfield=False, depth=depth)
            return new
        else:
            raise ValueError('Unsupported type <%s>.' % type(from_))

    def serialize(self):
        if self.root is None:
            return None
        return self[self.root].body(with_children=True)

    def _insert_from_dict(self, name, body, is_subfield, depth, pid=None):
        node = deserialize_field(name=name, depth=depth, is_subfield=is_subfield, body=body)
        self._insert_from_node(node, depth=depth, pid=pid, is_subfield=is_subfield)

    def _insert_from_node(self, field, depth, is_subfield, pid=None):
        # overriden to allow smooth DSL declaration
        field.depth = depth
        field.is_subfield = is_subfield
        field.reset_data()

        self.add_node(field, pid)
        for subfield in field.fields or []:
            if isinstance(subfield, dict):
                name, body = next(iteritems(subfield))
                self._insert_from_dict(name=name, body=body, pid=field.identifier, is_subfield=True, depth=depth + 1)
            elif isinstance(subfield, Field):
                self._insert_from_node(subfield, pid=field.identifier, depth=depth + 1, is_subfield=True)
            else:
                raise ValueError('Wrong type %s' % type(field))
        for subfield in field.properties or []:
            if isinstance(subfield, dict):
                name, body = next(iteritems(subfield))
                self._insert_from_dict(name=name, body=body, pid=field.identifier, is_subfield=False, depth=depth + 1)
            elif isinstance(subfield, Field):
                self._insert_from_node(subfield, pid=field.identifier, depth=depth + 1, is_subfield=False)
            else:
                raise ValueError('Wrong type %s' % type(field))

    def _insert(self, from_, depth, pid=None):
        inserted_tree = self.deserialize(from_=from_, depth=depth)
        if self.root is None:
            self.merge(nid=pid, new_tree=inserted_tree)
            return self
        self.paste(nid=pid, new_tree=inserted_tree)
        return self

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
            from_=self if with_tree else None
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
        return [
            self.node_path(nid)
            for nid in self.rsearch(
                self[field_path].identifier,
                filter=lambda n: n.KEY == 'nested'
            )
        ]
