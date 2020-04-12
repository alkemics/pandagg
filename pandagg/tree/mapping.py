#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy

from six import iteritems

from lighttree.exceptions import NotFoundNodeError

from pandagg.node.mapping.abstract import Field
from pandagg.node.mapping.deserializer import deserialize_field
from pandagg.exceptions import (
    AbsentMappingFieldError,
    InvalidOperationMappingFieldError,
)
from pandagg.node.mapping.field_datatypes import Object
from pandagg.tree._tree import Tree


class Mapping(Tree):

    node_class = Field

    def __init__(self, from_=None, properties=None, dynamic=False):
        if from_ is not None and properties is not None:
            raise ValueError('Can provide at most one of "from_" and "properties"')
        if properties is not None:
            from_ = Object(name="", properties=properties, dynamic=dynamic)
        super(Mapping, self).__init__()
        if from_ is not None:
            self._insert(from_)

    @classmethod
    def deserialize(cls, from_):
        if isinstance(from_, Mapping):
            return from_
        if isinstance(from_, Field):
            new = Mapping()
            new._insert_from_node(field=from_)
            return new
        if isinstance(from_, dict):
            from_ = copy.deepcopy(from_)
            new = Mapping()
            new._insert_from_dict(name="", body=from_)
            return new
        else:
            raise ValueError("Unsupported type <%s>." % type(from_))

    def serialize(self):
        if self.root is None:
            return None
        return self.get(self.root).body(with_children=True)

    def _insert_from_dict(self, name, body, pid=None, is_subfield=False):
        node = deserialize_field(name=name, body=body, is_subfield=is_subfield)
        self._insert_from_node(node, pid=pid)

    def _insert_from_node(self, field, pid=None, is_subfield=False):
        if is_subfield:
            field.is_subfield = True
        self.insert_node(field, pid)
        for subfield in field.fields or []:
            if isinstance(subfield, dict):
                name, body = next(iteritems(subfield))
                self._insert_from_dict(
                    name=name, body=body, pid=field.identifier, is_subfield=True
                )
            elif isinstance(subfield, Field):
                self._insert_from_node(subfield, pid=field.identifier, is_subfield=True)
            else:
                raise ValueError("Wrong type %s" % type(field))
        for subfield in field.properties or []:
            if isinstance(subfield, dict):
                name, body = next(iteritems(subfield))
                self._insert_from_dict(name=name, body=body, pid=field.identifier)
            elif isinstance(subfield, Field):
                self._insert_from_node(subfield, pid=field.identifier)
            else:
                raise ValueError("Wrong type %s" % type(field))

    def _insert(self, from_, pid=None):
        inserted_tree = self.deserialize(from_=from_)
        if self.root is None:
            self.merge(nid=pid, new_tree=inserted_tree)
            return self
        self.paste(nid=pid, new_tree=inserted_tree)
        return self

    def resolve_path_to_id(self, path):
        if path in self._nodes_map:
            return path
        nid = self.root
        names = path.split(".")
        for name in names:
            matching_children = [
                c for c in self.children(nid, id_only=False) if c.name == name
            ]
            if len(matching_children) != 1:
                return path
            nid = matching_children[0].identifier
        return nid

    def get(self, key):
        return super(Mapping, self).get(self.resolve_path_to_id(key))

    def _clone(self, with_tree=False, deep=False):
        return Mapping(from_=self if with_tree else None)

    def validate_agg_node(self, agg_node, exc=True):
        """Ensure if node has field or path that it exists in mapping, and that required aggregation type
        if allowed on this kind of field.
        :param agg_node: AggNode you want to validate on this mapping
        :param exc: boolean, if set to True raise exception if invalid
        :rtype: boolean
        """
        if hasattr(agg_node, "path"):
            if agg_node.path is None:
                # reverse nested
                return True
            return self.resolve_path_to_id(agg_node.path) in self

        if not hasattr(agg_node, "field"):
            return True

        # TODO take into account flattened data type
        field = self.resolve_path_to_id(agg_node.field)
        if field not in self:
            if not exc:
                return False
            raise AbsentMappingFieldError(
                u"Agg of type <%s> on non-existing field <%s>."
                % (agg_node.KEY, agg_node.field)
            )

        field_type = self.mapping_type_of_field(field)
        if not agg_node.valid_on_field_type(field_type):
            if not exc:
                return False
            raise InvalidOperationMappingFieldError(
                u"Agg of type <%s> not possible on field of type <%s>."
                % (agg_node.KEY, field_type)
            )
        return True

    def mapping_type_of_field(self, field_path):
        try:
            return self.get(field_path).KEY
        except NotFoundNodeError:
            raise AbsentMappingFieldError(
                u"<%s field is not present in mapping>" % field_path
            )

    def nested_at_field(self, field_path):
        nesteds = self.list_nesteds_at_field(field_path)
        if nesteds:
            return nesteds[0]
        return None

    def list_nesteds_at_field(self, field_path):
        path_nid = self.resolve_path_to_id(field_path)
        # from deepest to highest
        return [
            self.node_path(nid)
            for nid in self.ancestors(path_nid) + [path_nid]
            if self.get(nid).KEY == "nested"
        ]

    def node_path(self, nid):
        return ".".join(
            [
                self.get(id_).name
                for id_ in self.ancestors(nid, from_root=True) + [nid]
                if id_ != self.root
            ]
        )
