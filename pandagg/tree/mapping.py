#!/usr/bin/env python
# -*- coding: utf-8 -*-

from lighttree.exceptions import NotFoundNodeError

from pandagg.node.mapping.abstract import Field, ShadowRoot, StringField, ComplexField

# necessary to ensure all Fields are registered in meta-class
import pandagg.node.mapping.field_datatypes as fd  # noqa
import pandagg.node.mapping.meta_fields as mf  # noqa

from pandagg.exceptions import (
    AbsentMappingFieldError,
    InvalidOperationMappingFieldError,
)
from pandagg.tree._tree import Tree


class Mapping(Tree):

    node_class = Field

    def __init__(self, *args, **kwargs):
        super(Mapping, self).__init__()
        if (args and kwargs) or len(args) > 1:
            raise ValueError(
                "Invalid mapping declaration. Got:\n*args: %s\n**kwargs: %s"
                % (args, kwargs)
            )
        if args:
            arg = args[0]
            if isinstance(arg, Mapping):
                self.insert(arg)
            elif arg is None:
                pass
            else:
                # {'dynamic': False, 'properties': ...}
                self.insert(ShadowRoot(**arg))
        elif kwargs:
            self.insert(ShadowRoot(**kwargs))

    def __nonzero__(self):
        return not self.is_empty()

    __bool__ = __nonzero__

    def to_dict(self, from_=None, depth=None):
        if self.root is None:
            return None
        from_ = self.root if from_ is None else from_
        node = self.get(from_)
        children_queries = {}
        if depth is None or depth > 0:
            if depth is not None:
                depth -= 1
            for child_node in self.children(node.identifier, id_only=False):
                children_queries[child_node.name] = self.to_dict(
                    from_=child_node.identifier, depth=depth
                )
        serialized_node = node.body
        if children_queries:
            if isinstance(node, StringField):
                serialized_node["fields"] = children_queries
            elif isinstance(node, ComplexField):
                serialized_node["properties"] = children_queries
        return serialized_node

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
