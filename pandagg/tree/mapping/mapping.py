#!/usr/bin/env python
# -*- coding: utf-8 -*-

from future.utils import iteritems, string_types
from lighttree.exceptions import NotFoundNodeError

from pandagg.node.mapping.abstract import Field, ComplexField


from pandagg.exceptions import (
    AbsentMappingFieldError,
    InvalidOperationMappingFieldError,
)
from pandagg.tree._tree import Tree


class Mapping(Tree):

    _type_name = "mapping_tree"
    node_class = Field
    KEY = None

    def __init__(self, *args, **kwargs):
        super(Mapping, self).__init__()
        if not kwargs and not args:
            return
        if len(args) > 1:
            raise ValueError(
                "Invalid mapping declaration. Got:\n*args: %s\n**kwargs: %s"
                % (args, kwargs)
            )

        if args:
            arg = args[0]
            if arg is None:
                return
            if isinstance(arg, (Mapping, Field)):
                # Keyword
                self.insert(arg)
                return
            if isinstance(arg, dict):
                # {"properties": {}}
                name = kwargs.pop("name", "")
                kwargs = arg.copy()
            elif isinstance(arg, string_types):
                # Nested("actors", properties=...)
                name = arg
            else:
                raise ValueError(
                    "Wrong declaration: args %s, kwargs %s" % (args, kwargs)
                )
        else:
            name = kwargs.pop("name", "")

        # {'dynamic': False, 'properties': ...}
        properties = kwargs.pop("properties", None)
        fields = kwargs.pop("fields", None)
        is_subfield = kwargs.pop("is_subfield", None)

        if is_subfield and (properties or fields):
            raise ValueError("Invalid declaration")

        key = "object" if self.KEY is None else self.KEY
        node = self.get_node_dsl_class(key)(name, **kwargs)
        if is_subfield:
            node.is_subfield = True
        self.insert(node)
        if isinstance(properties, dict):
            # dict syntax
            for name, body in iteritems(properties):
                type_ = body.get("type", "object")
                sub_node = self.get_dsl_class(type_)(
                    name=name, is_subfield=is_subfield, **body
                )
                self.insert(sub_node, node.identifier)
        elif properties is None:
            pass
        else:
            # node syntax
            if not isinstance(properties, (list, tuple)):
                properties = (properties,)
            for p in properties:
                if not isinstance(p, (Field, Mapping)):
                    raise ValueError("Wrong mapping property: %s" % type(p))
                self.insert(p, node.identifier)

        if isinstance(fields, dict):
            # dict syntax
            for name, body in iteritems(fields):
                type_ = body.get("type", "object")
                sub_node = self.get_dsl_class(type_)(
                    name=name, is_subfield=True, **body
                )
                self.insert(sub_node, node.identifier)
        elif fields is None:
            pass
        else:
            # node syntax
            if not isinstance(fields, (list, tuple)):
                fields = (fields,)
            for f in fields:
                if isinstance(f, Field):
                    f.is_subfield = True
                    self.insert(f, node.identifier)
                elif isinstance(f, Mapping):
                    f.get(f.root).is_subfield = True
                    self.insert(f, node.identifier)
                else:
                    raise ValueError("Wrong mapping field: %s" % type(f))

    def __nonzero__(self):
        return not self.is_empty()

    __bool__ = __nonzero__

    def to_dict(self, from_=None, depth=None, root=True):
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
                    from_=child_node.identifier, depth=depth, root=False
                )
        serialized_node = node.body
        if children_queries:
            if isinstance(node, ComplexField):
                serialized_node["properties"] = children_queries
            elif isinstance(node, Field):
                serialized_node["fields"] = children_queries
        if root:
            serialized_node.pop("type", None)
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
