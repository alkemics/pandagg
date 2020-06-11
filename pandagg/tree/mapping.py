#!/usr/bin/env python
# -*- coding: utf-8 -*-

from future.utils import iteritems
from lighttree.exceptions import NotFoundNodeError

from pandagg.node.mapping.abstract import (
    Field,
    UnnamedField,
    UnnamedRegularField,
    UnnamedComplexField,
)


from pandagg.exceptions import (
    AbsentMappingFieldError,
    InvalidOperationMappingFieldError,
)
from pandagg.tree._tree import Tree


class Mapping(Tree):

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
                # Mapping(None)
                return
            if isinstance(arg, Mapping):
                # Mapping(Mapping())
                self.insert(arg)
                return
            if isinstance(arg, dict):
                # Mapping({"properties": {}})
                kwargs = arg.copy()
            else:
                raise ValueError(
                    "Wrong declaration: args %s, kwargs %s" % (args, kwargs)
                )

        # Mapping(dynamic=False, properties={...}}
        properties = kwargs.pop("properties", None)
        dynamic = kwargs.pop("dynamic", False)

        # root
        root_node = Field("_", "_", dynamic=dynamic)
        self.insert_node(root_node)
        if properties:
            self._insert(root_node.identifier, properties, False)

    def _insert(self, pid, el, is_subfield):
        if not isinstance(el, dict):
            raise ValueError("Wrong declaration, got %s" % el)
        for name, field in iteritems(el):
            if isinstance(field, dict):
                field = field.copy()
                field = UnnamedField.get_dsl_class(field.pop("type", "object"))(**field)
            if not isinstance(field, UnnamedField):
                raise ValueError("Unsupported type %s" % type(field))
            node = field.to_named_field(name, _subfield=is_subfield)
            self.insert_node(node, parent_id=pid)
            if isinstance(field, UnnamedComplexField) and field.properties:
                self._insert(node.identifier, field.properties, False)
            if isinstance(field, UnnamedRegularField) and field.fields:
                if is_subfield:
                    raise ValueError(
                        "Cannot insert subfields into a subfield on field %s" % name
                    )
                self._insert(node.identifier, field.fields, True)

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
            if node.KEY in ("_", "object", "nested"):
                serialized_node["properties"] = children_queries
            else:
                serialized_node["fields"] = children_queries
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
