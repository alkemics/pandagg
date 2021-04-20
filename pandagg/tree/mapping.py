#!/usr/bin/env python
# -*- coding: utf-8 -*-

from future.utils import iteritems

from pandagg.node.mapping.abstract import Field, RegularField, ComplexField


from pandagg.exceptions import (
    AbsentMappingFieldError,
    InvalidOperationMappingFieldError,
)
from pandagg.tree._tree import Tree


def _mapping(m):
    if m is None:
        return None
    if isinstance(m, dict):
        return Mapping(**m)
    if isinstance(m, Mapping):
        return m
    raise TypeError("Unsupported %s type for Mapping" % type(m))


class Mapping(Tree):

    node_class = Field
    KEY = None

    def __init__(self, properties=None, dynamic=False, **kwargs):
        """"""
        super(Mapping, self).__init__()
        root_node = Field(dynamic=dynamic, **kwargs)
        self.insert_node(root_node)
        if properties:
            self._insert(root_node.identifier, properties, False)

    def to_dict(self, from_=None, depth=None):
        """
        Serialize Mapping as dict.

        :param from_: identifier of a field, if provided, limits serialization to this field and its
        children (used for recursion, shouldn't be useful)
        :param depth: integer, if provided, limit the serialization to a given depth
        :return: dict
        """
        if self.root is None:
            return None
        from_ = self.root if from_ is None else from_
        key, node = self.get(from_)
        children_queries = {}
        if depth is None or depth > 0:
            if depth is not None:
                depth -= 1
            for child_key, child_node in self.children(node.identifier):
                children_queries[child_key] = self.to_dict(
                    from_=child_node.identifier, depth=depth
                )
        serialized_node = node.body
        if children_queries:
            if node.KEY is None or node.KEY in ("object", "nested"):
                serialized_node["properties"] = children_queries
            else:
                serialized_node["fields"] = children_queries
        return serialized_node

    def validate_agg_clause(self, agg_clause, exc=True):
        """
        Ensure that if aggregation clause relates to a field (`field` or `path`) this field exists in mapping, and that
        required aggregation type is allowed on this kind of field.

        :param agg_clause: AggClause you want to validate on this mapping
        :param exc: boolean, if set to True raise exception if invalid
        :rtype: boolean
        """
        if hasattr(agg_clause, "path"):
            if agg_clause.path is None:
                # reverse nested
                return True
            return self.resolve_path_to_id(agg_clause.path) in self

        if not hasattr(agg_clause, "field"):
            return True

        # TODO take into account flattened data type
        try:
            nid = self.get_node_id_by_path(agg_clause.field)
        except StopIteration:
            raise AbsentMappingFieldError(
                u"Agg of type <%s> on non-existing field <%s>."
                % (agg_clause.KEY, agg_clause.field)
            )
        _, field = self.get(nid)

        field_type = field.KEY
        if not agg_clause.valid_on_field_type(field_type):
            if not exc:
                return False
            raise InvalidOperationMappingFieldError(
                u"Agg of type <%s> not possible on field of type <%s>."
                % (agg_clause.KEY, field_type)
            )
        return True

    def mapping_type_of_field(self, field_path):
        """
        Return field type of provided field path.

        >>> mapping = Mapping(dynamic=False, properties={
        >>>     'id': {'type': 'keyword'},
        >>>     'comments': {'type': 'nested', 'properties': {
        >>>         'comment_text': {'type': 'text'},
        >>>         'date': {'type': 'date'}
        >>>     }}
        >>> })
        >>> mapping.mapping_type_of_field('id')
        'keyword'
        >>> mapping.mapping_type_of_field('comments')
        'nested'
        >>> mapping.mapping_type_of_field('comments.comment_text')
        'text'
        """
        try:
            _, node = self.get(field_path, by_path=True)
            return node.KEY
        except Exception:
            raise AbsentMappingFieldError(
                u"<%s field is not present in mapping>" % field_path
            )

    def nested_at_field(self, field_path):
        """
        Return nested path applied on a given path. Return `None` is none applies.

        >>> mapping = Mapping(dynamic=False, properties={
        >>>     'id': {'type': 'keyword'},
        >>>     'comments': {'type': 'nested', 'properties': {
        >>>         'comment_text': {'type': 'text'},
        >>>         'date': {'type': 'date'}
        >>>     }}
        >>> })
        >>> mapping.nested_at_field('id')
        None
        >>> mapping.nested_at_field('comments')
        'comments'
        >>> mapping.nested_at_field('comments.comment_text')
        'comments'
        """
        nesteds = self.list_nesteds_at_field(field_path)
        if nesteds:
            return nesteds[0]
        return None

    def list_nesteds_at_field(self, field_path):
        """
        List nested paths that apply at a given path.

        >>> mapping = Mapping(dynamic=False, properties={
        >>>     'id': {'type': 'keyword'},
        >>>     'comments': {'type': 'nested', 'properties': {
        >>>         'comment_text': {'type': 'text'},
        >>>         'date': {'type': 'date'}
        >>>     }}
        >>> })
        >>> mapping.list_nesteds_at_field('id')
        []
        >>> mapping.list_nesteds_at_field('comments')
        ['comments']
        >>> mapping.list_nesteds_at_field('comments.comment_text')
        ['comments']
        """
        path_nid = self.get_node_id_by_path(field_path)
        # from deepest to highest
        return [
            self.get_path(nid)
            for nid in self.ancestors_ids(path_nid, include_current=True)
            if self.get(nid)[1].KEY == "nested"
        ]

    def _insert(self, pid, properties, is_subfield):
        """
        Recursive method to insert properties in current mapping.

        :param pid: parent field identifier
        :param properties: fields definitions that are inserted below pid
        :param is_subfield: are provided properties `fields` mapping parameter, cf
        https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
        """
        if not isinstance(properties, dict):
            raise ValueError("Wrong declaration, got %s" % properties)
        for field_name, field in iteritems(properties):
            if isinstance(field, dict):
                field = field.copy()
                field = Field._get_dsl_class(field.pop("type", "object"))(
                    _subfield=is_subfield, **field
                )
            elif isinstance(field, Field):
                field._subfield = is_subfield
                pass
            else:
                raise ValueError("Unsupported type %s" % type(field))
            self.insert_node(field, key=field_name, parent_id=pid)
            if isinstance(field, ComplexField) and field.properties:
                self._insert(field.identifier, field.properties, False)
            if isinstance(field, RegularField) and field.fields:
                if is_subfield:
                    raise ValueError(
                        "Cannot insert subfields into a subfield on field %s"
                        % field_name
                    )
                self._insert(field.identifier, field.fields, True)
