from typing_extensions import TypedDict
from typing import Optional, Union, Any, List, Dict

from lighttree.node import NodeId
from lighttree import Tree
from pandagg.node.aggs.abstract import AggClause
from pandagg.node.mappings import Object, Nested
from pandagg.node.mappings.abstract import Field, RegularField, ComplexField, Root

from pandagg.exceptions import (
    AbsentMappingFieldError,
    InvalidOperationMappingFieldError,
)
from pandagg.tree._tree import TreeReprMixin
from pandagg.types import DocSource, MappingsDict, FieldName, FieldClauseDict


FieldPropertiesDictOrNode = Dict[FieldName, Union[FieldClauseDict, Field]]


class MappingsDictOrNode(TypedDict, total=False):
    properties: FieldPropertiesDictOrNode
    dynamic: bool


def _mappings(
    m: Optional[Union[MappingsDict, MappingsDictOrNode, "Mappings"]]
) -> Optional["Mappings"]:
    if m is None:
        return None
    if isinstance(m, dict):
        return Mappings(**m)
    if isinstance(m, Mappings):
        return m
    raise TypeError("Unsupported %s type for Mappings" % type(m))


class Mappings(TreeReprMixin, Tree[Field]):
    def __init__(
        self,
        properties: Optional[FieldPropertiesDictOrNode] = None,
        dynamic: bool = False,
        **body: Any
    ) -> None:
        super(Mappings, self).__init__()
        # a Mappings always has a root after __init__
        self.root: str
        root_node = Root(dynamic=dynamic, **body)
        self.insert_node(node=root_node)
        if properties:
            self._insert(
                pid=root_node.identifier, properties=properties, is_subfield=False
            )

    def to_dict(
        self, from_: Optional[NodeId] = None, depth: Optional[int] = None
    ) -> MappingsDict:
        """
        Serialize Mappings as dict.

        :param from_: identifier of a field, if provided, limits serialization to this field and its
        children (used for recursion, shouldn't be useful)
        :param depth: integer, if provided, limit the serialization to a given depth
        :return: dict
        """
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
            if isinstance(node, Root) or node.KEY in ("object", "nested"):
                serialized_node["properties"] = children_queries
            else:
                serialized_node["fields"] = children_queries
        return serialized_node

    def validate_agg_clause(self, agg_clause: AggClause, exc: bool = True) -> bool:
        """
        Ensure that if aggregation clause relates to a field (`field` or `path`) this field exists in mappings, and that
        required aggregation type is allowed on this kind of field.

        :param agg_clause: AggClause you want to validate on these mappings
        :param exc: boolean, if set to True raise exception if invalid
        :rtype: boolean
        """
        if hasattr(agg_clause, "path"):
            agg_path: Optional[str] = agg_clause.path  # type: ignore
            if agg_path is None:
                # reverse nested
                return True
            try:
                # nested
                self.get_node_id_by_path(agg_path.split("."))
                return True
            except Exception:
                return False

        if not hasattr(agg_clause, "field"):
            return True

        agg_field: str = agg_clause.field  # type: ignore

        # TODO take into account flattened data type
        try:
            nid = self.get_node_id_by_path(agg_field.split("."))
        except Exception:
            raise AbsentMappingFieldError(
                u"Agg of type <%s> on non-existing field <%s>."
                % (agg_clause.KEY, agg_field)
            )
        _, field_node = self.get(nid)

        field_type = field_node.KEY
        if not agg_clause.valid_on_field_type(field_type):
            if not exc:
                return False
            raise InvalidOperationMappingFieldError(
                u"Agg of type <%s> not possible on field of type <%s>."
                % (agg_clause.KEY, field_type)
            )
        return True

    def mapping_type_of_field(self, field_path: str) -> str:
        """
        Return field type of provided field path.

        >>> mappings = Mappings(dynamic=False, properties={
        >>>     'id': {'type': 'keyword'},
        >>>     'comments': {'type': 'nested', 'properties': {
        >>>         'comment_text': {'type': 'text'},
        >>>         'date': {'type': 'date'}
        >>>     }}
        >>> })
        >>> mappings.mapping_type_of_field('id')
        'keyword'
        >>> mappings.mapping_type_of_field('comments')
        'nested'
        >>> mappings.mapping_type_of_field('comments.comment_text')
        'text'
        """
        try:
            nid = self.get_node_id_by_path(field_path.split("."))
        except ValueError:
            raise AbsentMappingFieldError(
                u"<%s field is not present in mappings>" % field_path
            )
        _, node = self.get(nid)
        return node.KEY

    def nested_at_field(self, field_path: str) -> Optional[str]:
        """
        Return nested path applied on a given path. Return `None` is none applies.

        >>> mappings = Mappings(dynamic=False, properties={
        >>>     'id': {'type': 'keyword'},
        >>>     'comments': {'type': 'nested', 'properties': {
        >>>         'comment_text': {'type': 'text'},
        >>>         'date': {'type': 'date'}
        >>>     }}
        >>> })
        >>> mappings.nested_at_field('id')
        None
        >>> mappings.nested_at_field('comments')
        'comments'
        >>> mappings.nested_at_field('comments.comment_text')
        'comments'
        """
        nesteds = self.list_nesteds_at_field(field_path)
        if nesteds:
            return nesteds[0]
        return None

    def list_nesteds_at_field(self, field_path: str) -> List[str]:
        """
        List nested paths that apply at a given path.

        >>> mappings = Mappings(dynamic=False, properties={
        >>>     'id': {'type': 'keyword'},
        >>>     'comments': {'type': 'nested', 'properties': {
        >>>         'comment_text': {'type': 'text'},
        >>>         'date': {'type': 'date'}
        >>>     }}
        >>> })
        >>> mappings.list_nesteds_at_field('id')
        []
        >>> mappings.list_nesteds_at_field('comments')
        ['comments']
        >>> mappings.list_nesteds_at_field('comments.comment_text')
        ['comments']
        """
        path_nid = self.get_node_id_by_path(field_path.split("."))
        # from deepest to highest
        return [
            # all path items are strings
            ".".join(self.get_path(nid))  # type: ignore
            for nid in self.ancestors_ids(path_nid, include_current=True)
            if self.get(nid)[1].KEY == "nested"
        ]

    def _insert(
        self, pid: NodeId, properties: FieldPropertiesDictOrNode, is_subfield: bool
    ) -> None:
        """
        Recursive method to insert properties in current mappings.

        :param pid: parent field identifier
        :param properties: fields definitions that are inserted below pid
        :param is_subfield: are provided properties `fields` mappings parameter, cf
        https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
        """
        if not isinstance(properties, dict):
            raise ValueError("Wrong declaration, got %s" % properties)

        field: Field
        for field_name, field_ in properties.items():
            if isinstance(field_, dict):
                field_ = field_.copy()
                field = Field.get_dsl_class(field_.pop("type", "object"))(
                    _subfield=is_subfield, **field_
                )
            elif isinstance(field_, Field):
                field = field_
                field._subfield = is_subfield
                pass
            else:
                raise ValueError("Unsupported type %s" % type(field_))
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

    def validate_document(self, d: DocSource) -> None:
        self._validate_document(d, pid=self.root)

    def _validate_document(self, d: Any, pid: NodeId, path: str = "") -> None:
        if d is None:
            d = {}
        if not isinstance(d, dict):
            raise ValueError(
                "Invalid document type, expected dict, got <%s> at '%s'"
                % (type(d), path)
            )
        field_name: str
        for field_name, field in self.children(pid):  # type: ignore
            full_path = ".".join([path, field_name]) if path else field_name
            field_value = d.get(field_name)
            if not field._nullable and not field_value:
                raise ValueError("Field <%s> cannot be null" % full_path)

            if field._multiple is True:
                if field_value is not None:
                    if not isinstance(field_value, list):
                        raise ValueError("Field <%s> should be a array" % full_path)
                    field_value_list = field_value
                else:
                    field_value_list = []
                if not field._nullable and not any(field_value_list):
                    # deal with case: [None]
                    raise ValueError("Field <%s> cannot be null" % full_path)
            elif field._multiple is False:
                if isinstance(field_value, list):
                    raise ValueError("Field <%s> should not be an array" % full_path)
                field_value_list = [field_value] if field_value else []
            else:
                # field._multiple is None -> no restriction
                if isinstance(field_value, list):
                    field_value_list = field_value
                else:
                    field_value_list = [field_value]

            for value in field_value_list:
                # nullable check has been done beforehands
                if value:
                    if not field.is_valid_value(value):
                        raise ValueError(
                            "Field <%s> value <%s> is not compatible with field of type %s"
                            % (full_path, value, field.KEY)
                        )
                if isinstance(field, (Object, Nested)):
                    self._validate_document(value, field.identifier, path=full_path)
