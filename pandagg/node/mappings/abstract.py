import json

from pandagg.node._node import Node
from typing import Optional, Any, Tuple, Dict, Type

from pandagg.types import FieldType


class Field(Node):

    _classes: Dict[FieldType, Type["Field"]]

    _type_name = "field"
    KEY: str

    def __init__(
        self, multiple: Optional[bool] = None, nullable: bool = True, **body: Any
    ) -> None:
        """
        :param multiple: boolean, default None, if True field must be an array, if False field must be a single item
        :param nullable: boolean, default True, if False a `None` value will be considered as invalid.
        :param body: field body
        """
        super(Node, self).__init__()
        self._subfield = body.pop("_subfield", False)
        self._body = body.copy()
        self._multiple = multiple
        self._nullable = nullable

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        return "", self._display_pattern % self.KEY.capitalize()

    def is_valid_value(self, v: Any) -> bool:
        raise NotImplementedError()

    @property
    def body(self) -> Dict[str, Any]:
        b = self._body.copy()
        if self.KEY in ("object", ""):
            return b
        b["type"] = self.KEY
        return b

    @property
    def _display_pattern(self) -> str:
        if self.KEY == "object":
            return " {%s}"
        if self.KEY == "nested":
            return " [%s]"
        if self._subfield:
            return "~ %s"
        return "  %s"

    def __str__(self) -> str:
        return "<%s field>:\n%s" % (
            str(self.KEY).capitalize(),
            str(json.dumps(self.body, indent=4)),
        )


class ComplexField(Field):
    def __init__(self, **body: Any) -> None:
        properties = body.pop("properties", None) or {}
        if not isinstance(properties, dict):
            raise ValueError("Invalid properties %s" % properties)
        self.properties: Dict[str, Any] = properties
        super(ComplexField, self).__init__(**body)

    def is_valid_value(self, v: Any) -> bool:
        return isinstance(v, dict)


class RegularField(Field):
    def __init__(self, **body: Any) -> None:
        fields = body.pop("fields", None)
        if fields and not isinstance(fields, dict):
            raise ValueError("Invalid fields %s" % fields)
        self.fields = fields
        super(RegularField, self).__init__(**body)

    def is_valid_value(self, v: Any) -> bool:
        # TODO - implement per field type
        return True


class Root(Field):
    # used as root node for mappings
    KEY = ""

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        return "_", ""
