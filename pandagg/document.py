from typing import Any, Dict, Tuple

from pandagg import Mappings
from pandagg.node.mappings import ComplexField, Field


class DocumentMeta(type):

    """`DocumentSource` metaclass.

    Note: you shouldn't have to use it directly, see `DocumentSource` instead.
    """

    def __new__(cls, name: str, bases: Tuple, attrs: Dict) -> "DocumentMeta":
        """Document metaclass is responsible for:

        - registering `Fields`:
            - as a `Mappings` instance in `_mappings_` attribute
            - as a dict of `Fields` in `_field_attrs_`
        - keeping other attributes and methods as is
        - building `__init__` to accepts only those declared fields at `DocumentSource` instanciation

        In following example::

            class RestaurantInspection(DocumentSource):

                description = "Represent a new-york restaurant inspection."

                name = Text()
                borough = Keyword()
                cuisine = Keyword()

            document = RestaurantInspection(
                name="Almighty burger",
                borough="Brooklyn"
            )

        The `description` attribute isn't a `Field` instance, and won't be considered as a valid input at document
        instanciation.
        """
        regular_attrs: Dict = {}
        field_attrs: Dict = {}
        for k, v in attrs.items():
            if isinstance(v, Field):
                field_attrs[k] = v
                # field are instantiated with null value, instead of Field instance
                if v._multiple:
                    regular_attrs[k] = []
                else:
                    regular_attrs[k] = None
            else:
                # other class attributes that are not Fields are kept intact
                regular_attrs[k] = v

        regular_attrs["_field_attrs_"] = field_attrs
        regular_attrs["_mappings_"] = Mappings(properties=field_attrs)

        def __init__(self: "DocumentSource", **kwargs: Any) -> None:
            for k, v in kwargs.items():
                if k not in field_attrs.keys():
                    raise TypeError(
                        "%r is an invalid keyword argument for %s" % (k, type(self))
                    )
                setattr(self, k, v)
            self._post_init_()

        def __str__(self: "DocumentSource") -> str:
            return "{}({})".format(
                self.__class__.__name__,
                ", ".join(
                    "{}={!r}".format(key, getattr(self, key))
                    for key in field_attrs.keys()
                ),
            )

        regular_attrs["__init__"] = __init__
        regular_attrs["__str__"] = __str__
        regular_attrs["__repr__"] = __str__
        return super(DocumentMeta, cls).__new__(cls, name, bases, regular_attrs)


class DocumentSource(metaclass=DocumentMeta):

    """
    Model-like class for persisting documents in elasticsearch.

    It is both used for mappings declaration, and for document manipulation and persistence::

        class RestaurantInspection(DocumentSource):
            name = Text()
            borough = Keyword()
            cuisine = Keyword()
            grade = Keyword()
            score = Integer()
            location = GeoPoint()
            inspection_date = Date(format="MM/dd/yyyy")

    This document can be referenced in a DeclarativeIndex class to declare index mappings::

        class NYCRestaurants(DeclarativeIndex):
            name = "nyc-restaurants"
            document = RestaurantInspection



    Note: these mappings will be used to create index mappings when using DeclarativeIndex `save` method.

    It is possible to serialize mappings via `_mappings.to_dict` method::

        >>> NYCRestaurants._mappings.to_dict()
        {
            'properties': {
                'name': {'type': 'text'},
                'borough': {'type': 'keyword'},
                'cuisine': {'type': 'keyword'},
                'grade': {'type': 'keyword'},
                'score': {'type': 'integer'},
                'location': {'type': 'geo_point'},
                'inspection_date': {'format': 'MM/dd/yyyy', 'type': 'date'}
            }
        }

    """

    def __init__(self, **kwargs: Any) -> None:
        """Overridden by metaclass, it is declared here only so that pycharm doesn't highlight fake errors when
        instantiating documents.
        """

    def _post_init_(self) -> None:
        """Intended to be overwritten.
        Apply transformations after document instantiation. Executed both when manually instantiating documents, and
        when documents are instantiated while deserializing ElasticSearch search response.
        """

    def _pre_save_op_(self) -> None:
        """Intended to be overwritten.
        Apply transformations before any persisting operation (create / index / update).

        Example: update 'last_updated_at' date.
        """

    def _to_dict_(self, with_empty_keys: bool = False) -> Dict[str, Any]:
        """
        Serialize document as a json-compatible python dict.

        :param with_empty_keys: if True, empty field will be serialized with `None` value.
        """
        d: Dict[str, Any] = {}
        k: str
        field: Field
        for k, field in self._field_attrs_.items():  # type: ignore
            v = getattr(self, k)
            if with_empty_keys or v not in (None, []):
                if isinstance(v, list):
                    vs = []
                    for va in v:
                        if isinstance(va, DocumentSource):
                            va = va._to_dict_(with_empty_keys)
                        vs.append(va)
                    d[k] = vs
                    continue
                if isinstance(v, DocumentSource):
                    v = v._to_dict_(with_empty_keys=with_empty_keys)
                d[k] = v
        return d

    @classmethod
    def _from_dict_(
        cls, source: Dict, strict: bool = True, path: str = ""
    ) -> "DocumentSource":
        """
        Deserialize document source into a Document instance.

        :param source: document source (python dict) to deserialize.
        :param strict: if True, check that fields declared with `multiple=True` or `multiple=False` have the intended
        shape (fields with `multiple=True` are expected to contain a list or values, fields with `multiple=False` are
        expected to contain a single value.)
        """
        doc = cls()
        k: str
        field: Field
        for k, field in cls._field_attrs_.items():  # type: ignore
            v = source.get(k)
            child_path = k if not path else "%s.%s" % (path, k)

            if isinstance(v, list):
                if field._multiple is False and strict:
                    raise TypeError(
                        "Unexpected list for field %s, got %s" % (child_path, v)
                    )
                # remove null values
                v = [a for a in v if a is not None]
                # even if field is not declared as multiple, we set it up as a multiple (and ignore eventual typing
                # hints)
                if isinstance(field, ComplexField):
                    children = [
                        field._document._from_dict_(  # type: ignore
                            a, strict=strict, path=child_path
                        )
                        for a in v
                    ]
                else:
                    children = v
                setattr(doc, k, children)
                continue
            # single element
            if field._multiple and strict:
                raise TypeError("Expected list for field %s, got %s" % (child_path, v))
            if isinstance(field, ComplexField):
                child = field._document._from_dict_(  # type: ignore
                    v, strict=strict, path=child_path
                )
            else:
                child = v
            setattr(doc, k, child)
        return doc


class InnerDocSource(DocumentSource):

    """
    Common class for inner documents like Object or Nested
    """
