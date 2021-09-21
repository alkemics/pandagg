from typing import Tuple, Dict, Any

from pandagg import Mappings
from pandagg.node.mappings import Field, ComplexField


class DocumentMeta(type):
    def __new__(cls, name: str, bases: Tuple, attrs: Dict) -> "DocumentMeta":
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

        regular_attrs["__init__"] = __init__
        return super(DocumentMeta, cls).__new__(cls, name, bases, regular_attrs)


class DocumentSource(metaclass=DocumentMeta):

    # __init__ is overidden by metaclass, this is only so that pycharm doesn't highlights fake errors when instantiating
    # documents
    def __init__(self, **kwargs: Any) -> None:
        pass

    def _post_init_(self) -> None:
        # intended to be overwritten
        # apply transformations after instantiation
        # note: it applies both when instantiating manually documents, and when documents are instantiated while
        # deserializing ElasticSearch search response
        pass

    def _pre_save_op_(self) -> None:
        # intended to be overwritten
        # apply transformations before any persisting operation (create / index / update)
        # for instance to update 'last_updated_at' date
        pass

    def _to_dict_(self, with_empty_keys: bool = False) -> Dict[str, Any]:
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
    pass
