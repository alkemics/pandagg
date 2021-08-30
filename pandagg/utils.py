# adapted from https://github.com/elastic/elasticsearch-dsl-py/blob/master/elasticsearch_dsl/utils.py#L162
from typing import Dict, Tuple, Any


class DslMeta(type):
    """
    Base Metaclass for DslBase subclasses that builds a registry of all classes
    for given DslBase subclass (== all the query types for the Query subclass
    of DslBase).

    Types will be: 'agg', 'query', 'field'

    Each of those types will hold a `_classes` dictionary pointing to all classes of same type.
    """

    # registry for types
    _types: Dict[str, "DslMeta"] = {}
    # per type, registry for classes (not initialized here)
    _classes: Dict[str, "DslMeta"]

    # types keys
    _type_name: str = ""
    # classes keys
    KEY: str = ""

    def __init__(cls, name: str, bases: Tuple, attrs: Dict) -> None:
        super(DslMeta, cls).__init__(name, bases, attrs)
        if not cls._type_name:
            # skip for DSLMixin
            return
        if not cls.KEY:
            # abstract base class, register its shortcut
            cls._types[cls._type_name] = cls
            # and create a registry for subclasses
            if not hasattr(cls, "_classes"):
                cls._classes = {}
        elif cls.KEY not in cls._classes:
            # normal class, register it
            cls._classes[cls.KEY] = cls


class DSLMixin(metaclass=DslMeta):
    """Base class for all DSL objects - queries, filters, aggregations etc. Wraps
    a dictionary representing the object's json."""

    @classmethod
    def get_dsl_class(cls, name: str) -> DslMeta:
        try:
            return cls._classes[name]
        except KeyError:
            raise NotImplementedError(
                "DSL class `{}` does not exist in {}.".format(name, cls._type_name)
            )

    @staticmethod
    def get_dsl_type(name: str) -> DslMeta:
        try:
            return DslMeta._types[name]
        except KeyError:
            raise ValueError("DSL type %s does not exist." % name)


def ordered(obj: Any) -> Any:
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    return obj


def equal_queries(d1: Any, d2: Any) -> bool:
    """Compares if two queries are equivalent (do not consider nested list orders)."""
    return ordered(d1) == ordered(d2)


def equal_search(s1: Any, s2: Any) -> bool:
    if not isinstance(s1, dict) or not isinstance(s2, dict):
        raise ValueError("not a search")
    s1 = s1.copy()
    s2 = s2.copy()
    # sort order matters
    if not s1.pop("sort", None) == s2.pop("sort", None):
        return False
    return equal_queries(s1, s2)
