#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals


# adapted from https://github.com/elastic/elasticsearch-dsl-py/blob/master/elasticsearch_dsl/utils.py#L162
class DslMeta(type):
    """
    Base Metaclass for DslBase subclasses that builds a registry of all classes
    for given DslBase subclass (== all the query types for the Query subclass
    of DslBase).

    It then uses the information from that registry (as well as `name` and
    `deserializer` attributes from the base class) to construct any subclass based
    on it's name.
    """

    _types = {}

    def __init__(cls, name, bases, attrs):
        super(DslMeta, cls).__init__(name, bases, attrs)
        # skip for DslBase
        if not hasattr(cls, "_type_name") or cls._type_name is None:
            return

        key = cls.KEY
        if key is not None and hasattr(cls, "_prefix"):
            key = "%s%s" % (cls._prefix, key)

        if key is None:
            # abstract base class, register it's shortcut
            cls._types[cls._type_name] = cls._type_deserializer
            # and create a registry for subclasses
            if not hasattr(cls, "_classes"):
                cls._classes = {}
        elif key not in cls._classes:
            # normal class, register it
            cls._classes[key] = cls

    @classmethod
    def get_dsl_type(cls, name):
        try:
            return cls._types[name]
        except KeyError:
            raise ValueError("DSL type %s does not exist." % name)


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


def equal_queries(d1, d2):
    """Compares if two queries are equivalent (do not consider nested list orders).
    """
    return ordered(d1) == ordered(d2)
