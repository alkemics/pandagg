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

        if key is None:
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
        try:
            return sorted(ordered(x) for x in obj)
        except:
            # can only happen on sort query parameter, for instance, where order matters
            # ['title', {'category': {'order': 'desc'}}, '_score']
            pass
    return obj


def equal_queries(d1, d2):
    """Compares if two queries are equivalent (do not consider nested list orders).
    """
    return ordered(d1) == ordered(d2)
