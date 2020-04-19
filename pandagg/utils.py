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


def bool_if_required(conditions, operator="must"):
    assert operator in ("must", "should")
    # wrap conditions in bool only if necessary
    if len(conditions) == 1:
        return conditions[0]
    if len(conditions) > 1:
        flattened_sub_conditions = []
        for sub_condition in conditions:
            if (
                "bool" in sub_condition
                and len(sub_condition["bool"].keys()) == 1
                and list(sub_condition["bool"].keys())[0] == operator
            ):
                operator_cond = list(sub_condition["bool"].values())[0]
                # both are valid: {"must": [query_A, query_B]}, or {"must": queryA}
                if isinstance(operator_cond, list):
                    flattened_sub_conditions.extend(operator_cond)
                else:
                    flattened_sub_conditions.append(operator_cond)
            else:
                flattened_sub_conditions.append(sub_condition)
        return {"bool": {operator: flattened_sub_conditions}}
    return None
