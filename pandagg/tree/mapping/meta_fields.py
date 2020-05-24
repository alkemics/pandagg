#!/usr/bin/env python
# -*- coding: utf-8 -*-
from .mapping import Mapping


# Identity meta fields
class Index(Mapping):
    """The index to which the document belongs."""

    KEY = "_index"


class Type(Mapping):
    """The document’s mapping type."""

    KEY = "_type"


class Id(Mapping):
    """The document’s ID."""

    KEY = "_id"


# Document source meta-fields
class Source(Mapping):
    """The original JSON representing the body of the document."""

    KEY = "_source"


class Size(Mapping):
    """The size of the _source field in bytes, provided by the mapper-size plugin."""

    KEY = "_size"


# Indexing meta-fields
class FieldNames(Mapping):
    """All fields in the document which contain non-null values."""

    KEY = "_field_names"


class Ignored(Mapping):
    """All fields in the document that have been ignored at index time because of ignore_malformed."""

    KEY = "_ignored"


# Routing meta-field
class Routing(Mapping):
    """A custom routing value which routes a document to a particular shard."""

    KEY = "_routing"


# Other meta-field
class Meta(Mapping):
    """Application specific metadata."""

    KEY = "_meta"
