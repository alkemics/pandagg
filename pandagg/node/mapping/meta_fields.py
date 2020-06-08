#!/usr/bin/env python
# -*- coding: utf-8 -*-
from .abstract import UnnamedField


# Identity meta fields
class Index(UnnamedField):
    """The index to which the document belongs."""

    KEY = "_index"


class Type(UnnamedField):
    """The document’s mapping type."""

    KEY = "_type"


class Id(UnnamedField):
    """The document’s ID."""

    KEY = "_id"


# Document source meta-fields
class Source(UnnamedField):
    """The original JSON representing the body of the document."""

    KEY = "_source"


class Size(UnnamedField):
    """The size of the _source field in bytes, provided by the mapper-size plugin."""

    KEY = "_size"


# Indexing meta-fields
class FieldNames(UnnamedField):
    """All fields in the document which contain non-null values."""

    KEY = "_field_names"


class Ignored(UnnamedField):
    """All fields in the document that have been ignored at index time because of ignore_malformed."""

    KEY = "_ignored"


# Routing meta-field
class Routing(UnnamedField):
    """A custom routing value which routes a document to a particular shard."""

    KEY = "_routing"


# Other meta-field
class Meta(UnnamedField):
    """Application specific metadata."""

    KEY = "_meta"
