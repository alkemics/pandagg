#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .abstract import Field


# Identity meta fields
class Index(Field):
    """The index to which the document belongs."""

    KEY = "_index"


class Type(Field):
    """The document’s mappings type."""

    KEY = "_type"


class Id(Field):
    """The document’s ID."""

    KEY = "_id"


# Document source meta-fields
class Source(Field):
    """The original JSON representing the body of the document."""

    KEY = "_source"


class Size(Field):
    """The size of the _source field in bytes, provided by the mapper-size plugin."""

    KEY = "_size"


# Indexing meta-fields
class FieldNames(Field):
    """All fields in the document which contain non-null values."""

    KEY = "_field_names"


class Ignored(Field):
    """All fields in the document that have been ignored at index time because of ignore_malformed."""

    KEY = "_ignored"


# Routing meta-field
class Routing(Field):
    """A custom routing value which routes a document to a particular shard."""

    KEY = "_routing"


# Other meta-field
class Meta(Field):
    """Application specific metadata."""

    KEY = "_meta"
