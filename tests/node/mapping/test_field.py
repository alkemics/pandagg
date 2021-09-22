from pandagg.document import DocumentSource, InnerDocSource
from pandagg.node.mappings import Text, Keyword, Nested, Object
from pandagg.node.mappings.abstract import Root


class Comment(InnerDocSource):
    content = Text()
    author_name = Keyword()


class Post(DocumentSource):
    comments: Nested(Comment)


def test_nested_with_document_class():
    field = Nested(Comment)
    assert field._document is Comment
    assert field.properties == {
        "author_name": {"type": "keyword"},
        "content": {"type": "text"},
    }


def test_complex_field():
    n = Nested(properties={"title": Keyword(), "description": Text()}, path="comments")
    assert not hasattr(n, "_document")
    assert isinstance(n.properties, dict)
    assert isinstance(n.properties["title"], Keyword)
    assert n.to_dict() == {"type": "nested", "path": "comments"}


def test_field():
    field = Keyword(doc_values=False)
    assert field.to_dict() == {"type": "keyword", "doc_values": False}
    assert field.fields is None
    assert field._body == {"doc_values": False}
    assert field._multiple is None
    assert field._required is False
    assert field._source_only is False

    other_field = Keyword(required=True, multiple=True, source_only=True)
    assert other_field._multiple is True
    assert other_field._required is True
    assert other_field._source_only is True
    assert other_field._body == {}


def test_subfield():
    field = Keyword(fields={"raw": Text()}, doc_values=False)
    assert field.to_dict() == {"type": "keyword", "doc_values": False}
    assert isinstance(field.fields, dict)
    assert isinstance(field.fields["raw"], Text)
    assert field._body == {"doc_values": False}


def test_field_repr():
    assert Root().line_repr(depth=0) == ("_", "")
    assert Keyword(doc_values=False).line_repr(depth=0) == ("", "  Keyword")
    assert Nested(path="comments").line_repr(depth=0) == ("", " [Nested]")
    assert Object(dynamic=False).line_repr(depth=0) == ("", " {Object}")
