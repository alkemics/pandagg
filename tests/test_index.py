# adapted from elasticsearch-dsl-py

import pytest

from pandagg import Mappings, Search
from pandagg.document import DocumentSource
from pandagg.mappings import Keyword, Text, Date
from pandagg.index import DeclarativeIndex, DeclarativeIndexTemplate


class Post(DeclarativeIndex):
    name = "test-post"
    mappings = {"properties": {"title": Text(), "published_from": Date()}}
    settings = {"number_of_shards": 1}
    aliases = {"post": {}}


class PostTemplate(DeclarativeIndexTemplate):
    name = "test-template"
    index_patterns = "test-post*"
    template = {
        "mappings": {"properties": {"title": Text(), "published_from": Date()}},
        "settings": {"number_of_shards": 1},
        "aliases": {"post": {}},
    }


def test_declarative_without_name_raises():
    with pytest.raises(ValueError) as e:

        class MyIndex(DeclarativeIndex):
            pass

    assert e.value.args == ("<MyIndex> declarative index must have a name",)


def test_declarative_copy_of_parameters():
    ini_mappings = {"properties": {"name": Keyword()}}
    ini_settings = {}
    ini_aliases = {}

    class MyIndex(DeclarativeIndex):
        name = "my-index"
        mappings = ini_mappings
        settings = ini_settings
        aliases = ini_aliases

    assert MyIndex.mappings is not ini_mappings
    assert MyIndex.settings is not ini_settings
    assert MyIndex.aliases is not ini_aliases
    assert isinstance(MyIndex._mappings, Mappings)
    assert MyIndex._mappings.to_dict() == {"properties": {"name": {"type": "keyword"}}}


def test_index_without_client_raises_error_on_write_op():
    index = Post()
    with pytest.raises(ValueError) as e:
        index.save()
    assert e.value.args == (
        "An Elasticsearch client must be provided in order to execute queries.",
    )


def test_create_index(write_client):
    assert not write_client.indices.exists(index="test-post")
    index = Post(client=write_client)
    index.save()
    assert write_client.indices.exists(index="test-post")
    persisted_index = write_client.indices.get(index="test-post")["test-post"]
    assert persisted_index["aliases"] == {"post": {}}
    assert persisted_index["mappings"] == {
        "properties": {"published_from": {"type": "date"}, "title": {"type": "text"}}
    }
    assert persisted_index["settings"]["index"]["number_of_shards"] == "1"


def test_index_search():
    mock = object()
    index = Post(client=mock)
    search = index.search()
    assert isinstance(search, Search)
    assert search._index == ["test-post"]
    assert search._mappings.to_dict() == Post._mappings.to_dict()
    assert search._using is mock


def test_docwriter(write_client):
    index = Post(client=write_client)
    assert not index.exists()
    index.save()
    assert index.exists()

    index.docs.index(
        _id="my_post_article",
        _source={"title": "salut", "published_from": "2021-01-01"},
    )
    index.docs.index(
        _id="my_second_post_article",
        _source={"title": "re-salut", "published_from": "2021-01-02"},
    )
    index.docs.index(
        _source={"title": "salut-without-id", "published_from": "2021-01-03"}
    )
    res = index.docs.perform(refresh=True)
    assert res == (3, [])

    assert index.search().execute().hits.total == {"relation": "eq", "value": 3}
    index.docs.update(
        _id="my_second_post_article", doc={"title": "au revoir", "unmapped_field": 1}
    )
    assert index.docs.perform(refresh=True) == (1, [])
    response = index.search().query("ids", values=["my_second_post_article"]).execute()
    assert response.hits.total == {"relation": "eq", "value": 1}
    assert response.hits.hits[0]._source == {
        "published_from": "2021-01-02",
        "title": "au revoir",
        "unmapped_field": 1,
    }

    assert index.docs.delete("my_second_post_article").perform(refresh=True) == (1, [])
    assert index.search().query(
        "ids", values=["my_second_post_article"]
    ).execute().hits.total == {"relation": "eq", "value": 0}

    index.docs.delete("my_second_post_article")
    index.docs.rollback()
    assert len(list(index.docs._operations)) == 0


def test_docwriter_document_instances(write_client):
    index = Post(client=write_client)
    assert not index.exists()
    index.save()
    assert index.exists()

    index.docs.index(
        _id="my_post_article",
        _source={"title": "salut", "published_from": "2021-01-01"},
    )
    index.docs.index(
        _id="my_second_post_article",
        _source={"title": "re-salut", "published_from": "2021-01-02"},
    )
    index.docs.index(
        _source={"title": "salut-without-id", "published_from": "2021-01-03"}
    )
    res = index.docs.perform(refresh=True)
    assert res == (3, [])

    assert index.search().execute().hits.total == {"relation": "eq", "value": 3}
    index.docs.update(
        _id="my_second_post_article", doc={"title": "au revoir", "unmapped_field": 1}
    )
    assert index.docs.perform(refresh=True) == (1, [])
    response = index.search().query("ids", values=["my_second_post_article"]).execute()
    assert response.hits.total == {"relation": "eq", "value": 1}
    assert response.hits.hits[0]._source == {
        "published_from": "2021-01-02",
        "title": "au revoir",
        "unmapped_field": 1,
    }

    assert index.docs.delete("my_second_post_article").perform(refresh=True) == (1, [])
    assert index.search().query(
        "ids", values=["my_second_post_article"]
    ).execute().hits.total == {"relation": "eq", "value": 0}

    index.docs.delete("my_second_post_article")
    index.docs.rollback()
    assert len(list(index.docs._operations)) == 0


def test_docwriter_bulk(write_client):
    index = Post(client=write_client)
    index.save()

    def action_iterator():
        for a in [
            {"_id": 1, "_source": {"title": "salut"}},
            {"_id": 2, "_source": {"title": "au revoir"}},
        ]:
            yield a

    index.docs.bulk(actions=action_iterator())
    assert list(index.docs._operations) == [
        {"_id": 1, "_index": "test-post", "_source": {"title": "salut"}},
        {"_id": 2, "_index": "test-post", "_source": {"title": "au revoir"}},
    ]

    index.docs.bulk(actions=action_iterator(), _op_type_overwrite="index")
    assert list(index.docs._operations) == [
        {
            "_id": 1,
            "_index": "test-post",
            "_op_type": "index",
            "_source": {"title": "salut"},
        },
        {
            "_id": 2,
            "_index": "test-post",
            "_op_type": "index",
            "_source": {"title": "au revoir"},
        },
    ]


def test_index_template_invalid():
    with pytest.raises(ValueError) as e:

        class MyIndexTemplate(DeclarativeIndexTemplate):
            pass

    assert e.value.args == (
        "<MyIndexTemplate> declarative index template must have a name",
    )

    with pytest.raises(ValueError) as e:

        class MyIndexTemplate2(DeclarativeIndexTemplate):
            name = "template_2"

    assert e.value.args == (
        "<MyIndexTemplate2> declarative index template must have index_patterns",
    )


def test_template_declarative_copy_of_parameters():
    ini_template = {
        "mappings": {"properties": {"name": Keyword()}},
        "settings": {"number_of_shards": 1},
    }

    class MyIndexTemplate(DeclarativeIndexTemplate):
        name = "my_template"
        index_patterns = ["my_template-*"]
        template = {
            "mappings": {"properties": {"name": Keyword()}},
            "settings": {"number_of_shards": 1},
        }
        priority = 50
        version = 1

    assert MyIndexTemplate.template is not ini_template
    assert isinstance(MyIndexTemplate._template_index, DeclarativeIndex)
    assert MyIndexTemplate._template_index.to_dict() == {
        "mappings": {"properties": {"name": {"type": "keyword"}}},
        "settings": {"number_of_shards": 1},
    }


def test_template_save(write_client):
    template = PostTemplate(write_client)
    post_index = Post(write_client)
    assert not post_index.exists()
    assert not template.exists()
    template.save()
    assert template.exists()
    post_index.docs.index(
        _id="my_post_article",
        _source={"title": "salut", "published_from": "2021-01-01"},
    ).perform(refresh=True)
    assert post_index.exists()
    auto_created_index = write_client.indices.get(index="test-post")["test-post"]
    assert auto_created_index["mappings"] == {
        "properties": {"published_from": {"type": "date"}, "title": {"type": "text"}}
    }
    assert auto_created_index["settings"]["index"]["number_of_shards"] == "1"
    assert auto_created_index["aliases"] == {"post": {}}


def test_index_docwriter_has_pending_operation():
    index = Post()
    assert not index.docs.has_pending_operation()
    index.docs.index(
        _id="my_post_article",
        _source={"title": "salut", "published_from": "2021-01-01"},
    )
    assert index.docs.has_pending_operation()
    # assert it is still present afterwards (iterator is not consumed)
    assert index.docs.has_pending_operation()
    index.docs.rollback()
    assert not index.docs.has_pending_operation()


def test_index_mappings_consistency_with_document():
    with pytest.raises(TypeError) as e:

        class Post(DeclarativeIndex):
            name = "test-post"
            document = "yolo"

    assert e.value.args == (
        "<Post> declarative index 'document' attribute must be a <class "
        "'pandagg.document.DocumentSource'> subclass, got <yolo> of type <class "
        "'str'>",
    )

    # shouldn't raise error
    class PostDocument(DocumentSource):
        title = Text()
        published_from = Date()

    class PostIndex(DeclarativeIndex):
        name = "test-post"
        mappings = {
            "properties": {
                "title": Text(),
                "published_from": Date(),
                "created_at": Date(),
            }
        }
        document = PostDocument

    # equals the declared mappings (> Document)
    assert PostIndex._mappings.to_dict() == {
        "properties": {
            "created_at": {"type": "date"},
            "published_from": {"type": "date"},
            "title": {"type": "text"},
        }
    }

    with pytest.raises(TypeError) as e:
        # invalid because more mappings elements in mappings than in document
        class InvalidPostIndex(DeclarativeIndex):
            name = "test-post"
            mappings = {"properties": {"title": Text()}}
            document = PostDocument

    assert e.value.args == (
        "Incompatible index declaration, mappings and document do not match.",
    )

    # only document is provided
    class ValidPostIndex(DeclarativeIndex):
        name = "test-post"
        document = PostDocument

    assert ValidPostIndex._mappings.to_dict() == {
        "properties": {"published_from": {"type": "date"}, "title": {"type": "text"}}
    }

    # test that Document is forwared to search
    index = ValidPostIndex()
    s = index.search(deserialize_source=True)
    assert s._document_class is PostDocument

    s = index.search(deserialize_source=False)
    assert s._document_class is None
