# adapted from elasticsearch-dsl-py

import pytest

from pandagg import Mappings, Search
from pandagg.mappings import Keyword, Text, Date
from pandagg.index import DeclarativeIndex


class Post(DeclarativeIndex):
    name = "test-post"
    mappings = {"properties": {"title": Text(), "published_from": Date()}}
    settings = {"number_of_shards": 1}
    aliases = {"post": {}}


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
    assert MyIndex._mappings.to_dict() == {
        "dynamic": False,
        "properties": {"name": {"type": "keyword"}},
    }


def test_index_without_client_raises_error_on_write_op():
    index = Post()
    with pytest.raises(ValueError) as e:
        index.save()
    assert e.value.args == (
        "An Elasticsearch client must be provided in order to execute queries.",
    )


def test_create_index(write_client):
    assert not write_client.indices.exists("test-post")
    index = Post(client=write_client)
    index.save()
    assert write_client.indices.exists("test-post")
    persisted_index = write_client.indices.get("test-post")["test-post"]
    assert persisted_index["aliases"] == {"post": {}}
    assert persisted_index["mappings"] == {
        "dynamic": "false",
        "properties": {"published_from": {"type": "date"}, "title": {"type": "text"}},
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
    assert len(index.docs._operations) == 1
    index.docs.rollback()
    assert len(index.docs._operations) == 0
