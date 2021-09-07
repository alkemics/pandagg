# adapted from elasticsearch-dsl.py

import re

from mock import Mock
from unittest import SkipTest
from pytest import fixture, skip

from elasticsearch.helpers import bulk
from elasticsearch.helpers.test import get_test_client


from .test_data import TEST_GIT_DATA, create_git_index, GIT_MAPPINGS


@fixture(scope="session")
def client():
    try:
        return get_test_client()
    except SkipTest:
        skip()


@fixture(scope="session")
def es_version(client):
    info = client.info()
    print(info)
    yield tuple(
        int(x)
        for x in re.match(r"^([0-9.]+)", info["version"]["number"]).group(1).split(".")
    )


@fixture
def mock_client(dummy_response):
    client = Mock()
    client.search.return_value = dummy_response
    return client


@fixture
def git_mappings():
    return GIT_MAPPINGS


@fixture(scope="session")
def data_client(client):
    client.indices.delete("git", ignore=(404,))
    # create mappings
    create_git_index(client, "git")
    # load data
    bulk(client, TEST_GIT_DATA, raise_on_error=True, refresh=True)
    yield client
    client.indices.delete("git")


@fixture
def dummy_response():
    return {
        "_shards": {"failed": 0, "successful": 10, "total": 10},
        "hits": {
            "hits": [
                {
                    "_index": "test-index",
                    "_id": "elasticsearch",
                    "_score": 12.0,
                    "_source": {"city": "Amsterdam", "name": "Elasticsearch"},
                },
                {
                    "_index": "test-index",
                    "_id": "42",
                    "_score": 11.123,
                    "_source": {
                        "name": {"first": "Shay", "last": "Bannon"},
                        "lang": "java",
                        "twitter": "kimchy",
                    },
                },
                {
                    "_index": "test-index",
                    "_id": "47",
                    "_score": 1,
                    "_source": {
                        "name": {"first": "Honza", "last": "Král"},
                        "lang": "python",
                        "twitter": "honzakral",
                    },
                },
                {"_index": "test-index", "_id": "53", "_score": 16.0},
            ],
            "max_score": 12.0,
            "total": 123,
        },
        "timed_out": False,
        "took": 123,
    }


@fixture
def updatable_index(client):
    index = "test-git"
    create_git_index(client, index)
    bulk(client, TEST_GIT_DATA, raise_on_error=True, refresh=True)
    yield index
    client.indices.delete(index, ignore=404)
