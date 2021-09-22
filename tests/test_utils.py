import pytest

from pandagg.document import DocumentSource
from pandagg.node.mappings import Text, Keyword
from pandagg.utils import equal_queries, equal_search, is_subset, get_action_modifier


def test_equal():
    q1 = {"bool": {"must": [{"term": {"field_A": 1}}, {"term": {"field_B": 2}}]}}
    q2 = {"bool": {"must": [{"term": {"field_B": 2}}, {"term": {"field_A": 1}}]}}
    non_equal_q = {
        "bool": {"must": [{"term": {"field_B": 2}}, {"term": {"field_A": 123}}]}
    }
    assert equal_queries(q1, q2)
    assert not equal_queries(q1, non_equal_q)

    assert equal_search(
        {"query": q1, "sort": ["title", {"category": {"order": "desc"}}, "_score"]},
        {"query": q2, "sort": ["title", {"category": {"order": "desc"}}, "_score"]},
    )
    assert not equal_search(
        {"query": q1, "sort": ["title", {"category": {"order": "desc"}}, "_score"]},
        {"query": q2, "sort": ["title", "_score", {"category": {"order": "desc"}}]},
    )


def test_is_subset():
    assert is_subset(1, 1)
    assert is_subset({1}, {1, 2})
    assert not is_subset({1, 2}, {1})
    assert is_subset({"1": 1}, {"1": 1, "2": 2})
    assert not is_subset({"1": 1, "3": 3}, {"1": 1, "2": 2})
    assert is_subset([1, 2], [3, 2, 1])
    assert not is_subset([1, 2, 5], [3, 2, 1])


def test_get_action_modifier():
    modifier = get_action_modifier(index_name="test-index")
    update_modifier = get_action_modifier(
        index_name="test-index", _op_type_overwrite="update"
    )

    # simple source
    assert modifier({"_source": {"stuff": 1}}) == {
        "_index": "test-index",
        "_source": {"stuff": 1},
    }
    assert update_modifier({"_id": 1, "doc": {"stuff": 1}}) == {
        "_id": 1,
        "_index": "test-index",
        "_op_type": "update",
        "doc": {"stuff": 1},
    }

    class Article(DocumentSource):
        name = Text()
        type = Keyword()

    # document
    article = Article(name="hello", type="test")
    assert modifier(article) == {
        "_index": "test-index",
        "_source": {"name": "hello", "type": "test"},
    }
    assert modifier({"_source": article}) == {
        "_index": "test-index",
        "_source": {"name": "hello", "type": "test"},
    }

    with pytest.raises(TypeError) as e:
        update_modifier(article)
    assert e.value.args == ("Update operation requires an '_id'",)
    assert update_modifier({"doc": article, "_id": 1}) == {
        "_index": "test-index",
        "_id": 1,
        "_op_type": "update",
        "doc": {"name": "hello", "type": "test"},
    }
