from pandagg.query import (
    Wrapper,
    DistanceFeature,
    MoreLikeThis,
    Percolate,
    RankFeature,
    Script,
)


def test_distance_feature_clause():
    body = {"field": "production_date", "pivot": "7d", "origin": "now"}
    expected = {"distance_feature": body}

    q = DistanceFeature(field="production_date", pivot="7d", origin="now")
    assert q.body == body
    assert q.to_dict() == expected
    assert q.line_repr(depth=None) == (
        "distance_feature",
        'field="production_date", origin="now", pivot="7d"',
    )


def test_more_like_this_clause():
    body = {
        "fields": ["title", "description"],
        "like": "Once upon a time",
        "min_term_freq": 1,
        "max_query_terms": 12,
    }
    expected = {"more_like_this": body}

    q = MoreLikeThis(
        fields=["title", "description"],
        like="Once upon a time",
        min_term_freq=1,
        max_query_terms=12,
    )
    assert q.body == body
    assert q.to_dict() == expected
    assert q.line_repr(depth=None) == (
        "more_like_this",
        "fields=['title', 'description']",
    )


def test_percolate_clause():
    body = {
        "field": "query",
        "document": {"message": "A new bonsai tree in the office"},
    }
    expected = {"percolate": body}

    q = Percolate(
        field="query", document={"message": "A new bonsai tree in the office"}
    )
    assert q.body == body
    assert q.to_dict() == expected
    assert q.line_repr(depth=None) == (
        "percolate",
        'document={"message": "A new bonsai tree in the office"}, field="query"',
    )


def test_rank_feature_clause():
    body = {"field": "url_length", "boost": 0.1}
    expected = {"rank_feature": body}

    q = RankFeature(field="url_length", boost=0.1)
    assert q.body == body
    assert q.to_dict() == expected
    assert q.line_repr(depth=None) == ("rank_feature", 'boost=0.1, field="url_length"')


def test_script_clause():
    body = {
        "script": {
            "source": "doc['num1'].value > params.param1",
            "lang": "painless",
            "params": {"param1": 5},
        }
    }
    expected = {"script": body}

    q = Script(
        script={
            "source": "doc['num1'].value > params.param1",
            "lang": "painless",
            "params": {"param1": 5},
        }
    )
    assert q.body == body
    assert q.to_dict() == expected
    assert q.line_repr(depth=None) == (
        "script",
        'script={"lang": "painless", "params": {"param1": 5}, "source": '
        "\"doc['num1'].value > params.param1\"}",
    )


def test_wrapper_clause():
    body = {"query": "eyJ0ZXJtIiA6IHsgInVzZXIiIDogIktpbWNoeSIgfX0="}
    expected = {"wrapper": body}

    q = Wrapper(query="eyJ0ZXJtIiA6IHsgInVzZXIiIDogIktpbWNoeSIgfX0=")
    assert q.body == body
    assert q.to_dict() == expected
    assert q.line_repr(depth=None) == (
        "wrapper",
        'query="eyJ0ZXJtIiA6IHsgInVzZXIiIDogIktpbWNoeSIgfX0="',
    )
