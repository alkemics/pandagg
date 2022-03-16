from pandagg.node.query import MatchAll, MatchNone


def test_match_all_clause():
    q = MatchAll()
    assert q.body == {}
    assert q.to_dict() == {"match_all": {}}
    assert q.line_repr(depth=None) == ("match_all", "")

    q = MatchAll(boost=0.5)
    assert q.body == {"boost": 0.5}
    assert q.to_dict() == {"match_all": {"boost": 0.5}}
    assert q.line_repr(depth=None) == ("match_all", "boost=0.5")


def test_match_none_clause():
    q = MatchNone()
    assert q.body == {}
    assert q.to_dict() == {"match_none": {}}
    assert q.line_repr(depth=None) == ("match_none", "")
