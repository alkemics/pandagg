from copy import deepcopy

from mock import patch

from elasticsearch import Elasticsearch

from pandagg import Aggregations
from pandagg.node.aggs import Max, DateHistogram, Sum
from pandagg.search import Search
from pandagg.query import Query, Bool, Match
from pandagg.tree.mappings import Mappings
from pandagg.utils import ordered, equal_queries


class TestSearch:
    def test_expand__to_dot_is_respected(self):
        s = Search().query("match", a__b=42, _expand__to_dot=False)

        assert {"query": {"match": {"a__b": {"query": 42}}}} == s.to_dict()

    def test_search_query_combines_query(self):
        s = Search()

        s2 = s.query("match", f=42)
        assert s2._query.to_dict() == Query(Match(f=42)).to_dict()
        assert s._query.to_dict() is None

        s3 = s2.query("match", f=43)
        assert s2._query.to_dict() == Query(Match(f=42)).to_dict()
        assert ordered(s3._query.to_dict()) == ordered(
            Query(Bool(must=[Match(f=42), Match(f=43)])).to_dict()
        )

    def test_search_column_selection(self):
        mappings = Mappings(
            properties={"col1": {"type": "keyword"}, "col2": {"type": "integer"}}
        )
        assert Search(mappings=mappings)[["col1", "col2"]].to_dict() == {
            "_source": {"includes": ["col1", "col2"]}
        }

    def test_using(self):
        o = object()
        o2 = object()
        s = Search(using=o)
        assert s._using is o
        s2 = s.using(o2)
        assert s._using is o
        assert s2._using is o2

    def test_query_always_returns_search(self):
        s = Search()
        assert isinstance(s.query("match", f=42), Search)

    def test_source_copied_on_clone(self):
        s = Search().source(False)

        assert s._clone()._source == s._source
        assert s._clone()._source is False

        s2 = Search().source([])
        assert s2._clone()._source == s2._source
        assert s2._source == []

        s3 = Search().source(["some", "fields"])
        assert s3._clone()._source == s3._source
        assert s3._clone()._source == ["some", "fields"]

    def test_copy_clones(self):
        from copy import copy

        s1 = Search().source(["some", "fields"])
        s2 = copy(s1)

        assert s1 == s2
        assert s1 is not s2

    def test_aggs_allow_two_metric(self):
        s = Search()

        s = s.aggs({"a": Max(field="a"), "b": Max(field="b")})

        assert s.to_dict() == {
            "aggs": {"a": {"max": {"field": "a"}}, "b": {"max": {"field": "b"}}}
        }

    def test_search_index(self):
        s = Search(index="i")
        assert s._index == ["i"]
        s = s.index("i2")
        assert s._index == ["i", "i2"]
        s = s.index(u"i3")
        assert s._index == ["i", "i2", "i3"]
        s = s.index()
        assert s._index is None
        s = Search(index=("i", "i2"))
        assert s._index == ["i", "i2"]
        s = Search(index=["i", "i2"])
        assert s._index == ["i", "i2"]
        s = Search()
        s = s.index("i", "i2")
        assert s._index == ["i", "i2"]
        s2 = s.index("i3")
        assert s._index == ["i", "i2"]
        assert s2._index == ["i", "i2", "i3"]
        s = Search()
        s = s.index(["i", "i2"], "i3")
        assert s._index == ["i", "i2", "i3"]
        s2 = s.index("i4")
        assert s._index == ["i", "i2", "i3"]
        assert s2._index == ["i", "i2", "i3", "i4"]
        s2 = s.index(["i4"])
        assert s2._index == ["i", "i2", "i3", "i4"]
        s2 = s.index(("i4", "i5"))
        assert s2._index == ["i", "i2", "i3", "i4", "i5"]

    def test_sort(self):
        s = Search()
        s = s.sort("fielda", "-fieldb")

        assert ["fielda", {"fieldb": {"order": "desc"}}] == s._sort
        assert {"sort": ["fielda", {"fieldb": {"order": "desc"}}]} == s.to_dict()

        s = s.sort()
        assert [] == s._sort
        assert Search().to_dict() == s.to_dict()

    def test_slice(self):
        s = Search()
        assert {"from": 3, "size": 7} == s[3:10].to_dict()
        assert {"from": 0, "size": 5} == s[:5].to_dict()
        assert {"from": 3, "size": 10} == s[3:].to_dict()
        assert {"from": 0, "size": 0} == s[0:0].to_dict()

    def test_index(self):
        s = Search()
        assert {"from": 3, "size": 1} == s[3].to_dict()

    def test_search_to_dict(self):
        s = Search()
        assert {} == s.to_dict()

        s = s.query("match", f=42)
        assert {"query": {"match": {"f": {"query": 42}}}} == s.to_dict()

        assert {"query": {"match": {"f": {"query": 42}}}, "size": 10} == s.to_dict(
            size=10
        )

        s = s.groupby("per_tag", "terms", field="f").agg(
            "max_score", "max", field="score"
        )
        d = {
            "aggs": {
                "per_tag": {
                    "terms": {"field": "f"},
                    "aggs": {"max_score": {"max": {"field": "score"}}},
                }
            },
            "query": {"match": {"f": {"query": 42}}},
        }
        assert d == s.to_dict()

        s = Search().params(size=5)
        assert {"size": 5} == s.to_dict()
        s = s.params(from_=42)
        assert {"size": 5, "from": 42} == s.to_dict()

    def test_complex_example(self):
        s = (
            Search()
            .query("match", title="python")
            .must_not("match", title="ruby")
            .should("term", category="meetup")
            .should("term", category="conference")
            .post_filter("terms", tags=["prague", "czech"])
            .script_fields(more_attendees="doc['attendees'].value + 42")
            .groupby("per_country", "terms", field="country")
            .agg("avg_attendees", "avg", field="attendees")
            .bool(minimum_should_match=2)
            .highlight_options(order="score")
            .highlight("title", "body", fragment_size=50)
        )

        assert equal_queries(
            {
                "aggs": {
                    "per_country": {
                        "aggs": {"avg_attendees": {"avg": {"field": "attendees"}}},
                        "terms": {"field": "country"},
                    }
                },
                "highlight": {
                    "fields": {
                        "body": {"fragment_size": 50},
                        "title": {"fragment_size": 50},
                    },
                    "order": "score",
                },
                "post_filter": {"terms": {"tags": ["prague", "czech"]}},
                "query": {
                    "bool": {
                        "minimum_should_match": 2,
                        "must": [{"match": {"title": {"query": "python"}}}],
                        "must_not": [{"match": {"title": {"query": "ruby"}}}],
                        "should": [
                            {"term": {"category": {"value": "conference"}}},
                            {"term": {"category": {"value": "meetup"}}},
                        ],
                    }
                },
                "script_fields": {
                    "more_attendees": {"script": "doc['attendees'].value + 42"}
                },
            },
            s.to_dict(),
        )

    def test_reverse(self):
        d = {
            "aggs": {
                "per_country": {
                    "aggs": {"avg_attendees": {"avg": {"field": "attendees"}}},
                    "terms": {"field": "country"},
                }
            },
            "highlight": {"fields": {"title": {"fragment_size": 50}}, "order": "score"},
            "post_filter": {
                "bool": {"must": [{"terms": {"tags": ["prague", "czech"]}}]}
            },
            "query": {
                "bool": {
                    "filter": [
                        {
                            "bool": {
                                "should": [
                                    {"term": {"category": {"value": "meetup"}}},
                                    {"term": {"category": {"value": "conference"}}},
                                ]
                            }
                        }
                    ],
                    "must": [
                        {
                            "bool": {
                                "minimum_should_match": 2,
                                "must": [{"match": {"title": {"query": "python"}}}],
                                "must_not": [{"match": {"title": {"query": "ruby"}}}],
                            }
                        }
                    ],
                }
            },
            "script_fields": {
                "more_attendees": {"script": "doc['attendees'].value + 42"}
            },
            "size": 5,
            "sort": ["title", {"category": {"order": "desc"}}, "_score"],
            "suggest": {
                "my-title-suggestions-1": {
                    "term": {"field": "title", "size": 3},
                    "text": "devloping distibutd saerch " "engies",
                }
            },
        }

        d2 = deepcopy(d)

        s = Search.from_dict(d)

        # make sure we haven't modified anything in place
        assert d == d2
        assert d == s.to_dict()

    def test_from_dict_doesnt_need_query(self):
        s = Search.from_dict({"size": 5})

        assert {"size": 5} == s.to_dict()

    def test_source(self):
        assert {} == Search().source().to_dict()

        assert {
            "_source": {"includes": ["foo.bar.*"], "excludes": ["foo.one"]}
        } == Search().source(includes=["foo.bar.*"], excludes=["foo.one"]).to_dict()

        assert {"_source": False} == Search().source(False).to_dict()

        assert {"_source": ["f1", "f2"]} == Search().source(
            includes=["foo.bar.*"], excludes=["foo.one"]
        ).source(["f1", "f2"]).to_dict()

    def test_source_on_clone(self):
        assert {
            "_source": {"includes": ["foo.bar.*"], "excludes": ["foo.one"]},
            "query": {"bool": {"filter": [{"term": {"title": {"value": "python"}}}]}},
        } == Search().source(includes=["foo.bar.*"]).source(
            excludes=["foo.one"]
        ).filter(
            "term", title="python"
        ).to_dict()

        assert {
            "_source": False,
            "query": {"bool": {"filter": [{"term": {"title": {"value": "python"}}}]}},
        } == Search().source(False).filter("term", title="python").to_dict()

    def test_source_on_clear(self):
        assert (
            {}
            == Search()
            .source(includes=["foo.bar.*"])
            .source(includes=None, excludes=None)
            .to_dict()
        )

    def test_suggest_accepts_global_text(self):
        s = Search.from_dict(
            {
                "suggest": {
                    "text": "the amsterdma meetpu",
                    "my-suggest-1": {"term": {"field": "title"}},
                    "my-suggest-2": {"text": "other", "term": {"field": "body"}},
                }
            }
        )

        assert {
            "suggest": {
                "my-suggest-1": {
                    "term": {"field": "title"},
                    "text": "the amsterdma meetpu",
                },
                "my-suggest-2": {"term": {"field": "body"}, "text": "other"},
            }
        } == s.to_dict()

    def test_suggest(self):
        s = Search()
        s = s.suggest("my_suggestion", "pyhton", term={"field": "title"})

        assert {
            "suggest": {"my_suggestion": {"term": {"field": "title"}, "text": "pyhton"}}
        } == s.to_dict()

    def test_exclude(self):
        s = Search()
        s = s.exclude("match", title="python")

        assert {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "bool": {
                                "must_not": [{"match": {"title": {"query": "python"}}}]
                            }
                        }
                    ]
                }
            }
        } == s.to_dict()

    def test_update_from_dict(self):
        s = Search()
        s.update_from_dict({"indices_boost": [{"important-documents": 2}]})
        s.update_from_dict({"_source": ["id", "name"]})

        assert {
            "indices_boost": [{"important-documents": 2}],
            "_source": ["id", "name"],
        } == s.to_dict()

    @patch.object(Elasticsearch, "search")
    def test_repr_execution(self, client_search):
        client_search.return_value = {
            "took": 42,
            "timed_out": False,
            "_shards": {"total": 10, "successful": 10, "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": 34, "relation": "eq"},
                "max_score": 0.0,
                "hits": [
                    {
                        "_index": "my_index_01",
                        "_type": "_doc",
                        "_id": "1",
                        "_score": 1.0,
                        "_source": {"field_23": 1},
                    },
                    {
                        "_index": "my_index_01",
                        "_type": "_doc",
                        "_id": "2",
                        "_score": 0.2,
                        "_source": {"field_23": 2},
                    },
                ],
            },
        }
        s = Search(
            using=Elasticsearch(hosts=["..."]), index="yolo", repr_auto_execute=True
        )

        s.size(2).__repr__()
        client_search.assert_called_once()
        client_search.assert_any_call(body={"size": 2}, index=["yolo"])

        client_search.reset_mock()

        s.size(2)._repr_html_()
        client_search.assert_called_once()
        client_search.assert_any_call(body={"size": 2}, index=["yolo"])

    @patch.object(Elasticsearch, "search")
    def test_repr_aggs_execution(self, client_search):
        client_search.return_value = {
            "took": 42,
            "timed_out": False,
            "_shards": {"total": 10, "successful": 10, "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": 34, "relation": "eq"},
                "max_score": 0.0,
                "hits": [],
            },
            "aggregations": {
                "toto_terms": {
                    "buckets": [
                        {
                            "doc_count": 12,
                            "key": "toto_1",
                            "toto_avg_price": {"value": 50.2},
                        },
                        {
                            "doc_count": 15,
                            "key": "toto_2",
                            "toto_avg_price": {"value": 10.3},
                        },
                    ]
                }
            },
        }
        s = (
            Search(
                using=Elasticsearch(hosts=["..."]), index="yolo", repr_auto_execute=True
            )
            .groupby("toto_terms", "terms", field="toto")
            .agg("toto_avg_price", "avg", field="price")
        )

        # when aggs are present, repr dataframe of aggs, with size 0 hits
        s.__repr__()
        client_search.assert_called_once()
        client_search.assert_any_call(
            body={
                "size": 0,
                "aggs": {
                    "toto_terms": {
                        "terms": {"field": "toto"},
                        "aggs": {"toto_avg_price": {"avg": {"field": "price"}}},
                    }
                },
            },
            index=["yolo"],
        )

    def test_scan_composite_agg(self, data_client, git_mappings):
        search = (
            Search(using=data_client, index="git", mappings=git_mappings)
            .groupby(
                "compatible_histogram",
                DateHistogram(
                    field="authored_date", calendar_interval="1d", key_as_string=True
                ),
            )
            .agg("insertions_sum", Sum(field="stats.insertions"))
        )

        bucket_iterator = search.scan_composite_agg(size=5)
        assert hasattr(bucket_iterator, "__iter__")
        buckets = list(bucket_iterator)
        assert buckets == [
            {
                "doc_count": 2,
                "insertions_sum": {"value": 120.0},
                "key": {"compatible_histogram": 1394409600000},
            },
            {
                "doc_count": 4,
                "insertions_sum": {"value": 45.0},
                "key": {"compatible_histogram": 1394841600000},
            },
            {
                "doc_count": 2,
                "insertions_sum": {"value": 34.0},
                "key": {"compatible_histogram": 1395360000000},
            },
            {
                "doc_count": 2,
                "insertions_sum": {"value": 36.0},
                "key": {"compatible_histogram": 1395532800000},
            },
            {
                "doc_count": 10,
                "insertions_sum": {"value": 376.0},
                "key": {"compatible_histogram": 1395619200000},
            },
            {
                "doc_count": 2,
                "insertions_sum": {"value": 13.0},
                "key": {"compatible_histogram": 1397952000000},
            },
            {
                "doc_count": 2,
                "insertions_sum": {"value": 35.0},
                "key": {"compatible_histogram": 1398124800000},
            },
            {
                "doc_count": 3,
                "insertions_sum": {"value": 187.0},
                "key": {"compatible_histogram": 1398384000000},
            },
            {
                "doc_count": 2,
                "insertions_sum": {"value": 103.0},
                "key": {"compatible_histogram": 1398470400000},
            },
            {
                "doc_count": 2,
                "insertions_sum": {"value": 29.0},
                "key": {"compatible_histogram": 1398556800000},
            },
            {
                "doc_count": 2,
                "insertions_sum": {"value": 62.0},
                "key": {"compatible_histogram": 1398902400000},
            },
            {
                "doc_count": 1,
                "insertions_sum": {"value": 23.0},
                "key": {"compatible_histogram": 1398988800000},
            },
        ]

    def test_scan_composite_agg_at_once(self, data_client, git_mappings):
        search = (
            Search(using=data_client, index="git", mappings=git_mappings)
            .groupby(
                "compatible_histogram",
                DateHistogram(
                    field="authored_date", calendar_interval="1d", key_as_string=False
                ),
            )
            .groupby("author", "terms", field="author.name.raw")
            .agg("insertions_sum", Sum(field="stats.insertions"))
        )

        agg_response = search.scan_composite_agg_at_once(size=5)
        assert isinstance(agg_response, Aggregations)
        assert agg_response.to_tabular(index_orient=True) == (
            ["compatible_histogram", "author"],
            {
                (1394409600000, "Honza Král"): {
                    "doc_count": 2,
                    "insertions_sum": 120.0,
                },
                (1394841600000, "Honza Král"): {"doc_count": 4, "insertions_sum": 45.0},
                (1395360000000, "Honza Král"): {"doc_count": 2, "insertions_sum": 34.0},
                (1395532800000, "Honza Král"): {"doc_count": 2, "insertions_sum": 36.0},
                (1395619200000, "Honza Král"): {
                    "doc_count": 10,
                    "insertions_sum": 376.0,
                },
                (1397952000000, "Honza Král"): {"doc_count": 2, "insertions_sum": 13.0},
                (1398124800000, "Honza Král"): {"doc_count": 2, "insertions_sum": 35.0},
                (1398384000000, "Honza Král"): {
                    "doc_count": 3,
                    "insertions_sum": 187.0,
                },
                (1398470400000, "Honza Král"): {
                    "doc_count": 2,
                    "insertions_sum": 103.0,
                },
                (1398556800000, "Honza Král"): {"doc_count": 2, "insertions_sum": 29.0},
                (1398902400000, "Honza Král"): {"doc_count": 2, "insertions_sum": 62.0},
                (1398988800000, "Honza Král"): {"doc_count": 1, "insertions_sum": 23.0},
            },
        )
