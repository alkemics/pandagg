# adapted from elasticsearch-dsl/search.py

from __future__ import annotations

import copy
import json
from typing import (
    Optional,
    Union,
    Tuple,
    List,
    Any,
    TypeVar,
    Dict,
    Iterator,
    TYPE_CHECKING,
)

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from pandagg.node.aggs.abstract import TypeOrAgg
from pandagg.query import Bool
from pandagg.response import SearchResponse, Hit, Aggregations
from pandagg.tree.mappings import _mappings, Mappings
from pandagg.tree.query import (
    Query,
    ADD,
    TypeOrQuery,
    InsertionModes,
    SingleOrMultipleQueryClause,
)
from pandagg.tree.aggs import Aggs, AggsDictOrNode
from pandagg.types import (
    MappingsDict,
    QueryName,
    ClauseBody,
    AggName,
    SearchResponseDict,
    DeleteByQueryResponse,
    SearchDict,
    BucketDict,
    AfterKey,
)
from pandagg.utils import DSLMixin

if TYPE_CHECKING:
    import pandas as pd

# because Search.bool method shadows bool typing
bool_ = bool

T = TypeVar("T", bound="Request")


class Request:
    def __init__(
        self: T,
        using: Optional[Elasticsearch],
        index: Optional[Union[str, Tuple[str], List[str]]] = None,
    ) -> None:
        self._using: Optional[Elasticsearch] = using
        self._index: Optional[List[str]] = None

        if isinstance(index, (tuple, list)):
            self._index = list(index)
        elif index:
            self._index = [index]

        self._params: Dict[str, Any] = {}

    def _get_connection(self) -> Elasticsearch:
        if self._using is None:
            raise ValueError(
                "An Elasticsearch client must be provided in order to execute queries."
            )
        return self._using

    def params(self: T, **kwargs: Any) -> T:
        """
        Specify query params to be used when executing the search. All the
        keyword arguments will override the current values. See
        https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search
        for all available parameters.

        Example::

            s = Search()
            s = s.params(routing='user-1', preference='local')
        """
        from_ = kwargs.pop("from_", None)
        if from_ is not None:
            kwargs["from"] = from_
        s = self._clone()
        s._params.update(kwargs)
        return s

    def index(self: T, *index: Union[str, List[str], Tuple[str]]) -> T:
        """
        Set the index for the search. If called empty it will remove all information.

        Example:

            s = Search()
            s = s.index('twitter-2015.01.01', 'twitter-2015.01.02')
            s = s.index(['twitter-2015.01.01', 'twitter-2015.01.02'])
        """
        # .index() resets
        s = self._clone()
        if not index:
            s._index = None
        else:
            indexes = []
            for i in index:
                if isinstance(i, str):
                    indexes.append(i)
                elif isinstance(i, list):
                    indexes += i
                elif isinstance(i, tuple):
                    indexes += list(i)

            s._index = (self._index or []) + indexes

        return s

    def using(self: T, client: Elasticsearch) -> T:
        """
        Associate the search request with an elasticsearch client. A fresh copy
        will be returned with current instance remaining unchanged.

        :arg client: an instance of ``elasticsearch.Elasticsearch`` to use or
            an alias to look up in ``elasticsearch_dsl.connections``

        """
        s = self._clone()
        s._using = client
        return s

    def _clone(self: T) -> T:
        s = self.__class__(using=self._using, index=self._index)
        s._params = self._params.copy()
        return s

    def __copy__(self: T) -> T:
        return self._clone()


class Search(DSLMixin, Request):

    _type_name = "search"

    def __init__(
        self,
        using: Optional[Elasticsearch] = None,
        index: Optional[Union[str, Tuple[str], List[str]]] = None,
        mappings: Optional[Union[MappingsDict, Mappings]] = None,
        nested_autocorrect: bool = False,
        repr_auto_execute: bool = False,
    ) -> None:
        """
        Search request to elasticsearch.

        :arg using: `Elasticsearch` instance to use
        :arg index: limit the search to index
        :arg mappings: mappings used for query validation
        :arg nested_autocorrect: in case of missing nested clause, will insert it automatically
        :arg repr_auto_execute: execute query and display results as dataframe, requires client to be provided

        All the parameters supplied (or omitted) at creation type can be later
        overridden by methods (`using`, `index` and `mappings` respectively).
        """

        self._sort: List[Union[str, Dict[str, Any]]] = []
        self._source: Any = None
        self._highlight: Dict[str, Any] = {}
        self._highlight_opts: Dict[str, Any] = {}
        self._suggest: Dict[str, Any] = {}
        self._script_fields: Dict[str, Any] = {}
        mappings = _mappings(mappings)
        self._mappings: Optional[Mappings] = mappings
        self._aggs: Aggs = Aggs(
            mappings=mappings, nested_autocorrect=nested_autocorrect
        )
        self._query: Query = Query(
            mappings=mappings, nested_autocorrect=nested_autocorrect
        )
        self._post_filter: Query = Query(
            mappings=mappings, nested_autocorrect=nested_autocorrect
        )
        self._repr_auto_execute: bool = repr_auto_execute
        super(Search, self).__init__(using=using, index=index)

    def query(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        compound_param: str = None,
        **body: Any
    ) -> "Search":
        s = self._clone()
        s._query = s._query.query(
            type_or_query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            compound_param=compound_param,
            **body
        )
        return s

    query.__doc__ = Query.query.__doc__

    def bool(
        self,
        must: Optional[SingleOrMultipleQueryClause] = None,
        should: Optional[SingleOrMultipleQueryClause] = None,
        must_not: Optional[SingleOrMultipleQueryClause] = None,
        filter: Optional[SingleOrMultipleQueryClause] = None,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        **body: Any
    ) -> "Search":
        s = self._clone()
        s._query = s._query.bool(
            must=must,
            should=should,
            filter=filter,
            must_not=must_not,
            insert_below=insert_below,
            on=on,
            mode=mode,
            **body
        )
        return s

    bool.__doc__ = Query.bool.__doc__

    def filter(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        bool_body: ClauseBody = None,
        **body: Any
    ) -> "Search":
        s = self._clone()
        s._query = s._query.filter(
            type_or_query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            bool_body=bool_body,
            **body
        )
        return s

    filter.__doc__ = Query.filter.__doc__

    def must_not(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        bool_body: ClauseBody = None,
        **body: Any
    ) -> "Search":
        s = self._clone()
        s._query = s._query.must_not(
            type_or_query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            bool_body=bool_body,
            **body
        )
        return s

    must_not.__doc__ = Query.must_not.__doc__

    def should(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        bool_body: ClauseBody = None,
        **body: Any
    ) -> "Search":
        s = self._clone()
        s._query = s._query.should(
            type_or_query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            bool_body=bool_body,
            **body
        )
        return s

    should.__doc__ = Query.should.__doc__

    def must(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        bool_body: ClauseBody = None,
        **body: Any
    ) -> "Search":
        s = self._clone()
        s._query = s._query.must(
            type_or_query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            bool_body=bool_body,
            **body
        )
        return s

    must.__doc__ = Query.must.__doc__

    def exclude(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        bool_body: ClauseBody = None,
        **body: Any
    ) -> "Search":
        """Must not wrapped in filter context."""
        s = self._clone()
        s._query = s._query.filter(
            Bool(must_not=Query._q(type_or_query=type_or_query, **body)),
            insert_below=insert_below,
            on=on,
            mode=mode,
            bool_body=bool_body,
        )
        return s

    def post_filter(
        self,
        type_or_query: TypeOrQuery,
        insert_below: Optional[QueryName] = None,
        on: Optional[QueryName] = None,
        mode: InsertionModes = ADD,
        compound_param: str = None,
        **body: Any
    ) -> "Search":
        s = self._clone()
        s._post_filter = s._post_filter.query(
            type_or_query=type_or_query,
            insert_below=insert_below,
            on=on,
            mode=mode,
            compound_param=compound_param,
            **body
        )
        return s

    def agg(
        self,
        name: AggName,
        type_or_agg: Optional[TypeOrAgg] = None,
        insert_below: Optional[AggName] = None,
        at_root: bool_ = False,
        **body: Any
    ) -> "Search":
        s = self._clone()
        s._aggs = s._aggs.agg(
            name,
            type_or_agg=type_or_agg,
            insert_below=insert_below,
            at_root=at_root,
            **body
        )
        return s

    agg.__doc__ = Aggs.agg.__doc__

    def aggs(
        self,
        aggs: Union[AggsDictOrNode, "Aggs"],
        insert_below: Optional[AggName] = None,
        at_root: bool_ = False,
    ) -> "Search":
        s = self._clone()
        s._aggs = s._aggs.aggs(aggs, insert_below=insert_below, at_root=at_root)
        return s

    aggs.__doc__ = Aggs.aggs.__doc__

    def groupby(
        self,
        name: AggName,
        type_or_agg: Optional[TypeOrAgg] = None,
        insert_below: Optional[AggName] = None,
        at_root: bool_ = False,
        **body: Any
    ) -> "Search":
        s = self._clone()
        s._aggs = s._aggs.groupby(
            name,
            type_or_agg=type_or_agg,
            insert_below=insert_below,
            at_root=at_root,
            **body
        )
        return s

    groupby.__doc__ = Aggs.groupby.__doc__

    def __iter__(self) -> Iterator[Hit]:
        """
        Iterate over the hits. Return iterable of ``pandagg.response.Hit``.
        """
        return iter(self.execute())

    def __getitem__(self, n: Union[slice, List, int]) -> "Search":
        """
        Support slicing the `Search` instance for pagination.

        Slicing equates to the from/size parameters. E.g.::

            s = Search().query(...)[0:25]

        is equivalent to::

            s = Search().query(...).params(from=0, size=25)

        """
        s = self._clone()

        if isinstance(n, slice):
            # If negative slicing, abort.
            if n.start and n.start < 0 or n.stop and n.stop < 0:
                raise ValueError("Search does not support negative slicing.")
            # Elasticsearch won't get all results so we default to size: 10 if
            # stop not given.
            s._params["from"] = n.start or 0
            s._params["size"] = n.stop - (n.start or 0) if n.stop is not None else 10
            return s
        if isinstance(n, list):
            return s.source(includes=n)
        # This is an index lookup, equivalent to slicing by [n:n+1].
        # If negative index, abort.
        if n < 0:
            raise ValueError("Search does not support negative indexing.")
        s._params["from"] = n
        s._params["size"] = 1
        return s

    def size(self, size: int) -> "Search":
        """
        Equivalent to::

            s = Search().params(size=size)

        """
        s = self._clone()
        s._params["size"] = size
        return s

    @classmethod
    def from_dict(cls, d: Dict) -> "Search":
        """
        Construct a new `Search` instance from a raw dict containing the search
        body. Useful when migrating from raw dictionaries.

        Example::

            s = Search.from_dict({
                "query": {
                    "bool": {
                        "must": [...]
                    }
                },
                "aggs": {...}
            })
            s = s.filter('term', published=True)
        """
        s = cls()
        s.update_from_dict(d)
        return s

    def _clone(self) -> "Search":
        """
        Return a clone of the current search request. Performs a shallow copy
        of all the underlying objects. Used internally by most state modifying
        APIs.
        """
        s = Search(using=self._using, index=self._index, mappings=self._mappings)
        s._params = self._params.copy()
        s._sort = self._sort[:]
        s._source = copy.copy(self._source) if self._source is not None else None
        s._highlight = self._highlight.copy()
        s._highlight_opts = self._highlight_opts.copy()
        s._suggest = self._suggest.copy()
        s._script_fields = self._script_fields.copy()
        s._aggs = self._aggs.clone()
        s._query = self._query.clone()
        s._post_filter = self._post_filter.clone()
        s._mappings = None if self._mappings is None else self._mappings.clone()
        s._repr_auto_execute = self._repr_auto_execute
        return s

    def update_from_dict(self, d: Dict) -> "Search":
        """
        Apply options from a serialized body to the current instance. Modifies
        the object in-place. Used mostly by ``from_dict``.
        """
        d = d.copy()
        if "query" in d:
            self._query = Query(d.pop("query"))
        if "post_filter" in d:
            self._post_filter = Query(d.pop("post_filter"))

        aggs = d.pop("aggs", d.pop("aggregations", {}))
        if aggs:
            self._aggs = Aggs(aggs)
        if "sort" in d:
            self._sort = d.pop("sort")
        if "_source" in d:
            self._source = d.pop("_source")
        if "highlight" in d:
            high = d.pop("highlight").copy()
            self._highlight = high.pop("fields")
            self._highlight_opts = high
        if "suggest" in d:
            self._suggest = d.pop("suggest")
            if "text" in self._suggest:
                text = self._suggest.pop("text")
                for s in self._suggest.values():
                    s.setdefault("text", text)
        if "script_fields" in d:
            self._script_fields = d.pop("script_fields")
        self._params.update(d)
        return self

    def script_fields(self, **kwargs: Any) -> "Search":
        """
        Define script fields to be calculated on hits. See
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-script-fields.html
        for more details.

        Example::

            s = Search()
            s = s.script_fields(times_two="doc['field'].value * 2")
            s = s.script_fields(
                times_three={
                    'script': {
                        'inline': "doc['field'].value * params.n",
                        'params': {'n': 3}
                    }
                }
            )

        """
        s = self._clone()
        for name in kwargs:
            if isinstance(kwargs[name], str):
                kwargs[name] = {"script": kwargs[name]}
        s._script_fields.update(kwargs)
        return s

    def source(
        self, fields: Union[str, List[str], Dict[str, Any]] = None, **kwargs: Any
    ) -> "Search":
        """
        Selectively control how the _source field is returned.

        :arg fields: wildcard string, array of wildcards, or dictionary of includes and excludes

        If ``fields`` is None, the entire document will be returned for
        each hit.  If fields is a dictionary with keys of 'includes' and/or
        'excludes' the fields will be either included or excluded appropriately.

        Calling this multiple times with the same named parameter will override the
        previous values with the new ones.

        Example::

            s = Search()
            s = s.source(includes=['obj1.*'], excludes=["*.description"])

            s = Search()
            s = s.source(includes=['obj1.*']).source(excludes=["*.description"])

        """
        s = self._clone()

        if fields and kwargs:
            raise ValueError("You cannot specify fields and kwargs at the same time.")

        if fields is not None:
            s._source = fields
            return s

        if kwargs and not isinstance(s._source, dict):
            s._source = {}

        for key, value in kwargs.items():
            if value is None:
                try:
                    del s._source[key]
                except KeyError:
                    pass
            else:
                s._source[key] = value

        return s

    def sort(self, *keys: Union[str, Dict[str, Any]]) -> "Search":
        """
        Add sorting information to the search request. If called without
        arguments it will remove all sort requirements. Otherwise it will
        replace them. Acceptable arguments are::

            'some.field'
            '-some.other.field'
            {'different.field': {'any': 'dict'}}

        so for example::

            s = Search().sort(
                'category',
                '-title',
                {"price" : {"order" : "asc", "mode" : "avg"}}
            )

        will sort by ``category``, ``title`` (in descending order) and
        ``price`` in ascending order using the ``avg`` mode.

        The API returns a copy of the Search object and can thus be chained.
        """
        s = self._clone()
        s._sort = []
        for k in keys:
            if isinstance(k, str) and k.startswith("-"):
                if k[1:] == "_score":
                    raise ValueError("Sorting by `-_score` is not allowed.")
                k = {k[1:]: {"order": "desc"}}
            s._sort.append(k)
        return s

    def highlight_options(self, **kwargs: Any) -> "Search":
        """
        Update the global highlighting options used for this request. For
        example::

            s = Search()
            s = s.highlight_options(order='score')
        """
        s = self._clone()
        s._highlight_opts.update(kwargs)
        return s

    def highlight(self, *fields: str, **kwargs: Any) -> "Search":
        """
        Request highlighting of some fields. All keyword arguments passed in will be
        used as parameters for all the fields in the ``fields`` parameter. Example::

            Search().highlight('title', 'body', fragment_size=50)

        will produce the equivalent of::

            {
                "highlight": {
                    "fields": {
                        "body": {"fragment_size": 50},
                        "title": {"fragment_size": 50}
                    }
                }
            }

        If you want to have different options for different fields you can call ``highlight`` twice::

            Search().highlight('title', fragment_size=50).highlight('body', fragment_size=100)

        which will produce::

            {
                "highlight": {
                    "fields": {
                        "body": {"fragment_size": 100},
                        "title": {"fragment_size": 50}
                    }
                }
            }

        """
        s = self._clone()
        for f in fields:
            s._highlight[f] = kwargs
        return s

    def suggest(self, name: str, text: str, **kwargs: Any) -> "Search":
        """
        Add a suggestions request to the search.

        :arg name: name of the suggestion
        :arg text: text to suggest on

        All keyword arguments will be added to the suggestions body. For example::

            s = Search()
            s = s.suggest('suggestion-1', 'Elasticsearch', term={'field': 'body'})
        """
        s = self._clone()
        s._suggest[name] = {"text": text}
        s._suggest[name].update(kwargs)
        return s

    def to_dict(self, count: bool_ = False, **kwargs: Any) -> SearchDict:
        """
        Serialize the search into the dictionary that will be sent over as the
        request's body.

        :arg count: a flag to specify if we are interested in a body for count -
            no aggregations, no pagination bounds etc.

        All additional keyword arguments will be included into the dictionary.
        """
        d: SearchDict = {}

        if self._query:
            dq = self._query.to_dict()
            if dq:
                d["query"] = dq

        # count request doesn't care for sorting and other things
        if not count:
            if self._post_filter:
                pfd = self._post_filter.to_dict()
                if pfd:
                    d["post_filter"] = pfd

            if self._aggs:
                d["aggs"] = self._aggs.to_dict()

            if self._sort:
                d["sort"] = self._sort

            # query params are not typed in search dict
            d.update(self._params)  # type: ignore

            if self._source not in (None, {}):
                d["_source"] = self._source

            if self._highlight:
                highlights: Dict[str, Any] = {"fields": self._highlight}
                highlights.update(self._highlight_opts)
                d["highlight"] = highlights

            if self._suggest:
                d["suggest"] = self._suggest

            if self._script_fields:
                d["script_fields"] = self._script_fields

        # TODO: check if those kwargs are really useful
        d.update(kwargs)  # type: ignore
        return d

    def count(self) -> int:
        """
        Return the number of hits matching the query and filters. Note that
        only the actual number is returned.
        """
        es = self._get_connection()

        d = self.to_dict(count=True)
        return es.count(index=self._index, body=d)["count"]

    def execute(self) -> SearchResponse:
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.
        """
        es = self._get_connection()
        raw_data = es.search(index=self._index, body=self.to_dict())
        return SearchResponse(data=raw_data, _search=self)

    def scan_composite_agg(self, size: int) -> Iterator[BucketDict]:
        """Iterate over the whole aggregation composed buckets, yields buckets."""
        s: Search = self._clone().size(0)
        s._aggs = s._aggs.as_composite(size=size)
        a_name, _ = s._aggs.get_composition_supporting_agg()
        r: SearchResponse = s.execute()
        buckets: List[BucketDict] = r.aggregations.data[a_name][  # type: ignore
            "buckets"
        ]
        after_key: AfterKey = r.aggregations.data[a_name]["after_key"]  # type: ignore

        init: bool = True
        while init or len(buckets) == size:
            init = False
            s._aggs = s._aggs.as_composite(size=size, after=after_key)
            r = s.execute()
            agg_clause_response = r.aggregations.data[a_name]
            buckets = agg_clause_response["buckets"]  # type: ignore
            for bucket in buckets:
                yield bucket
            if "after_key" in agg_clause_response:
                after_key = agg_clause_response["after_key"]  # type: ignore
            else:
                break

    def scan_composite_agg_at_once(self, size: int) -> Aggregations:
        """Iterate over the whole aggregation composed buckets (converting Aggs into composite agg if possible), and
        return all buckets at once in a Aggregations instance.
        """
        all_buckets = list(self.scan_composite_agg(size=size))
        s: Search = self._clone().size(0)
        s._aggs = s._aggs.as_composite(size=size)
        agg_name: AggName
        agg_name, _ = s._aggs.get_composition_supporting_agg()  # type: ignore
        # artificially merge all buckets as if they were returned in a single query
        return Aggregations(_search=s, data={agg_name: {"buckets": all_buckets}})

    def scan(self) -> Iterator[Hit]:
        """
        Turn the search into a scan search and return a generator that will
        iterate over all the documents matching the query.

        Use ``params`` method to specify any additional arguments you with to
        pass to the underlying ``scan`` helper from ``elasticsearch-py`` -
        https://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.scan

        """
        es = self._get_connection()
        for hit in scan(es, query=self.to_dict(), index=self._index):
            yield Hit(hit)

    def delete(self) -> DeleteByQueryResponse:
        """
        delete() executes the query by delegating to delete_by_query()
        """

        es = self._get_connection()
        return es.delete_by_query(index=self._index, body=self.to_dict())

    def __eq__(self, other: Any) -> bool_:
        return (
            isinstance(other, Search)
            and other._index == self._index
            and other.to_dict() == self.to_dict()
        )

    def _auto_execution_df_result(self) -> pd.DataFrame:
        try:
            import pandas as pd  # noqa
        except ImportError:
            raise ImportError("repr_auto_execute requires pandas dependency")
        if self._aggs.to_dict():
            # hits are not necessary to display aggregation results
            r = self.size(0).execute()
            return r.aggregations.to_dataframe()
        r = self.execute()
        return r.hits.to_dataframe()

    def __repr__(self) -> str:
        # inspired by https://github.com/elastic/eland/blob/master/eland/dataframe.py#L471 idea to execute search at
        # __repr__ to have more interactive experience
        if not self._repr_auto_execute:
            return json.dumps(self.to_dict(), indent=2)
        return self._auto_execution_df_result().__repr__()

    def _repr_html_(self) -> Optional[str]:
        if not self._repr_auto_execute:
            return None
        return self._auto_execution_df_result()._repr_html_()


class MultiSearch(Request):
    """
    Combine multiple :class:`~elasticsearch_dsl.Search` objects into a single
    request.
    """

    def __init__(
        self,
        using: Optional[Elasticsearch],
        index: Optional[Union[str, Tuple[str], List[str]]] = None,
    ) -> None:
        super(MultiSearch, self).__init__(using=using, index=index)
        self._searches: List[Search] = []

    def __getitem__(self, key: int) -> Search:
        return self._searches[key]

    def __iter__(self) -> Iterator[Search]:
        return iter(self._searches)

    def _clone(self) -> "MultiSearch":
        ms = self.__class__(using=self._using, index=self._index)
        ms._params = self._params.copy()
        ms._searches = self._searches[:]
        return ms

    def add(self: "MultiSearch", search: Search) -> "MultiSearch":
        """
        Adds a new :class:`~elasticsearch_dsl.Search` object to the request::

            ms = MultiSearch(index='my-index')
            ms = ms.add(Search(doc_type=Category).filter('term', category='python'))
            ms = ms.add(Search(doc_type=Blog))
        """
        ms = self._clone()
        ms._searches.append(search)
        return ms

    def to_dict(self) -> List[Union[Dict, SearchDict]]:
        out: List[Union[Dict, SearchDict]] = []
        s: Search
        for s in self._searches:
            meta = {}
            if s._index:
                meta["index"] = s._index
            meta.update(s._params)

            out.append(meta)
            out.append(s.to_dict())

        return out

    def execute(self) -> List[SearchResponseDict]:
        """
        Execute the multi search request and return a list of search results.
        """
        es = self._get_connection()
        return es.msearch(index=self._index, body=self.to_dict(), **self._params)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Search)
            and other._index == self._index
            and other.to_dict() == self.to_dict()
        )

    def __repr__(self) -> str:
        return json.dumps(self.to_dict(), indent=2)
