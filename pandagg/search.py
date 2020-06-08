# adapted from elasticsearch-dsl/search.py
import copy
import json

from elasticsearch.helpers import scan
from future.utils import string_types

from pandagg.connections import get_connection
from pandagg.query import Bool
from pandagg.response import Response
from pandagg.tree.mapping import Mapping
from pandagg.tree.query.abstract import Query
from pandagg.tree.aggs.aggs import Aggs


class Request(object):
    def __init__(self, using, index=None):
        self._using = using

        self._index = None
        if isinstance(index, (tuple, list)):
            self._index = list(index)
        elif index:
            self._index = [index]

        self._params = {}

    def params(self, **kwargs):
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

    def index(self, *index):
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
                if isinstance(i, string_types):
                    indexes.append(i)
                elif isinstance(i, list):
                    indexes += i
                elif isinstance(i, tuple):
                    indexes += list(i)

            s._index = (self._index or []) + indexes

        return s

    def using(self, client):
        """
        Associate the search request with an elasticsearch client. A fresh copy
        will be returned with current instance remaining unchanged.

        :arg client: an instance of ``elasticsearch.Elasticsearch`` to use or
            an alias to look up in ``elasticsearch_dsl.connections``

        """
        s = self._clone()
        s._using = client
        return s

    def _clone(self):
        s = self.__class__(using=self._using, index=self._index)
        s._params = self._params.copy()
        return s

    def __copy__(self):
        return self._clone()


class Search(Request):
    def __init__(self, using=None, index=None, mapping=None, nested_autocorrect=False):
        """
        Search request to elasticsearch.

        :arg using: `Elasticsearch` instance to use
        :arg index: limit the search to index
        :arg mapping: mapping used for query validation

        All the parameters supplied (or omitted) at creation type can be later
        overridden by methods (`using`, `index` and `mapping` respectively).
        """

        self._sort = []
        self._source = None
        self._highlight = {}
        self._highlight_opts = {}
        self._suggest = {}
        self._script_fields = {}
        mapping = Mapping(mapping)
        self._mapping = mapping
        self._aggs = Aggs(mapping=mapping, nested_autocorrect=nested_autocorrect)
        self._query = Query(mapping=mapping, nested_autocorrect=nested_autocorrect)
        self._post_filter = Query(
            mapping=mapping, nested_autocorrect=nested_autocorrect
        )
        super(Search, self).__init__(using=using, index=index)

    def query(self, *args, **kwargs):
        s = self._clone()
        s._query = s._query.query(*args, **kwargs)
        return s

    query.__doc__ = Query.query.__doc__

    def bool(self, *args, **kwargs):
        s = self._clone()
        s._query = s._query.bool(*args, **kwargs)
        return s

    bool.__doc__ = Query.bool.__doc__

    def filter(self, *args, **kwargs):
        s = self._clone()
        s._query = s._query.filter(*args, **kwargs)
        return s

    filter.__doc__ = Query.filter.__doc__

    def must_not(self, *args, **kwargs):
        s = self._clone()
        s._query = s._query.must_not(*args, **kwargs)
        return s

    must_not.__doc__ = Query.must_not.__doc__

    def should(self, *args, **kwargs):
        s = self._clone()
        s._query = s._query.should(*args, **kwargs)
        return s

    should.__doc__ = Query.should.__doc__

    def must(self, *args, **kwargs):
        s = self._clone()
        s._query = s._query.must(*args, **kwargs)
        return s

    must.__doc__ = Query.must.__doc__

    def exclude(self, *args, **kwargs):
        """Must not wrapped in filter context."""
        s = self._clone()
        s._query = s._query.filter(Bool(must_not=[Query(*args, **kwargs)]))
        return s

    def post_filter(self, *args, **kwargs):
        s = self._clone()
        s._post_filter = s._post_filter.query(*args, **kwargs)
        return s

    def aggs(self, *args, **kwargs):
        s = self._clone()
        s._aggs = s._aggs.aggs(*args, **kwargs)
        return s

    aggs.__doc__ = Aggs.aggs.__doc__

    def groupby(self, *args, **kwargs):
        s = self._clone()
        s._aggs = s._aggs.groupby(*args, **kwargs)
        return s

    groupby.__doc__ = Aggs.groupby.__doc__

    def __iter__(self):
        """
        Iterate over the hits. Return iterable of ``pandagg.response.Hit``.
        """
        return iter(self.execute())

    def __getitem__(self, n):
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
        else:  # This is an index lookup, equivalent to slicing by [n:n+1].
            # If negative index, abort.
            if n < 0:
                raise ValueError("Search does not support negative indexing.")
            s._params["from"] = n
            s._params["size"] = 1
            return s

    def size(self, size):
        """
        Equivalent to::

            s = Search().params(size=size)

        """
        s = self._clone()
        s._params["size"] = size
        return s

    @classmethod
    def from_dict(cls, d):
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

    def _clone(self):
        """
        Return a clone of the current search request. Performs a shallow copy
        of all the underlying objects. Used internally by most state modifying
        APIs.
        """
        s = self.__class__(using=self._using, index=self._index, mapping=self._mapping)
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
        s._mapping = self._mapping.clone()
        return s

    def update_from_dict(self, d):
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

    def script_fields(self, **kwargs):
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
            if isinstance(kwargs[name], string_types):
                kwargs[name] = {"script": kwargs[name]}
        s._script_fields.update(kwargs)
        return s

    def source(self, fields=None, **kwargs):
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

    def sort(self, *keys):
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
            if isinstance(k, string_types) and k.startswith("-"):
                if k[1:] == "_score":
                    raise ValueError("Sorting by `-_score` is not allowed.")
                k = {k[1:]: {"order": "desc"}}
            s._sort.append(k)
        return s

    def highlight_options(self, **kwargs):
        """
        Update the global highlighting options used for this request. For
        example::

            s = Search()
            s = s.highlight_options(order='score')
        """
        s = self._clone()
        s._highlight_opts.update(kwargs)
        return s

    def highlight(self, *fields, **kwargs):
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

    def suggest(self, name, text, **kwargs):
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

    def to_dict(self, count=False, **kwargs):
        """
        Serialize the search into the dictionary that will be sent over as the
        request's body.

        :arg count: a flag to specify if we are interested in a body for count -
            no aggregations, no pagination bounds etc.

        All additional keyword arguments will be included into the dictionary.
        """
        d = {}

        if self._query:
            d["query"] = self._query.to_dict()

        # count request doesn't care for sorting and other things
        if not count:
            if self._post_filter:
                d["post_filter"] = self._post_filter.to_dict()

            if self._aggs:
                d["aggs"] = self._aggs.to_dict()

            if self._sort:
                d["sort"] = self._sort

            d.update(self._params)

            if self._source not in (None, {}):
                d["_source"] = self._source

            if self._highlight:
                d["highlight"] = {"fields": self._highlight}
                d["highlight"].update(self._highlight_opts)

            if self._suggest:
                d["suggest"] = self._suggest

            if self._script_fields:
                d["script_fields"] = self._script_fields

        d.update(kwargs)
        return d

    def count(self):
        """
        Return the number of hits matching the query and filters. Note that
        only the actual number is returned.
        """
        es = get_connection(self._using)

        d = self.to_dict(count=True)
        return es.count(index=self._index, body=d)["count"]

    def execute(self):
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.
        """
        es = get_connection(self._using)
        return Response(es.search(index=self._index, body=self.to_dict()), search=self)

    def scan(self):
        """
        Turn the search into a scan search and return a generator that will
        iterate over all the documents matching the query.

        Use ``params`` method to specify any additional arguments you with to
        pass to the underlying ``scan`` helper from ``elasticsearch-py`` -
        https://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.scan

        """
        es = get_connection(self._using)

        for hit in scan(es, query=self.to_dict(), index=self._index):
            yield hit

    def delete(self):
        """
        delete() executes the query by delegating to delete_by_query()
        """

        es = get_connection(self._using)

        return es.delete_by_query(index=self._index, body=self.to_dict())

    def __eq__(self, other):
        return (
            isinstance(other, Search)
            and other._index == self._index
            and other.to_dict() == self.to_dict()
        )

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)


class MultiSearch(Request):
    """
    Combine multiple :class:`~elasticsearch_dsl.Search` objects into a single
    request.
    """

    def __init__(self, **kwargs):
        super(MultiSearch, self).__init__(**kwargs)
        self._searches = []

    def __getitem__(self, key):
        return self._searches[key]

    def __iter__(self):
        return iter(self._searches)

    def _clone(self):
        ms = super(MultiSearch, self)._clone()
        ms._searches = self._searches[:]
        return ms

    def add(self, search):
        """
        Adds a new :class:`~elasticsearch_dsl.Search` object to the request::

            ms = MultiSearch(index='my-index')
            ms = ms.add(Search(doc_type=Category).filter('term', category='python'))
            ms = ms.add(Search(doc_type=Blog))
        """
        ms = self._clone()
        ms._searches.append(search)
        return ms

    def to_dict(self):
        out = []
        for s in self._searches:
            meta = {}
            if s._index:
                meta["index"] = s._index
            meta.update(s._params)

            out.append(meta)
            out.append(s.to_dict())

        return out

    def execute(self):
        """
        Execute the multi search request and return a list of search results.
        """
        es = get_connection(self._using)
        return es.msearch(index=self._index, body=self.to_dict(), **self._params)

    def __eq__(self, other):
        return (
            isinstance(other, Search)
            and other._index == self._index
            and other.to_dict() == self.to_dict()
        )

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)
