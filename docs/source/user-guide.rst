##########
User Guide
##########


**pandagg** library provides interfaces to perform **read** operations on cluster.


.. toctree::
   :maxdepth: 2

   user-guide.search
   user-guide.query
   user-guide.aggs
   user-guide.response
   user-guide.interactive


.. note::

    Examples will be based on :doc:`IMDB` data.



:class:`~pandagg.search.Search` class is intended to perform request (see :doc:`user-guide.search`)

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .aggs('avg_rank', 'avg', field='rank')\
    >>>     .aggs('avg_nb_roles', 'avg', field='nb_roles')\
    >>>     .filter('range', year={"gte": 1990})

    >>> search
    {
      "query": {
        "bool": {
          "filter": [
            {
              "range": {
                "year": {
                  "gte": 1990
                }
              }
            }
          ]
        }
      },
      "aggs": {
        "decade": {
          "histogram": {
            "field": "year",
            "interval": 10
          },
          "aggs": {
            "genres": {
              "terms": {
            ...
            ..truncated..
            ...
          }
        }
      },
      "size": 2
    }

It relies on:

- :class:`~pandagg.query.Query` to build queries (see :doc:`user-guide.query`),
- :class:`~pandagg.aggs.Aggs` to build aggregations (see :doc:`user-guide.aggs`)

    >>> search._query.show()
    <Query>
    bool
    └── filter
        └── range, field=year, gte=1990

    >>> search._aggs.show()
    <Aggregations>
    decade                                         <histogram, field="year", interval=10>
    └── genres                                            <terms, field="genres", size=3>
        ├── avg_nb_roles                                          <avg, field="nb_roles">
        └── avg_rank                                                  <avg, field="rank">

Executing a :class:`~pandagg.search.Search` request using :func:`~pandagg.search.Search.execute` will return a
:class:`~pandagg.response.Response` instance (see :doc:`user-guide.response`).

    >>> response = search.execute()
    >>> response
    <Response> took 58ms, success: True, total result >=10000, contains 2 hits

    >>> response.hits.hits
    [<Hit 640> score=0.00, <Hit 641> score=0.00]

    >>> response.aggregations.to_dataframe()
                            avg_nb_roles  avg_rank  doc_count
    decade genres
    1990.0 Drama           18.518067  5.981429      12232
           Short            3.023284  6.311326      12197
           Documentary      3.778982  6.517093       8393
    2000.0 Short            4.053082  6.836253      13451
           Drama           14.385391  6.269675      11500
           Documentary      5.581433  6.980898       8639

On top of that some interactive features are available (see :doc:`user-guide.interactive`).
