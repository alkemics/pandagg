********
Response
********

When executing a search request via :func:`~pandagg.search.Search.execute` method of :class:`~pandagg.search.Search`,
a :class:`~pandagg.response.Response` instance is returned.

    >>> from elasticsearch import Elasticsearch
    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> response = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .filter('term', 'genres', 'Documentary')\
    >>>     .aggs('avg_rank', 'avg', field='rank')\
    >>>     .execute()

    >>> response
    <Response> took 9ms, success: True, total result >=10000, contains 2 hits

    >>> response.__class__
    pandagg.response.Response


ElasticSearch raw dict response is available under `data` attribute:

    >>> response.data
    {
        'took': 9, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0},
        'hits': {'total': {'value': 10000, 'relation': 'gte'},
        'max_score': 0.0,
        'hits': [{'_index': 'movies', ...}],
        'aggregations': {'avg_rank': {'value': 6.496829211219546}}
    }

Hits
====

Hits are available under `hits` attribute:

    >>> response.hits
    <Hits> total: >10000, contains 2 hits

    >>> response.hits.total
    {'value': 10000, 'relation': 'gte'}

    >>> response.hits.hits
    [<Hit 642> score=0.00, <Hit 643> score=0.00]

Those hits are instances of :class:`~pandagg.response.Hit`.

Directly iterating over :class:`~pandagg.response.Response` will return those hits:

    >>> list(response)
    [<Hit 642> score=0.00, <Hit 643> score=0.00]

    >>> hit = next(iter(response))

Each hit contains the raw dict under `data` attribute:

    >>> hit.data
    {'_index': 'movies',
     '_type': '_doc',
     '_id': '642',
     '_score': 0.0,
     '_source': {'movie_id': 642,
      'name': '10 Tage in Calcutta',
      'year': 1984,
      'genres': ['Documentary'],
      'roles': None,
      'nb_roles': 0,
      'directors': [{'director_id': 33096,
        'first_name': 'Reinhard',
        'last_name': 'Hauff',
        'full_name': 'Reinhard Hauff',
        'genres': ['Documentary', 'Drama', 'Musical', 'Short']}],
      'nb_directors': 1,
      'rank': None}}

    >>> hit._index
    'movies'

    >>> hit._source
    {'movie_id': 642,
     'name': '10 Tage in Calcutta',
     'year': 1984,
     'genres': ['Documentary'],
     'roles': None,
     'nb_roles': 0,
     'directors': [{'director_id': 33096,
       'first_name': 'Reinhard',
       'last_name': 'Hauff',
       'full_name': 'Reinhard Hauff',
       'genres': ['Documentary', 'Drama', 'Musical', 'Short']}],
     'nb_directors': 1,
     'rank': None}


Aggregations
============

Aggregations are handled differently, the `aggregations` attribute of a :class:`~pandagg.response.Response` returns
a :class:`~pandagg.response.Aggregations` instance, that provides specific parsing abilities in addition to exposing
raw aggregations response under `data` attribute.

Let's build a bit more complex aggregation query to showcase its functionalities:

    >>> from elasticsearch import Elasticsearch
    >>> from pandagg.search import Search
    >>>
    >>> client = Elasticsearch(hosts=['localhost:9200'])
    >>> response = Search(using=client, index='movies')\
    >>>     .size(0)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .aggs('avg_rank', 'avg', field='rank')\
    >>>     .aggs('avg_nb_roles', 'avg', field='nb_roles')\
    >>>     .filter('range', year={"gte": 1990})\
    >>>     .execute()

.. note::
    for more details about how to build aggregation query, consult :doc:`user-guide.aggs` section


Using `data` attribute:

    >>> response.aggregations.data
    {'decade': {'buckets': [{'key': 1990.0,
    'doc_count': 79495,
    'genres': {'doc_count_error_upper_bound': 0,
     'sum_other_doc_count': 38060,
     'buckets': [{'key': 'Drama',
       'doc_count': 12232,
       'avg_nb_roles': {'value': 18.518067364290385},
       'avg_rank': {'value': 5.981429367965072}},
      {'key': 'Short',
    ...


Tree serialization
------------------

Using :func:`~pandagg.response.Aggregations.to_normalized`:

    >>> response.aggregations.to_normalized()
    {'level': 'root',
     'key': None,
     'value': None,
     'children': [{'level': 'decade',
       'key': 1990.0,
       'value': 79495,
       'children': [{'level': 'genres',
         'key': 'Drama',
         'value': 12232,
         'children': [{'level': 'avg_rank',
           'key': None,
           'value': 5.981429367965072},
          {'level': 'avg_nb_roles', 'key': None, 'value': 18.518067364290385}]},
        {'level': 'genres',
         'key': 'Short',
         'value': 12197,
         'children': [{'level': 'avg_rank',
           'key': None,
           'value': 6.311325829450123},
        ...


Using :func:`~pandagg.response.Aggregations.to_interactive_tree`:

    >>> response.aggregations.to_interactive_tree()
    <IResponse>
    root
    ├── decade=1990                                        79495
    │   ├── genres=Documentary                              8393
    │   │   ├── avg_nb_roles                  3.7789824854045038
    │   │   └── avg_rank                       6.517093241977517
    │   ├── genres=Drama                                   12232
    │   │   ├── avg_nb_roles                  18.518067364290385
    │   │   └── avg_rank                       5.981429367965072
    │   └── genres=Short                                   12197
    │       ├── avg_nb_roles                   3.023284414200213
    │       └── avg_rank                       6.311325829450123
    └── decade=2000                                        57649
        ├── genres=Documentary                              8639
        │   ├── avg_nb_roles                   5.581433036231045
        │   └── avg_rank                       6.980897812811443
        ├── genres=Drama                                   11500
        │   ├── avg_nb_roles                  14.385391304347825
        │   └── avg_rank                       6.269675415719865
        └── genres=Short                                   13451
            ├── avg_nb_roles                   4.053081555274701
            └── avg_rank                        6.83625304327684


Tabular serialization
---------------------

Doing so requires to identify a level that will draw the line between:

- grouping levels: those which will be used to identify rows (here decades, and genres), and provide **doc_count** per row
- columns levels: those which will be used to populate columns and cells (here avg_nb_roles and avg_rank)

The tabular format will suit especially well aggregations with a T shape.


Using :func:`~pandagg.response.Aggregations.to_dataframe`:

    >>> response.aggregations.to_dataframe()
                            avg_nb_roles  avg_rank  doc_count
    decade genres
    1990.0 Drama           18.518067  5.981429      12232
           Short            3.023284  6.311326      12197
           Documentary      3.778982  6.517093       8393
    2000.0 Short            4.053082  6.836253      13451
           Drama           14.385391  6.269675      11500
           Documentary      5.581433  6.980898       8639


Using :func:`~pandagg.response.Aggregations.to_tabular`:

    >>> response.aggregations.to_tabular()
    (['decade', 'genres'],
     {(1990.0, 'Drama'): {'doc_count': 12232,
       'avg_rank': 5.981429367965072,
       'avg_nb_roles': 18.518067364290385},
      (1990.0, 'Short'): {'doc_count': 12197,
       'avg_rank': 6.311325829450123,
       'avg_nb_roles': 3.023284414200213},
      (1990.0, 'Documentary'): {'doc_count': 8393,
       'avg_rank': 6.517093241977517,
       'avg_nb_roles': 3.7789824854045038},
      (2000.0, 'Short'): {'doc_count': 13451,
       'avg_rank': 6.83625304327684,
       'avg_nb_roles': 4.053081555274701},
      (2000.0, 'Drama'): {'doc_count': 11500,
       'avg_rank': 6.269675415719865,
       'avg_nb_roles': 14.385391304347825},
      (2000.0, 'Documentary'): {'doc_count': 8639,
       'avg_rank': 6.980897812811443,
       'avg_nb_roles': 5.581433036231045}})


.. note::

    TODO - explain parameters:

        - index_orient
        - grouped_by
        - expand_columns
        - expand_sep
        - normalize
        - with_single_bucket_groups
