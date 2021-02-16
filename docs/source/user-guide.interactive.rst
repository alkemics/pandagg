
********************
Interactive features
********************

Features described in this module are primarly designed for interactive usage, for instance in an
`ipython shell<https://ipython.org/>_`, since one of the key features is the intuitive usage provided by auto-completion.

Cluster indices discovery
=========================

:func:`~pandagg.discovery.discover` function list all indices on a cluster matching a provided pattern:

    >>> from elasticsearch import Elasticsearch
    >>> from pandagg.discovery import discover
    >>> client = Elasticsearch(hosts=['xxx'])
    >>> indices = discover(client, index='mov*')
    >>> indices
    <Indices> ['movies', 'movies_fake']

Each of the indices is accessible via autocompletion:

    >>> indices.movies
     <Index 'movies'>


An :class:`~pandagg.discovery.Index` exposes: settings, mapping (interactive), aliases and name:

    >>> movies = indices.movies
    >>> movies.settings
    {'index': {'creation_date': '1591824202943',
      'number_of_shards': '1',
      'number_of_replicas': '1',
      'uuid': 'v6Amj9x1Sk-trBShI-188A',
      'version': {'created': '7070199'},
      'provided_name': 'movies'}}

    >>> movies.mapping
    <Mapping>
    _
    ├── directors                                                [Nested]
    │   ├── director_id                                           Keyword
    │   ├── first_name                                            Text
    │   │   └── raw                                             ~ Keyword
    │   ├── full_name                                             Text
    │   │   └── raw                                             ~ Keyword
    │   ├── genres                                                Keyword
    │   └── last_name                                             Text
    │       └── raw                                             ~ Keyword
    ├── genres                                                    Keyword
    ├── movie_id                                                  Keyword
    ├── name                                                      Text
    │   └── raw                                                 ~ Keyword
    ├── nb_directors                                              Integer
    ├── nb_roles                                                  Integer
    ├── rank                                                      Float
    ├── roles                                                    [Nested]
    │   ├── actor_id                                              Keyword
    │   ├── first_name                                            Text
    │   │   └── raw                                             ~ Keyword
    │   ├── full_name                                             Text
    │   │   └── raw                                             ~ Keyword
    │   ├── gender                                                Keyword
    │   ├── last_name                                             Text
    │   │   └── raw                                             ~ Keyword
    │   └── role                                                  Keyword
    └── year                                                      Integer


Navigable mapping
=================

The :class:`~pandagg.discovery.Index` **mapping** attribute returns a :class:`~pandagg.interactive.mapping.IMapping`
instance that provides navigation features with autocompletion to quickly discover a large
mapping:


    >>> movies.roles
    <Mapping subpart: roles>
    roles                                                    [Nested]
    ├── actor_id                                              Integer
    ├── first_name                                            Text
    │   └── raw                                             ~ Keyword
    ├── gender                                                Keyword
    ├── last_name                                             Text
    │   └── raw                                             ~ Keyword
    └── role                                                  Keyword
    >>> movies.roles.first_name
    <IMapping subpart: roles.first_name>
    first_name                                            Text
    └── raw                                             ~ Keyword


.. note::

    a navigable mapping can be obtained directly using :class:`~pandagg.interactive.mapping.IMapping` class without
    using discovery module:

        >>> from pandagg.mapping import IMapping
        >>> from examples.imdb.load import mapping
        >>> m = IMapping(mapping)
        >>> m.roles.first_name
        <Mapping subpart: roles.first_name>
        first_name                                                    Text
        └── raw                                                     ~ Keyword


To get the complete field definition, just call it:

    >>> movies.roles.first_name()
    <Mapping Field first_name> of type text:
    {
        "type": "text",
        "fields": {
            "raw": {
                "type": "keyword"
            }
        }
    }

A **IMapping** instance can be bound to an Elasticsearch client to get quick access to aggregations computation on mapping fields.

Suppose you have the following client:

    >>> from elasticsearch import Elasticsearch
    >>> client = Elasticsearch(hosts=['localhost:9200'])

Client can be bound at instantiation:

    >>> movies = IMapping(mapping, client=client, index_name='movies')

Doing so will generate a **a** attribute on mapping fields, this attribute will list all available aggregation for that
field type (with autocompletion):

    >>> movies.roles.gender.a.terms()
    [('M', {'key': 'M', 'doc_count': 2296792}),
    ('F', {'key': 'F', 'doc_count': 1135174})]


.. note::

    Nested clauses will be automatically taken into account.


Navigable aggregation response
==============================

When executing a :class:`~pandagg.search.Search` request with aggregations, resulting aggregations can be parsed in
multiple formats as described :doc:`user-guide.response`.

Suppose we execute the following search request:

    >>> from elasticsearch import Elasticsearch
    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> response = Search(using=client, index='movies')\
    >>>     .size(0)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .agg('avg_rank', 'avg', field='rank')\
    >>>     .aggs('avg_nb_roles', 'avg', field='nb_roles')\
    >>>     .filter('range', year={"gte": 1990})\
    >>>     .execute()

One of the available serialization methods for aggregations, :func:`~pandagg.response.Aggregations.to_interactive_tree`,
generates an interactive tree of class :class:`~pandagg.interactive.response.IResponse`:

    >>> tree = response.aggregations.to_interactive_tree()
    >>> tree
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

This tree provides auto-completion on each node to select a subpart of the tree:

    >>> tree.decade_1990
    <IResponse subpart: decade_1990>
    decade=1990                                            79495
    ├── genres=Documentary                                  8393
    │   ├── avg_nb_roles                      3.7789824854045038
    │   └── avg_rank                           6.517093241977517
    ├── genres=Drama                                       12232
    │   ├── avg_nb_roles                      18.518067364290385
    │   └── avg_rank                           5.981429367965072
    └── genres=Short                                       12197
        ├── avg_nb_roles                       3.023284414200213
        └── avg_rank                           6.311325829450123

    >>> tree.genres_Drama
    <IResponse subpart: decade_1990.genres_Drama>
    genres=Drama                                           12232
    ├── avg_nb_roles                          18.518067364290385
    └── avg_rank                               5.981429367965072

:func:`~pandagg.interactive.response.IResponse.get_bucket_filter` returns the query that filters documents belonging
to the given bucket:

    >>> tree.decade_1990.genres_Drama.get_bucket_filter()
    {'bool': {
        'must': [
            {'term': {'genres': {'value': 'Drama'}}},
            {'range': {'year': {'gte': 1990.0, 'lt': 2000.0}}}
        ],
        'filter': [{'range': {'year': {'gte': 1990}}}]
        }
    }

:func:`~pandagg.interactive.response.IResponse.list_documents` method actually execute this query to list documents
belonging to bucket:

    >>> tree.decade_1990.genres_Drama.list_documents(size=2, _source={"include": ['name']})
    {'took': 10,
     'timed_out': False,
     '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0},
     'hits': {'total': {'value': 10000, 'relation': 'gte'},
      'max_score': 2.4539857,
      'hits': [{'_index': 'movies',
        '_type': '_doc',
        '_id': '706',
        '_score': 2.4539857,
        '_source': {'name': '100 meter fri'}},
       {'_index': 'movies',
        '_type': '_doc',
        '_id': '714',
        '_score': 2.4539857,
        '_source': {'name': '100 Proof'}}]}}
