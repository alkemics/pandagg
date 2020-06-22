##########
User Guide
##########


.. note::

    Examples will be based on :doc:`IMDB` data.
    This is a work in progress. Some sections still need to be furnished.


*****
Query
*****

The :class:`~pandagg.tree.query.abstract.Query` class provides :

- multiple syntaxes to declare and udpate a query
- query validation (with nested clauses validation)
- ability to insert clauses at specific points
- tree-like visual representation

Instantiation
=============

From native "dict" query
------------------------

Given the following query:

    >>> expected_query = {'bool': {'must': [
    >>>    {'terms': {'genres': ['Action', 'Thriller']}},
    >>>    {'range': {'rank': {'gte': 7}}},
    >>>    {'nested': {
    >>>        'path': 'roles',
    >>>        'query': {'bool': {'must': [
    >>>            {'term': {'roles.gender': {'value': 'F'}}},
    >>>            {'term': {'roles.role': {'value': 'Reporter'}}}]}
    >>>         }
    >>>    }}
    >>> ]}}

To instantiate :class:`~pandagg.tree.query.abstract.Query`, simply pass "dict" query as argument:

    >>> from pandagg.query import Query
    >>> q = Query(expected_query)

A visual representation of the query is available with :func:`~pandagg.tree.query.abstract.Query.show`:

    >>> q.show()
    <Query>
    bool
    └── must
        ├── nested, path="roles"
        │   └── query
        │       └── bool
        │           └── must
        │               ├── term, field=roles.gender, value="F"
        │               └── term, field=roles.role, value="Reporter"
        ├── range, field=rank, gte=7
        └── terms, genres=["Action", "Thriller"]


Call :func:`~pandagg.tree.query.abstract.Query.to_dict` to convert it to native dict:

    >>> q.to_dict()
    {'bool': {
        'must': [
            {'range': {'rank': {'gte': 7}}},
            {'terms': {'genres': ['Action', 'Thriller']}},
            {'bool': {'must': [
                {'term': {'roles.role': {'value': 'Reporter'}}},
                {'term': {'roles.gender': {'value': 'F'}}}]}}}}
            ]}
        ]
    }}

    >>> from pandagg.utils import equal_queries
    >>> equal_queries(q.to_dict(), expected_query)
    True


.. note::
    `equal_queries` function won't consider order of clauses in must/should parameters since it actually doesn't matter
    in Elasticsearch execution, ie

        >>> equal_queries({'must': [A, B]}, {'must': [B, A]})
        True

With DSL classes
----------------

Pandagg provides a DSL to declare this query in a quite similar fashion:

    >>> from pandagg.query import Nested, Bool, Range, Term, Terms

    >>> q = Bool(must=[
    >>>     Terms(genres=['Action', 'Thriller']),
    >>>     Range(rank={"gte": 7}),
    >>>     Nested(
    >>>         path='roles',
    >>>         query=Bool(must=[
    >>>             Term(roles__gender='F'),
    >>>             Term(roles__role='Reporter')
    >>>         ])
    >>>     )
    >>> ])

All these classes inherit from :class:`~pandagg.tree.query.abstract.Query` and thus provide the same interface.

    >>> from pandagg.query import Query
    >>> isinstance(q, Query)
    True

With single clause as flattened syntax
--------------------------------------

In the flattened syntax, the query clause type is used as first argument:

    >>> from pandagg.query import Query
    >>> q = Query('terms', genres=['Action', 'Thriller'])


Query enrichment
================

All methods described below return a new :class:`~pandagg.tree.query.abstract.Query` instance, and keep unchanged the
initial query.

For instance:

    >>> from pandagg.query import Query
    >>> initial_q = Query()
    >>> enriched_q = initial_q.query('terms', genres=['Comedy', 'Short'])

    >>> initial_q.to_dict()
    None

    >>> enriched_q.to_dict()
    {'terms': {'genres': ['Comedy', 'Short']}}

.. note::

    Calling :func:`~pandagg.tree.query.abstract.Query.to_dict` on an empty Query returns `None`

        >>> from pandagg.query import Query
        >>> Query().to_dict()
        None


query() method
--------------

The base method to enrich a :class:`~pandagg.tree.query.abstract.Query` is :func:`~pandagg.tree.query.abstract.Query.query`.


Considering this query:

    >>> from pandagg.query import Query
    >>> q = Query()

:func:`~pandagg.tree.query.abstract.Query.query` accepts following syntaxes:

from dictionnary::


    >>> q.query({"terms": {"genres": ['Comedy', 'Short']})

flattened syntax::


    >>> q.query("terms", genres=['Comedy', 'Short'])


from Query instance (this includes DSL classes)::

    >>> from pandagg.query import Terms
    >>> q.query(Terms(genres=['Action', 'Thriller']))


Compound clauses specific methods
---------------------------------

:class:`~pandagg.tree.query.abstract.Query` instance also exposes following methods for specific compound queries:

(TODO: detail allowed syntaxes)

Specific to bool queries:

- :func:`~pandagg.tree.query.abstract.Query.bool`
- :func:`~pandagg.tree.query.abstract.Query.filter`
- :func:`~pandagg.tree.query.abstract.Query.must`
- :func:`~pandagg.tree.query.abstract.Query.must_not`
- :func:`~pandagg.tree.query.abstract.Query.should`

Specific to other compound queries:

- :func:`~pandagg.tree.query.abstract.Query.nested`
- :func:`~pandagg.tree.query.abstract.Query.constant_score`
- :func:`~pandagg.tree.query.abstract.Query.dis_max`
- :func:`~pandagg.tree.query.abstract.Query.function_score`
- :func:`~pandagg.tree.query.abstract.Query.has_child`
- :func:`~pandagg.tree.query.abstract.Query.has_parent`
- :func:`~pandagg.tree.query.abstract.Query.parent_id`
- :func:`~pandagg.tree.query.abstract.Query.pinned_query`
- :func:`~pandagg.tree.query.abstract.Query.script_score`
- :func:`~pandagg.tree.query.abstract.Query.boost`


Inserted clause location
------------------------

On all insertion methods detailed above, by default, the inserted clause is placed at the top level of your query, and
generates a bool clause if necessary.

Considering the following query:

    >>> from pandagg.query import Query
    >>> q = Query('terms', genres=['Action', 'Thriller'])
    >>> q.show()
    <Query>
    terms, genres=["Action", "Thriller"]

A bool query will be created:

    >>> q = q.query('range', rank={"gte": 7})
    >>> q.show()
    <Query>
    bool
    └── must
        ├── range, field=rank, gte=7
        └── terms, genres=["Action", "Thriller"]

And reused if necessary:

    >>> q = q.must_not('range', year={"lte": 1970})
    >>> q.show()
    <Query>
    bool
    ├── must
    │   ├── range, field=rank, gte=7
    │   └── terms, genres=["Action", "Thriller"]
    └── must_not
        └── range, field=year, lte=1970

Specifying a specific location requires to `name queries <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#request-body-search-queries-and-filters>`_ :

    >>> from pandagg.query import Nested

    >>> q = q.nested(path='roles', _name='nested_roles', query=Term('roles.gender', value='F'))
    >>> q.show()
    <Query>
    bool
    ├── must
    │   ├── nested, _name=nested_roles, path="roles"
    │   │   └── query
    │   │       └── term, field=roles.gender, value="F"
    │   ├── range, field=rank, gte=7
    │   └── terms, genres=["Action", "Thriller"]
    └── must_not
        └── range, field=year, lte=1970

Doing so allows to insert clauses above/below given clause using `parent`/`child` parameters:

    >>> q = q.query('term', roles__role='Reporter', parent='nested_roles')
    >>> q.show()
    <Query>
    bool
    ├── must
    │   ├── nested, _name=nested_roles, path="roles"
    │   │   └── query
    │   │       └── bool
    │   │           └── must
    │   │               ├── term, field=roles.role, value="Reporter"
    │   │               └── term, field=roles.gender, value="F"
    │   ├── range, field=rank, gte=7
    │   └── terms, genres=["Action", "Thriller"]
    └── must_not
        └── range, field=year, lte=1970


TODO: explain `parent_param`, `child_param`, `mode` merging strategies on same named clause etc..

***********
Aggregation
***********

The :class:`~pandagg.tree.aggs.aggs.Aggs` class provides :

- multiple syntaxes to declare and udpate a aggregation
- clause validation (with nested clauses validation)
- ability to insert clauses at specific points


Aggregation declaration
=======================



Aggregation response
====================

TODO

******
Search
******

TODO

*******
Mapping
*******

Interactive mapping
===================

In interactive context, the :class:`~pandagg.interactive.mapping.IMapping` class provides navigation features with autocompletion to quickly discover a large
mapping:

    >>> from pandagg.mapping import IMapping
    >>> from examples.imdb.load import mapping
    >>> m = IMapping(imdb_mapping)
    >>> m.roles
    <IMapping subpart: roles>
    roles                                                    [Nested]
    ├── actor_id                                              Integer
    ├── first_name                                            Text
    │   └── raw                                             ~ Keyword
    ├── gender                                                Keyword
    ├── last_name                                             Text
    │   └── raw                                             ~ Keyword
    └── role                                                  Keyword
    >>> m.roles.first_name
    <IMapping subpart: roles.first_name>
    first_name                                            Text
    └── raw                                             ~ Keyword

To get the complete field definition, just call it:

    >>> m.roles.first_name()
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

    >>> m = IMapping(imdb_mapping, client=client, index_name='movies')

Doing so will generate a **a** attribute on mapping fields, this attribute will list all available aggregation for that
field type (with autocompletion):

    >>> m.roles.gender.a.terms()
    [('M', {'key': 'M', 'doc_count': 2296792}),
    ('F', {'key': 'F', 'doc_count': 1135174})]


.. note::

    Nested clauses will be automatically taken into account.


*************************
Cluster indices discovery
*************************

TODO
