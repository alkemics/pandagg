##########
User Guide
##########

.. toctree::


.. note::

    This is a work in progress. Some sections still need to be furnished.

**********
Philosophy
**********

**pandagg** is designed for both for "regular" code repository usage, and "interactive" usage (ipython or jupyter
notebook usage with autocompletion features inspired by `pandas <https://github.com/pandas-dev/pandas>`_ design).

This library focuses on two principles:

* stick to the **tree** structure of Elasticsearch objects
* provide simple and flexible interfaces to make it easy and intuitive to use in an interactive usage

Elasticsearch tree structures
=============================

Many Elasticsearch objects have a **tree** structure, ie they are built from a hierarchy of **nodes**:

* a `mapping <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html>`_ (tree) is a hierarchy of `fields <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html>`_ (nodes)
* a `query <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>`_ (tree) is a hierarchy of query clauses (nodes)
* an `aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html>`_ (tree) is a hierarchy of aggregation clauses (nodes)
* an aggregation response (tree) is a hierarchy of response buckets (nodes)

This library aims to stick to that structure by providing a flexible syntax distinguishing **trees** and **nodes**.

Interactive usage
=================


*****
Query
*****

The :class:`~pandagg.tree.query.Query` class allows multiple ways to declare and udpate an Elasticsearch query.

Let's explore the multiple ways we have to declare the following query:

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


Pandagg DSL
===========

Pandagg provides a DSL to declare this query in a quite similar fashion:

    >>> from pandagg.query import Nested, Bool, Query, Range, Term, Terms

    >>> q = Query(
    >>>    Bool(must=[
    >>>        Terms('genres', terms=['Action', 'Thriller']),
    >>>        Range('rank', gte=7),
    >>>        Nested(
    >>>            path='roles',
    >>>            query=Bool(must=[
    >>>                Term('roles.gender', value='F'),
    >>>                Term('roles.role', value='Reporter')
    >>>            ])
    >>>        )
    >>>    ])
    >>>)

The serialized query is then available with `query_dict` method:

    >>> q.query_dict() == expected_query
    True

A visual representation of the query helps to have a clearer view:

    >>> q
    <Query>
    bool
    └── must
        ├── nested
        │   ├── path="roles"
        │   └── query
        │       └── bool
        │           └── must
        │               ├── term, field=roles.gender, value="F"
        │               └── term, field=roles.role, value="Reporter"
        ├── range, field=rank, gte=7
        └── terms, field=genres, values=['Action', 'Thriller']



Chaining
========
Another way to declare this query is through chaining:

    >>> from pandagg.utils import equal_queries
    >>> from pandagg.query import Nested, Bool, Query, Range, Term, Terms

    >>> q = Query()\
    >>>     .query({'terms': {'genres': ['Action', 'Thriller']}})\
    >>>     .nested(path='roles', _name='nested_roles', query=Term('roles.gender', value='F'))\
    >>>     .query(Range('rank', gte=7))\
    >>>     .query(Term('roles.role', value='Reporter'), parent='nested_roles')

    >>> equal_queries(q.query_dict(), expected_query)
    True

.. note::
    `equal_queries` function won't consider order of clauses in must/should parameters since it actually doesn't matter
    in Elasticsearch execution, ie
        >>> equal_queries({'must': [A, B]}, {'must': [B, A]})
        True

Regular syntax
==============
Eventually, you can also use regular Elasticsearch dict syntax:

    >>> q = Query(expected_query)
    >>> q
    <Query>
    bool
    └── must
        ├── nested
        │   ├── path="roles"
        │   └── query
        │       └── bool
        │           └── must
        │               ├── term, field=roles.gender, value="F"
        │               └── term, field=roles.role, value="Reporter"
        ├── range, field=rank, gte=7
        └── terms, field=genres, values=['Action', 'Thriller']


*******
Mapping
*******

Mapping declaration
===================

Mapping navigation
==================

***********
Aggregation
***********

Aggregation declaration
=======================

Aggregation response
====================

TODO

*************************
Cluster indices discovery
*************************

TODO
