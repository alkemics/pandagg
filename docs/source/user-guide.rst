User Guide
==========
.. toctree::

Introduction
------------

.. note::

    This is a work in progress. Some sections still need to be furnished.

About tree structure.
About interactive objects.


Build Search query
------------------

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
^^^^^^^^^^^

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
^^^^^^^^
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
^^^^^^^^^^^^^^
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


Build Aggregation query
-----------------------
TODO

Parse Aggregation response
--------------------------

TODO


Explore your cluster indices
----------------------------
TODO


Navigate in a mapping
---------------------
TODO
