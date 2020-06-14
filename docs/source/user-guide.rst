##########
User Guide
##########


.. note::

    Examples will be based on :doc:`IMDB` data.
    This is a work in progress. Some sections still need to be furnished.


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

    >>> q.to_dict() == expected_query
    True

A visual representation of the query helps to have a clearer view:

    >>> q.show()
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

    >>> q.to_dict() == expected_query
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

    >>> q.to_dict() == expected_query
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

    >>> q.to_dict() == expected_query
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

    >>> q.to_dict() == expected_query
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

    >>> q.to_dict() == expected_query
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

    >>> equal_queries(q.to_dict(), expected_query)
    True

    >>> from pandagg.utils import equal_queries
    >>> from pandagg.query import Nested, Bool, Query, Range, Term, Terms

    >>> q = Query()\
    >>>     .query({'terms': {'genres': ['Action', 'Thriller']}})\
    >>>     .nested(path='roles', _name='nested_roles', query=Term('roles.gender', value='F'))\
    >>>     .query(Range('rank', gte=7))\
    >>>     .query(Term('roles.role', value='Reporter'), parent='nested_roles')

    >>> equal_queries(q.to_dict(), expected_query)
    True

    >>> from pandagg.utils import equal_queries
    >>> from pandagg.query import Nested, Bool, Query, Range, Term, Terms

    >>> q = Query()\
    >>>     .query({'terms': {'genres': ['Action', 'Thriller']}})\
    >>>     .nested(path='roles', _name='nested_roles', query=Term('roles.gender', value='F'))\
    >>>     .query(Range('rank', gte=7))\
    >>>     .query(Term('roles.role', value='Reporter'), parent='nested_roles')

    >>> equal_queries(q.to_dict(), expected_query)
    True

    >>> from pandagg.utils import equal_queries
    >>> from pandagg.query import Nested, Bool, Query, Range, Term, Terms

    >>> q = Query()\
    >>>     .query({'terms': {'genres': ['Action', 'Thriller']}})\
    >>>     .nested(path='roles', _name='nested_roles', query=Term('roles.gender', value='F'))\
    >>>     .query(Range('rank', gte=7))\
    >>>     .query(Term('roles.role', value='Reporter'), parent='nested_roles')

    >>> equal_queries(q.to_dict(), expected_query)
    True

    >>> from pandagg.utils import equal_queries
    >>> from pandagg.query import Nested, Bool, Query, Range, Term, Terms

    >>> q = Query()\
    >>>     .query({'terms': {'genres': ['Action', 'Thriller']}})\
    >>>     .nested(path='roles', _name='nested_roles', query=Term('roles.gender', value='F'))\
    >>>     .query(Range('rank', gte=7))\
    >>>     .query(Term('roles.role', value='Reporter'), parent='nested_roles')

    >>> equal_queries(q.to_dict(), expected_query)
    True

    >>> from pandagg.utils import equal_queries
    >>> from pandagg.query import Nested, Bool, Query, Range, Term, Terms

    >>> q = Query()\
    >>>     .query({'terms': {'genres': ['Action', 'Thriller']}})\
    >>>     .nested(path='roles', _name='nested_roles', query=Term('roles.gender', value='F'))\
    >>>     .query(Range('rank', gte=7))\
    >>>     .query(Term('roles.role', value='Reporter'), parent='nested_roles')

    >>> equal_queries(q.to_dict(), expected_query)
    True

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


***********
Aggregation
***********

Aggregation declaration
=======================

Aggregation response
====================

TODO

*******
Mapping
*******

Here is a portion of :doc:`IMDB` example mapping:

    >>> imdb_mapping = {
    >>>     'dynamic': False,
    >>>     'properties': {
    >>>         'movie_id': {'type': 'integer'},
    >>>         'name': {
    >>>             'type': 'text',
    >>>             'fields': {
    >>>                 'raw': {'type': 'keyword'}
    >>>             }
    >>>         },
    >>>         'year': {
    >>>             'type': 'date',
    >>>             'format': 'yyyy'
    >>>         },
    >>>         'rank': {'type': 'float'},
    >>>         'genres': {'type': 'keyword'},
    >>>         'roles': {
    >>>             'type': 'nested',
    >>>             'properties': {
    >>>                 'role': {'type': 'keyword'},
    >>>                 'actor_id': {'type': 'integer'},
    >>>                 'gender': {'type': 'keyword'},
    >>>                 'first_name':  {
    >>>                     'type': 'text',
    >>>                     'fields': {
    >>>                         'raw': {'type': 'keyword'}
    >>>                     }
    >>>                 },
    >>>                 'last_name':  {
    >>>                     'type': 'text',
    >>>                     'fields': {
    >>>                         'raw': {'type': 'keyword'}
    >>>                     }
    >>>                 }
    >>>             }
    >>>         }
    >>>     }
    >>> }

Mapping DSL
===========

The :class:`~pandagg.tree.mapping.Mapping` class provides a more compact view, which can help when dealing with large mappings:

    >>> from pandagg.mapping import Mapping
    >>> m = Mapping(imdb_mapping)
    <Mapping>
                                                                 {Object}
    ├── genres                                                    Keyword
    ├── movie_id                                                  Integer
    ├── name                                                      Text
    │   └── raw                                                 ~ Keyword
    ├── rank                                                      Float
    ├── roles                                                    [Nested]
    │   ├── actor_id                                              Integer
    │   ├── first_name                                            Text
    │   │   └── raw                                             ~ Keyword
    │   ├── gender                                                Keyword
    │   ├── last_name                                             Text
    │   │   └── raw                                             ~ Keyword
    │   └── role                                                  Keyword
    └── year                                                      Date


With pandagg DSL, an equivalent declaration would be the following:

    >>> from pandagg.mapping import Mapping, Object, Nested, Float, Keyword, Date, Integer, Text
    >>>
    >>> dsl_mapping = Mapping(properties=[
    >>>     Integer('movie_id'),
    >>>     Text('name', fields=[
    >>>         Keyword('raw')
    >>>     ]),
    >>>     Date('year', format='yyyy'),
    >>>     Float('rank'),
    >>>     Keyword('genres'),
    >>>     Nested('roles', properties=[
    >>>         Keyword('role'),
    >>>         Integer('actor_id'),
    >>>         Keyword('gender'),
    >>>         Text('first_name', fields=[
    >>>             Keyword('raw')
    >>>         ]),
    >>>         Text('last_name', fields=[
    >>>             Keyword('raw')
    >>>         ])
    >>>     ])
    >>> ])

Which is exactly equivalent to initial mapping:

    >>> dsl_mapping.serialize() == imdb_mapping
    True


Interactive mapping
===================

In interactive context, the :class:`~pandagg.interactive.mapping.IMapping` class provides navigation features with autocompletion to quickly discover a large
mapping:

    >>> from pandagg.mapping import IMapping
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

Client can be bound either at initiation:

    >>> m = IMapping(imdb_mapping, client=client, index_name='movies')

or afterwards through `bind` method:

    >>> m = IMapping(imdb_mapping)
    >>> m.bind(client=client, index_name='movies')

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

