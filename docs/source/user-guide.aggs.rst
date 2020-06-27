***********
Aggregation
***********

The :class:`~pandagg.tree.aggs.aggs.Aggs` class provides :

- multiple syntaxes to declare and udpate a aggregation
- aggregation clause validation
- ability to insert clauses at specific locations (and not just below last manipulated clause)


Declaration
===========

From native "dict" query
------------------------

Given the following aggregation:

    >>> expected_aggs = {
    >>>   "decade": {
    >>>     "histogram": {"field": "year", "interval": 10},
    >>>     "aggs": {
    >>>       "genres": {
    >>>         "terms": {"field": "genres", "size": 3},
    >>>         "aggs": {
    >>>           "max_nb_roles": {
    >>>             "max": {"field": "nb_roles"}
    >>>           },
    >>>           "avg_rank": {
    >>>             "avg": {"field": "rank"}
    >>>           }
    >>>         }
    >>>       }
    >>>     }
    >>>   }
    >>> }

To declare :class:`~pandagg.tree.aggs.aggs.Aggs`, simply pass "dict" query as argument:

    >>> from pandagg.aggs import Aggs
    >>> a = Aggs(expected_aggs)

A visual representation of the query is available with :func:`~pandagg.tree.aggs.aggs.Aggs.show`:

    >>> a.show()
    <Aggregations>
    decade                                         <histogram, field="year", interval=10>
    └── genres                                            <terms, field="genres", size=3>
        ├── max_nb_roles                                          <max, field="nb_roles">
        └── avg_rank                                                  <avg, field="rank">


Call :func:`~pandagg.tree.aggs.aggs.Aggs.to_dict` to convert it to native dict:

    >>> a.to_dict() == expected_aggs
    True

With DSL classes
----------------

Pandagg provides a DSL to declare this query in a quite similar fashion:

    >>> from pandagg.aggs import Histogram, Terms, Max, Avg
    >>>
    >>> a = Histogram("decade", field='year', interval=10, aggs=[
    >>>     Terms("genres", field="genres", size=3, aggs=[
    >>>         Max("max_nb_roles", field="nb_roles"),
    >>>         Avg("avg_rank", field="range")
    >>>     ]),
    >>> ])

All these classes inherit from :class:`~pandagg.tree.aggs.aggs.Aggs` and thus provide the same interface.

    >>> from pandagg.aggs import Aggs
    >>> isinstance(a, Aggs)
    True

With flattened syntax
---------------------

In the flattened syntax, the first argument is the aggregation name, the second argument is the aggregation type, the
following keyword arguments define the aggregation body:

    >>> from pandagg.query import Aggs
    >>> a = Aggs('genres', 'terms', size=3)
    >>> a.to_dict()
    {'genres': {'terms': {'field': 'genres', 'size': 3}}}


Aggregations enrichment
=======================

Aggregations can be enriched using two methods:

- :func:`~pandagg.tree.aggs.aggs.Aggs.aggs`
- :func:`~pandagg.tree.aggs.aggs.Aggs.groupby`

Both methods return a new :class:`~pandagg.tree.aggs.aggs.Aggs` instance, and keep unchanged the initial Aggregation.

For instance:

    >>> from pandagg.aggs import Aggs
    >>> initial_a = Aggs()
    >>> enriched_a = initial_a.aggs('genres_agg', 'terms', field='genres')

    >>> initial_q.to_dict()
    None

    >>> enriched_q.to_dict()
    {'genres_agg': {'terms': {'field': 'genres'}}}

.. note::

    Calling :func:`~pandagg.tree.aggs.aggs.Aggs.to_dict` on an empty Aggregation returns `None`

        >>> from pandagg.aggs import Aggs
        >>> Aggs().to_dict()
        None


TODO
