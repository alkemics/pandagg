##########
Principles
##########

.. note::

    This is a work in progress. Some sections still need to be furnished.


This library focuses on two principles:

* stick to the **tree** structure of Elasticsearch objects
* provide simple and flexible interfaces to make it easy and intuitive to use in an interactive usage


*****************************
Elasticsearch tree structures
*****************************

Many Elasticsearch objects have a **tree** structure, ie they are built from a hierarchy of **nodes**:

* a `mapping <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html>`_ (tree) is a hierarchy of `fields <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html>`_ (nodes)
* a `query <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>`_ (tree) is a hierarchy of query clauses (nodes)
* an `aggregation <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html>`_ (tree) is a hierarchy of aggregation clauses (nodes)
* an aggregation response (tree) is a hierarchy of response buckets (nodes)

This library sticks to that structure by providing a flexible syntax distinguishing **trees** and **nodes**, **trees** all inherit from
lighttree.Tree class, whereas nodes all inherit from lighttree.Node class.


*****************
Interactive usage
*****************

**pandagg** is designed for both for "regular" code repository usage, and "interactive" usage (ipython or jupyter
notebook usage with autocompletion features inspired by `pandas <https://github.com/pandas-dev/pandas>`_ design).

Some classes are not intended to be used elsewhere than in interactive mode (ipython), since their purpose is to serve
auto-completion features and convenient representations.

Namely:

* :class:`~pandagg.interactive.mapping.IMapping`: used to interactively navigate in mapping and run quick aggregations on some fields
* :class:`~pandagg.interactive.client.Elasticsearch`: used to discover cluster indices, and eventually navigate their mappings, or run quick access aggregations or queries.
* :class:`~pandagg.interactive.response.IResponse`: used to interactively navigate in an aggregation response

These use case will be detailed in following sections.
