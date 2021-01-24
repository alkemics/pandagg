
******
Search
******

:class:`~pandagg.search.Search` class is intended to perform requests, and refers to
Elasticsearch `search api <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html>`_:

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .aggs('avg_rank', 'avg', field='rank')\
    >>>     .agg('avg_nb_roles', 'avg', field='nb_roles')\
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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

-

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .aggs('avg_rank', 'avg', field='rank')\
    >>>     .agg('avg_nb_roles', 'avg', field='nb_roles')\
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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

-

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .aggs('avg_rank', 'avg', field='rank')\
    >>>     .agg('avg_nb_roles', 'avg', field='nb_roles')\
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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

-

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .aggs('avg_rank', 'avg', field='rank')\
    >>>     .agg('avg_nb_roles', 'avg', field='nb_roles')\
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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

-

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .agg('avg_rank', 'avg', field='rank')\
    >>>     .agg('avg_nb_roles', 'avg', field='nb_roles')\
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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

-

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .agg('avg_rank', 'avg', field='rank')\
    >>>     .agg('avg_nb_roles', 'avg', field='nb_roles')\
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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

-

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .agg('avg_rank', 'avg', field='rank')\
    >>>     .agg('avg_nb_roles', 'avg', field='nb_roles')\
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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

-

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .agg('avg_rank', 'avg', field='rank')\
    >>>     .agg('avg_nb_roles', 'avg', field='nb_roles')\
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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

-

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .agg('avg_rank', 'avg', field='rank')\
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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

-

    >>> from pandagg.search import Search
    >>>
    >>> client = ElasticSearch(hosts=['localhost:9200'])
    >>> search = Search(using=client, index='movies')\
    >>>     .size(2)\
    >>>     .groupby('decade', 'histogram', interval=10, field='year')\
    >>>     .groupby('genres', size=3)\
    >>>     .agg('avg_rank', 'avg', field='rank')\
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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

-

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
                "field": "genres",
                "size": 3
              },
              "aggs": {
                "avg_rank": {
                  "avg": {
                    "field": "rank"
                  }
                },
                "avg_nb_roles": {
                  "avg": {
                    "field": "nb_roles"
                  }
                }
              }
            }
          }
        }
      },
      "size": 2
    }

It relies on:

- :class:`~pandagg.query.Query` to build queries, **query** or **post_filter** (see :doc:`user-guide.query`),
- :class:`~pandagg.aggs.Aggs` to build aggregations (see :doc:`user-guide.aggs`)


.. note::

    All methods described below return a new :class:`~pandagg.search.Search` instance, and keep unchanged the
    initial search request.

        >>> from pandagg.search import Search
        >>> initial_s = Search()
        >>> enriched_s = initial_s.query('terms', genres=['Comedy', 'Short'])

        >>> initial_s.to_dict()
        {}

        >>> enriched_s.to_dict()
        {'query': {'terms': {'genres': ['Comedy', 'Short']}}}



Query part
==========

The **query** or **post_filter** parts of a :class:`~pandagg.search.Search` instance are available respectively
under **_query** and **_post_filter** attributes.

    >>> search._query.__class__
    pandagg.tree.query.abstract.Query
    >>> search._query.show()
    <Query>
    bool
    └── filter
        └── range, field=year, gte=1990


To enrich **query** of a search request, methods are exactly the same as for a
:class:`~pandagg.query.Query` instance.

    >>> Search().must_not('range', year={'lt': 1980})
    {
      "query": {
        "bool": {
          "must_not": [
            {
              "range": {
                "year": {
                  "lt": 1980
                }
              }
            }
          ]
        }
      }
    }

See section :doc:`user-guide.query` for more details.


To enrich **post_filter** of a search request, use :func:`~pandagg.search.post_filter`:

    >>> Search().post_filter('term', genres='Short')
    {
      "post_filter": {
        "term": {
          "genres": {
            "value": "Short"
          }
        }
      }
    }


Aggregations part
=================

The **aggregations** part of a :class:`~pandagg.search.Search` instance is available under **_aggs** attribute.

    >>> search._aggs.__class__
    pandagg.tree.aggs.aggs.Aggs
    >>> search._aggs.show()
    <Aggregations>
    decade                                         <histogram, field="year", interval=10>
    └── genres                                            <terms, field="genres", size=3>
        ├── avg_nb_roles                                          <avg, field="nb_roles">
        └── avg_rank                                                  <avg, field="rank">


To enrich **aggregations** of a search request, methods are exactly the same as for a
:class:`~pandagg.aggs.Aggs` instance.

    >>> Search()\
    >>> .groupby('decade', 'histogram', interval=10, field='year')\
    >>> .agg('avg_rank', 'avg', field='rank')
    {
      "aggs": {
        "decade": {
          "histogram": {
            "field": "year",
            "interval": 10
          },
          "aggs": {
            "avg_rank": {
              "avg": {
                "field": "rank"
              }
            }
          }
        }
      }
    }


See section

    >>> Search()\
    >>> .groupby('decade', 'histogram', interval=10, field='year')\
    >>> .agg('avg_rank', 'avg', field='rank')
    {
      "aggs": {
        "decade": {
          "histogram": {
            "field": "year",
            "interval": 10
          },
          "aggs": {
            "avg_rank": {
              "avg": {
                "field": "rank"
              }
            }
          }
        }
      }
    }


See section

    >>> Search()\
    >>> .groupby('decade', 'histogram', interval=10, field='year')\
    >>> .aggs('avg_rank', 'avg', field='rank')
    {
      "aggs": {
        "decade": {
          "histogram": {
            "field": "year",
            "interval": 10
          },
          "aggs": {
            "avg_rank": {
              "avg": {
                "field": "rank"
              }
            }
          }
        }
      }
    }


See section :doc:`user-guide.aggs` for more details.

Other search request parameters
===============================

**size**, **sources**, **limit** etc, all those parameters are documented in :class:`~pandagg.search.Search`
documentation and their usage is quite self-explanatory.


Request execution
=================


To a execute a search request, you must first have bound it to an Elasticsearch client beforehand:

    >>> from elasticsearch import Elasticsearch
    >>> client = Elasticsearch(hosts=['localhost:9200'])

Either at instantiation:

    >>> from pandagg.search import Search
    >>> search = Search(using=client, index='movies')

Either with :func:`~pandagg.search.Search.using`
method:

    >>> from pandagg.search import Search
    >>> search = Search()\
    >>> .using(client=client)\
    >>> .index('movies')

Executing a :class:`~pandagg.search.Search` request using :func:`~pandagg.search.Search.execute` will return a
:class:`~pandagg.response.Response` instance (see more in :doc:`user-guide.response`).


    >>> response = search.execute()
    >>> response
    <Response> took 58ms, success: True, total result >=10000, contains 2 hits
    >>> response.__class__
    pandagg.response.Response
