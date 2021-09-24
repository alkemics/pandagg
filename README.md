[![PyPI Latest Release](https://img.shields.io/pypi/v/pandagg.svg)](https://pypi.org/project/pandagg/)
[![License](https://img.shields.io/pypi/l/pandagg.svg)](https://github.com/alkemics/pandagg/blob/master/LICENSE)
![Python package](https://github.com/alkemics/pandagg/workflows/Python%203%20Tests/badge.svg)
[![Coverage](https://codecov.io/github/alkemics/pandagg/coverage.svg?branch=master)](https://codecov.io/gh/alkemics/pandagg)
[![Docs](https://readthedocs.org/projects/pandagg/badge/?version=latest&style=flat)](https://pandagg.readthedocs.io/en/latest/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)


## What is it?

**pandagg** is a Python package providing a simple interface to manipulate ElasticSearch queries and aggregations. Its goal is to make it
the easiest possible to explore data indexed in an Elasticsearch cluster.

Some of its interactive features are inspired by [pandas](https://github.com/pandas-dev/pandas) library, hence the name **pandagg** which aims to apply **panda**s to Elasticsearch
**agg**regations.

**pandagg** is also greatly inspired by the official high level python client [elasticsearch-dsl](https://github.com/elastic/elasticsearch-dsl-py),
and is intended to make it more convenient to deal with deeply nested queries and aggregations.

## Why another library

`pandagg` provides the following features:
- interactive mode for cluster discovery
- richer aggregations syntax, and aggregations parsing features
- declarative indices
- bulk ORM operations
- typing annotations

## Documentation

Full documentation and user-guide are available [here on read-the-docs](https://pandagg.readthedocs.io/en/latest/).


## Installation
```
pip install pandagg
```

## Dependencies
**Hard dependency**: [ligthtree](https://pypi.org/project/lighttree/)

**Soft dependency**: to parse aggregation results as tabular dataframe: [pandas](https://github.com/pandas-dev/pandas/)


## Quick demo

Discover indices on cluster with matching pattern:
```python
>>> from elasticsearch import Elasticsearch
>>> from pandagg.discovery import discover
>>> client = Elasticsearch(hosts=['localhost:9200'])


>>> indices = discover(client, "mov*")
>>> indices
<Indices> ['movies', 'movies_fake']
```

Explore index mappings:

```python
>>> movies = indices.movies
>>> movies.imappings
<Mappings>
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
...
```
```python
>>> movies.imappings.roles
<Mappings subpart: roles>
roles                                                        [Nested]
├── actor_id                                                  Keyword
├── first_name                                                Text
│   └── raw                                                 ~ Keyword
├── full_name                                                 Text
│   └── raw                                                 ~ Keyword
├── gender                                                    Keyword
├── last_name                                                 Text
│   └── raw                                                 ~ Keyword
└── role                                                      Keyword

```
Execute aggregation on field:

```python
>>> movies.imappings.roles.gender.a.terms()
   doc_count key
M    2296792   M
F    1135174   F
```

Build search request:

```python
>> > search = movies
    .search()
    .size(2)
    .groupby('decade', 'histogram', interval=10, field='year')
    .groupby('genres', size=3)
    .agg('avg_rank', 'avg', field='rank')
    .agg('avg_nb_roles', 'avg', field='nb_roles')
    .filter('range', year={"gte": 1990})

>> > search.to_dict()
{'aggs': {'decade': {u'aggs': {'genres': {u'aggs': {'avg_nb_roles': {u'avg': {'field': 'nb_roles'}},
                                                    'avg_rank': {u'avg': {'field': 'rank'}}},
                                          'terms': {'field': 'genres', 'size': 3}}},
                     'histogram': {'field': 'year', 'interval': 10}}},
 'query': {'bool': {u'filter': [{'range': {'year': {'gte': 1990}}}]}},
 'size': 2}
```

Execute it:
```python
>>> response = search.execute()
>>> response
<Response> took 52ms, success: True, total result >=10000, contains 2 hits
```

Parse it in tabular format:
```python
>>> response.aggregations.to_dataframe()
                    avg_nb_roles  avg_rank  doc_count
decade genres
1990.0 Documentary      3.778982  6.517093       8393
       Drama           18.518067  5.981429      12232
       Short            3.023284  6.311326      12197
2000.0 Documentary      5.581433  6.980898       8639
       Drama           14.385391  6.269675      11500
       Short            4.053082  6.836253      13451
```

## Disclaimers

It does not ensure retro-compatible with previous versions of elasticsearch (intended to work with >=7). It is part
of the roadmap to tag **pandagg** versions according to the ElasticSearch versions they are related to (ie
v7.1.4 would work with Elasticsearch v7.X.X).

## Contributing

All contributions, bug reports, bug fixes, documentation improvements, enhancements and ideas are welcome.


## Roadmap priorities

- clean and proper documentation
- package versions for different ElasticSearch versions
- onboard new contributors
