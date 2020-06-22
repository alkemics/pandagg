[![PyPI Latest Release](https://img.shields.io/pypi/v/pandagg.svg)](https://pypi.org/project/pandagg/)
[![License](https://img.shields.io/pypi/l/pandagg.svg)](https://github.com/alkemics/pandagg/blob/master/LICENSE)
![Python package](https://github.com/alkemics/pandagg/workflows/Python%203%20Tests/badge.svg)
![Python package](https://github.com/alkemics/pandagg/workflows/Python%202%20Tests/badge.svg)
[![Coverage](https://codecov.io/github/alkemics/pandagg/coverage.svg?branch=master)](https://codecov.io/gh/alkemics/pandagg)
[![Docs](https://readthedocs.org/projects/pandagg/badge/?version=latest&style=flat)](https://pandagg.readthedocs.io/en/latest/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


## What is it?

**pandagg** is a Python package providing a simple interface to manipulate ElasticSearch queries and aggregations. Its goal is to make it
the easiest possible to explore data indexed in an Elasticsearch cluster.

Some of its interactive features are inspired by [pandas](https://github.com/pandas-dev/pandas) library, hence the name **pandagg** which aims to apply **panda**s to Elasticsearch
**agg**regations.

**pandagg** is also greatly inspired by the official high level python client [elasticsearch-dsl](https://github.com/elastic/elasticsearch-dsl-py),
and is intended to make it more convenient to deal with deeply nested queries and aggregations.


## Features

- flexible aggregation and search queries declaration, with ability to insert clauses at specific points (and not only below last manipulated clause)
- query validation based on provided mapping
- parsing of aggregation results in convenient formats: tree with interactive navigation, csv-like tabular breakdown, pandas dataframe, and others
- cluster indices discovery module, and mapping interactive navigation


## Documentation

Full documentation and user-guide are available [here on read-the-docs](https://pandagg.readthedocs.io/en/latest/).


## Installation
```
pip install pandagg
```

## Dependencies
**Hard dependency**: [ligthtree](https://pypi.org/project/lighttree/)

**Soft dependency**: to parse aggregation results as tabular dataframe: [pandas](https://github.com/pandas-dev/pandas/)


### Disclaimers

It does not ensure retro-compatible with previous versions of elasticsearch (intended to work with >=7). It is part
of the roadmap to tag **pandagg** versions according to the ElasticSearch versions they are related to (ie
v7.1.4 would work with Elasticsearch v7.X.X).

It doesn't provide yet all functionalities provided by the official client (for instance ORM like insert/updates, index
operations etc..). Primary focus of **pandagg** was on read operations.

## Contributing

All contributions, bug reports, bug fixes, documentation improvements, enhancements and ideas are welcome.


## Roadmap

- on aggregation `nodes`, ensure all allowed `fields` are listed
- expand functionalities: proper ORM similar to elasticsearch-dsl Document classes, index managing operations
- package versions for different ElasticSearch versions
- composite aggregation iterator
- clean and proper documentation
