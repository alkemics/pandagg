## What is it?

**pandagg** is a Python package providing a simple interface to manipulate ElasticSearch aggregations.

***Disclaimer*** *:this is a pre-release version*
## Features

- classes to navigate into an index mapping, list possible aggregations on chosen field, and compute those aggregations ([example here](docs/mapping.md))
- flexible interface to declare aggregation queries with:
    - automated handling of nested aggregations
    - aggregation validation
    - parsing of results in several formats
- classes to display, navigate, manipulate results
- ability to build filter query listing documents belonging to an aggregation bucket

## Usage
See examples in [docs](docs) directory.

## Installation
Compatible on python 2 and python 3.
### Once added in pypi, and treelib PR merged
```
pip install pandagg
```

### Until then

```
git clone git@github.com:alkemics/pandagg.git

# create virtualenv for your project
cd pandagg
virtualenv env
source env/bin/activate
python setup.py develop


# depending on your usage you might want to install as well
# because of https://github.com/pypa/pip/issues/6667 issue, you might have to install numpy before pandas
pip install numpy pandas jupyter matplotlib
```

## Dependencies
Only one dependency:
- [treelib](https://pypi.org/project/treelib/): 1.5.6 or higher (/!\ waiting for [this PR](https://github.com/caesar0301/treelib/pull/120) approval)

Parsing of aggregation results as dataframe will require to install as well:
- [pandas](https://github.com/pandas-dev/pandas/)

## Motivations

A [high level python client](https://github.com/elastic/elasticsearch-dsl-py) already exists for ElasticSearch,
but despite many qualities, its api was not convenient when dealing with deeply nested aggregations.

The fundamental difference between those libraries is how we deal with the tree structure of aggregation queries
and their responses.

Suppose we have this aggregation structure: (types of agg don't matter). Let's call all of **A**, **B**, **C**, **D** our aggregation **nodes**, and the whole structure our **tree**.
```
A           (Terms agg)
└── B       (Filters agg)
    ├── C   (Avg agg)
    └── D   (Sum agg)
```


Question is who has the charge of storing the **tree structure** (how **nodes** are connected)?

In ***elasticsearch-dsl*** library, each aggregation **node** is responsible of knowing which are its direct children.

In ***pandagg***, all **nodes** are agnostic about which are their parents/children, and a **tree** object is in charge
of storing this structure. It becomes much easier to add/update/remove aggregation **nodes** or **sub-trees** in
specific locations of the initial **tree**, thus making it easier to build your aggregation.

## Contributing

All contributions, bug reports, bug fixes, documentation improvements, enhancements and ideas are welcome.


## Roadmap

- documentation
- more precise doc on pandagg.aggs.agg.ClientBoundAgg/Agg
- ES 6-7 compatibility
