## What is it?

**pandagg** is a Python package providing a simple interface to manipulate ElasticSearch queries and aggregations.

***Disclaimer*** *:this is a pre-release version*
## Features

- flexible aggregation and search queries declaration
- query validation based on provided mapping
- parsing of aggregation results in handy formats: tree with interactive navigation, csv-like tabular breakdown, and others
- mapping interactive navigation


## Usage

Full documentation and HOW-TO are available here: 

***TODO** - redirect to sphinx documentation*

***TODO** - find good simple example*

## Installation
```
pip install pandagg
```

## Dependencies
**Hard dependency**: [treelib](https://pypi.org/project/treelib/): 1.5.6 or higher (/!\ waiting for [this PR](https://github.com/caesar0301/treelib/pull/120) approval)

**Soft dependency**: to parse aggregation results as tabular dataframe: [pandas](https://github.com/pandas-dev/pandas/)

## Motivations

`pandagg` only focuses on read operations (queries and aggregations), a 
high level python client [elasticsearch-dsl](https://github.com/elastic/elasticsearch-dsl-py) already exists for ElasticSearch, 
but despite many qualities, in some cases its api was not always convenient when dealing with deeply 
nested queries and aggregations.

The fundamental difference between those libraries is how they deal with the tree structure of aggregation queries
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


## Generate doc
```
# auto doc generation
rm -r docs/source/reference/*
sphinx-apidoc -o docs/source/reference pandagg -Te
# build html
rm -r docs/build/*
sphinx-build -b html docs/source docs/build
```

## Roadmap

- choose simple example to showcase pandagg in readme
- write sphinx documentation
- implement CI workflow: python2/3 tests, coverage
- nested fields: automatic handling and validation in `Query` instances
- `Query.query`, `Agg.agg`, `Agg.groupby` methods: allow passing of `tree` instance, in addition to current `dict` and `node` syntaxes
- documentation; explain challenges induced by nested `nodes` syntaxes: for instance why are nested query clauses
saved in `children` attribute before tree deserialization
- extend test coverage on `named` queries serialization
- evaluate interest and tradeoffs of using metaclasses like similarly to `elasticsearch-dsl` library to declare `node` classes
- on aggregation `nodes`, ensure all allowed `fields` are listed
- on aggregation response `tree`, use `Query` DSL to compute bucket filters
- package versions for different ElasticSearch versions
- remove `Bucket` `nodes` knowledge of their `depth` once [this `treelib` issue is resolved](https://github.com/caesar0301/treelib/issues/149)