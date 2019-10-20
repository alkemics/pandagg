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
### Once added in pypi, and treelib PR merged
```
pip install pandagg
```

### Until then
You will need to:
- clone **treelib** fork
- clone **pandagg**

```
# clone libraries
git clone git@github.com:leonardbinet/treelib.git --branch node_dict_pointer --single-branch
git clone git@github.com:alkemics/pandagg.git

# create virtualenv for your project
cd pandagg
virtualenv env
source env/bin/activate
python setup.py develop

# still using your pandagg environment
cd ../treelib
python setup.py develop
cd ../pandagg

# depending on your usage you might need as well
pip install pandas seaborn jupyter elasticsearch
```

## Dependencies
Only one dependency:
- [treelib](https://pypi.org/project/treelib/): 1.5.6 or higher (/!\ waiting for [this PR](https://github.com/caesar0301/treelib/pull/120) approval)

Parsing of aggregation results as dataframe will require to install as well:
- [pandas](https://github.com/pandas-dev/pandas/)

## Contributing

All contributions, bug reports, bug fixes, documentation improvements, enhancements and ideas are welcome.


## Roadmap

- examples
- missing tests: some in pandagg.aggs.agg / pandagg.aggs.response_tree / pandagg.mapping.types
- more precise doc on pandagg.aggs.agg.ClientBoundAgg/Agg
- python 2/3 compatibility
- ES 6-7 compatibility
- nested conditions of Filters aggregation propagated to children Filters aggregations
