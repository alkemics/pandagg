## What is it?

**pandagg** is a Python package providing a simple interface to manipulate ElasticSearch aggregations.

***Disclaimer*** *:this is a pre-release version*
## Features

- class to navigate into an index mapping
- flexible interface to build aggregation queries (with automatic handling of nested aggregations, and aggregation validation if a mapping is provided)
- classes to display, navigate, manipulate results
- ability to build filter query listing documents belonging to an aggregation bucket

## Usage

### Indices and mapping exploration
```
>>> from elasticsearch import Elasticsearch
>>> from pandagg import PandAgg

>>> uri = '### MY ES URI ###'
>>> es = Elasticsearch(hosts=[uri])

>>> p = PandAgg(client=es)
>>> p.fetch_indices()
>>> p.indices
<Indices> ['classification_report', 'other_index']

# attribute access
>>> report = p.indices.classification_report
>>> report.mapping

<ClientBoundMapping>
classification_report
├── classification_type                                     String
├── date                                                    Date
├── global_metrics                                         {Object}
│   ├── dataset                                            {Object}
│   │   ├── labels_count                                    Integer
│   │   ├── labels_diversity                                Integer
│   │   ├── nb_classes                                      Integer
│   │   ├── nb_samples                                      Integer
│   │   ├── support_test                                    Integer
│   │   └── support_train                                   Integer
│   ├── field                                              {Object}
│   │   ├── id                                              Integer
...

# navigation in mapping (with autocomplete)
>>> report.mapping.global_metrics.dataset

<ClientBoundMapping subpart: global_metrics.dataset>
dataset                                            {Object}
├── labels_count                                    Integer
├── labels_diversity                                Integer
├── nb_classes                                      Integer
├── nb_samples                                      Integer
├── support_test                                    Integer
└── support_train                                   Integer

# show exact mapping definition of a field by calling it
>>> report.mapping.global_metrics.dataset()

<Mapping Field global_metrics.dataset> of type object:
{
    "dynamic": "false",
    "properties": {
        "labels_count": {
            "type": "integer"
        },
        "nb_classes": {
            "type": "integer"
        },
        "nb_samples": {
            "type": "integer"
        },
        ...
    }
}

# compute available aggregations on a given field using 'a' attribute (autocomplete available)
>>> report.mapping.global_metrics.dataset.nb_classes.a. (press tab)
report.mapping.global_metrics.dataset.nb_classes.a.avg
report.mapping.global_metrics.dataset.nb_classes.a.max
report.mapping.global_metrics.dataset.nb_classes.a.min
report.mapping.global_metrics.dataset.nb_classes.a.terms

>>> report.mapping.global_metrics.dataset.nb_classes.a.avg()
# bucket key, bucket
[(None, {u'value': 94.00464330941325})]
...
```

### Build aggregations


### Manipulate ElasticSearch response
```
>>> from pandagg.aggs import Agg
>>> from my_mapping import my_index_mapping

>>> agg = Agg(mapping=my_index_mapping) \
    .groupby([week, "local_metrics.field_class.name"], default_size=10) \
    .agg([
        Min(agg_name='min_f1_score', field='local_metrics.performance.test.f1_score'),
        Max(agg_name='max_f1_score', field='local_metrics.performance.test.f1_score'),
        Avg(agg_name='avg_f1_score', field='local_metrics.performance.test.f1_score')
    ])
```

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
- missing tests: pandagg.aggs.agg / pandagg.aggs.response_tree / pandagg.wrapper.wrapper / pandagg.mapping.types
- more precise doc on pandagg.aggs.agg.ClientBoundAgg/Agg
- python 2/3 compatibility
- ES 6-7 compatibility
- nested conditions of Filters aggregation propagated to children Filters aggregations
