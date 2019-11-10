### Indices and mapping exploration


## Detached mode

## Client mode
```
>>> from pandagg import PandAgg

>>> uri = '### MY ES URI ###'
>>> p = PandAgg(hosts=[uri])
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
... (truncated)

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
>>> report.mapping.global_metrics.dataset.labels_count()

<Mapping Field global_metrics.dataset.labels_count> of type integer:
{
    "type": "integer"
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