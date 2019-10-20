## Pandagg client wrapper

```
>>> from elasticsearch import Elasticsearch
>>> from pandagg import PandAgg

>>> uri = 'localhost:9200/'
>>> es = Elasticsearch(hosts=[uri])

>>> p = PandAgg(client=es)
>>> p.fetch_indices()
>>> p.indices
<Indices> ['classification_report', 'other_index']


# attribute access
>>> report = p.indices.classification_report
>>> report

<ClientBoundIndex> ['warmers', 'name', 'settings', 'mapping', 'client', 'aliases']
```

`ClientBound` refers to the fact that you can directly operate on ElasticSearch index through this Index object.

```
>>> report\
        .groupby(['classification_type', 'global_metrics.field.name'])\
        .agg([
            Avg('avg_nb_classes', field='global_metrics.dataset.nb_classes'),
            Avg('avg_f1_micro', field='global_metrics.performance.test.micro.f1_score'),
        ], output='dataframe')


# dataframe
                                               avg_f1_micro  avg_nb_classes  doc_count
classification_type global_metrics.field.name
multilabel          hazardpictograms               0.830353        5.203252        369
                    islabeledby                    0.815382       88.726287        369
                    flavors                        0.416222       27.577657        367
                    hasnotableingredients          0.837541      107.824268        239
                    allergentypelist               0.829144       65.592308        130
                    ispracticecompatible           0.725455       18.710938        128
                    gpc                            0.953765      183.210084        119
                    preservationmethods            0.803796        9.973684         76
multiclass          kind                           0.896817      206.502703        370
                    gpc                            0.932126      211.126263        198
```
