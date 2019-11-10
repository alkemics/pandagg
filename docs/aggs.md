```
>>> from pandagg.aggs import Agg
>>> from my_mapping import my_index_mapping

>>> agg = Agg(mapping=my_index_mapping) \
    .groupby([week, "local_metrics.field_class.name"], size=10) \
    .agg([
        Min(agg_name='min_f1_score', field='local_metrics.performance.test.f1_score'),
        Max(agg_name='max_f1_score', field='local_metrics.performance.test.f1_score'),
        Avg(agg_name='avg_f1_score', field='local_metrics.performance.test.f1_score')
    ])

# it automatically handles nested
>>> agg
<Aggregation>
week
└── nested_below_week
    └── local_metrics.field_class.name
        ├── avg_f1_score
        ├── max_f1_score
        └── min_f1_score

>>> agg.dict_query()
{
    "week": {
        "aggs": {
            "nested_below_week": {
                "aggs": {
                    "local_metrics.field_class.name": {
                        "aggs": {
                            "avg_f1_score": {
                                "avg": {
                                    "field": "local_metrics.performance.test.f1_score"
                                }
                            },
                            "max_f1_score": {
                                "max": {
                                    "field": "local_metrics.performance.test.f1_score"
                                }
                            },
                            "min_f1_score": {
                                "min": {
                                    "field": "local_metrics.performance.test.f1_score"
                                }
                            }
                        },
                        "terms": {
                            "field": "local_metrics.field_class.name",
                            "size": 10
                        }
                    }
                },
                "nested": {
                    "path": "local_metrics"
                }
            }
        },
        "date_histogram": {
            "field": "date",
            "format": "yyyy-MM-dd",
            "interval": "1w"
        }
    }
}
```