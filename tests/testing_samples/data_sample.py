#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Sample regrouping all representations of a given aggregation, and its expected ES response/pandagg expected parsing
results.
"""

from pandagg.aggs import Aggs, Avg, Terms
from tests.testing_samples.mapping_example import MAPPINGS


EXPECTED_AGG_QUERY = {
    "classification_type": {
        "aggs": {
            "global_metrics.field.name": {
                "aggs": {
                    "avg_f1_micro": {
                        "avg": {
                            "field": "global_metrics.performance.test.micro.f1_score"
                        }
                    },
                    "avg_nb_classes": {
                        "avg": {"field": "global_metrics.dataset.nb_classes"}
                    },
                },
                "terms": {"field": "global_metrics.field.name"},
            }
        },
        "terms": {"field": "classification_type"},
    }
}


def get_wrapper_declared_agg():
    return (
        Aggs(mappings=MAPPINGS)
        .groupby("classification_type")
        .groupby("global_metrics.field.name")
        .agg("avg_nb_classes", Avg(field="global_metrics.dataset.nb_classes"))
        .agg(
            "avg_f1_micro", Avg(field="global_metrics.performance.test.micro.f1_score")
        )
    )


def get_node_hierarchy():
    return {
        "classification_type": Terms(
            field="classification_type",
            aggs={
                "global_metrics.field.name": Terms(
                    field="global_metrics.field.name",
                    aggs={
                        "avg_nb_classes": Avg(
                            field="global_metrics.dataset.nb_classes"
                        ),
                        "avg_f1_micro": Avg(
                            field="global_metrics.performance.test.micro.f1_score"
                        ),
                    },
                )
            },
        )
    }


ES_AGG_RESPONSE = {
    "classification_type": {
        "buckets": [
            {
                "doc_count": 1797,
                "global_metrics.field.name": {
                    "buckets": [
                        {
                            "avg_f1_micro": {"value": 0.72},
                            "avg_nb_classes": {"value": 18.71},
                            "doc_count": 128,
                            "key": "ispracticecompatible",
                        },
                        {
                            "avg_f1_micro": {"value": 0.80},
                            "avg_nb_classes": {"value": 9.97},
                            "doc_count": 76,
                            "key": "preservationmethods",
                        },
                    ],
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                },
                "key": "multilabel",
            },
            {
                "doc_count": 568,
                "global_metrics.field.name": {
                    "buckets": [
                        {
                            "avg_f1_micro": {"value": 0.89},
                            "avg_nb_classes": {"value": 206.50},
                            "doc_count": 370,
                            "key": "kind",
                        },
                        {
                            "avg_f1_micro": {"value": 0.93},
                            "avg_nb_classes": {"value": 211.12},
                            "doc_count": 198,
                            "key": "gpc",
                        },
                    ],
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                },
                "key": "multiclass",
            },
        ],
        "doc_count_error_upper_bound": 0,
        "sum_other_doc_count": 0,
    }
}

EXPECTED_RESP_REPR = """
├── classification_type=multilabel                      1797
│   ├── global_metrics.field.name=ispracticecompatible   128
│   │   ├── avg_f1_micro                                0.72
│   │   └── avg_nb_classes                             18.71
│   └── global_metrics.field.name=preservationmethods     76
│       ├── avg_f1_micro                                 0.8
│       └── avg_nb_classes                              9.97
└── classification_type=multiclass                       568
    ├── global_metrics.field.name=kind                   370
    │   ├── avg_f1_micro                                0.89
    │   └── avg_nb_classes                             206.5
    └── global_metrics.field.name=gpc                    198
        ├── avg_f1_micro                                0.93
        └── avg_nb_classes                            211.12
"""
EXPECTED_RESPONSE_REPR = """<IResponse>\n%s""" % EXPECTED_RESP_REPR
EXPECTED_RESPONSE_TREE_REPR = """<AggsResponseTree>\n%s""" % EXPECTED_RESP_REPR


EXPECTED_NORMALIZED_RESPONSE = {
    "children": [
        {
            "children": [
                {
                    "children": [
                        {"key": None, "level": "avg_nb_classes", "value": 18.71},
                        {"key": None, "level": "avg_f1_micro", "value": 0.72},
                    ],
                    "key": "ispracticecompatible",
                    "level": "global_metrics.field.name",
                    "value": 128,
                },
                {
                    "children": [
                        {"key": None, "level": "avg_nb_classes", "value": 9.97},
                        {"key": None, "level": "avg_f1_micro", "value": 0.8},
                    ],
                    "key": "preservationmethods",
                    "level": "global_metrics.field.name",
                    "value": 76,
                },
            ],
            "key": "multilabel",
            "level": "classification_type",
            "value": 1797,
        },
        {
            "children": [
                {
                    "children": [
                        {"key": None, "level": "avg_nb_classes", "value": 206.5},
                        {"key": None, "level": "avg_f1_micro", "value": 0.89},
                    ],
                    "key": "kind",
                    "level": "global_metrics.field.name",
                    "value": 370,
                },
                {
                    "children": [
                        {"key": None, "level": "avg_nb_classes", "value": 211.12},
                        {"key": None, "level": "avg_f1_micro", "value": 0.93},
                    ],
                    "key": "gpc",
                    "level": "global_metrics.field.name",
                    "value": 198,
                },
            ],
            "key": "multiclass",
            "level": "classification_type",
            "value": 568,
        },
    ],
    "key": None,
    "level": "root",
    "value": None,
}
