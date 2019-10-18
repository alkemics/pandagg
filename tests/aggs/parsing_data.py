#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandagg.aggs import Agg
from pandagg.nodes import Avg
from tests.mapping.mapping_example import MAPPING_DETAIL, MAPPING_NAME


def get_agg_instance():
    return Agg(mapping={MAPPING_NAME: MAPPING_DETAIL}) \
        .groupby(['classification_type', 'global_metrics.field.name']) \
        .agg([
            Avg('avg_nb_classes', field='global_metrics.dataset.nb_classes'),
            Avg('avg_f1_micro', field='global_metrics.performance.test.micro.f1_score'),
        ])


ES_AGG_RESPONSE = {
    "classification_type": {
        "buckets": [
            {
                "doc_count": 1797,
                "global_metrics.field.name": {
                    "buckets": [
                        {
                            "avg_f1_micro": {
                                "value": 0.83
                            },
                            "avg_nb_classes": {
                                "value": 5.20
                            },
                            "doc_count": 369,
                            "key": "hazardpictograms"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.81
                            },
                            "avg_nb_classes": {
                                "value": 88.72
                            },
                            "doc_count": 369,
                            "key": "islabeledby"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.41
                            },
                            "avg_nb_classes": {
                                "value": 27.57
                            },
                            "doc_count": 367,
                            "key": "flavors"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.83
                            },
                            "avg_nb_classes": {
                                "value": 107.82
                            },
                            "doc_count": 239,
                            "key": "hasnotableingredients"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.82
                            },
                            "avg_nb_classes": {
                                "value": 65.59
                            },
                            "doc_count": 130,
                            "key": "allergentypelist"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.72
                            },
                            "avg_nb_classes": {
                                "value": 18.71
                            },
                            "doc_count": 128,
                            "key": "ispracticecompatible"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.95
                            },
                            "avg_nb_classes": {
                                "value": 183.21
                            },
                            "doc_count": 119,
                            "key": "gpc"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.80
                            },
                            "avg_nb_classes": {
                                "value": 9.97
                            },
                            "doc_count": 76,
                            "key": "preservationmethods"
                        }
                    ],
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0
                },
                "key": "multilabel"
            },
            {
                "doc_count": 568,
                "global_metrics.field.name": {
                    "buckets": [
                        {
                            "avg_f1_micro": {
                                "value": 0.89
                            },
                            "avg_nb_classes": {
                                "value": 206.50
                            },
                            "doc_count": 370,
                            "key": "kind"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.93
                            },
                            "avg_nb_classes": {
                                "value": 211.12
                            },
                            "doc_count": 198,
                            "key": "gpc"
                        }
                    ],
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0
                },
                "key": "multiclass"
            }
        ],
        "doc_count_error_upper_bound": 0,
        "sum_other_doc_count": 0
    }
}

EXPECTED_TREE = u"""
<AggResponse>
aggs
├── classification_type=multilabel                  1797
│   ├── global_metrics.field.name=hazardpictograms    369
│   │   ├── avg_nb_classes                           5.2
│   │   └── avg_f1_micro                            0.83
│   ├── global_metrics.field.name=islabeledby        369
│   │   ├── avg_nb_classes                         88.72
│   │   └── avg_f1_micro                            0.81
│   ├── global_metrics.field.name=flavors            367
│   │   ├── avg_nb_classes                         27.57
│   │   └── avg_f1_micro                            0.41
│   ├── global_metrics.field.name=hasnotableingredients    239
│   │   ├── avg_nb_classes                        107.82
│   │   └── avg_f1_micro                            0.83
│   ├── global_metrics.field.name=allergentypelist    130
│   │   ├── avg_nb_classes                         65.59
│   │   └── avg_f1_micro                            0.82
│   ├── global_metrics.field.name=ispracticecompatible    128
│   │   ├── avg_nb_classes                         18.71
│   │   └── avg_f1_micro                            0.72
│   ├── global_metrics.field.name=gpc                119
│   │   ├── avg_nb_classes                        183.21
│   │   └── avg_f1_micro                            0.95
│   └── global_metrics.field.name=preservationmethods    76
│       ├── avg_nb_classes                          9.97
│       └── avg_f1_micro                             0.8
└── classification_type=multiclass                   568
    ├── global_metrics.field.name=kind               370
    │   ├── avg_nb_classes                         206.5
    │   └── avg_f1_micro                            0.89
    └── global_metrics.field.name=gpc                198
        ├── avg_nb_classes                        211.12
        └── avg_f1_micro                            0.93
"""


EXPECTED_NORMALIZED = {
    "children": [
        {
            "children": [
                {
                    "children": [
                        {
                            "key": None,
                            "level": "avg_nb_classes",
                            "value": 5.20
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.83
                        }
                    ],
                    "key": "hazardpictograms",
                    "level": "global_metrics.field.name",
                    "value": 369
                },
                {
                    "children": [
                        {
                            "key": None,
                            "level": "avg_nb_classes",
                            "value": 88.72
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.81
                        }
                    ],
                    "key": "islabeledby",
                    "level": "global_metrics.field.name",
                    "value": 369
                },
                {
                    "children": [
                        {
                            "key": None,
                            "level": "avg_nb_classes",
                            "value": 27.57
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.41
                        }
                    ],
                    "key": "flavors",
                    "level": "global_metrics.field.name",
                    "value": 367
                },
                {
                    "children": [
                        {
                            "key": None,
                            "level": "avg_nb_classes",
                            "value": 107.82
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.83
                        }
                    ],
                    "key": "hasnotableingredients",
                    "level": "global_metrics.field.name",
                    "value": 239
                },
                {
                    "children": [
                        {
                            "key": None,
                            "level": "avg_nb_classes",
                            "value": 65.59
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.82
                        }
                    ],
                    "key": "allergentypelist",
                    "level": "global_metrics.field.name",
                    "value": 130
                },
                {
                    "children": [
                        {
                            "key": None,
                            "level": "avg_nb_classes",
                            "value": 18.71
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.72
                        }
                    ],
                    "key": "ispracticecompatible",
                    "level": "global_metrics.field.name",
                    "value": 128
                },
                {
                    "children": [
                        {
                            "key": None,
                            "level": "avg_nb_classes",
                            "value": 183.21
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.95
                        }
                    ],
                    "key": "gpc",
                    "level": "global_metrics.field.name",
                    "value": 119
                },
                {
                    "children": [
                        {
                            "key": None,
                            "level": "avg_nb_classes",
                            "value": 9.97
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.80
                        }
                    ],
                    "key": "preservationmethods",
                    "level": "global_metrics.field.name",
                    "value": 76
                }
            ],
            "key": "multilabel",
            "level": "classification_type",
            "value": 1797
        },
        {
            "children": [
                {
                    "children": [
                        {
                            "key": None,
                            "level": "avg_nb_classes",
                            "value": 206.50
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.89
                        }
                    ],
                    "key": "kind",
                    "level": "global_metrics.field.name",
                    "value": 370
                },
                {
                    "children": [
                        {
                            "key": None,
                            "level": "avg_nb_classes",
                            "value": 211.12
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.93
                        }
                    ],
                    "key": "gpc",
                    "level": "global_metrics.field.name",
                    "value": 198
                }
            ],
            "key": "multiclass",
            "level": "classification_type",
            "value": 568
        }
    ],
    "key": None,
    "level": "root",
    "value": None
}

EXPECTED_DICT_ROWS = [
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'hazardpictograms'
        },
        {
            'value': 5.20
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'islabeledby'
        },
        {
            'value': 88.72
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'flavors'
        },
        {
            'value': 27.57
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'hasnotableingredients'
        },
        {
            'value': 107.82
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'allergentypelist'
        },
        {
            'value': 65.59
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'ispracticecompatible'
        },
        {
            'value': 18.71
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'gpc'
        },
        {
            'value': 183.21
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'preservationmethods'
        },
        {
            'value': 9.97
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multiclass',
            'global_metrics.field.name': 'kind'
        },
        {
            'value': 206.50
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multiclass',
            'global_metrics.field.name': 'gpc'
        },
        {
            'value': 211.12
        }
    )
]
