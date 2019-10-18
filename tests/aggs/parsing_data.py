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
                                "value": 0.8303531152284566
                            },
                            "avg_nb_classes": {
                                "value": 5.203252032520325
                            },
                            "doc_count": 369,
                            "key": "hazardpictograms"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.8153823573737933
                            },
                            "avg_nb_classes": {
                                "value": 88.72628726287263
                            },
                            "doc_count": 369,
                            "key": "islabeledby"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.4162220981692748
                            },
                            "avg_nb_classes": {
                                "value": 27.577656675749317
                            },
                            "doc_count": 367,
                            "key": "flavors"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.8375406716657982
                            },
                            "avg_nb_classes": {
                                "value": 107.82426778242677
                            },
                            "doc_count": 239,
                            "key": "hasnotableingredients"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.829143613576889
                            },
                            "avg_nb_classes": {
                                "value": 65.5923076923077
                            },
                            "doc_count": 130,
                            "key": "allergentypelist"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.7254550759680569
                            },
                            "avg_nb_classes": {
                                "value": 18.7109375
                            },
                            "doc_count": 128,
                            "key": "ispracticecompatible"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.9537652951328695
                            },
                            "avg_nb_classes": {
                                "value": 183.21008403361344
                            },
                            "doc_count": 119,
                            "key": "gpc"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.8037959227436468
                            },
                            "avg_nb_classes": {
                                "value": 9.973684210526315
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
                                "value": 0.8968166083902926
                            },
                            "avg_nb_classes": {
                                "value": 206.5027027027027
                            },
                            "doc_count": 370,
                            "key": "kind"
                        },
                        {
                            "avg_f1_micro": {
                                "value": 0.9321256066211546
                            },
                            "avg_nb_classes": {
                                "value": 211.12626262626262
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
│   │   ├── avg_nb_classes                 5.20325203252
│   │   └── avg_f1_micro                  0.830353115228
│   ├── global_metrics.field.name=islabeledby        369
│   │   ├── avg_nb_classes                 88.7262872629
│   │   └── avg_f1_micro                  0.815382357374
│   ├── global_metrics.field.name=flavors            367
│   │   ├── avg_nb_classes                 27.5776566757
│   │   └── avg_f1_micro                  0.416222098169
│   ├── global_metrics.field.name=hasnotableingredients    239
│   │   ├── avg_nb_classes                 107.824267782
│   │   └── avg_f1_micro                  0.837540671666
│   ├── global_metrics.field.name=allergentypelist    130
│   │   ├── avg_nb_classes                 65.5923076923
│   │   └── avg_f1_micro                  0.829143613577
│   ├── global_metrics.field.name=ispracticecompatible    128
│   │   ├── avg_nb_classes                    18.7109375
│   │   └── avg_f1_micro                  0.725455075968
│   ├── global_metrics.field.name=gpc                119
│   │   ├── avg_nb_classes                 183.210084034
│   │   └── avg_f1_micro                  0.953765295133
│   └── global_metrics.field.name=preservationmethods    76
│       ├── avg_nb_classes                 9.97368421053
│       └── avg_f1_micro                  0.803795922744
└── classification_type=multiclass                   568
    ├── global_metrics.field.name=kind               370
    │   ├── avg_nb_classes                 206.502702703
    │   └── avg_f1_micro                   0.89681660839
    └── global_metrics.field.name=gpc                198
        ├── avg_nb_classes                 211.126262626
        └── avg_f1_micro                  0.932125606621
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
                            "value": 5.203252032520325
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.8303531152284566
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
                            "value": 88.72628726287263
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.8153823573737933
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
                            "value": 27.577656675749317
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.4162220981692748
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
                            "value": 107.82426778242677
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.8375406716657982
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
                            "value": 65.5923076923077
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.829143613576889
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
                            "value": 18.7109375
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.7254550759680569
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
                            "value": 183.21008403361344
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.9537652951328695
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
                            "value": 9.973684210526315
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.8037959227436468
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
                            "value": 206.5027027027027
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.8968166083902926
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
                            "value": 211.12626262626262
                        },
                        {
                            "key": None,
                            "level": "avg_f1_micro",
                            "value": 0.9321256066211546
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
            'value': 5.203252032520325
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'islabeledby'
        },
        {
            'value': 88.72628726287263}
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'flavors'
        },
        {
            'value': 27.577656675749317
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'hasnotableingredients'
        },
        {
            'value': 107.82426778242677
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'allergentypelist'
        },
        {
            'value': 65.5923076923077
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'ispracticecompatible'
        },
        {
            'value': 18.7109375
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'gpc'
        },
        {
            'value': 183.21008403361344
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multilabel',
            'global_metrics.field.name': 'preservationmethods'
        },
        {
            'value': 9.973684210526315
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multiclass',
            'global_metrics.field.name': 'kind'
        },
        {
            'value': 206.5027027027027
        }
    ),
    (
        {
            'avg_nb_classes': None,
            'classification_type': 'multiclass',
            'global_metrics.field.name': 'gpc'
        },
        {
            'value': 211.12626262626262
        }
    )
]
