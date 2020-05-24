#!/usr/bin/env python
# -*- coding: utf-8 -*-

MAPPING = {
    "dynamic": False,
    "properties": {
        "classification_type": {"type": "keyword"},
        "date": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
        "global_metrics": {
            "dynamic": False,
            "properties": {
                "field": {
                    "dynamic": False,
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {
                            "type": "text",
                            "fields": {
                                # subfield
                                "raw": {"type": "keyword"}
                            },
                        },
                        "type": {"type": "keyword"},
                    },
                },
                "dataset": {
                    "dynamic": False,
                    "properties": {
                        "nb_classes": {"type": "integer"},
                        "support_train": {"type": "integer"},
                    },
                },
                "performance": {
                    "dynamic": False,
                    "properties": {
                        "test": {
                            "dynamic": False,
                            "properties": {
                                "macro": {
                                    "dynamic": False,
                                    "properties": {
                                        "f1_score": {"type": "float"},
                                        "precision": {"type": "float"},
                                        "recall": {"type": "float"},
                                    },
                                },
                                "micro": {
                                    "dynamic": False,
                                    "properties": {
                                        "f1_score": {"type": "float"},
                                        "precision": {"type": "float"},
                                        "recall": {"type": "float"},
                                    },
                                },
                            },
                        }
                    },
                },
            },
        },
        "id": {"type": "keyword"},
        "language": {"type": "keyword"},
        "local_metrics": {
            "type": "nested",
            "dynamic": False,
            "properties": {
                "dataset": {
                    "dynamic": False,
                    "properties": {
                        "support_test": {"type": "integer"},
                        "support_train": {"type": "integer"},
                    },
                },
                "field_class": {
                    "dynamic": False,
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "keyword"},
                    },
                },
                "performance": {
                    "dynamic": False,
                    "properties": {
                        "test": {
                            "dynamic": False,
                            "properties": {
                                "f1_score": {"type": "float"},
                                "precision": {"type": "float"},
                                "recall": {"type": "float"},
                            },
                        }
                    },
                },
            },
        },
        "workflow": {"type": "keyword"},
    },
}

EXPECTED_MAPPING_REPR = """_
├── classification_type                                       Keyword
├── date                                                      Date
├── global_metrics                                           {Object}
│   ├── dataset                                              {Object}
│   │   ├── nb_classes                                        Integer
│   │   └── support_train                                     Integer
│   ├── field                                                {Object}
│   │   ├── id                                                Integer
│   │   ├── name                                              Text
│   │   │   └── raw                                         ~ Keyword
│   │   └── type                                              Keyword
│   └── performance                                          {Object}
│       └── test                                             {Object}
│           ├── macro                                        {Object}
│           │   ├── f1_score                                  Float
│           │   ├── precision                                 Float
│           │   └── recall                                    Float
│           └── micro                                        {Object}
│               ├── f1_score                                  Float
│               ├── precision                                 Float
│               └── recall                                    Float
├── id                                                        Keyword
├── language                                                  Keyword
├── local_metrics                                            [Nested]
│   ├── dataset                                              {Object}
│   │   ├── support_test                                      Integer
│   │   └── support_train                                     Integer
│   ├── field_class                                          {Object}
│   │   ├── id                                                Integer
│   │   └── name                                              Keyword
│   └── performance                                          {Object}
│       └── test                                             {Object}
│           ├── f1_score                                      Float
│           ├── precision                                     Float
│           └── recall                                        Float
└── workflow                                                  Keyword
"""

EXPECTED_MAPPING_TREE_REPR = """<Mapping>\n%s""" % EXPECTED_MAPPING_REPR
EXPECTED_CLIENT_BOUND_MAPPING_REPR = """<IMapping>\n%s""" % EXPECTED_MAPPING_REPR
