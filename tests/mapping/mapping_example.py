#!/usr/bin/env python
# -*- coding: utf-8 -*-

MAPPING_NAME = 'classification_report'
MAPPING_DETAIL = {
    "dynamic": False,
    "properties": {
        "classification_type": {
            "type": "string",
            "index": "not_analyzed"
        },
        "date": {
            "type": "date",
            "format": "strict_date_optional_time||epoch_millis"
        },
        "global_metrics": {
            "dynamic": False,
            "properties": {
                "field": {
                    "dynamic": False,
                    "properties": {
                        "id": {
                            "type": "integer"
                        },
                        "name": {
                            "type": "string",
                            "fields": {
                                # subfield
                                "raw": {
                                    "index": "not_analyzed",
                                    "type": "string"
                                }
                            },
                        },
                        "type": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                    }
                },
                "dataset": {
                    "dynamic": False,
                    "properties": {
                        "nb_classes": {
                            "type": "integer"
                        },
                        "support_train": {
                            "type": "integer"
                        }
                    }
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
                                        "f1_score": {
                                            "type": "float"
                                        },
                                        "precision": {
                                            "type": "float"
                                        },
                                        "recall": {
                                            "type": "float"
                                        }
                                    }
                                },
                                "micro": {
                                    "dynamic": False,
                                    "properties": {
                                        "f1_score": {
                                            "type": "float"
                                        },
                                        "precision": {
                                            "type": "float"
                                        },
                                        "recall": {
                                            "type": "float"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "id": {
            "type": "string",
            "index": "not_analyzed"
        },
        "language": {
            "type": "string",
            "index": "not_analyzed"
        },
        "local_metrics": {
            "type": "nested",
            "dynamic": False,
            "properties": {
                "dataset": {
                    "dynamic": False,
                    "properties": {
                        "support_test": {
                            "type": "integer"
                        },
                        "support_train": {
                            "type": "integer"
                        }
                    }
                },
                "field_class": {
                    "dynamic": False,
                    "properties": {
                        "id": {
                            "type": "integer"
                        },
                        "name": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                    }
                },
                "performance": {
                    "dynamic": False,
                    "properties": {
                        "test": {
                            "dynamic": False,
                            "properties": {
                                "f1_score": {
                                    "type": "float"
                                },
                                "precision": {
                                    "type": "float"
                                },
                                "recall": {
                                    "type": "float"
                                }
                            }
                        }
                    }
                }
            }
        },
        "workflow": {
            "type": "string",
            "index": "not_analyzed"
        }
    }
}

EXPECTED_MAPPING_REPR = """classification_report                                       
├── classification_type                                     String
├── date                                                    Date
├── global_metrics                                         {Object}
│   ├── dataset                                            {Object}
│   │   ├── nb_classes                                      Integer
│   │   └── support_train                                   Integer
│   ├── field                                              {Object}
│   │   ├── id                                              Integer
│   │   ├── name                                            String
│   │   │   └── raw                                       ~ String
│   │   └── type                                            String
│   └── performance                                        {Object}
│       └── test                                           {Object}
│           ├── macro                                      {Object}
│           │   ├── f1_score                                Float
│           │   ├── precision                               Float
│           │   └── recall                                  Float
│           └── micro                                      {Object}
│               ├── f1_score                                Float
│               ├── precision                               Float
│               └── recall                                  Float
├── id                                                      String
├── language                                                String
├── local_metrics                                          [Nested]
│   ├── dataset                                            {Object}
│   │   ├── support_test                                    Integer
│   │   └── support_train                                   Integer
│   ├── field_class                                        {Object}
│   │   ├── id                                              Integer
│   │   └── name                                            String
│   └── performance                                        {Object}
│       └── test                                           {Object}
│           ├── f1_score                                    Float
│           ├── precision                                   Float
│           └── recall                                      Float
└── workflow                                                String
"""

EXPECTED_MAPPING_TREE_REPR = """<MappingTree>\n%s""" % EXPECTED_MAPPING_REPR
EXPECTED_CLIENT_BOUND_MAPPING_REPR = """<ClientBoundMapping>\n%s""" % EXPECTED_MAPPING_REPR
