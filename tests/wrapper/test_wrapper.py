#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase
from pandagg import PandAgg
from mock import Mock

from pandagg.exceptions import InvalidElasticSearchClientError
from pandagg.index.index import ClientBoundIndex
from pandagg.mapping.mapping import ClientBoundMapping
from tests.mapping.mapping_example import MAPPING_DETAIL
from tests.wrapper.settings_example import SETTINGS

indices_mock = {
    "classification_report": {
        "aliases": {},
        "mappings": {
            "classification_report": MAPPING_DETAIL
        },
        "settings": SETTINGS,
        "warmers": {}
    }
}


class WrapperTestCase(TestCase):

    def test_fetch_indices(self):

        wrong_client = Mock(spec=['info', 'not_all_required_methods'])
        with self.assertRaises(InvalidElasticSearchClientError):
            PandAgg(wrong_client)

        client_mock = Mock(spec=['info', 'search', 'validate', 'indices'])
        client_mock.indices = Mock(spec=['get'])
        client_mock.indices.get = Mock(return_value=indices_mock)
        p = PandAgg(client=client_mock)
        p.fetch_indices(index='*report*')
        client_mock.indices.get.assert_called_once_with(index="*report*")

        self.assertTrue(hasattr(p.indices, 'classification_report'))
        self.assertIsInstance(p.indices.classification_report, ClientBoundIndex)

        report_index = p.indices.classification_report
        self.assertEqual(
            report_index.__repr__().decode('utf-8'),
            u"<ClientBoundIndex> ['warmers', 'name', 'settings', 'mapping', 'client', 'aliases']"
        )

        self.assertIsInstance(report_index.mapping, ClientBoundMapping)
        self.assertEqual(
            report_index.mapping.__repr__().decode('utf-8'),
            u"""
<ClientBoundMapping>
classification_report                                       
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
        )
