#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase
from pandagg import PandAgg
from mock import Mock

from pandagg.exceptions import InvalidElasticSearchClientError
from pandagg.index.index import ClientBoundIndex
from pandagg.mapping.mapping import ClientBoundMapping
from tests.mapping.mapping_example import MAPPING_DETAIL, EXPECTED_CLIENT_BOUND_MAPPING_REPR
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

    def test_wrong_client(self):
        wrong_client = Mock(spec=['info', 'not_all_required_methods'])
        with self.assertRaises(InvalidElasticSearchClientError):
            PandAgg(wrong_client)

    def test_pandagg_wrapper(self):

        client_mock = Mock(spec=['info', 'search', 'validate', 'indices'])
        client_mock.indices = Mock(spec=['get'])
        client_mock.indices.get = Mock(return_value=indices_mock)

        # fetch indices
        p = PandAgg(client=client_mock)
        p.fetch_indices(index='*report*')
        client_mock.indices.get.assert_called_once_with(index="*report*")

        # ensure indices presence
        self.assertTrue(hasattr(p.indices, 'classification_report'))
        report_index = p.indices.classification_report
        self.assertIsInstance(report_index, ClientBoundIndex)
        self.assertEqual(
            report_index.__str__(),
            u"<ClientBoundIndex> ['aliases', 'client', 'mapping', 'name', 'settings', 'warmers']"
        )

        # ensure mapping presence
        report_mapping = report_index.mapping
        self.assertIsInstance(report_mapping, ClientBoundMapping)
        self.assertEqual(report_mapping.__str__(), EXPECTED_CLIENT_BOUND_MAPPING_REPR)
