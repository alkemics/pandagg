#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase

from elasticsearch.client import IndicesClient

from pandagg import Elasticsearch
from mock import patch

from pandagg.interactive.index import Index
from pandagg.interactive.mapping import IMapping
from tests.base.mapping_example import MAPPING, EXPECTED_CLIENT_BOUND_MAPPING_REPR
from tests.base.settings_example import SETTINGS

indices_mock = {
    # index name
    "classification_report_one": {
        "aliases": {},
        "mappings": MAPPING,
        "settings": SETTINGS
    }
}


class WrapperTestCase(TestCase):

    @patch.object(IndicesClient, 'get')
    def test_pandagg_wrapper(self, indice_get_mock):
        indice_get_mock.return_value = indices_mock

        # fetch indices
        p = Elasticsearch()
        indices = p.fetch_indices(index='*report*')
        indice_get_mock.assert_called_once_with(index="*report*")

        # ensure indices presence
        self.assertTrue(hasattr(indices, 'classification_report_one'))
        report_index = indices.classification_report_one
        self.assertIsInstance(report_index, Index)
        self.assertEqual(
            report_index.__str__(),
            u"<Index> ['aliases', 'client', 'mapping', 'name', 'settings']"
        )
        self.assertEqual(report_index.name, 'classification_report_one')

        # ensure mapping presence
        report_mapping = report_index.mapping
        self.assertIsInstance(report_mapping, IMapping)
        self.assertEqual(report_mapping.__str__(), EXPECTED_CLIENT_BOUND_MAPPING_REPR)
        self.assertEqual(report_mapping._index_name, 'classification_report_one')
