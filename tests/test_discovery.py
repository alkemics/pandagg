from unittest import TestCase

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

from pandagg.discovery import discover, Index
from mock import patch

from pandagg.interactive.mappings import IMappings
from tests.testing_samples.mapping_example import MAPPINGS
from tests.testing_samples.settings_example import SETTINGS

indices_mock = {
    # index name
    "classification_report_one": {
        "aliases": {},
        "mappings": MAPPINGS,
        "settings": SETTINGS,
    }
}


class WrapperTestCase(TestCase):
    @patch.object(IndicesClient, "get")
    def test_pandagg_wrapper(self, indice_get_mock):
        indice_get_mock.return_value = indices_mock

        # fetch indices
        p = Elasticsearch()
        indices = discover(using=p, index="*report*")
        indice_get_mock.assert_called_once_with(index="*report*")

        # ensure indices presence
        self.assertTrue(hasattr(indices, "classification_report_one"))
        report_index = indices.classification_report_one
        self.assertIsInstance(report_index, Index)
        self.assertEqual(report_index.__str__(), "<Index 'classification_report_one'>")
        self.assertEqual(report_index.name, "classification_report_one")

        # ensure mappings presence
        self.assertIsInstance(report_index.imappings, IMappings)
