from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient

from pandagg.discovery import discover, Index
from mock import patch

from pandagg.interactive.mappings import IMappings
from tests.test_data import GIT_MAPPINGS
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


class TestDiscovery:
    @patch.object(IndicesClient, "get")
    def test_pandagg_wrapper(self, indice_get_mock):
        indice_get_mock.return_value = indices_mock

        # fetch indices
        p = Elasticsearch()
        indices = discover(using=p, index="*report*")
        indice_get_mock.assert_called_once_with(index="*report*")

        # ensure indices presence
        assert hasattr(indices, "classification_report_one")
        report_index = indices.classification_report_one
        assert isinstance(report_index, Index)
        assert report_index.__str__() == "<Index 'classification_report_one'>"
        assert report_index.name, "classification_report_one"

        # ensure mappings presence
        assert isinstance(report_index.imappings, IMappings)

    def test_discovery_itg_index(self, data_client):
        indices = discover(data_client)
        assert "git" in indices
        git = indices.git
        assert isinstance(git, Index)
        existing_mappings = git.mappings.copy()
        # False cast to "false" in ES apis for some reason
        existing_mappings.pop("dynamic")
        expected_mappings = GIT_MAPPINGS.copy()
        expected_mappings.pop("dynamic")
        assert existing_mappings == expected_mappings
