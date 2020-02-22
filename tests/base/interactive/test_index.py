#!/usr/bin/env python
# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
from mock import Mock, patch
from unittest import TestCase

from pandagg.tree.agg import Agg
from pandagg.interactive.index import Index
from pandagg.node.agg.metric import Avg

from tests.base.mapping_example import MAPPING


class IndexTestCase(TestCase):

    @staticmethod
    def get_index():
        return Index(
            name='my_index_name',
            settings={},
            mapping=MAPPING,
            aliases={}
        )

    def test_index_groupby(self):
        index = self.get_index()

        grouped_agg = index \
            .groupby(['classification_type', 'global_metrics.field.name'])
        self.assertIsInstance(grouped_agg, Agg)

        equivalent_agg = Agg().groupby(['classification_type', 'global_metrics.field.name'])
        self.assertEqual(
            grouped_agg.query_dict(),
            equivalent_agg.query_dict()
        )
        self.assertEqual(grouped_agg.__str__(), equivalent_agg.__str__())

    def test_index_agg(self):
        index = self.get_index()

        agg = index \
            .agg(
                [
                    Avg('avg_nb_classes', field='global_metrics.dataset.nb_classes'),
                    Avg('avg_f1_micro', field='global_metrics.performance.test.micro.f1_score')
                ]
            )
        self.assertIsInstance(agg, Agg)

        equivalent_agg = Agg().agg(
            [
                Avg('avg_nb_classes', field='global_metrics.dataset.nb_classes'),
                Avg('avg_f1_micro', field='global_metrics.performance.test.micro.f1_score')
            ]
        )
        self.assertEqual(agg.__str__(), equivalent_agg.__str__())


class ClientBoundTestCase(TestCase):

    @staticmethod
    def get_client_bound_index(es_response=None):
        client_mock = Elasticsearch()
        client_mock.search = Mock(return_value=es_response)
        return client_mock, Index(
            client=client_mock,
            name='my_index_name',
            settings={},
            mapping=MAPPING,
            aliases={}
        )

    def test_client_bound_query(self):
        client_mock, index = self.get_client_bound_index()

        agg = index\
            .query({'term': {'workflow': {'value': 'some_workflow'}}})
        self.assertIsInstance(agg, Agg)
        self.assertIs(agg.client, client_mock)
        self.assertEqual(agg._query.query_dict(), {'term': {'workflow': {'value': 'some_workflow'}}})
        self.assertEqual(agg.index_name, 'my_index_name')

    def test_client_bound_groupby(self):
        client_mock, index = self.get_client_bound_index()

        grouped_agg = index\
            .groupby(['classification_type', 'global_metrics.field.name'])
        self.assertIsInstance(grouped_agg, Agg)
        self.assertIs(grouped_agg.client, client_mock)
        self.assertEqual(grouped_agg.index_name, 'my_index_name')

        equivalent_agg = Agg().groupby(['classification_type', 'global_metrics.field.name'])
        self.assertEqual(
            grouped_agg.query_dict(),
            equivalent_agg.query_dict()
        )

    def test_client_bound_not_executed_agg(self):
        client_mock, index = self.get_client_bound_index()

        not_executed_agg = index\
            .agg(
                [
                    Avg('avg_nb_classes', field='global_metrics.dataset.nb_classes'),
                    Avg('avg_f1_micro', field='global_metrics.performance.test.micro.f1_score')
                ],
                execute=False
            )
        self.assertIsInstance(not_executed_agg, Agg)
        self.assertIs(not_executed_agg.client, client_mock)
        self.assertEqual(not_executed_agg.index_name, 'my_index_name')

        equivalent_agg = Agg().agg(
            [
                Avg('avg_nb_classes', field='global_metrics.dataset.nb_classes'),
                Avg('avg_f1_micro', field='global_metrics.performance.test.micro.f1_score')
            ]
        )
        self.assertEqual(not_executed_agg.query_dict(), equivalent_agg.query_dict())

    @patch.object(Agg, 'serialize_response')
    def test_client_bound_executed_agg(self, serialize_mock):
        # we test the execution, not agg query generation nor the parsing which are tested in test_aggs module
        client_mock, index = self.get_client_bound_index(es_response={"aggregations": "response_mock"})
        serialize_mock.return_value = 'some_parsed_result'

        results = index \
            .query({'term': {'workflow': {'value': 'some_workflow'}}})\
            .agg(
                [
                    Avg('avg_nb_classes', field='global_metrics.dataset.nb_classes'),
                    Avg('avg_f1_micro', field='global_metrics.performance.test.micro.f1_score')
                ]
            )\
            .execute()
        equivalent_agg = Agg().agg(
            [
                Avg('avg_nb_classes', field='global_metrics.dataset.nb_classes'),
                Avg('avg_f1_micro', field='global_metrics.performance.test.micro.f1_score')
            ]
        )
        client_mock.search.assert_called_once()
        client_mock.search.assert_called_with(
            body={
                "aggs": equivalent_agg.query_dict(),
                "size": 0,
                "query": {'term': {'workflow': {'value': 'some_workflow'}}}
            },
            index="my_index_name"
        )

        serialize_mock.assert_called_once()
        serialize_mock.assert_called_with(aggs="response_mock", output=Agg.DEFAULT_OUTPUT)

        self.assertEqual(results, "some_parsed_result")
