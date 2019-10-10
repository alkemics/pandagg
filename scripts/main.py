#!/usr/bin/env python
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from pandagg.aggs.aggregation import Aggregation
from pandagg.wrapper.wrapper import PandAgg

if __name__ == '__main__':
    uri = 'localhost:9200/'
    es = Elasticsearch(hosts=[uri])
    p = PandAgg(client=es)
    p.fetch_indices()

    a = Aggregation(mapping=p.indices.productversion_2.mapping)
