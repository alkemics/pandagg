#!/usr/bin/env python
# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from pandagg.aggs.agg import Agg
from pandagg.wrapper.wrapper import PandAgg

if __name__ == '__main__':
    uri = 'localhost:9200/'
    es = Elasticsearch(hosts=[uri])
    p = PandAgg(client=es)
    p.fetch_indices()

    a = Agg(mapping=p.indices.productversion_2.mapping)
