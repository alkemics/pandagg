#!/usr/bin/env python
# -*- coding: utf-8 -*-


EXPECTED_REPR = u""""""

EXPECTED_AGG_QUERY = {
    "week": {
        "aggs": {
            "nested_below_week": {
                "aggs": {
                    "local_metrics.field_class.name": {
                        "aggs": {
                            "min_f1_score": {
                                "min": {
                                    "field": "local_metrics.performance.test.f1_score"
                                }
                            }
                        },
                        "terms": {
                            "field": "local_metrics.field_class.name",
                            "size": 10,
                        },
                    }
                },
                "nested": {"path": "local_metrics"},
            }
        },
        "date_histogram": {"field": "date", "format": "yyyy-MM-dd", "interval": "1w"},
    }
}
