from six import iteritems

from .bucket import BUCKET_AGGS
from .metric import METRIC_AGGS
from .pipeline import PIPELINE_AGGS

PUBLIC_AGGS = {}
for key, agg in iteritems(BUCKET_AGGS):
    PUBLIC_AGGS[key] = agg
for key, agg in iteritems(METRIC_AGGS):
    PUBLIC_AGGS[key] = agg
for key, agg in iteritems(PIPELINE_AGGS):
    PUBLIC_AGGS[key] = agg
