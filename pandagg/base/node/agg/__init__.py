
from .bucket import BUCKET_AGGS
from .metric import METRIC_AGGS
from .pipeline import PIPELINE_AGGS

AGGS = {a.AGG_TYPE: a for a in BUCKET_AGGS + METRIC_AGGS + PIPELINE_AGGS}


def deserialize_agg(agg_type, agg_name, agg_body, meta):
    if agg_type not in AGGS.keys():
        raise NotImplementedError('Unknown aggregation type <%s>' % agg_type)
    agg_class = AGGS[agg_type]
    return agg_class.deserialize(name=agg_name, meta=meta, **agg_body)
