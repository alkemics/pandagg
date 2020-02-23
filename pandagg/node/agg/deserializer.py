from six import iteritems

from pandagg.node.agg.abstract import BucketAggNode, AggNode
from .bucket import BUCKET_AGGS
from .metric import METRIC_AGGS
from .pipeline import PIPELINE_AGGS

AGGS = {a.KEY: a for a in BUCKET_AGGS + METRIC_AGGS + PIPELINE_AGGS}


def deserialize_agg(d):
    if len(d.keys()) > 1:
        raise ValueError('Invalid aggregation, expected one single key, got: %s' % d.keys())
    agg_name, agg_detail = next(iteritems(d))
    meta = agg_detail.pop('meta', None)
    children_aggs = agg_detail.pop('aggs', None) or agg_detail.pop('aggregations', None) or {}
    if len(agg_detail.keys()) != 1:
        raise ValueError('Invalid aggregation, expected one single key, got %s' % agg_detail.keys())
    agg_type, agg_body = next(iteritems(agg_detail))
    if agg_type not in AGGS.keys():
        raise NotImplementedError('Unknown aggregation type <%s>' % agg_type)
    agg_class = AGGS[agg_type]
    if children_aggs and not issubclass(agg_class, BucketAggNode):
        raise ValueError('Aggregation of type %s doesn\'t accept sub-aggregations, got <%s>.' % (
            agg_class.__name__, children_aggs))
    if children_aggs:
        if isinstance(children_aggs, dict):
            children_aggs = [{k: v for k, v in iteritems(children_aggs)}]
        elif isinstance(children_aggs, AggNode):
            children_aggs = (children_aggs,)
        agg_body['aggs'] = children_aggs
    return agg_class.deserialize(name=agg_name, meta=meta, body=agg_body)
