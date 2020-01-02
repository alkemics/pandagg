from pandagg.base.node.agg import PUBLIC_AGGS


def deserialize_agg(agg_type, agg_name, agg_body, meta):
    if agg_type not in PUBLIC_AGGS.keys():
        raise NotImplementedError('Unknown aggregation type <%s>' % agg_type)
    agg_class = PUBLIC_AGGS[agg_type]
    return agg_class.deserialize(name=agg_name, meta=meta, **agg_body)
