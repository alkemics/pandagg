from six import iteritems

from .bucket import (
    MatchAll,
    Terms,
    Filters,
    Histogram,
    DateHistogram,
    Global,
    Filter,
    Nested,
    ReverseNested,
    BUCKET_AGGS
)

from .metrics import (
    Avg,
    Max,
    Min,
    Cardinality,
    Stats,
    ExtendedStats,
    Percentiles,
    PercentileRanks,
    GeoBound,
    GeoCentroid,
    TopHits,
    ValueCount,
    METRIC_AGGS
)

from .pipeline import (
    AvgBucket,
    Derivative,
    MaxBucket,
    MinBucket,
    SumBucket,
    StatsBucket,
    ExtendedStatsBucket,
    PercentilesBucket,
    MovingAvg,
    CumulativeSum,
    BucketScript,
    BucketSelector,
    SerialDiff,
    PIPELINE_AGGS
)

PUBLIC_AGGS = {}
for key, agg in iteritems(BUCKET_AGGS):
    PUBLIC_AGGS[key] = agg
for key, agg in iteritems(METRIC_AGGS):
    PUBLIC_AGGS[key] = agg
for key, agg in iteritems(PIPELINE_AGGS):
    PUBLIC_AGGS[key] = agg
