import dataclasses
from typing import List, Type, Any, Callable, Dict

from pandagg.search import Search

from pandagg.node.aggs.abstract import AggClause, BucketAggClause
from pandagg.node.types import MAPPING_TYPES
from pandagg.types import FieldType


def list_available_aggs_on_field(field_type: FieldType) -> List[Type[AggClause]]:
    """For a given field type, return all aggregations that can be operated on this field.
    If WHITELISTED_MAPPING_TYPES is defined, field type must be in it. Else if BLACKLISTED_MAPPING_TYPES is defined,
    field type must not be in it.
    """
    return [
        agg_class
        for agg_class in AggClause._classes.values()
        if agg_class.valid_on_field_type(field_type)
    ]


@dataclasses.dataclass
class FieldAggregations:
    _field: str
    _search: Search


def aggregator_factory(agg_klass: Type[AggClause]) -> Callable:
    def aggregator(self: FieldAggregations, **kwargs: Any) -> Search:
        if issubclass(agg_klass, BucketAggClause):
            return self._search.groupby(
                "%s_%s" % (agg_klass.KEY, self._field),
                agg_klass(field=self._field, **kwargs),
            )
        return self._search.agg(
            "%s_%s" % (agg_klass.KEY, self._field),
            agg_klass(field=self._field, **kwargs),
        )

    aggregator.__doc__ = agg_klass.__init__.__doc__ or agg_klass.__doc__
    return aggregator


def field_type_klass_factory(field_type: str) -> Type[FieldAggregations]:
    methods = {
        agg_klass.KEY: aggregator_factory(agg_klass)
        for agg_klass in list_available_aggs_on_field(field_type)
    }
    return type("%sAggs" % field_type.capitalize(), (FieldAggregations,), methods)


field_classes_per_name: Dict[FieldType, Type[FieldAggregations]] = {
    field_type: field_type_klass_factory(field_type) for field_type in MAPPING_TYPES
}
