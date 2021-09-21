from pandagg.node.aggs.abstract import AggClause


def test_abstract_agg_node():
    class CustomAgg(AggClause):
        KEY = "custom_type"
        VALUE_ATTRS = ["bucket_value_path"]
        # would mean this agg can be applied only on boolean fields
        WHITELISTED_MAPPING_TYPES = ["boolean"]
        BLACKLISTED_MAPPING_TYPES = None

        # example for unique bucket agg
        def extract_buckets(self, response_value):
            yield (None, response_value)

    node = CustomAgg(custom_body={"stuff": 2})
    assert node.to_dict() == {"custom_type": {"custom_body": {"stuff": 2}}}

    node = CustomAgg(custom_body={"stuff": 2}, meta={"stuff": "meta_stuff"})
    assert node.to_dict() == {
        "custom_type": {"custom_body": {"stuff": 2}},
        "meta": {"stuff": "meta_stuff"},
    }

    assert (
        node.__str__()
        == '<CustomAgg, type=custom_type, body={"custom_body": {"stuff": 2}}>'
    )

    # suppose this aggregation type provide buckets in the following format
    hypothetic_es_response_bucket = {"bucket_value_path": 43}
    assert node.extract_bucket_value(hypothetic_es_response_bucket) == 43

    assert not node.valid_on_field_type("string")
    assert node.valid_on_field_type("boolean")
