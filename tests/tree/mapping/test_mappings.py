import pytest

from pandagg.exceptions import AbsentMappingFieldError
from pandagg.mappings import Keyword, Object, Text, Nested, Integer, Mappings
from pandagg.node.mappings.abstract import Field
from tests.testing_samples.mapping_example import MAPPINGS, EXPECTED_MAPPING_TREE_REPR


def test_deserialization():
    mapping_dict = {
        "dynamic": False,
        "properties": {
            "classification_type": {
                "type": "keyword",
                "fields": {"raw": {"type": "text"}},
            },
            "local_metrics": {
                "type": "nested",
                "dynamic": False,
                "properties": {
                    "dataset": {
                        "dynamic": False,
                        "properties": {
                            "support_test": {"type": "integer"},
                            "support_train": {"type": "integer"},
                        },
                    }
                },
            },
        },
    }

    m1 = Mappings(**mapping_dict)

    m2 = Mappings(
        dynamic=False,
        properties={
            "classification_type": Keyword(fields={"raw": Text()}),
            "local_metrics": Nested(
                dynamic=False,
                properties={
                    "dataset": Object(
                        dynamic=False,
                        properties={
                            "support_test": Integer(),
                            "support_train": Integer(),
                        },
                    )
                },
            ),
        },
    )

    expected_repr = """<Mappings>
_
├── classification_type                              Keyword
│   └── raw                                           ~ Text
└── local_metrics                                   [Nested]
    └── dataset                                     {Object}
        ├── support_test                             Integer
        └── support_train                            Integer
"""
    for m in (m1, m2):
        assert m.__repr__() == expected_repr
        assert m.to_dict() == mapping_dict


def test_parse_tree_from_dict():
    mapping_tree = Mappings(**MAPPINGS)

    assert mapping_tree.__str__() == EXPECTED_MAPPING_TREE_REPR


def test_nesteds_applied_at_field():
    mapping_tree = Mappings(**MAPPINGS)

    assert mapping_tree.nested_at_field("classification_type") is None
    assert mapping_tree.list_nesteds_at_field("classification_type") == []
    assert mapping_tree.nested_at_field("date") is None
    assert mapping_tree.list_nesteds_at_field("date") == []
    assert mapping_tree.nested_at_field("global_metrics") is None
    assert mapping_tree.list_nesteds_at_field("global_metrics") == []

    assert mapping_tree.nested_at_field("local_metrics") == "local_metrics"
    assert mapping_tree.list_nesteds_at_field("local_metrics") == ["local_metrics"]
    assert (
        mapping_tree.nested_at_field("local_metrics.dataset.support_test")
        == "local_metrics"
    )
    assert mapping_tree.list_nesteds_at_field("local_metrics.dataset.support_test") == [
        "local_metrics"
    ]


def test_mapping_type_of_field():
    mapping_tree = Mappings(**MAPPINGS)
    with pytest.raises(AbsentMappingFieldError):
        mapping_tree.mapping_type_of_field("yolo")

    assert mapping_tree.mapping_type_of_field("global_metrics") == "object"
    assert mapping_tree.mapping_type_of_field("local_metrics") == "nested"
    assert (
        mapping_tree.mapping_type_of_field("global_metrics.field.name.raw") == "keyword"
    )
    assert (
        mapping_tree.mapping_type_of_field("local_metrics.dataset.support_test")
        == "integer"
    )


def test_node_path():
    mapping_tree = Mappings(**MAPPINGS)
    # get node by path syntax
    nid = mapping_tree.get_node_id_by_path(["local_metrics", "dataset", "support_test"])
    k, node = mapping_tree.get(nid)
    assert isinstance(node, Field)
    assert mapping_tree.get_path(node.identifier) == [
        "local_metrics",
        "dataset",
        "support_test",
    ]


def test_validate_doc():
    tts = [
        {
            "name": "non nullable",
            "properties": {"pizza": Keyword(required=True)},
            "documents_expected_results": [
                ({"pizza": "yolo"}, None),
                ({"pizza": None}, "Field <pizza> is required"),
                ({}, "Field <pizza> is required"),
                ({"pizza": ["yo", "lo"]}, None),
            ],
        },
        {
            "name": "nullable",
            "properties": {"pizza": Keyword(required=False)},
            "documents_expected_results": [
                ({"pizza": "yolo"}, None),
                ({"pizza": None}, None),
                ({}, None),
                ({"pizza": ["yo", "lo"]}, None),
            ],
        },
        {
            "name": "multiple nullable",
            "properties": {"pizza": Keyword(multiple=True)},
            "documents_expected_results": [
                ({"pizza": "yolo"}, "Field <pizza> should be a array"),
                ({"pizza": None}, None),
                ({}, None),
                ({"pizza": ["yo", "lo"]}, None),
            ],
        },
        {
            "name": "multiple non nullable",
            "properties": {"pizza": Keyword(multiple=True, required=True)},
            "documents_expected_results": [
                ({"pizza": "yolo"}, "Field <pizza> should be a array"),
                ({"pizza": None}, "Field <pizza> is required"),
                ({}, "Field <pizza> is required"),
                ({"pizza": ["yo", "lo"]}, None),
            ],
        },
        {
            "name": "non multiple",
            "properties": {"pizza": Keyword(multiple=False)},
            "documents_expected_results": [
                ({"pizza": "yolo"}, None),
                ({"pizza": None}, None),
                ({}, None),
                ({"pizza": ["yo", "lo"]}, "Field <pizza> should not be an array"),
            ],
        },
        {
            "name": "nested multiple non nullable",
            "properties": {
                "some_good": Object(
                    properties={"pizza": Keyword(multiple=True, required=True)}
                )
            },
            "documents_expected_results": [
                (
                    {"some_good": {"pizza": "yolo"}},
                    "Field <some_good.pizza> should be a array",
                ),
                ({"some_good": {"pizza": None}}, "Field <some_good.pizza> is required"),
                ({}, "Field <some_good.pizza> is required"),
                ({"some_good": {"pizza": ["yo", "lo"]}}, None),
            ],
        },
    ]
    for tt in tts:
        mappings = Mappings(properties=tt["properties"])
        for doc, expected_error in tt["documents_expected_results"]:
            if expected_error:
                with pytest.raises(ValueError) as e:
                    mappings.validate_document(doc)
                assert e.value.args == (expected_error,)
            else:
                # must not raise error
                mappings.validate_document(doc)
