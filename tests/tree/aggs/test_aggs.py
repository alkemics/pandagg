from unittest import TestCase

import pytest
from mock import patch

from pandagg.tree.aggs import Aggs
from pandagg.exceptions import (
    InvalidOperationMappingFieldError,
    AbsentMappingFieldError,
)
from pandagg.aggs import DateHistogram, Terms, Avg, Min, Filter

import tests.testing_samples.data_sample as sample

from tests.testing_samples.mapping_example import MAPPINGS


class AggTestCase(TestCase):
    def setUp(self):
        patcher = patch("uuid.uuid4", side_effect=range(1000))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_deserialize_nodes_with_subaggs(self):
        expected = {
            "genres": {
                "terms": {"field": "genres", "size": 3},
                "aggs": {
                    "movie_decade": {
                        "date_histogram": {"field": "year", "fixed_interval": "3650d"}
                    }
                },
            }
        }
        agg1 = Aggs(expected)
        agg2 = Aggs(
            {
                "genres": Terms(
                    field="genres",
                    size=3,
                    aggs={
                        "movie_decade": DateHistogram(
                            field="year", fixed_interval="3650d"
                        )
                    },
                )
            }
        )
        agg3 = Aggs(
            {
                "genres": Terms(
                    field="genres",
                    size=3,
                    aggs={
                        "movie_decade": DateHistogram(
                            field="year", fixed_interval="3650d"
                        )
                    },
                )
            }
        )
        agg4 = Aggs(
            {
                "genres": Terms(
                    field="genres",
                    size=3,
                    aggs={
                        "movie_decade": {
                            "date_histogram": {
                                "field": "year",
                                "fixed_interval": "3650d",
                            }
                        }
                    },
                )
            }
        )
        agg5 = Aggs(
            {
                "genres": {
                    "terms": {"field": "genres", "size": 3},
                    "aggs": {
                        "movie_decade": DateHistogram(
                            field="year", fixed_interval="3650d"
                        )
                    },
                }
            }
        )
        for a in (agg1, agg2, agg3, agg4, agg5):
            self.assertEqual(a.to_dict(), expected)
            self.assertEqual(
                a.show(stdout=False),
                """<Aggregations>
genres                                           <terms, field="genres", size=3>
└── movie_decade          <date_histogram, field="year", fixed_interval="3650d">
""",
            )

    def test_add_node_with_mapping(self):
        with_mapping = Aggs(mappings=MAPPINGS, nested_autocorrect=True)

        # add regular node
        with_mapping = with_mapping.agg("workflow", Terms(field="workflow"))
        self.assertEqual(
            with_mapping.to_dict(), {"workflow": {"terms": {"field": "workflow"}}}
        )

        # try to add field aggregation on non-existing field will fail
        with self.assertRaises(AbsentMappingFieldError):
            with_mapping.agg(
                "imaginary_agg", Terms(field="imaginary_field"), insert_below="workflow"
            )

        # try to add aggregation on a non-compatible field will fail
        with self.assertRaises(InvalidOperationMappingFieldError):
            with_mapping.agg(
                "average_of_string",
                Avg(field="classification_type"),
                insert_below="workflow",
            )

        # add field aggregation on field passing through nested will automatically add nested
        with_mapping = with_mapping.agg(
            "local_f1_score",
            Avg(field="local_metrics.performance.test.f1_score"),
            insert_below="workflow",
        )
        self.assertEqual(
            with_mapping.to_dict(),
            {
                "workflow": {
                    "aggs": {
                        "nested_below_workflow": {
                            "aggs": {
                                "local_f1_score": {
                                    "avg": {
                                        "field": "local_metrics.performance.test.f1_score"
                                    }
                                }
                            },
                            "nested": {"path": "local_metrics"},
                        }
                    },
                    "terms": {"field": "workflow"},
                }
            },
        )
        self.assertEqual(
            with_mapping.show(stdout=False),
            """<Aggregations>
workflow                                               <terms, field="workflow">
└── nested_below_workflow                         <nested, path="local_metrics">
    └── local_f1_score    <avg, field="local_metrics.performance.test.f1_score">
""",
        )
        auto_nested_id = with_mapping.id_from_key("nested_below_workflow")
        k, node = with_mapping.get(auto_nested_id)
        self.assertEqual(k, "nested_below_workflow")
        self.assertEqual(node.KEY, "nested")
        self.assertEqual(node.path, "local_metrics")

        # add other agg requiring nested will reuse nested agg as parent
        with_mapping = with_mapping.agg(
            "local_precision",
            Avg(field="local_metrics.performance.test.precision"),
            insert_below="workflow",
        )
        self.assertEqual(
            {
                "workflow": {
                    "aggs": {
                        "nested_below_workflow": {
                            "aggs": {
                                "local_f1_score": {
                                    "avg": {
                                        "field": "local_metrics.performance.test.f1_score"
                                    }
                                },
                                "local_precision": {
                                    "avg": {
                                        "field": "local_metrics.performance.test.precision"
                                    }
                                },
                            },
                            "nested": {"path": "local_metrics"},
                        }
                    },
                    "terms": {"field": "workflow"},
                }
            },
            with_mapping.to_dict(),
        )

        # add under a nested parent a field aggregation that requires to be located under root will automatically
        # add reverse-nested
        with_mapping = with_mapping.agg(
            "language_terms",
            Terms(field="language"),
            insert_below="nested_below_workflow",
        )
        self.assertEqual(
            with_mapping.to_dict(),
            {
                "workflow": {
                    "aggs": {
                        "nested_below_workflow": {
                            "aggs": {
                                "local_f1_score": {
                                    "avg": {
                                        "field": "local_metrics.performance.test.f1_score"
                                    }
                                },
                                "local_precision": {
                                    "avg": {
                                        "field": "local_metrics.performance.test.precision"
                                    }
                                },
                                "reverse_nested_below_nested_below_workflow": {
                                    "aggs": {
                                        "language_terms": {
                                            "terms": {"field": "language"}
                                        }
                                    },
                                    "reverse_nested": {},
                                },
                            },
                            "nested": {"path": "local_metrics"},
                        }
                    },
                    "terms": {"field": "workflow"},
                }
            },
        )

    # TODO - finish these tests (reverse nested)
    def test_paste_tree_with_mapping(self):
        # with explicit nested
        initial_agg_1 = Aggs(
            {
                "week": {
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w",
                    }
                }
            },
            mappings=MAPPINGS,
        )
        self.assertEqual({k for k, _ in initial_agg_1.list()}, {None, "week"})
        pasted_agg_1 = Aggs(
            {
                "nested_below_week": {
                    "nested": {"path": "local_metrics"},
                    "aggs": {
                        "local_metrics.field_class.name": {
                            "terms": {
                                "field": "local_metrics.field_class.name",
                                "size": 10,
                            }
                        }
                    },
                }
            }
        )
        self.assertEqual(
            {k for k, _ in pasted_agg_1.list()},
            {None, "nested_below_week", "local_metrics.field_class.name"},
        )

        agg_2 = initial_agg_1.aggs(pasted_agg_1, insert_below="week")
        self.assertEqual(
            {k for k, _ in agg_2.list()},
            {None, "week", "nested_below_week", "local_metrics.field_class.name"},
        )
        self.assertEqual(
            agg_2.to_dict(),
            {
                "week": {
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w",
                    },
                    "aggs": {
                        "nested_below_week": {
                            "nested": {"path": "local_metrics"},
                            "aggs": {
                                "local_metrics.field_class.name": {
                                    "terms": {
                                        "field": "local_metrics.field_class.name",
                                        "size": 10,
                                    }
                                }
                            },
                        }
                    },
                }
            },
        )

        # without explicit nested
        initial_agg_2 = Aggs(
            {
                "week": {
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w",
                    }
                }
            },
            mappings=MAPPINGS,
            nested_autocorrect=True,
        )
        self.assertEqual({k for k, _ in initial_agg_2.list()}, {None, "week"})
        pasted_agg_2 = Aggs(
            {
                "local_metrics.field_class.name": {
                    "terms": {"field": "local_metrics.field_class.name", "size": 10}
                }
            }
        )
        self.assertEqual(
            {k for k, _ in pasted_agg_2.list()},
            {None, "local_metrics.field_class.name"},
        )

        agg_3 = initial_agg_2.aggs(pasted_agg_2, insert_below="week")
        self.assertEqual(
            {k for k, _ in agg_3.list()},
            {None, "week", "nested_below_week", "local_metrics.field_class.name"},
        )
        self.assertEqual(
            agg_3.to_dict(),
            {
                "week": {
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w",
                    },
                    "aggs": {
                        "nested_below_week": {
                            "nested": {"path": "local_metrics"},
                            "aggs": {
                                "local_metrics.field_class.name": {
                                    "terms": {
                                        "field": "local_metrics.field_class.name",
                                        "size": 10,
                                    }
                                }
                            },
                        }
                    },
                }
            },
        )

    def test_insert_tree_without_mapping(self):
        # with explicit nested
        initial_agg_1 = Aggs(
            {
                "week": {
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w",
                    }
                }
            }
        )
        self.assertEqual({k for k, _ in initial_agg_1.list()}, {None, "week"})

        pasted_agg_1 = Aggs(
            {
                "nested_below_week": {
                    "nested": {"path": "local_metrics"},
                    "aggs": {
                        "local_metrics.field_class.name": {
                            "terms": {
                                "field": "local_metrics.field_class.name",
                                "size": 10,
                            }
                        }
                    },
                }
            }
        )
        self.assertEqual(
            {k for k, _ in pasted_agg_1.list()},
            {None, "nested_below_week", "local_metrics.field_class.name"},
        )

        agg_2 = initial_agg_1.aggs(pasted_agg_1, insert_below="week")
        self.assertEqual(
            {k for k, _ in agg_2.list()},
            {None, "week", "nested_below_week", "local_metrics.field_class.name"},
        )
        self.assertEqual(
            agg_2.to_dict(),
            {
                "week": {
                    "date_histogram": {
                        "field": "date",
                        "format": "yyyy-MM-dd",
                        "interval": "1w",
                    },
                    "aggs": {
                        "nested_below_week": {
                            "nested": {"path": "local_metrics"},
                            "aggs": {
                                "local_metrics.field_class.name": {
                                    "terms": {
                                        "field": "local_metrics.field_class.name",
                                        "size": 10,
                                    }
                                }
                            },
                        }
                    },
                }
            },
        )

    def test_interpret_agg_string(self):
        some_agg = Aggs()
        some_agg = some_agg.agg("some_field", insert_below=None)
        self.assertEqual(
            some_agg.to_dict(), {"some_field": {"terms": {"field": "some_field"}}}
        )

        # with default size
        some_agg = Aggs()
        some_agg = some_agg.agg("some_field", insert_below=None, size=10)
        self.assertEqual(
            some_agg.to_dict(),
            {"some_field": {"terms": {"field": "some_field", "size": 10}}},
        )

        # with parent
        some_agg = Aggs(
            {"root_agg_name": {"terms": {"field": "some_field", "size": 5}}}
        )
        some_agg = some_agg.agg("child_field", insert_below="root_agg_name")
        self.assertEqual(
            some_agg.to_dict(),
            {
                "root_agg_name": {
                    "aggs": {"child_field": {"terms": {"field": "child_field"}}},
                    "terms": {"field": "some_field", "size": 5},
                }
            },
        )

        # with required nested
        some_agg = Aggs(
            {"term_workflow": {"terms": {"field": "workflow", "size": 5}}},
            mappings=MAPPINGS,
            nested_autocorrect=True,
        )
        some_agg = some_agg.agg(
            "local_metrics.field_class.name", insert_below="term_workflow"
        )
        self.assertEqual(
            some_agg.to_dict(),
            {
                "term_workflow": {
                    "aggs": {
                        "nested_below_term_workflow": {
                            "aggs": {
                                "local_metrics.field_class.name": {
                                    "terms": {"field": "local_metrics.field_class.name"}
                                }
                            },
                            "nested": {"path": "local_metrics"},
                        }
                    },
                    "terms": {"field": "workflow", "size": 5},
                }
            },
        )

    def test_aggs(self):
        node = Terms(field="some_field", size=10)
        some_agg = Aggs().agg("some_name", node, insert_below=None)
        self.assertEqual(
            some_agg.to_dict(),
            {"some_name": {"terms": {"field": "some_field", "size": 10}}},
        )
        # with parent with required nested
        some_agg = Aggs(
            {"term_workflow": {"terms": {"field": "workflow", "size": 5}}},
            mappings=MAPPINGS,
            nested_autocorrect=True,
        )
        node = Avg(field="local_metrics.performance.test.f1_score")
        some_agg = some_agg.agg("min_local_f1", node, insert_below="term_workflow")
        self.assertEqual(
            some_agg.to_dict(),
            {
                "term_workflow": {
                    "aggs": {
                        "nested_below_term_workflow": {
                            "aggs": {
                                "min_local_f1": {
                                    "avg": {
                                        "field": "local_metrics.performance.test.f1_score"
                                    }
                                }
                            },
                            "nested": {"path": "local_metrics"},
                        }
                    },
                    "terms": {"field": "workflow", "size": 5},
                }
            },
        )

    def test_aggs_at_root(self):
        # not at root
        a = (
            Aggs()
            .groupby("zero", "terms", field="terms_zero")
            .agg("one", "terms", field="terms_one")
            .agg("two", "terms", field="terms_two")
        )
        self.assertEqual(
            a.to_dict(),
            {
                "zero": {
                    "terms": {"field": "terms_zero"},
                    "aggs": {
                        "one": {"terms": {"field": "terms_one"}},
                        "two": {"terms": {"field": "terms_two"}},
                    },
                }
            },
        )

        # at root
        a = (
            Aggs()
            .groupby("zero", "terms", field="terms_zero")
            .agg("one", "terms", field="terms_one")
            .agg("two", "terms", field="terms_two", at_root=True)
        )
        self.assertEqual(
            a.to_dict(),
            {
                "zero": {
                    "terms": {"field": "terms_zero"},
                    "aggs": {"one": {"terms": {"field": "terms_one"}}},
                },
                "two": {"terms": {"field": "terms_two"}},
            },
        )

    def test_aggs_strings(self):
        self.assertEqual(
            Aggs().agg("yolo1").agg("yolo2").to_dict(),
            {
                "yolo1": {"terms": {"field": "yolo1"}},
                "yolo2": {"terms": {"field": "yolo2"}},
            },
        )

    def test_init_from_node_hierarchy(self):
        node_hierarchy = sample.get_node_hierarchy()

        agg = Aggs(node_hierarchy, mappings=MAPPINGS)
        self.assertEqual(agg.to_dict(), sample.EXPECTED_AGG_QUERY)

        # with nested
        node_hierarchy = {
            "week": DateHistogram(
                field="date",
                interval="1w",
                aggs={
                    "local_metrics.field_class.name": Terms(
                        field="local_metrics.field_class.name",
                        size=10,
                        aggs={
                            "min_f1_score": Min(
                                field="local_metrics.performance.test.f1_score"
                            )
                        },
                    )
                },
            )
        }
        agg = Aggs(node_hierarchy, mappings=MAPPINGS, nested_autocorrect=True)
        self.assertEqual(
            agg.to_dict(),
            {
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
                    "date_histogram": {"field": "date", "interval": "1w"},
                }
            },
        )
        self.assertEqual(
            agg.to_dict(),
            {
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
                    "date_histogram": {"field": "date", "interval": "1w"},
                }
            },
        )

    def test_agg_init(self):
        agg = sample.get_wrapper_declared_agg()
        self.assertEqual(agg.to_dict(), sample.EXPECTED_AGG_QUERY)

    def test_groupby_args_syntax(self):
        a = Aggs().groupby("some_name", "terms", field="some_field")
        self.assertEqual(a.to_dict(), {"some_name": {"terms": {"field": "some_field"}}})

    def test_groupby_at_root(self):
        a = (
            Aggs()
            .groupby("one", "terms", field="terms_one")
            .groupby("two", "terms", field="terms_two", at_root=True)
        )
        self.assertEqual(
            a.to_dict(),
            {
                "two": {
                    "terms": {"field": "terms_two"},
                    "aggs": {"one": {"terms": {"field": "terms_one"}}},
                }
            },
        )

        # not at root: default behavior
        a = (
            Aggs()
            .groupby("one", "terms", field="terms_one")
            .groupby("two", "terms", field="terms_two")
        )
        self.assertEqual(
            a.to_dict(),
            {
                "one": {
                    "terms": {"field": "terms_one"},
                    "aggs": {"two": {"terms": {"field": "terms_two"}}},
                }
            },
        )

    def test_groupby_insert_below(self):
        a1 = Aggs(
            {"A": Terms(field="A", aggs={"B": Terms(field="B"), "C": Terms(field="C")})}
        )
        self.assertEqual(
            a1.to_dict(),
            {
                "A": {
                    "terms": {"field": "A"},
                    "aggs": {
                        "C": {"terms": {"field": "C"}},
                        "B": {"terms": {"field": "B"}},
                    },
                }
            },
        )

        self.assertEqual(
            a1.groupby("D", Terms(field="D"), insert_below="A").to_dict(),
            {
                "A": {
                    "terms": {"field": "A"},
                    "aggs": {
                        "D": {
                            "terms": {"field": "D"},
                            "aggs": {
                                "B": {"terms": {"field": "B"}},
                                "C": {"terms": {"field": "C"}},
                            },
                        }
                    },
                }
            },
        )

    def test_agg_insert_below(self):
        a1 = Aggs(
            {"A": Terms(field="A", aggs={"B": Terms(field="B"), "C": Terms(field="C")})}
        )
        self.assertEqual(
            a1.to_dict(),
            {
                "A": {
                    "terms": {"field": "A"},
                    "aggs": {
                        "C": {"terms": {"field": "C"}},
                        "B": {"terms": {"field": "B"}},
                    },
                }
            },
        )

        expected = {
            "A": {
                "aggs": {
                    "B": {"terms": {"field": "B"}},
                    "C": {"terms": {"field": "C"}},
                    "D": {"terms": {"field": "D"}},
                },
                "terms": {"field": "A"},
            }
        }
        self.assertEqual(
            a1.agg(name="D", type_or_agg=Terms(field="D"), insert_below="A").to_dict(),
            expected,
        )
        self.assertEqual(
            a1.agg(
                name="D", type_or_agg="terms", field="D", insert_below="A"
            ).to_dict(),
            expected,
        )
        self.assertEqual(
            a1.agg(
                name="D", type_or_agg={"terms": {"field": "D"}}, insert_below="A"
            ).to_dict(),
            expected,
        )

    def test_applied_nested_path_at_node(self):
        """Check that correct nested path is detected at node levels:
        week
        └── nested_below_week
            └── local_metrics.field_class.name
                ├── avg_f1_score
                ├── max_f1_score
                └── min_f1_score
        """
        node_hierarchy = {
            "week": DateHistogram(
                field="date",
                interval="1w",
                aggs={
                    "local_metrics.field_class.name": Terms(
                        field="local_metrics.field_class.name",
                        size=10,
                        aggs={
                            "min_f1_score": Min(
                                field="local_metrics.performance.test.f1_score"
                            )
                        },
                    )
                },
            )
        }
        agg = Aggs(node_hierarchy, mappings=MAPPINGS, nested_autocorrect=True)

        self.assertEqual(agg.applied_nested_path_at_node(agg.id_from_key("week")), None)
        for node_key in (
            "nested_below_week",
            "local_metrics.field_class.name",
            "min_f1_score",
        ):
            self.assertEqual(
                agg.applied_nested_path_at_node(agg.id_from_key(node_key)),
                "local_metrics",
            )

    def test_groupby_pointer(self):
        a = (
            Aggs()
            .groupby("A", "terms", field="a")
            .groupby("B", "date_histogram", fixed_interval="1d", field="b")
        )

        self.assertEqual(a.get_key(a._groupby_ptr), "B")

        a1 = a.agg("C1", "terms", field="c1").agg("C2", "terms", field="c2")
        self.assertEqual(
            a1.show(stdout=False),
            """<Aggregations>
A                                                             <terms, field="a">
└── B                           <date_histogram, field="b", fixed_interval="1d">
    ├── C1                                                   <terms, field="c1">
    └── C2                                                   <terms, field="c2">
""",
        )
        self.assertEqual(
            a1.to_dict(),
            {
                "A": {
                    "aggs": {
                        "B": {
                            "aggs": {
                                "C1": {"terms": {"field": "c1"}},
                                "C2": {"terms": {"field": "c2"}},
                            },
                            "date_histogram": {"field": "b", "fixed_interval": "1d"},
                        }
                    },
                    "terms": {"field": "a"},
                }
            },
        )

    def test_deepest_linear_agg(self):
        # deepest_linear_bucket_agg
        """
        week
        └── nested_below_week
            └── local_metrics.field_class.name   <----- HERE because then metric aggregation
                └── avg_f1_score
        """
        node_hierarchy = {
            "week": DateHistogram(
                field="date",
                interval="1w",
                aggs={
                    "local_metrics.field_class.name": Terms(
                        field="local_metrics.field_class.name",
                        size=10,
                        aggs={
                            "min_f1_score": Min(
                                field="local_metrics.performance.test.f1_score"
                            )
                        },
                    )
                },
            )
        }
        agg = Aggs(node_hierarchy, mappings=MAPPINGS, nested_autocorrect=True)
        self.assertEqual(
            agg.get_key(agg._deepest_linear_bucket_agg),
            "local_metrics.field_class.name",
        )

        # week is last bucket linear bucket
        node_hierarchy_2 = {
            "week": DateHistogram(
                field="date",
                interval="1w",
                aggs={
                    "local_metrics.field_class.name": Terms(
                        field="local_metrics.field_class.name", size=10
                    ),
                    "f1_score_above_threshold": Filter(
                        filter={
                            "range": {
                                "local_metrics.performance.test.f1_score": {"gte": 0.5}
                            }
                        }
                    ),
                },
            )
        }
        agg2 = Aggs(node_hierarchy_2, mappings=MAPPINGS, nested_autocorrect=True)
        self.assertEqual(agg2.get_key(agg2._deepest_linear_bucket_agg), "week")

    def test_grouped_by(self):
        a = Aggs().aggs(
            {
                "some_agg": {
                    "terms": {"field": "some_field"},
                    "aggs": {"below_agg": {"terms": {"field": "other_field"}}},
                }
            }
        )
        self.assertEqual(a._groupby_ptr, a.root)
        self.assertEqual(
            a.agg("age_avg", "avg", field="age").to_dict(),
            {
                "some_agg": {
                    "terms": {"field": "some_field"},
                    "aggs": {"below_agg": {"terms": {"field": "other_field"}}},
                },
                "age_avg": {"avg": {"field": "age"}},
            },
        )

        # select a specific agg
        new_a = a.grouped_by("some_agg")
        self.assertEqual(new_a._groupby_ptr, new_a.id_from_key("some_agg"))
        self.assertEqual(
            new_a.agg("age_avg", "avg", field="age").to_dict(),
            {
                "some_agg": {
                    "terms": {"field": "some_field"},
                    "aggs": {
                        "below_agg": {"terms": {"field": "other_field"}},
                        "age_avg": {"avg": {"field": "age"}},
                    },
                }
            },
        )

        # deepest
        last_a = a.grouped_by(deepest=True)
        self.assertEqual(last_a._groupby_ptr, new_a.id_from_key("below_agg"))
        self.assertEqual(
            last_a.agg("age_avg", "avg", field="age").to_dict(),
            {
                "some_agg": {
                    "terms": {"field": "some_field"},
                    "aggs": {
                        "below_agg": {
                            "terms": {"field": "other_field"},
                            "aggs": {"age_avg": {"avg": {"field": "age"}}},
                        }
                    },
                }
            },
        )

    def test_get_composition_supporting_agg(self):
        # OK
        name, agg_clause = (
            Aggs()
            .agg("compatible_terms", "terms", field="some_field")
            .get_composition_supporting_agg()
        )
        assert name == "compatible_terms"
        assert isinstance(agg_clause, Terms)

        # OK
        name, agg_clause = (
            Aggs()
            .groupby("compatible_terms", "terms", field="some_field")
            .agg("max_metric", "max", field="some_other_field")
            .get_composition_supporting_agg()
        )
        assert name == "compatible_terms"
        assert isinstance(agg_clause, Terms)

        # Not OK, since include is not authorized as source in term composition
        with pytest.raises(ValueError) as e:
            Aggs().agg(
                "incompatible_terms", "terms", field="some_field", include="pref.*"
            ).get_composition_supporting_agg()
        assert e.value.args == (
            "<incompatible_terms> agg clause is not convertible into a composite aggregation.",
        )

        # Not OK, because incompatible agg type
        with pytest.raises(ValueError) as e:
            Aggs().agg(
                "incompatible_metric", "max", field="some_field"
            ).get_composition_supporting_agg()
        assert e.value.args == (
            "<incompatible_metric> agg clause is not convertible into a composite "
            "aggregation.",
        )

        # Not OK because there are two clauses at the root of the aggregation
        with pytest.raises(ValueError) as e:
            Aggs().aggs(
                {
                    "user_id": {"terms": {"field": "user.id"}},
                    "user_name": {"terms": {"field": "user.name"}},
                }
            ).get_composition_supporting_agg()
        assert e.value.args == (
            "There can be only one root aggregation clause to be able to convert it into a composite aggregation.",
        )

    def test_as_composite(self):
        # terms converted
        initial_agg = (
            Aggs()
            .groupby("compatible_terms", "terms", field="some_field")
            .agg("max_metric", "max", field="some_other_field")
        )

        comp_agg = initial_agg.as_composite(size=10)

        assert isinstance(comp_agg, Aggs)
        assert comp_agg.to_dict() == {
            "compatible_terms": {
                "composite": {
                    "size": 10,
                    "sources": [
                        {"compatible_terms": {"terms": {"field": "some_field"}}}
                    ],
                },
                "aggs": {"max_metric": {"max": {"field": "some_other_field"}}},
            }
        }
        # ensure initial agg wasn't modified
        assert initial_agg.to_dict() == {
            "compatible_terms": {
                "aggs": {"max_metric": {"max": {"field": "some_other_field"}}},
                "terms": {"field": "some_field"},
            }
        }

        initial_agg = (
            Aggs()
            .groupby(
                "compatible_terms",
                "composite",
                sources=[{"terms_source": {"field": "some_field"}}],
            )
            .agg("max_metric", "max", field="some_other_field")
        )

        # composite modified
        comp_agg = initial_agg.as_composite(size=10, after={"terms_source": "yolo"})
        assert isinstance(comp_agg, Aggs)
        assert comp_agg.to_dict() == {
            "compatible_terms": {
                "aggs": {"max_metric": {"max": {"field": "some_other_field"}}},
                "composite": {
                    "after": {"terms_source": "yolo"},
                    "size": 10,
                    "sources": [{"terms_source": {"field": "some_field"}}],
                },
            }
        }
        # ensure initial agg wasn't modified
        assert initial_agg.to_dict() == {
            "compatible_terms": {
                "aggs": {"max_metric": {"max": {"field": "some_other_field"}}},
                "composite": {"sources": [{"terms_source": {"field": "some_field"}}]},
            }
        }
