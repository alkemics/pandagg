# Nested challenges

## Current situation

Let's consider the following aggregation mixing two filters aggregations on the same nested field.
```
{
    'nested_filter' {
        'filter': {
            'filter': {
                'nested': {
                    'path': 'nested_path',
                    'query': {'terms': {'nested_path.code': [1, 2]}}
                }
            },
            'aggs': {
                'sub_nested_filter': {
                    'filter': {
                        'filter': {
                            'path': 'nested_path',
                             'query': {'range': {'nested_path.value': {'gt': 17}}}
                        }
                    }
                }
            }
        }
    }
}
```

We could expect the `sub_nested_filter` to consider documents having both:
- `nested_path.value > 17 AND nested_path.id in [1, 2]`

Actually, the way ElasticSearch will interpret it is instead:
- `nested_path.value > 17 OR nested_path.id in [1, 2]`


**Why is that?**

Because:
```
{
  "must": [
    {
      "nested": {
        "path": "nested_path",
        "filter": "<filter_A>"
      }
    },
    {
      "nested": {
        "path": "nested_path",
        "filter": "<filter_B>"
      }
    }
  ]
}
```

differs from:
```
{
  "nested": {
    "path": "nested_path",
    "bool": {
      "must": [
        "<filter_A>",
        "<filter_B>"
      ]
    }
  }
}
```

The difference is the same as between:

1. A house having `[a blue window]` `AND` `[a round window]`
2. A house having a `[blue AND round]` window


Thus, considering our initial aggregations, to obtain the second `AND` operation on nested filters, we would have to write the following aggregation query:
```
{
    'nested_filter' {
        'filter': {
            'filter': {
                'nested': {
                    'path': 'nested_path',
                    'query': {'terms': {'nested_path.code': [1, 2]}}
                }
            },
            'aggs': {
                'sub_nested_filter': {
                    'filter': {
                        'filter': {
                            'path': 'nested_path',
                            # CHANGE HERE #
                            'bool': {
                                'must': [
                                    {'terms': {'nested_path.code': [1, 2]}},
                                    {'range': {'nested_path.value': {'gt': 17}}}
                                ]
                             }
                        }
                    }
                }
            }
        }
    }
}
```

## Goal
Provide ability to interpret nested `AND` aggregation conditions inside nested, instead of outside.

Requires to handle cases including:
- multiple levels of aggregations, with Filters, or field specific aggregations (Terms/Histogram/Metric aggregations)
- `filters` aggregations, containing `should` condition with nested filters in some sub_filters, not in others.


## Cases


### Must
```
Translation:
A           with garage AND with blue window
└── B       with round window

- Houses with garage, AND having a blue window, AND having a round window
        DIFFERENT THAN

A           with garage AND with blue window
└── B       with [blue AND round] window

- Houses with garage, AND having a [blue AND round] window
```

 ### Should not
```
 Translation:
 A           with garage AND with blue window
 └── B       AND WITHOUT [round OR green] window

 - Houses with garage, AND having a blue window, AND NOT a [round AND green] window

        DIFFERENT THAN

 A           with garage AND with blue window
 └── B       WITH a [blue AND NOT [green AND round]] window

  - Houses with garage, AND having a [blue AND NOT [green AND round]] window

        DIFFERENT THAN

 A           with garage AND with blue window
 └── B       WITH a [blue AND NOT green AND not round] window

   - Houses with garage, AND having a [blue AND NOT green AND NOT round] window

```

 ### Must not
 Ambiguous
 ```
 Translation:
 A           with garage AND with blue window
 └── B       AND WITHOUT [round AND green] window

 - Houses with garage, AND having a blue window, WITHOUT a [round AND green] window

        DIFFERENT THAN

 A           with garage AND with blue window
 └── B       WITH a [blue AND NOT [green AND round]] window

  - Houses with garage, AND having a [blue AND NOT [green AND round]] window

        DIFFERENT THAN

 A           with garage AND with blue window
 └── B       WITH a [blue AND NOT green AND not round] window

   - Houses with garage, AND having a [blue AND NOT green AND NOT round] window

```

#### Complex

### Should
 Warning: not most intuitive
```
 A           with garage OR with blue window
 └── B       with round window

 - Houses [with garage, OR having a blue windows] AND having a round window
        DIFFERENT THAN
 - Houses with garage, OR having a [blue AND round] window
```

```
A           (Terms agg)
└── B       (Filters agg)
    ├── C   (Avg agg)
    └── D   (Sum agg)
```
