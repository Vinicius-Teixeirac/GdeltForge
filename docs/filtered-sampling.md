# Filtered Sampling: Filter Syntax Reference

`FilteredSampler` (the `gdeltforge sample --mode filtered` command) filters GDELT Parquet files using a flexible JSON dictionary passed via `--filter`. Filters define what rows are kept before sampling, and support:

- simple equality
- lists (`IN`)
- numeric ranges
- relational operators (`>`, `<`)
- nested `AND` / `OR` logical blocks

Below is the complete specification.

## Basic Filter Types (Single Column)

**Equality**

```json
{ "ActionGeo_CountryCode": "USA" }
```

Equivalent to `ActionGeo_CountryCode == "USA"`.

**IN list**

```json
{ "QuadClass": [1, 2, 3] }
```

Equivalent to `QuadClass ∈ {1, 2, 3}`.

**Numeric range**

Range filters require the explicit dictionary form (see below):

```json
{ "GoldsteinScale": { "op": "between", "min": 0, "max": 5 } }
```

Equivalent to `0 ≤ GoldsteinScale ≤ 5`.

Note: a JSON array such as `[0, 5]` is always treated as an IN-list (`isin`), not a range.

## Dictionary Operator (Explicit)

All operator forms:

| Operator | Example | Meaning |
|----------|---------|---------|
| `equals` | `{ "IsRootEvent": { "op": "equals", "value": 1 } }` | `IsRootEvent == 1` |
| `in_list` | `{ "QuadClass": { "op": "in_list", "values": [1, 2] } }` | `QuadClass ∈ {1, 2}` |
| `gt` | `{ "NumArticles": { "op": "gt", "value": 20 } }` | `NumArticles > 20` |
| `lt` | `{ "NumMentions": { "op": "lt", "value": 5 } }` | `NumMentions < 5` |
| `between` / `range` | `{ "GoldsteinScale": { "op": "between", "min": -2, "max": 2 } }` | `-2 ≤ GoldsteinScale ≤ 2` |

All of the above apply to any numeric or categorical GDELT column.

## Logical Groups

`filter_dict` can contain nested `AND` / `OR` blocks to build richer logic.

**Top-level AND** (default behavior -- multiple keys are combined with AND)

```json
{
  "ActionGeo_CountryCode": "USA",
  "QuadClass": [1, 2]
}
```

Equivalent to `CountryCode="USA" AND QuadClass in {1,2}`.

**Top-level OR**

```json
{
  "OR": {
    "ActionGeo_CountryCode": "USA",
    "Actor1CountryCode": "USA"
  }
}
```

Equivalent to `CountryCode="USA" OR Actor1CountryCode="USA"`.

**Nested AND inside OR**

```json
{
  "OR": {
    "Actor1CountryCode": "BRA",
    "AND": {
      "Actor2CountryCode": "BRA",
      "ActionGeo_CountryCode": "BR"
    }
  }
}
```

Equivalent to `Actor1="BRA" OR (Actor2="BRA" AND ActionGeo="BR")`.

**Nested OR inside AND**

Example: keep USA events and events where either actor is Russia:

```json
{
  "AND": {
    "ActionGeo_CountryCode": "USA",
    "OR": {
      "Actor1CountryCode": "RUS",
      "Actor2CountryCode": "RUS"
    }
  }
}
```

Equivalent to `ActionGeoCountry="USA" AND (Actor1="RUS" OR Actor2="RUS")`.

**Deeply nested example**

You can combine arbitrarily:

```json
{
  "OR": {
    "AND": {
      "IsRootEvent": 1,
      "QuadClass": [1, 2]
    },
    "OR": {
      "Actor1CountryCode": "CHN",
      "Actor2CountryCode": "CHN"
    }
  }
}
```

Equivalent to `(IsRootEvent=1 AND QuadClass in {1,2}) OR (Actor1="CHN" OR Actor2="CHN")`.

## Selecting Specific Columns

You may restrict the output to specific columns, which is a memory-friendly practice:

```
gdeltforge sample \
  --mode filtered \
  --filter '{"ActionGeo_CountryCode": "USA"}' \
  --columns GlobalEventID Year Actor1Code \
  -n 1000
```

## Sampling Methods Compatible With Filters

Once the filter is applied, sampling works normally:

**Random sample**

```
gdeltforge sample --mode filtered -n 5000 --filter '{"QuadClass":[1,2]}'
```

**Stratified by column**

```
gdeltforge sample \
    --mode filtered \
    --filter '{"ActionGeo_CountryCode":"USA"}' \
    --stratify QuadClass \
    --n-per-group 500
```

## Quick Reference

| Filter type | Example JSON | Meaning |
|-------------|--------------|---------|
| equal | `"X": "USA"` | `X == "USA"` |
| in list | `"X": [1,2,3]` | `X ∈ {1,2,3}` |
| op:equals | `"X": {"op":"equals","value":10}` | explicit equality |
| op:gt | `"X": {"op":"gt","value":0}` | `X > 0` |
| op:lt | `"X": {"op":"lt","value":5}` | `X < 5` |
| op:between | `"X":{"op":"between","min":0,"max":10}` | `0 ≤ X ≤ 10` |
| AND block | `"AND": {...}` | all conditions must match |
| OR block | `"OR": {...}` | any condition may match |
| nested logic | `{"OR": {"X":1, "AND": {...}}}` | combine logic trees |

See [Recipes](recipes.md) for complete, runnable examples built on this syntax.
