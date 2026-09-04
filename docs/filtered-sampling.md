# Filtered sampling: filter syntax reference

`FilteredSampler` (the `gdeltforge sample --mode filtered` command) filters GDELT Parquet files using a flexible JSON dictionary passed via `--filter`. Filters define what rows are kept before sampling, and support:

- simple equality
- lists (`IN`)
- numeric ranges
- relational operators (`>`, `<`)
- nested `AND` / `OR` logical blocks

Below is the complete specification. For runnable end-to-end examples built on it, see [Recipes](recipes.md); to check that a filter value is a valid code before running, see [`gdeltforge codes`](cli-reference.md#gdeltforge-codes).

!!! note "Which columns you can filter on depends on your dataset"

    `--dataset events` reads GDELT's daily archive, still exported in the older, 58-column, GDELT-1.0-compatible schema for backward compatibility. `--dataset events-15min` reads the same underlying events in GDELT 2.0's native 61-column format instead, adding `Actor1Geo_ADM2Code`/`Actor2Geo_ADM2Code`/`ActionGeo_ADM2Code`, finer administrative-region geocoding than the `ADM1Code` both schemas already carry. A filter or `--columns` entry naming one of those three fields works against `events-15min` but fails with a missing-column error against `events`. See [Configuration](configuration.md#datasets-and-dataset) for the full schema comparison.

## Basic filter types (single column)

**Equality**

```json
{ "ActionGeo_CountryCode": "US" }
```

Equivalent to `ActionGeo_CountryCode == "US"`.

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

!!! warning "Arrays are lists, never ranges"

    A JSON array such as `[0, 5]` is always treated as an IN-list (`isin`), matching exactly 0 or 5, not the range 0-5. Ranges need the explicit `{"op": "between"}` form above.

## Dictionary operator (explicit)

All operator forms:

| Operator | Example | Meaning |
|----------|---------|---------|
| `equals` | `{ "IsRootEvent": { "op": "equals", "value": 1 } }` | `IsRootEvent == 1` |
| `in_list` | `{ "QuadClass": { "op": "in_list", "values": [1, 2] } }` | `QuadClass ∈ {1, 2}` |
| `gt` | `{ "NumArticles": { "op": "gt", "value": 20 } }` | `NumArticles > 20` |
| `lt` | `{ "NumMentions": { "op": "lt", "value": 5 } }` | `NumMentions < 5` |
| `between` / `range` | `{ "GoldsteinScale": { "op": "between", "min": -2, "max": 2 } }` | `-2 ≤ GoldsteinScale ≤ 2` |

All of the above apply to any numeric or categorical GDELT column.

## Logical groups

`filter_dict` can contain nested `AND` / `OR` blocks to build richer logic.

**Top-level AND** (default behavior: multiple keys are combined with AND)

```json
{
  "ActionGeo_CountryCode": "US",
  "QuadClass": [1, 2]
}
```

Equivalent to `ActionGeo_CountryCode="US" AND QuadClass in {1,2}`.

**Top-level OR**

```json
{
  "OR": {
    "ActionGeo_CountryCode": "US",
    "Actor1CountryCode": "USA"
  }
}
```

Equivalent to `ActionGeo_CountryCode="US" OR Actor1CountryCode="USA"`.

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
    "ActionGeo_CountryCode": "US",
    "OR": {
      "Actor1CountryCode": "RUS",
      "Actor2CountryCode": "RUS"
    }
  }
}
```

Equivalent to `ActionGeo_CountryCode="US" AND (Actor1="RUS" OR Actor2="RUS")`.

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

## Selecting specific columns

You may restrict the output to specific columns, which is a memory-friendly practice:

```bash
gdeltforge sample \
  --dataset events \
  --mode filtered \
  --filter '{"ActionGeo_CountryCode": "US"}' \
  --columns GlobalEventID Year Actor1Code \
  -n 1000
```

## Sampling methods compatible with filters

Once the filter is applied, sampling works normally:

**Random sample**

```bash
gdeltforge sample --dataset events --mode filtered -n 5000 --filter '{"QuadClass":[1,2]}'
```

**Stratified by column**

```bash
gdeltforge sample \
    --dataset events \
    --mode filtered \
    --filter '{"ActionGeo_CountryCode":"US"}' \
    --stratify QuadClass \
    --n-per-group 500
```

## Quick reference

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
