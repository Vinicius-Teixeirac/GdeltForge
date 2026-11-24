# Filtered sampling: filter syntax reference

`FilteredSampler` (the `gdeltforge sample --mode filtered` command) filters GDELT Parquet files using a flexible JSON dictionary passed via `--filter`. Filters define what rows are kept before sampling, and support:

- simple equality
- lists (`IN`)
- numeric ranges
- relational operators (`>`, `<`)
- missing values (`is_null`, `is_blank`)
- nested `AND` / `OR` / `NOT` logical blocks
- one condition over a group of columns (`ANY` / `ALL`)

!!! note "Filtered sampling vs the `clean` stage"
    Filtered sampling selects rows that are *relevant* to your study, per run, without writing anything but the sample. The [`clean`](data-cleaning.md) stage decides whether rows are *usable* at all, once, for the whole dataset. Row selection by content lives only here; see [Data Cleaning](data-cleaning.md#quality-not-relevance) for why.

Below is the complete specification. For runnable end-to-end examples built on it, see [Recipes](recipes.md); to check that a filter value is a valid code before running, see [`gdeltforge codes`](cli-reference.md#gdeltforge-codes).

!!! note "Which columns you can filter on depends on your dataset"

    `--dataset events` reads GDELT's daily archive, still exported in the older, 58-column, GDELT-1.0-compatible schema for backward compatibility. `--dataset events-15min` reads the same underlying events in GDELT 2.0's native 61-column format instead, adding `Actor1Geo_ADM2Code`/`Actor2Geo_ADM2Code`/`ActionGeo_ADM2Code`, finer administrative-region geocoding than the `ADM1Code` both schemas already carry. `--mode filtered`'s own `--filter` and `--columns` both reject one of those three fields against `events` at startup, before any file is even scanned: `--filter` with `Invalid filter column: ...`, `--columns` with `Invalid columns: {...}`, since filtered sampling validates both against the dataset's own declared schema up front, plus the columns the clean stage adds to cleaned files (the `*_original` columns, `EventDate`, `<column>_Label`; see [Data Cleaning](data-cleaning.md)). `--mode indexed`/`calendar`'s `--columns` behaves differently: it isn't validated against a declared schema at all, so naming one of those three fields against `events` there just warns that it wasn't found in the scanned data and drops it, the same graceful handling any other absent-but-non-essential column already gets; those two modes don't take a `--filter` condition at all. See [Configuration](configuration.md#datasets-and-dataset) for the full schema comparison.

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

    An array holds plain values only. A condition inside one (`{"Actor1Code": [{"op": "is_null"}]}`) stops `--filter` before any file is read; to match the values or the condition, put them in an `OR` block: `{"OR": {"Actor1CountryCode": ["USA", "BRA"], "OR": {"Actor1CountryCode": {"op": "is_null"}}}}`. The same holds for `in_list`'s `values` and an `ANY`/`ALL` `match`; the error prints this fix with your own column, values and columns. For `ALL` it is one such `OR` block per column, since each column must match one of them, all under one `AND` key. Every such fix has one key, so it can replace the refused key as it stands, inside `NOT` or an `OR` block too; in a block that already holds that key, nest it one level down.

    `equals`, `gt`, `lt` and `between` compare with one value. For several, write the list on its own (`{"QuadClass": [1, 2]}`) or use `in_list`.

## Dictionary operator (explicit)

All operator forms:

| Operator | Example | Meaning |
|----------|---------|---------|
| `equals` | `{ "IsRootEvent": { "op": "equals", "value": 1 } }` | `IsRootEvent == 1` |
| `in_list` | `{ "QuadClass": { "op": "in_list", "values": [1, 2] } }` | `QuadClass ∈ {1, 2}` |
| `gt` | `{ "NumArticles": { "op": "gt", "value": 20 } }` | `NumArticles > 20` |
| `lt` | `{ "NumMentions": { "op": "lt", "value": 5 } }` | `NumMentions < 5` |
| `between` / `range` | `{ "GoldsteinScale": { "op": "between", "min": -2, "max": 2 } }` | `-2 ≤ GoldsteinScale ≤ 2` |
| `is_null` | `{ "ActionGeo_CountryCode": { "op": "is_null" } }` | `ActionGeo_CountryCode` is missing |
| `is_blank` | `{ "Actor1Code": { "op": "is_blank" } }` | `Actor1Code` is missing, empty or whitespace only |

All of the above apply to any numeric or categorical GDELT column.

A dict condition needs one of these `op`s and the keys its row shows (`value`, `values` as a list, or `min` and `max`). Another `op`, no `op` or a missing key stops `--filter` before any file is read, naming the column and the valid `op`s.

**Missing values.** A row whose column is null never matches a comparison on that column: `{"Actor1CountryCode": "USA"}` and `{"Actor1CountryCode": {"op": "gt", ...}}` both leave it out, as do `AND` and `OR` blocks built on them unless another branch of an `OR` matches. `is_null` selects those rows. `convert` reads an empty field as null; a string of spaces stays a string unless `clean`'s `blank_to_null` turns it into null (see [Data Cleaning](data-cleaning.md#normalize-whitespace)), so `is_blank` matches null, empty and whitespace-only strings. On a column that isn't a string, `is_blank` is `is_null`. Neither takes a value: `{"op": "is_null", "value": ...}` is an error.

**Dates.** GDELT's own date columns are numbers: `Day` and `DATEADDED` compare as `YYYYMMDD` (`{"Day": {"op": "between", "min": 20200101, "max": 20200131}}`). `EventDate`, the real date [`clean.derive`](data-cleaning.md#derive-added-columns) adds, takes a date in either form, the string `"2020-01-02"` (exactly `YYYY-MM-DD`) or the number `20200102`, in every operator above; anything else, the string `"20200102"` included, fails with a message naming both forms.

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

**NOT**

`NOT` holds a block, like `AND`, and keeps the rows that block doesn't match. Keep every event not located in the United States:

```json
{ "NOT": { "ActionGeo_CountryCode": "US" } }
```

Rows where the block can't be decided because a column it reads is null count as not matching, so `NOT` keeps them: the example above keeps the events with no `ActionGeo_CountryCode`. To leave those out too, exclude them in a second `NOT`, nested in an `AND` since a block holds each key once: `{"NOT": {"ActionGeo_CountryCode": "US"}, "AND": {"NOT": {"ActionGeo_CountryCode": {"op": "is_null"}}}}`. `NOT` sits beside other keys like any condition, so `{"QuadClass": 4, "NOT": {...}}` means `QuadClass=4 AND NOT (...)`. An empty `NOT` block is an error.

**ANY and ALL over a group of columns**

`ANY` applies one condition to several columns and keeps a row where at least one of them meets it; `ALL` keeps a row where every one does. Either actor is Brazilian:

```json
{
  "ANY": {
    "columns": ["Actor1CountryCode", "Actor2CountryCode"],
    "match": "BRA"
  }
}
```

Equivalent to `Actor1="BRA" OR Actor2="BRA"`. `match` takes any single-column condition: a value, a list, or an `{"op": ...}` dictionary, `is_null` and `is_blank` included. A null column counts as not meeting it, so `ANY` still matches through the other columns and `ALL` doesn't match. Both actors coded, with `NOT`:

```json
{
  "NOT": {
    "ANY": {
      "columns": ["Actor1CountryCode", "Actor2CountryCode"],
      "match": { "op": "is_blank" }
    }
  }
}
```

The block holds exactly `columns` (one or more column names) and `match`. `ANY` and `ALL` sit beside other keys and inside `AND`, `OR` and `NOT` like any condition.

!!! note "One key per block"
    A JSON object holds each key once, so a block can't name the same column, or `NOT`, `ANY`, `ALL`, `AND` or `OR`, twice. JSON itself would keep only the last and drop the other condition, so `--filter` stops with an error naming the repeated key and the fix for where it sits. Each fix is one `AND` block (both must hold) or one `OR` block (either may) that replaces the two keys: `{"AND": {"NOT": {...}, "AND": {"NOT": {...}}}}` and `{"OR": {"NOT": {...}, "OR": {"NOT": {...}}}}`. For `AND` written twice, the both-must-hold form is one `AND` block with the first block's contents and the second block nested, `{"AND": {..., "AND": {...}}}`; for `OR` twice, the either-may form is the same with `OR`. Inside an `ANY`/`ALL` block, `"match"` takes a list for several values (`"match": ["USA", "BRA"]`), and a second column group is a second `ANY`/`ALL` block in such a pair. Inside a column's `{"op": ...}` condition, a second condition on that column goes in such a pair, replacing the column: `{"AND": {"Day": {"op": "gt", "value": 20200101}, "AND": {"Day": {"op": "lt", "value": 20210101}}}}` keeps the rows meeting both, `{"OR": {"Day": {"op": "lt", "value": 20200101}, "OR": {"Day": {"op": "gt", "value": 20210101}}}}` the rows meeting either.

    The pair is a block of its own because a key beside a condition is joined the way its block joins: in an `AND` block (or at the top), `{"Day": {...}, "OR": {"Day": {...}}}` keeps only the rows meeting both, and in an `OR` block an `AND` key beside a condition is ORed with it.

    Each fix has one key, so it replaces the two keys as they stand, inside `NOT` or an `OR` block too. In a block that already holds that key, the fix's key would be repeated there and is refused in turn: nest it one level down, inside the key already there. With `QuadClass` and an `AND` holding a first condition on `Day` already in the block, the second goes inside that `AND`: `{"AND": {"QuadClass": 1, "AND": {"Day": {"op": "gt", "value": 20200101}, "AND": {"Day": {"op": "lt", "value": 20210101}}}}}`. `AND` (or `OR`) written twice where the first block already holds one is written out in the error the same way: the second block in the first's innermost `AND` (or `OR`), ready to run.

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

!!! warning "Stratified output is not representative of the natural class distribution"

    `--stratify` draws the same `--n-per-group` count from every distinct value of the stratify column, on purpose, so the result is class-balanced regardless of how common each class actually is in the archive. That's the right shape for a model training set that needs coverage of a rare class, not a population-level estimate: computing an unweighted rate off a stratified sample (e.g. "what fraction of US events are QuadClass 4") reads back the sampling ratio you chose, not the real one. Use `--mode filtered` without `--stratify` (or `--mode indexed`) when the goal is an estimate of the natural distribution instead. See [Limitations](limitations-and-roadmap.md#representativeness) for the full picture across all three sampling modes.

    Every stratified run also writes `<out>.strata.json`, recording each stratify value's true row count in the filtered archive regardless of `--n-per-group`. That's the number an unweighted rate above is missing: reweight each group by `true_count / n_per_group` before computing a population-level statistic, and the equal-allocation bias washes out.

## Quick reference

| Filter type | Example JSON | Meaning |
|-------------|--------------|---------|
| equal | `"X": "USA"` | `X == "USA"` |
| in list | `"X": [1,2,3]` | `X ∈ {1,2,3}` |
| op:equals | `"X": {"op":"equals","value":10}` | explicit equality |
| op:gt | `"X": {"op":"gt","value":0}` | `X > 0` |
| op:lt | `"X": {"op":"lt","value":5}` | `X < 5` |
| op:between | `"X":{"op":"between","min":0,"max":10}` | `0 ≤ X ≤ 10` |
| op:is_null | `"X": {"op":"is_null"}` | `X` is missing |
| op:is_blank | `"X": {"op":"is_blank"}` | `X` is missing, empty or whitespace only |
| AND block | `"AND": {...}` | all conditions must match |
| OR block | `"OR": {...}` | any condition may match |
| NOT block | `"NOT": {...}` | the block doesn't match (a null counts as no match) |
| ANY group | `"ANY": {"columns": ["X","Y"], "match": ...}` | `X` or `Y` meets `match` |
| ALL group | `"ALL": {"columns": ["X","Y"], "match": ...}` | `X` and `Y` both meet `match` |
| nested logic | `{"OR": {"X":1, "AND": {...}}}` | combine logic trees |

See [Recipes](recipes.md) for complete, runnable examples built on this syntax.
