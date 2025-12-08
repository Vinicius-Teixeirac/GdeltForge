## 5.6 Filtered Sampling — Complete List of Valid Inputs

FilteredSampler allows powerful filtering over GDELT Parquet files using a flexible JSON dictionary (--filter).
Filters define what rows are kept before sampling, and support:

- simple equality

- lists (IN)

- numeric ranges

- relational operators

- nested AND / OR logical blocks

Below is the complete specification.

### 5.6.1 Basic Filter Types (Single Column)

- Equality

  **{ "ActionGeo_CountryCode": "USA" }**

  Equivalent to:

  ```
  ActionGeo_CountryCode == "USA"
  ```

- IN List
  **{ "QuadClass": [1, 2, 3] }**


  Equivalent to:

  $$\text{QuadClass} \in \{1, 2, 3\}$$

- Numeric Range (Tuple Style)

  **{ "GoldsteinScale": [0, 5] }**


  Equivalent to:

  $$0 \leq GoldsteinScale \leq 5$$

### Dictionary Operator (Explicit)

All operator forms:

- equals:
  **{ "IsRootEvent": { "op": "equals", "value": 1 } }**

- in_list:
  **{ "QuadClass": { "op": "in_list", "values": [1, 2] } }**

- greater-than:
  **{ "NumArticles": { "op": "gt", "value": 20 } }**

- less-than:
  **{ "NumMentions": { "op": "lt", "value": 5 } }**

- between / range:
  **{ "GoldsteinScale": { "op": "between", "min": -2, "max": 2 } }**


All of the above apply to any numeric or categorical GDELT column.

### Logical Groups 

filter_dict can contain nested AND / OR blocks to build richer logic.

- Top-level AND (default behavior)

  This is how filtering worked before.
  Multiple keys → combined with AND.

  ```
  {
    "ActionGeo_CountryCode": "USA",
    "QuadClass": [1, 2]
  }
  ```

  Equivalent to:

  ```
  CountryCode="USA" AND QuadClass in {1,2}
  ```

- Top-level OR

  ```
  {
    "OR": {
      "ActionGeo_CountryCode": "USA",
      "Actor1CountryCode": "USA"
    }
  }
  ```

  Equivalent to:
  ```
  CountryCode="USA" OR Actor1CountryCode="USA"
  ```

- Nested AND inside OR
  ```
  {
    "OR": {
      "Actor1_CountryCode": "BRA",
      "AND": {
        "Actor2_CountryCode": "BRA",
        "ActionGeo_CountryCode": "BR"
      }
    }
  }
  ```

  Equivalent to:

  ```
  Actor1="BRA"
  OR (Actor2="BRA" AND ActionGeo="BR")
  ```

- Nested OR inside AND

  Example: keep USA events and events where either actor is Russia:

  ```
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

  Equivalent to:

  ```
  ActionGeoCountry="USA"
  AND (Actor1="RUS" OR Actor2="RUS")
  ```

- Deeply Nested Example

  You can combine arbitrarily:

  ```
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

  Equivalent to:

  ```
  (IsRootEvent=1 AND QuadClass in {1,2})
  OR
  (Actor1="CHN" OR Actor2="CHN")
  ```

### Selecting Specific Columns

You may include only specific output columns:

```
python main.py sample \
  --mode filtered \
  --filter '{"ActionGeo_CountryCode": "USA"}' \
  --columns GLOBALEVENTID Year Actor1Code \
  -n 1000
```

### Sampling Methods Compatible With Filters

Once the filter is applied, sampling works normally:

- Random sample
  ```
  python main.py sample --mode filtered -n 5000 --filter '{"QuadClass":[1,2]}'
  ```

- Stratified by column
  ```
  python main.py sample \
      --mode filtered \
      --filter '{"ActionGeo_CountryCode":"USA"}' \
      --stratify QuadClass \
      --n-per-group 500
  ```

| Filter Type | Example JSON | Description |
|-------------|--------------|-------------|
| equal | `"X": "USA"` | $\text{X} == "USA"$ |
| in list | `"X": [1,2,3]` | $\text{X} \in \{1,2,3\}$ |
| tuple range | `"X": [0,5]` | $0 \leq \text{X} \leq 5$ |
| op:equals | `"X": {"op":"equals","value":10}` | explicit equality |
| op:gt | `"X": {"op":"gt","value":0}` | $\text{X} > 0$ |
| op:lt | `"X": {"op":"lt","value":5}` | $\text{X} < 5$ |
| op:between | `"X":{"op":"between","min":0,"max":10}` | between range |
| AND block | `"AND": {...}` | all conditions must match |
| OR block | `"OR": {...}` | any condition may match |
| nested logic | `{"OR": {"X":1, "AND": {...}}}` | combine logic trees |