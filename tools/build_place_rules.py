"""
build_place_rules.py

The place-resolution rules, applied to the combinations table
build_place_table.py writes (see its docstring for the two stages, and
docs/data-cleaning.md, "Places", for what the rules decide and why).

Every "most frequent" choice breaks ties by its key, so a rebuild from the
same archive gives the same tables. Features and points are integer-indexed
and every per-combination decision that can be a join is one; only the
noise combinations are decided row by row.
"""

import json
import math
import os
import re
import unicodedata

import polars as pl

# A feature's non-main point counts as its own (legitimate) when the feature
# holds at least this share of the point's values; below it the point is
# noise for that feature.
NOISE_SHARE = 0.5
# A country or state whose every candidate point is this far from where its
# own features are has no correct point in GDELT: its features' median.
FAR_FROM_OWN_FEATURES_KM = 5000
# Canonical names come from the latest era a place was written in.
LATEST_ERA_FLOOR = 2015
# Which reading of an ID two gazetteers share keeps the bare ID: a
# country's FIPS code is also its CountryCode, and GNIS before GNS follows
# the codebook's own order.
PRECEDENCE = {"country": 0, "state": 1, "gnis": 2, "gns": 3}
# Recorded in the table's metadata and in the clean step's fingerprint:
# bump it whenever a rule changes, so data resolved under the old table is
# cleaned again. A rebuild from a longer archive under the same rules
# changes the source it records, and the fingerprint with it.
TABLE_VERSION = 1

Point = tuple[float, float]


def _norm(name) -> str:
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z ]", "", s.split(",")[0]).strip()


def _key(name) -> str:
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z ]", "", s).strip()


def _km(a: Point, b: Point) -> float:
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = (math.sin((la2 - la1) / 2) ** 2
         + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2)
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def _namespace(t: pl.Expr) -> pl.Expr:
    """The gazetteer a Type reads a FeatureID in: FIPS countries, US states,
    GNIS (US places) or GNS (the rest of the world)."""
    return (pl.when(t == 1).then(pl.lit("country")).when(t == 2).then(pl.lit("state"))
            .when(t == 3).then(pl.lit("gnis")).otherwise(pl.lit("gns")))


def classify(combos: pl.DataFrame) -> pl.DataFrame:
    """Each combination's class, integer type, 6-decimal point and, for a
    typed feature, its namespaced key (country:US, state:GA, gnis:531871,
    gns:449676)."""
    fid = pl.col("FeatureID").fill_null("")
    has_pt = pl.col("Lat").is_not_null() & pl.col("Long").is_not_null()
    zero = has_pt & (pl.col("Lat") == 0) & (pl.col("Long") == 0)
    t = pl.col("Type").fill_null(-1)
    cls = (
        pl.when(~has_pt).then(pl.lit("no_point"))
        .when(zero).then(pl.lit("zero_zero"))
        .when(fid == "0").then(pl.lit("placeholder"))
        .when(fid == "").then(pl.lit("no_id"))
        .when(fid.str.contains(r"^[A-Z]{2}$")).then(pl.lit("two_letter"))
        .when(fid.str.contains(r"^-?\d+$")).then(pl.lit("numeric"))
        .otherwise(pl.lit("other"))
    )
    ns = _namespace(t)
    out = combos.with_columns(fid.alias("fid"), t.alias("T"), cls.alias("cls"),
                              pl.col("Lat").round(6).alias("lat6"),
                              pl.col("Long").round(6).alias("lon6"))
    typed = pl.col("cls").is_in(["numeric", "two_letter"]) & (pl.col("T") >= 1)
    return out.with_columns(
        pl.when(typed).then(ns + pl.lit(":") + pl.col("fid")).otherwise(None).alias("nk")
    )


def _repr_key(df: pl.DataFrame) -> pl.Series:
    """"lat,lon" as Python writes the floats: the order ties are broken in."""
    return pl.Series("pk", [f"{la!r},{lo!r}" for la, lo in df.select("lat6", "lon6").iter_rows()],
                     dtype=pl.String)


def build_places(combos_path: str, out_dir: str) -> None:
    t = classify(pl.read_parquet(combos_path)).with_row_index("row")
    typed = t.filter(pl.col("nk").is_not_null())

    # --- features (nodes) and points, integer-indexed -------------------------
    # ni follows (values desc, key), so a smaller ni is a larger feature.
    edges = typed.group_by(["nk", "lat6", "lon6"]).agg(pl.col("values").sum())
    nodes = (edges.group_by("nk").agg(pl.col("values").sum().alias("node_v"))
             .sort(["node_v", "nk"], descending=[True, False]).with_row_index("ni"))
    points = edges.select("lat6", "lon6").unique()
    points = (points.with_columns(_repr_key(points)).sort("pk").with_row_index("pi"))
    point_v = edges.group_by(["lat6", "lon6"]).agg(pl.col("values").sum().alias("point_v"))
    edges = (edges.join(nodes.select("nk", "ni"), on="nk")
             .join(points, on=["lat6", "lon6"]).join(point_v, on=["lat6", "lon6"])
             .with_columns((pl.col("values") / pl.col("point_v")).alias("s_p")))
    n_nodes, n_points = nodes.height, points.height

    main = (edges.sort(["ni", "values", "pk"], descending=[False, True, False])
            .unique("ni", keep="first", maintain_order=True)
            .select("ni", pl.col("pi").alias("main_pi")))
    edges = edges.join(main, on="ni").with_columns(
        pl.when(pl.col("pi") == pl.col("main_pi")).then(pl.lit("main"))
        .when(pl.col("s_p") >= NOISE_SHARE).then(pl.lit("legit"))
        .otherwise(pl.lit("noise")).alias("kind")
    )
    main_name = (typed.filter(pl.col("FullName").is_not_null())
                 .group_by(["nk", "FullName"]).agg(pl.col("values").sum())
                 .sort(["nk", "values", "FullName"], descending=[False, True, False])
                 .unique("nk", keep="first", maintain_order=True)
                 .select("nk", pl.col("FullName").alias("main_name")))
    owner = (edges.sort(["pi", "values", "nk"], descending=[False, True, False])
             .unique("pi", keep="first", maintain_order=True)
             .select("pi", pl.col("ni").alias("owner_ni")))

    # --- identity: a place is a location ----------------------------------
    # Features join the place of their main point, then of their legitimate
    # points, largest feature first. Two different countries or states never
    # become one place through a shared point (the Paracel Islands written at
    # Palmyra Atoll's point): the smaller one keeps a place of its own.
    # Node ni is element ni, point pi is element n_nodes + pi.
    parent = list(range(n_nodes + n_points))

    def find(x: int) -> int:
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    is_admin = nodes["nk"].str.contains(r"^(country|state):").to_list()
    admin: dict[int, int] = {ni: ni for ni in range(n_nodes) if is_admin[ni]}

    def join(ni: int, pi: int) -> None:
        a, b = find(ni), find(n_nodes + pi)
        if a == b:
            return
        ka, kb = admin.get(a), admin.get(b)
        if ka is not None and kb is not None and ka != kb:
            return
        parent[b] = a
        if ka is None and kb is not None:
            admin[a] = kb

    for ni, pi in enumerate(main.sort("ni")["main_pi"].to_list()):
        join(ni, pi)
    legit = edges.filter(pl.col("kind") == "legit").sort(["nk", "pk"])
    for ni, pi in legit.select("ni", "pi").iter_rows():
        join(ni, pi)
    node_root = pl.DataFrame(
        {"ni": range(n_nodes), "root": [find(i) for i in range(n_nodes)]},
        schema={"ni": pl.UInt32, "root": pl.Int64})
    # Only a point some feature joined has a place; a point that is only ever
    # noise has none.
    joined = edges.filter(pl.col("kind").is_in(["main", "legit"])).select("pi").unique()
    point_root = pl.DataFrame(
        {"pi": range(n_points), "proot": [find(n_nodes + j) for j in range(n_points)]},
        schema={"pi": pl.UInt32, "proot": pl.Int64}).join(joined, on="pi", how="semi")

    # --- every combination to a place ---------------------------------------
    t = (t.join(nodes.select("nk", "ni"), on="nk", how="left")
         .join(points.select("lat6", "lon6", "pi"), on=["lat6", "lon6"], how="left")
         .join(edges.select("ni", "pi", "kind"), on=["ni", "pi"], how="left")
         .join(node_root, on="ni", how="left")
         .join(point_root, on="pi", how="left"))
    # Old Type 0 or null: the typed feature with that ID at that point, when
    # exactly one has it.
    by_fid = (edges.with_columns(pl.col("nk").str.split(":").list.get(1).alias("fid"))
              .group_by(["fid", "lat6", "lon6"]).agg(pl.col("ni").first(), pl.len().alias("k"))
              .filter(pl.col("k") == 1).join(node_root, on="ni")
              .select("fid", "lat6", "lon6", pl.col("root").alias("legacy_root")))
    t = t.join(by_fid, on=["fid", "lat6", "lon6"], how="left")

    cls, kind = pl.col("cls"), pl.col("kind")
    no_place = cls.is_in(["no_point", "zero_zero"])
    legacy = cls.is_in(["numeric", "two_letter"]) & pl.col("nk").is_null()
    by_point = legacy | cls.is_in(["placeholder", "no_id"])
    t = t.with_columns(
        pl.when(no_place).then(None)
        .when(kind.is_in(["main", "legit"])).then(pl.col("root"))
        .when(legacy & pl.col("legacy_root").is_not_null()).then(pl.col("legacy_root"))
        .when(by_point & pl.col("proot").is_not_null()).then(pl.col("proot"))
        .otherwise(None).alias("place_root"),
        pl.when(no_place).then(pl.lit("no location"))
        .when(kind.is_in(["main", "legit"])).then(pl.lit("by id (") + kind + pl.lit(")"))
        .when(kind == "noise").then(pl.lit("noise"))
        .when(legacy & pl.col("legacy_root").is_not_null()).then(pl.lit("legacy type, by id"))
        .when(by_point & pl.col("proot").is_not_null()).then(cls + pl.lit(", by point"))
        # No known place at this combination: the clean step leaves it as
        # GDELT wrote it.
        .otherwise(cls + pl.lit(", unresolved")).alias("how"),
    )
    t = _decide_noise(t, owner, node_root, nodes, main_name)
    t = _resolve_without_location(t, nodes, node_root)

    # --- place attributes from the dominant feature ---------------------------
    dom = (node_root.group_by("root").agg(pl.col("ni").min())
           .join(nodes.select("ni", "nk"), on="ni").join(main, on="ni")
           .join(points.select(pl.col("pi").alias("main_pi"), "lat6", "lon6"), on="main_pi"))
    dom_type = (typed.group_by(["nk", "T"]).agg(pl.col("values").sum())
                .sort(["nk", "values", "T"], descending=[False, True, False])
                .unique("nk", keep="first", maintain_order=True).select("nk", "T"))
    dom = (dom.join(dom_type, on="nk").join(main_name, on="nk", how="left")
           .with_columns(pl.col("nk").str.split(":").list.get(0).alias("ns"),
                         pl.col("nk").str.split(":").list.get(1).alias("dfid")))
    dom = (dom.join(_canonical_points(t, edges, node_root, dom), on="root", how="left")
           .with_columns(pl.coalesce("new_lat", "lat6").alias("place_lat"),
                         pl.coalesce("new_lon", "lon6").alias("place_lon")))
    dom = dom.join(_canonical_names(t), on="root", how="left").with_columns(
        pl.coalesce("canonical_name", "main_name").alias("place_name"))

    # Place ids: GDELT's own FeatureID. Where one ID names places in two
    # gazetteers, the reading first in PRECEDENCE keeps it and the other
    # takes a form of its own: a US state its ADM1 code, as GDELT writes it
    # in ADM1Code (CA -> USCA, since CA is Canada), a GNS place the gns:
    # prefix (gns:449676, since 449676 is Indiana University in GNIS). A
    # fixed order, not the larger reading, so a rebuild never swaps them.
    rank = pl.col("ns").replace_strict(PRECEDENCE, return_dtype=pl.Int8)
    dom = dom.with_columns(rank.alias("rank")).with_columns(
        pl.when(pl.col("rank") == pl.col("rank").min().over("dfid")).then(pl.col("dfid"))
        .when((pl.col("ns") == "state") & pl.col("dfid").str.contains(r"^[A-Z]{2}$"))
        .then(pl.lit("US") + pl.col("dfid"))
        .otherwise(pl.col("ns") + pl.lit(":") + pl.col("dfid")).alias("place"))
    if dom["place"].n_unique() != dom.height:
        raise SystemExit("two places would share a place ID")
    places = (dom.sort("nk").with_row_index("place_id")
              .select(pl.col("place_id").cast(pl.Int32), "root", "place",
                      pl.col("T").cast(pl.Int8).alias("place_type"),
                      "place_lat", "place_lon", "place_name"))

    # --- the table: one row per (FeatureID, Lat, Long) with a place ---------
    # Only combinations the rules resolved; the clean step handles the rest
    # by rule. A point without a location is keyed by its FeatureID alone,
    # with Lat and Long null, a point at (0, 0) included.
    t = t.join(places.select("root", "place_id"), left_on="place_root", right_on="root",
               how="left")
    resolved = t.filter(pl.col("place_id").is_not_null()).with_columns(
        pl.when(pl.col("cls").is_in(["no_point", "zero_zero"])).then(None)
        .otherwise(pl.col(c)).alias(c) for c in ("Lat", "Long")
    )
    lookup = resolved.group_by(["fid", "Lat", "Long"]).agg(pl.col("place_id").unique())
    clashes = lookup.filter(pl.col("place_id").list.len() > 1)
    if clashes.height:
        raise SystemExit(f"{clashes.height} (FeatureID, Lat, Long) combinations resolve to "
                         f"more than one place; lookup by the triple would be ambiguous")
    table = (lookup.select(pl.col("fid").alias("FeatureID"), "Lat", "Long",
                           pl.col("place_id").list.first())
             .join(places.drop("root"), on="place_id").drop("place_id")
             .sort(["FeatureID", "Lat", "Long"], nulls_last=True))

    source = pl.read_parquet_metadata(combos_path).get("gdeltforge:place_combos")
    info = {
        "version": TABLE_VERSION,
        "source": json.loads(source) if source else None,
        "combinations": t.height,
        "geo_values": int(t["values"].sum()),
        "places": places.height,
        "rows": table.height,
    }
    os.makedirs(out_dir, exist_ok=True)
    table.write_parquet(os.path.join(out_dir, "places.parquet"), compression="zstd",
                        compression_level=22, row_group_size=1 << 20,
                        metadata={"gdeltforge:places": json.dumps(info)})
    t.select("FeatureID", "Type", "FullName", "Lat", "Long", "values", "cls", "how",
             "place_id").write_parquet(os.path.join(out_dir, "assignments.parquet"))
    total = int(t["values"].sum())
    print(f"places: {places.height:,} | table rows: {table.height:,} | {info}")
    summary = t.group_by("how").agg(pl.col("values").sum()).sort("values", descending=True)
    for how, v in summary.iter_rows():
        print(f"  {how:32} {v:>15,} ({v / total:.4%})")


def _decide_noise(t, owner, node_root, nodes, main_name) -> pl.DataFrame:
    """A feature written at a point that isn't legitimately its own: the
    point's owner when the written name is the owner's and not the
    feature's (the ID is wrong), otherwise the feature (the point is)."""
    noisy = t.filter(pl.col("how") == "noise")
    if noisy.is_empty():
        return t
    names = dict(main_name.iter_rows())
    nk_of = dict(nodes.select("ni", "nk").iter_rows())
    root_of = dict(node_root.iter_rows())
    owner_of = dict(owner.iter_rows())
    decided = []
    for row, ni, pi, nk, name in noisy.select("row", "ni", "pi", "nk", "FullName").iter_rows():
        o, said = owner_of.get(pi), _norm(name)
        if (o is not None and said == _norm(names.get(nk_of[o], ""))
                and said != _norm(names.get(nk, ""))):
            decided.append((row, root_of[o], "noise: id wrong, by point"))
        else:
            decided.append((row, root_of[ni], "noise: point wrong, by id"))
    fix = pl.DataFrame(decided, schema={"row": pl.UInt32, "noise_root": pl.Int64,
                                        "noise_how": pl.String}, orient="row")
    return (t.join(fix, on="row", how="left")
            .with_columns(pl.coalesce("noise_root", "place_root").alias("place_root"),
                          pl.coalesce("noise_how", "how").alias("how"))
            .drop("noise_root", "noise_how"))


def _resolve_without_location(t, nodes, node_root) -> pl.DataFrame:
    """
    A point with no location (no coordinates, or (0, 0), which is the same
    thing) names a place when its FeatureID does: read in the gazetteer
    its Type says, or, untyped, in the only gazetteer that has the ID, it
    is a feature with a real point. The clean step looks such a point up
    by its FeatureID alone, so every such point of one FeatureID must name
    the same place, or none of them does. The placeholder 0 and a missing
    ID name nothing.
    """
    no_location = pl.col("cls").is_in(["no_point", "zero_zero"])
    shaped = pl.col("fid").str.contains(r"^([A-Z]{2}|-?\d+)$")
    typed_root = node_root.join(nodes.select("ni", "nk"), on="ni").select(
        pl.col("nk").alias("__nk"), pl.col("root").alias("__typed_root"))
    untyped_root = (
        nodes.select("ni", pl.col("nk").str.split(":").list.get(1).alias("fid"))
        .group_by("fid").agg(pl.col("ni").first(), pl.len().alias("k"))
        .filter(pl.col("k") == 1).join(node_root, on="ni")
        .select("fid", pl.col("root").alias("__untyped_root"))
    )
    t = (t.with_columns(
            pl.when(pl.col("T") >= 1)
            .then(_namespace(pl.col("T")) + pl.lit(":") + pl.col("fid")).alias("__nk"))
         .join(typed_root, on="__nk", how="left")
         .join(untyped_root, on="fid", how="left")
         .with_columns(
            pl.when(no_location & shaped)
            .then(pl.when(pl.col("T") >= 1).then(pl.col("__typed_root"))
                  .otherwise(pl.col("__untyped_root")))
            .alias("__named")))
    agreed = (t.filter(no_location).group_by("fid")
              .agg(pl.col("__named").null_count().alias("unnamed"),
                   pl.col("__named").n_unique().alias("n"), pl.col("__named").first())
              .filter((pl.col("unnamed") == 0) & (pl.col("n") == 1))
              .select("fid", pl.col("__named").alias("__by_id")))
    t = t.join(agreed, on="fid", how="left")
    return t.with_columns(
        pl.when(no_location).then(pl.col("__by_id")).otherwise(pl.col("place_root"))
        .alias("place_root"),
        pl.when(no_location & pl.col("__by_id").is_not_null())
        .then(pl.lit("no location, by id")).otherwise(pl.col("how")).alias("how"),
    ).drop("__nk", "__typed_root", "__untyped_root", "__named", "__by_id")


def _canonical_points(t, edges, node_root, dom) -> pl.DataFrame:
    """Countries and states: of the points legitimately theirs, the one
    closest to where the place's own features are (its cities for a country,
    its GNIS features for a state). The most frequent point is sometimes the
    ISO reading of the FIPS code (Mauritius written at the Northern
    Marianas)."""
    world = t.filter((pl.col("cls") == "numeric") & pl.col("T").is_in([4, 5])
                     & pl.col("FullName").is_not_null())
    city_c = _medians(world, pl.col("FullName").str.split(",").list.last())
    us = t.filter((pl.col("cls") == "numeric") & (pl.col("T") == 3)
                  & pl.col("FullName").is_not_null())
    state_c = _medians(us, pl.col("FullName").str.split(",").list.get(-2, null_on_oob=True))

    admin = dom.filter(pl.col("ns").is_in(["country", "state"]))
    good = (edges.filter(pl.col("kind").is_in(["main", "legit"])).join(node_root, on="ni")
            .join(admin.select("root"), on="root", how="semi").sort(["nk", "pk"]))
    cands: dict[int, list[Point]] = {}
    for root, la, lo in good.select("root", "lat6", "lon6").iter_rows():
        lst = cands.setdefault(root, [])
        if (la, lo) not in lst:
            lst.append((la, lo))
    out = []
    for root, ns, name in admin.select("root", "ns", "main_name").iter_rows():
        ref = (city_c if ns == "country" else state_c).get(_key(str(name or "").split(",")[0]))
        if ref is None or ref[2] < 5 or not cands.get(root):
            continue
        dist = [_km(c, (ref[0], ref[1])) for c in cands[root]]
        best = min(range(len(dist)), key=dist.__getitem__)
        if dist[best] > FAR_FROM_OWN_FEATURES_KM:
            out.append((root, round(ref[0], 6), round(ref[1], 6)))
        else:
            out.append((root, *cands[root][best]))
    return pl.DataFrame(out, schema={"root": pl.Int64, "new_lat": pl.Float64,
                                     "new_lon": pl.Float64}, orient="row")


def _medians(rows: pl.DataFrame, part: pl.Expr) -> dict:
    keyed = rows.with_columns(part.alias("part")).filter(pl.col("part").is_not_null())
    keyed = keyed.with_columns(pl.col("part").map_elements(_key, return_dtype=pl.String).alias("k"))
    agg = keyed.group_by("k").agg(pl.col("Lat").median(), pl.col("Long").median(),
                                  pl.len().alias("n"))
    return {k: (la, lo, n) for k, la, lo, n in agg.iter_rows()}


def _canonical_names(t: pl.DataFrame) -> pl.DataFrame:
    """The most used clean name in the latest era a place was written in."""
    r = t.filter(pl.col("place_root").is_not_null() & pl.col("FullName").is_not_null()
                 & (pl.col("FullName") != "") & ~pl.col("FullName").str.contains("[?�]"))
    r = r.with_columns(pl.col("last_year").max().over("place_root").alias("era"))
    r = r.filter(pl.col("last_year") >= pl.min_horizontal(pl.col("era"), pl.lit(LATEST_ERA_FLOOR)))
    return (r.group_by(["place_root", "FullName"]).agg(pl.col("values").sum())
            .sort(["place_root", "values", "FullName"], descending=[False, True, False])
            .unique("place_root", keep="first", maintain_order=True)
            .select(pl.col("place_root").alias("root"),
                    pl.col("FullName").alias("canonical_name")))
