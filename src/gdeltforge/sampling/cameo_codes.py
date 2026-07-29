"""
cameo_codes.py

Reference lookups for GDELT's CAMEO-coded actor and geo fields: short,
fixed-vocabulary codes with a code -> name mapping, across six column
families:
    - CAMEO actor-country codes (3-letter, e.g. "USA"): Actor1CountryCode,
      Actor2CountryCode
    - FIPS 10-4 geo-country codes (2-letter, e.g. "US"): ActionGeo_CountryCode,
      Actor1Geo_CountryCode, Actor2Geo_CountryCode
    - CAMEO ethnic codes: Actor1EthnicCode, Actor2EthnicCode
    - CAMEO known-group codes (IGOs, NGOs, and similar organizations):
      Actor1KnownGroupCode, Actor2KnownGroupCode
    - CAMEO religion codes: Actor1Religion1Code, Actor1Religion2Code,
      Actor2Religion1Code, Actor2Religion2Code
    - CAMEO actor-type codes: Actor1Type1Code, Actor1Type2Code,
      Actor1Type3Code, Actor2Type1Code, Actor2Type2Code, Actor2Type3Code

Sourced from GDELT's own CAMEO codebook, then cross-checked against every
distinct value actually appearing across a full archive scan (~542M rows).
Actor-country, ethnic, type, and religion codes matched in full at the
time of that check. FIPS geo codes were missing a handful of small,
mostly-uninhabited US-administered Pacific islands, added after
confirming each one's name directly from the archive's own
ActionGeo_FullName values. Known-group codes were missing about a
quarter of what's actually used; the public CAMEO actor codebook doesn't
document all of them (PLO notably: historically given a "state-like"
code rather than a known-group one), so those were confirmed individually
against the actor names appearing alongside them in the real data instead
(e.g. "FID" only ever appears next to "INTERNATIONAL FEDERATION OF HUMAN
RIGHTS" / "FIDH").

None of this is exhaustive by construction, just by verification against
one archive snapshot: FIPS 10-4 was retired as a standard in 2008 and the
CAMEO known-group list isn't actively maintained, so a miss here means
"not recognized," not "definitely wrong."

Ethnic codes are stored lowercase in real GDELT data but uppercase in the
source codebook; is_recognized_code() compares case-insensitively
everywhere to avoid that mismatch alone producing false warnings.
"""

import json
from functools import cache, lru_cache
from importlib.resources import files

CAMEO_ACTOR_COUNTRY_COLUMNS = frozenset({"Actor1CountryCode", "Actor2CountryCode"})
FIPS_GEO_COLUMNS = frozenset({
    "ActionGeo_CountryCode", "Actor1Geo_CountryCode", "Actor2Geo_CountryCode",
})
CAMEO_ETHNIC_COLUMNS = frozenset({"Actor1EthnicCode", "Actor2EthnicCode"})
CAMEO_KNOWN_GROUP_COLUMNS = frozenset({"Actor1KnownGroupCode", "Actor2KnownGroupCode"})
CAMEO_RELIGION_COLUMNS = frozenset({
    "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor2Religion1Code", "Actor2Religion2Code",
})
CAMEO_TYPE_COLUMNS = frozenset({
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
})

_FAMILY_KEY_FOR_COLUMN: dict[str, str] = {
    **dict.fromkeys(CAMEO_ACTOR_COUNTRY_COLUMNS, "ACTOR_COUNTRY_CODES"),
    **dict.fromkeys(FIPS_GEO_COLUMNS, "GEO_COUNTRY_CODES"),
    **dict.fromkeys(CAMEO_ETHNIC_COLUMNS, "ACTOR_ETHNIC_CODES"),
    **dict.fromkeys(CAMEO_KNOWN_GROUP_COLUMNS, "ACTOR_KNOWN_GROUP_CODES"),
    **dict.fromkeys(CAMEO_RELIGION_COLUMNS, "ACTOR_RELIGION_CODES"),
    **dict.fromkeys(CAMEO_TYPE_COLUMNS, "ACTOR_TYPE_CODES"),
}

_FAMILY_DISPLAY_NAME: dict[str, str] = {
    "ACTOR_COUNTRY_CODES": "CAMEO actor-country",
    "GEO_COUNTRY_CODES": "FIPS geo-country",
    "ACTOR_ETHNIC_CODES": "CAMEO ethnic",
    "ACTOR_KNOWN_GROUP_CODES": "CAMEO known-group",
    "ACTOR_RELIGION_CODES": "CAMEO religion",
    "ACTOR_TYPE_CODES": "CAMEO actor-type",
}


@lru_cache(maxsize=1)
def _load() -> dict[str, dict[str, str]]:
    text = (files("gdeltforge") / "data" / "cameo_codes.json").read_text(encoding="utf-8")
    return json.loads(text)


def actor_country_codes() -> dict[str, str]:
    """3-letter CAMEO code -> country/region name."""
    return _load()["ACTOR_COUNTRY_CODES"]


def geo_country_codes() -> dict[str, str]:
    """2-letter FIPS 10-4 code -> country/region name."""
    return _load()["GEO_COUNTRY_CODES"]


def ethnic_codes() -> dict[str, str]:
    """CAMEO ethnic code -> ethnicity name."""
    return _load()["ACTOR_ETHNIC_CODES"]


def known_group_codes() -> dict[str, str]:
    """CAMEO known-group code -> organization name."""
    return _load()["ACTOR_KNOWN_GROUP_CODES"]


def religion_codes() -> dict[str, str]:
    """CAMEO religion code -> religion name."""
    return _load()["ACTOR_RELIGION_CODES"]


def type_codes() -> dict[str, str]:
    """CAMEO actor-type code -> type description."""
    return _load()["ACTOR_TYPE_CODES"]


def code_family_for_column(column: str) -> dict[str, str] | None:
    """Return the reference dict for column's code family, or None if
    column isn't a recognized CAMEO-coded column."""
    key = _FAMILY_KEY_FOR_COLUMN.get(column)
    return _load()[key] if key else None


def family_name_for_column(column: str) -> str | None:
    """Human-readable name of column's code family, for messages."""
    key = _FAMILY_KEY_FOR_COLUMN.get(column)
    return _FAMILY_DISPLAY_NAME.get(key) if key else None


@cache
def _uppercase_keys(family_key: str) -> frozenset[str]:
    return frozenset(k.upper() for k in _load()[family_key])


def is_recognized_code(column: str, value: str) -> bool | None:
    """
    True/False if column is a known CAMEO-coded column and value is/isn't
    in its reference list; None if column isn't a coded column at all.
    """
    family_key = _FAMILY_KEY_FOR_COLUMN.get(column)
    if family_key is None:
        return None
    return value.upper() in _uppercase_keys(family_key)
