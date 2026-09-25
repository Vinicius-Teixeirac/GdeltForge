"""
cameo_codes.py

Reference lookups for GDELT's CAMEO-coded actor, geo, and event fields:
short, fixed-vocabulary codes with a code -> name mapping, across seven
column families:
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
    - CAMEO event codes (2-digit root, 3-digit base, up to 4-digit fully
      specified, all sharing one namespace): EventCode, EventBaseCode,
      EventRootCode

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

Event codes matched in full except two 4-digit leaf codes under "121:
Reject material cooperation" (1213, 1214): the public CAMEO manual only
documents that branch up to economic/military (1211/1212), not the
judicial/intelligence continuation (X13/X14) it uses everywhere else
(e.g. 0213/0214, 0313/0314, 1014). Confirmed both against the
TABARI/PETRARCH verb-pattern dictionary GDELT's own event coder runs on
(openeventdata/Dictionaries, CAMEO.verbpatterns.txt): 1213 appears
repeatedly on extradition/tribunal/judge-order patterns (judicial), 1214
on a records-related pattern (intelligence), consistent with the X13/X14
convention rather than assumed from numbering alone.

EventCode/EventBaseCode/EventRootCode also carry three markers that
aren't CAMEO event categories, listed here so every value in the record
resolves. "---" (EventCode/EventBaseCode, 325 rows in the 1979 to 2026
archive) is the CAMEO null code: PETRARCH's reader defines it as the
code of a verb pattern that "does not generate an event", used on 693
patterns in the CAMEO verb dictionary to block phrases that match an
event verb without being a political event. "--" is its first two
characters, written to EventRootCode on the same rows. "X" (9 rows,
all three columns) is undocumented anywhere in CAMEO or PETRARCH; every
row carrying it is QuadClass 4, material conflict, with no Goldstein
score, so it reads as a coercion-type event whose code was lost.

Every name was then checked again, in 2026-09, against GDELT's own lookup
tables (gdeltproject.org/data/lookups/*.txt) and against the full Events
archive (869M rows, 1979 to 2026-07): a geo-country code against the
FullName GDELT writes for a country-level place, an actor code against the
actor names coded with it. Where the two sources disagree, the archive
wins. That pass corrected guessed names (known-group SCE is the OSCE, XFM
is Oxfam, WAS is ECOWAS; geo HQ is Howland Island) and spelling slips
("Columbia" for Colombia), added every code GDELT's tables list that was
missing here (Hamas, WHO and 47 more known groups, 38 ethnic codes,
peacekeepers and 4 more actor types, Druze, Akrotiri and Dhekelia), and
dropped keys that aren't codes of their family: complete actor codes
such as IGOUNO in the known-group and type tables, and ISO 3166 codes
(AD, SR, UM) among the FIPS ones. GDELT's own tables carry errors of their own
(ethnic "bod" as Tibetan where the archive codes it beside "BODO", geo LO
as Czechoslovakia), so they aren't copied blindly either.

None of this is exhaustive by construction, just by verification against
one archive snapshot: FIPS 10-4 was retired as a standard in 2008 and the
CAMEO known-group list isn't actively maintained, so a miss here means
"not recognized," not "definitely wrong."

Ethnic codes are keyed lowercase, the way GDELT writes them in the
Events data and in its own lookup table (99.5% of the archive's ethnic
values). Every other family is uppercase, matching the data. A few
ethnic codes also occur uppercase in the record ("PAL", "ARB", "KUR"),
so lookup() and is_recognized_code() both compare case-insensitively;
translate codes through lookup(), never a raw dict index, and no
spelling of a code falls through.
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
CAMEO_EVENT_COLUMNS = frozenset({"EventCode", "EventBaseCode", "EventRootCode"})

_FAMILY_KEY_FOR_COLUMN: dict[str, str] = {
    **dict.fromkeys(CAMEO_ACTOR_COUNTRY_COLUMNS, "ACTOR_COUNTRY_CODES"),
    **dict.fromkeys(FIPS_GEO_COLUMNS, "GEO_COUNTRY_CODES"),
    **dict.fromkeys(CAMEO_ETHNIC_COLUMNS, "ACTOR_ETHNIC_CODES"),
    **dict.fromkeys(CAMEO_KNOWN_GROUP_COLUMNS, "ACTOR_KNOWN_GROUP_CODES"),
    **dict.fromkeys(CAMEO_RELIGION_COLUMNS, "ACTOR_RELIGION_CODES"),
    **dict.fromkeys(CAMEO_TYPE_COLUMNS, "ACTOR_TYPE_CODES"),
    **dict.fromkeys(CAMEO_EVENT_COLUMNS, "EVENT_CODES"),
}

_FAMILY_DISPLAY_NAME: dict[str, str] = {
    "ACTOR_COUNTRY_CODES": "CAMEO actor-country",
    "GEO_COUNTRY_CODES": "FIPS geo-country",
    "ACTOR_ETHNIC_CODES": "CAMEO ethnic",
    "ACTOR_KNOWN_GROUP_CODES": "CAMEO known-group",
    "ACTOR_RELIGION_CODES": "CAMEO religion",
    "ACTOR_TYPE_CODES": "CAMEO actor-type",
    "EVENT_CODES": "CAMEO event",
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


def event_codes() -> dict[str, str]:
    """CAMEO event code (root, base, or fully specified) -> event description."""
    return _load()["EVENT_CODES"]


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
def _uppercase_keys(family_key: str) -> dict[str, str]:
    return {k.upper(): name for k, name in _load()[family_key].items()}


def lookup(column: str, value: str) -> str | None:
    """
    The name for value in column's code family, case-insensitively, or
    None if column isn't a CAMEO-coded column or value isn't a known code.
    """
    family_key = _FAMILY_KEY_FOR_COLUMN.get(column)
    if family_key is None:
        return None
    return _uppercase_keys(family_key).get(value.upper())


def is_recognized_code(column: str, value: str) -> bool | None:
    """
    True/False if column is a known CAMEO-coded column and value is/isn't
    in its reference list; None if column isn't a coded column at all.
    """
    family_key = _FAMILY_KEY_FOR_COLUMN.get(column)
    if family_key is None:
        return None
    return value.upper() in _uppercase_keys(family_key)
