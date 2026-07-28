"""
country_codes.py

Reference lookups for GDELT's two country-code families:
    - CAMEO actor codes (3-letter, e.g. "USA"): Actor1CountryCode, Actor2CountryCode
    - FIPS 10-4 geo codes (2-letter, e.g. "US"): ActionGeo_CountryCode,
      Actor1Geo_CountryCode, Actor2Geo_CountryCode

Sourced from GDELT's own CAMEO codebook. NIST retired FIPS 10-4 as a
standard in 2008, so this list can lag reality (a newly formed country,
for instance) -- treat a miss as "not recognized," not "definitely wrong."
"""

import json
from functools import lru_cache
from importlib.resources import files

# GDELT columns that hold 3-letter CAMEO actor-country codes.
CAMEO_ACTOR_COLUMNS = frozenset({"Actor1CountryCode", "Actor2CountryCode"})

# GDELT columns that hold 2-letter FIPS 10-4 geo-country codes.
FIPS_GEO_COLUMNS = frozenset({
    "ActionGeo_CountryCode", "Actor1Geo_CountryCode", "Actor2Geo_CountryCode",
})


@lru_cache(maxsize=1)
def _load() -> dict[str, dict[str, str]]:
    text = (files("gdeltforge") / "data" / "country_codes.json").read_text(encoding="utf-8")
    return json.loads(text)


def actor_country_codes() -> dict[str, str]:
    """3-letter CAMEO code -> country/region name."""
    return _load()["ACTOR_COUNTRY_CODES"]


def geo_country_codes() -> dict[str, str]:
    """2-letter FIPS 10-4 code -> country/region name."""
    return _load()["GEO_COUNTRY_CODES"]


def code_family_for_column(column: str) -> dict[str, str] | None:
    """Return the reference dict for column's code family, or None if
    column isn't a recognized country-code column."""
    if column in CAMEO_ACTOR_COLUMNS:
        return actor_country_codes()
    if column in FIPS_GEO_COLUMNS:
        return geo_country_codes()
    return None
