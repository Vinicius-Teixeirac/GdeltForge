from gdeltforge.sampling import country_codes


class TestActorCountryCodes:
    def test_has_known_entries(self):
        codes = country_codes.actor_country_codes()
        assert codes["USA"] == "United States"
        assert codes["RUS"] == "Russia"

    def test_is_three_letter_codes(self):
        codes = country_codes.actor_country_codes()
        assert all(len(c) == 3 for c in codes)


class TestGeoCountryCodes:
    def test_has_known_entries(self):
        codes = country_codes.geo_country_codes()
        assert codes["US"] == "United States"
        # FIPS 10-4 uses idiosyncratic codes that differ from ISO 3166-1
        # alpha-2 (UK not GB, RS not RU, KS/KN not KR/KP): these three
        # indicate the bundled list is really FIPS, not some other scheme.
        assert codes["UK"] == "United Kingdom"
        assert codes["RS"] == "Russia"
        assert codes["KS"] == "Korea, South"
        assert codes["KN"] == "Korea, North"
        assert "USA" not in codes

    def test_is_two_letter_codes(self):
        codes = country_codes.geo_country_codes()
        assert all(len(c) == 2 for c in codes)


class TestCodeFamilyForColumn:
    def test_actor_columns_return_cameo_family(self):
        for col in ("Actor1CountryCode", "Actor2CountryCode"):
            assert country_codes.code_family_for_column(col) == country_codes.actor_country_codes()

    def test_geo_columns_return_fips_family(self):
        for col in ("ActionGeo_CountryCode", "Actor1Geo_CountryCode", "Actor2Geo_CountryCode"):
            assert country_codes.code_family_for_column(col) == country_codes.geo_country_codes()

    def test_unknown_column_returns_none(self):
        assert country_codes.code_family_for_column("GlobalEventID") is None
        assert country_codes.code_family_for_column("QuadClass") is None
