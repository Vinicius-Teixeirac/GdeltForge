from gdeltforge.sampling import cameo_codes


class TestActorCountryCodes:
    def test_has_known_entries(self):
        codes = cameo_codes.actor_country_codes()
        assert codes["USA"] == "United States"
        assert codes["RUS"] == "Russia"

    def test_is_three_letter_codes(self):
        codes = cameo_codes.actor_country_codes()
        assert all(len(c) == 3 for c in codes)


class TestGeoCountryCodes:
    def test_has_known_entries(self):
        codes = cameo_codes.geo_country_codes()
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
        codes = cameo_codes.geo_country_codes()
        assert all(len(c) == 2 for c in codes)

    def test_has_pacific_island_codes(self):
        # Added after cross-referencing ActionGeo_FullName against a real
        # archive scan: small US-administered Pacific islands missing from
        # the original bundled reference.
        codes = cameo_codes.geo_country_codes()
        assert codes["JQ"] == "Johnston Atoll"
        assert codes["MQ"] == "Midway Islands"


class TestEthnicCodes:
    def test_has_known_entries(self):
        codes = cameo_codes.ethnic_codes()
        assert codes["AAR"] == "Afar"
        assert codes["KUR"] == "Kurd"


class TestKnownGroupCodes:
    def test_has_known_entries(self):
        codes = cameo_codes.known_group_codes()
        assert codes["ADB"] == "Asian Development Bank"

    def test_has_codes_confirmed_via_real_actor_names(self):
        # Not in the public CAMEO manual; confirmed by cross-referencing
        # the actor names appearing alongside these codes in real GDELT
        # data rather than guessing from the code alone.
        codes = cameo_codes.known_group_codes()
        assert codes["PLO"] == "Palestine Liberation Organization"
        assert codes["FID"] == "International Federation for Human Rights (FIDH)"
        assert codes["NON"] == "Non-Aligned Movement (Organization of Non-Aligned Countries)"

    def test_ambiguous_code_documents_both_real_meanings(self):
        # CEM maps to two distinct organizations in real archive data
        # (CEMAC, dominant; a COMESA-related usage, rare); both are kept
        # rather than silently picking one.
        codes = cameo_codes.known_group_codes()
        assert "CEMAC" in codes["CEM"]
        assert "Common Market for Eastern and Southern Africa" in codes["CEM"]


class TestReligionCodes:
    def test_has_known_entries(self):
        codes = cameo_codes.religion_codes()
        assert codes["CHR"] == "Christianity"
        assert codes["MOS"] == "Islam"

    def test_has_codes_confirmed_via_real_actor_names(self):
        codes = cameo_codes.religion_codes()
        assert codes["JHW"] == "Jehovah's Witnesses"
        assert codes["MRN"] == "Maronite Church"


class TestTypeCodes:
    def test_has_known_entries(self):
        codes = cameo_codes.type_codes()
        assert codes["GOV"].startswith("Government")
        assert codes["MIL"].startswith("Military")
        assert codes["REB"].startswith("Rebels")


class TestEventCodes:
    def test_has_known_entries_at_each_specificity_level(self):
        # Root (2-digit), base (3-digit), and fully specified (4-digit)
        # codes all share one flat namespace.
        codes = cameo_codes.event_codes()
        assert codes["01"] == "MAKE PUBLIC STATEMENT"
        assert codes["010"] == "Make statement, not specified below"
        assert codes["190"] == "Use conventional military force, not specified below"

    def test_has_codes_confirmed_via_verb_pattern_dictionary(self):
        # Not listed in the public CAMEO manual's "121: Reject material
        # cooperation" branch (which stops at economic/military); confirmed
        # instead via the TABARI/PETRARCH verb-pattern dictionary GDELT's
        # own event coder runs on, consistent with the judicial/intelligence
        # continuation (X13/X14) used elsewhere in the scheme (e.g. 0213/0214).
        codes = cameo_codes.event_codes()
        assert codes["1213"] == "Reject judicial cooperation"
        assert codes["1214"] == "Reject intelligence cooperation"

    def test_malformed_record_markers_are_not_included(self):
        # GDELT's own sentinels for rows its event coder couldn't classify
        # at all, not real CAMEO codes; deliberately excluded so filtering
        # on one still (correctly) warns.
        codes = cameo_codes.event_codes()
        assert "X" not in codes
        assert "--" not in codes
        assert "---" not in codes


class TestCodeFamilyForColumn:
    def test_actor_country_columns_return_actor_country_family(self):
        for col in cameo_codes.CAMEO_ACTOR_COUNTRY_COLUMNS:
            assert cameo_codes.code_family_for_column(col) == cameo_codes.actor_country_codes()

    def test_geo_columns_return_fips_family(self):
        for col in cameo_codes.FIPS_GEO_COLUMNS:
            assert cameo_codes.code_family_for_column(col) == cameo_codes.geo_country_codes()

    def test_ethnic_columns_return_ethnic_family(self):
        for col in cameo_codes.CAMEO_ETHNIC_COLUMNS:
            assert cameo_codes.code_family_for_column(col) == cameo_codes.ethnic_codes()

    def test_known_group_columns_return_known_group_family(self):
        for col in cameo_codes.CAMEO_KNOWN_GROUP_COLUMNS:
            assert cameo_codes.code_family_for_column(col) == cameo_codes.known_group_codes()

    def test_religion_columns_return_religion_family(self):
        for col in cameo_codes.CAMEO_RELIGION_COLUMNS:
            assert cameo_codes.code_family_for_column(col) == cameo_codes.religion_codes()

    def test_type_columns_return_type_family(self):
        for col in cameo_codes.CAMEO_TYPE_COLUMNS:
            assert cameo_codes.code_family_for_column(col) == cameo_codes.type_codes()

    def test_event_columns_return_event_family(self):
        for col in cameo_codes.CAMEO_EVENT_COLUMNS:
            assert cameo_codes.code_family_for_column(col) == cameo_codes.event_codes()

    def test_unknown_column_returns_none(self):
        assert cameo_codes.code_family_for_column("GlobalEventID") is None
        assert cameo_codes.code_family_for_column("QuadClass") is None


class TestFamilyNameForColumn:
    def test_known_columns_get_display_names(self):
        assert cameo_codes.family_name_for_column("Actor1CountryCode") == "CAMEO actor-country"
        assert cameo_codes.family_name_for_column("ActionGeo_CountryCode") == "FIPS geo-country"
        assert cameo_codes.family_name_for_column("Actor1EthnicCode") == "CAMEO ethnic"
        assert cameo_codes.family_name_for_column("Actor1KnownGroupCode") == "CAMEO known-group"
        assert cameo_codes.family_name_for_column("Actor1Religion1Code") == "CAMEO religion"
        assert cameo_codes.family_name_for_column("Actor1Type1Code") == "CAMEO actor-type"
        assert cameo_codes.family_name_for_column("EventCode") == "CAMEO event"

    def test_unknown_column_returns_none(self):
        assert cameo_codes.family_name_for_column("GlobalEventID") is None


class TestIsRecognizedCode:
    def test_recognized_code_returns_true(self):
        assert cameo_codes.is_recognized_code("Actor1CountryCode", "USA") is True
        assert cameo_codes.is_recognized_code("ActionGeo_CountryCode", "US") is True

    def test_unrecognized_code_returns_false(self):
        assert cameo_codes.is_recognized_code("Actor1CountryCode", "ZZZ") is False

    def test_non_coded_column_returns_none(self):
        assert cameo_codes.is_recognized_code("QuadClass", "1") is None

    def test_is_case_insensitive(self):
        # Real GDELT data stores Actor1/2EthnicCode lowercase; the bundled
        # reference uses uppercase keys. A naive membership check on raw
        # case would report every real ethnic code as unrecognized.
        assert cameo_codes.is_recognized_code("Actor1EthnicCode", "aar") is True
        assert cameo_codes.is_recognized_code("Actor1EthnicCode", "AAR") is True
        assert cameo_codes.is_recognized_code("Actor1CountryCode", "usa") is True

    def test_event_code_at_any_specificity_level_is_recognized(self):
        assert cameo_codes.is_recognized_code("EventRootCode", "01") is True
        assert cameo_codes.is_recognized_code("EventBaseCode", "010") is True
        assert cameo_codes.is_recognized_code("EventCode", "1213") is True

    def test_malformed_record_marker_is_not_recognized(self):
        # A real value that occurs in real GDELT data (the event coder's
        # own "couldn't classify this row" marker), correctly still flagged
        # since it isn't a CAMEO code.
        assert cameo_codes.is_recognized_code("EventCode", "X") is False
        assert cameo_codes.is_recognized_code("EventRootCode", "--") is False
