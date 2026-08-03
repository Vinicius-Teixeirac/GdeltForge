import hashlib
import logging
from datetime import date

import pytest

import gdeltforge.scraping.scraper as scraper
from gdeltforge.scraping.scraper import (
    GdeltFile,
    _download_one,
    _is_gdelt_dataset_file,
    _warn_if_large_scrape,
    collect_gdelt_links,
    download_gdelt_files,
    filter_urls_by_date,
    parse_file_date,
    parse_gdelt_gkg_v1_file_date,
    parse_gdeltv2_file_date,
)


# ------------------------------------------------------------
# parse_file_date
# ------------------------------------------------------------
class TestParseFileDate:
    def test_daily(self):
        assert parse_file_date("20200315.export.CSV.zip") == (date(2020, 3, 15), date(2020, 3, 15))

    def test_monthly(self):
        assert parse_file_date("202003.zip") == (date(2020, 3, 1), date(2020, 3, 31))

    def test_yearly(self):
        assert parse_file_date("2020.zip") == (date(2020, 1, 1), date(2020, 12, 31))

    def test_leap_year_month_end(self):
        assert parse_file_date("202002.zip") == (date(2020, 2, 1), date(2020, 2, 29))

    def test_unrecognized_filename(self):
        assert parse_file_date("md5sums") == (None, None)

    def test_invalid_calendar_date(self):
        assert parse_file_date("20201332.export.CSV.zip") == (None, None)


# ------------------------------------------------------------
# parse_gdeltv2_file_date
# ------------------------------------------------------------
class TestParseGdeltv2FileDate:
    def test_gkg_filename(self):
        # Real GKG 2.1 filename, first batch ever published (Feb 18, 2015).
        assert parse_gdeltv2_file_date("20150218233000.gkg.csv.zip") == (
            date(2015, 2, 18), date(2015, 2, 18),
        )

    def test_mentions_filename(self):
        # Real Mentions filename, same launch window.
        assert parse_gdeltv2_file_date("20150218230000.mentions.CSV.zip") == (
            date(2015, 2, 18), date(2015, 2, 18),
        )

    def test_unrecognized_filename(self):
        assert parse_gdeltv2_file_date("masterfilelist.txt") == (None, None)

    def test_too_short_timestamp_is_rejected(self):
        # A daily Events-style 8-digit prefix must not be misread as a
        # truncated 14-digit v2 timestamp.
        assert parse_gdeltv2_file_date("20200315.export.CSV.zip") == (None, None)

    def test_invalid_calendar_date(self):
        assert parse_gdeltv2_file_date("20201332120000.gkg.csv.zip") == (None, None)

    def test_invalid_time_of_day_is_rejected(self):
        # Valid calendar date, garbage time; must not silently accept it.
        assert parse_gdeltv2_file_date("20200315256199.gkg.csv.zip") == (None, None)


# ------------------------------------------------------------
# parse_gdelt_gkg_v1_file_date
# ------------------------------------------------------------
class TestParseGdeltGkgV1FileDate:
    def test_main_gkg_filename(self):
        # April 1 2013: GKG 1.0's real start date.
        assert parse_gdelt_gkg_v1_file_date("20130401.gkg.csv.zip") == (
            date(2013, 4, 1), date(2013, 4, 1),
        )

    def test_counts_filename(self):
        assert parse_gdelt_gkg_v1_file_date("20130401.gkgcounts.csv.zip") == (
            date(2013, 4, 1), date(2013, 4, 1),
        )

    def test_unrecognized_filename(self):
        assert parse_gdelt_gkg_v1_file_date("index.html") == (None, None)

    def test_invalid_calendar_date(self):
        assert parse_gdelt_gkg_v1_file_date("20131332.gkg.csv.zip") == (None, None)

    def test_reads_only_the_leading_8_digits(self):
        # Deliberately narrow contract: extracts whatever date the first 8
        # characters encode, without validating the rest of the filename
        # (that's _is_gdelt_gkg_v1_file's job, tested separately). A
        # gdeltv2-style 14-digit timestamp still starts with a real date,
        # so it parses. Discovery never hands this parser a v2 filename
        # in practice, since collect_gdelt_links dispatches by dataset.
        assert parse_gdelt_gkg_v1_file_date("20150218233000.gkg.csv.zip") == (
            date(2015, 2, 18), date(2015, 2, 18),
        )


# ------------------------------------------------------------
# _is_gdelt_dataset_file
# ------------------------------------------------------------
class TestIsGdeltDatasetFile:
    @pytest.mark.parametrize(
        "filename,expected",
        [
            ("20200315.export.CSV.zip", True),
            ("202003.zip", True),
            ("2020.zip", True),
            ("md5sums", False),
            ("filesizes", False),
            ("GDELT.MASTERREDUCEDV2.1979-2013.zip", False),
            ("202003.csv", False),  # 6-digit prefix, right length, wrong suffix
            ("2020.csv", False),  # 4-digit prefix, right length, wrong suffix
        ],
    )
    def test_cases(self, filename, expected):
        assert _is_gdelt_dataset_file(filename) == expected


# ------------------------------------------------------------
# _is_gdelt_gkg_v1_file
# ------------------------------------------------------------
class TestIsGdeltGkgV1File:
    @pytest.mark.parametrize(
        "filename,suffix,expected",
        [
            ("20130401.gkg.csv.zip", ".gkg.csv.zip", True),
            ("20130401.gkgcounts.csv.zip", ".gkgcounts.csv.zip", True),
            # Wrong suffix for the requested dataset.
            ("20130401.gkgcounts.csv.zip", ".gkg.csv.zip", False),
            ("20130401.gkg.csv.zip", ".gkgcounts.csv.zip", False),
            # Translation mirror: ends with the suffix, but isn't a plain
            # YYYYMMDD<suffix> filename, so it must not match on endswith alone.
            ("20130401.translation.gkg.csv.zip", ".gkg.csv.zip", False),
            ("index.html", ".gkg.csv.zip", False),
            ("md5sums", ".gkg.csv.zip", False),
        ],
    )
    def test_cases(self, filename, suffix, expected):
        assert scraper._is_gdelt_gkg_v1_file(filename, suffix) == expected


# ------------------------------------------------------------
# filter_urls_by_date
# ------------------------------------------------------------
class TestFilterUrlsByDate:
    def _files(self):
        return [
            GdeltFile(url="http://x/20200101.export.CSV.zip"),
            GdeltFile(url="http://x/202006.zip"),
            GdeltFile(url="http://x/2019.zip"),
            GdeltFile(url="http://x/unparseable.txt"),
        ]

    def test_no_bounds_returns_unchanged(self):
        files = self._files()
        assert filter_urls_by_date(files, None, None) == files

    def test_range_keeps_overlapping_files_only(self):
        kept = filter_urls_by_date(self._files(), date(2020, 1, 1), date(2020, 12, 31))
        assert {f.url for f in kept} == {
            "http://x/20200101.export.CSV.zip",
            "http://x/202006.zip",
        }

    def test_open_ended_start(self):
        kept = filter_urls_by_date(self._files(), date(2020, 1, 1), None)
        assert {f.url for f in kept} == {
            "http://x/20200101.export.CSV.zip",
            "http://x/202006.zip",
        }

    def test_open_ended_end(self):
        kept = filter_urls_by_date(self._files(), None, date(2019, 12, 31))
        assert {f.url for f in kept} == {"http://x/2019.zip"}

    def test_date_parser_can_be_overridden_for_gdeltv2_filenames(self):
        files = [
            GdeltFile(url="http://x/20150218233000.gkg.csv.zip"),
            GdeltFile(url="http://x/20200101000000.gkg.csv.zip"),
        ]
        kept = filter_urls_by_date(
            files, date(2015, 1, 1), date(2015, 12, 31),
            date_parser=parse_gdeltv2_file_date,
        )
        assert {f.url for f in kept} == {"http://x/20150218233000.gkg.csv.zip"}


# ------------------------------------------------------------
# _collect_gdelt_links_requests: MD5 parsing off the directory listing
# ------------------------------------------------------------
class TestCollectLinksRequests:
    def test_parses_urls_and_md5_ignoring_non_dataset_entries(self, monkeypatch):
        html = (
            '<LI><A HREF="md5sums">md5sums</A>\n'
            '<LI><A HREF="20260722.export.CSV.zip">20260722.export.CSV.zip</A> '
            '(7.7MB) (MD5: BE29FB979F2832A9CC3126352E27E0F6)\n'
            '<LI><A HREF="202006.zip">202006.zip</A> '
            '(1.2MB) (MD5: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)\n'
            '<LI><A HREF="notes.txt">notes.txt</A>\n'
        )

        class FakeResponse:
            text = html

            def raise_for_status(self):
                pass

        monkeypatch.setattr(scraper.requests, "get", lambda *a, **kw: FakeResponse())

        files = scraper._collect_gdelt_links_requests({"scraping": {"timeout": 5}})
        by_url = {f.url: f.md5 for f in files}

        assert len(files) == 2
        assert by_url["http://data.gdeltproject.org/events/20260722.export.CSV.zip"] == (
            "be29fb979f2832a9cc3126352e27e0f6"
        )
        assert by_url["http://data.gdeltproject.org/events/202006.zip"] == "a" * 32


# ------------------------------------------------------------
# _collect_gdeltv2_links: masterfilelist.txt parsing
# ------------------------------------------------------------
class TestCollectGdeltv2Links:
    # Real lines, confirmed against the live master file list (this
    # environment can't reach data.gdeltproject.org directly to fetch it
    # itself): the very first GKG 2.1 and Mentions batches ever published,
    # Feb 18, 2015. Note the differing "csv"/"CSV" capitalization between
    # the two: that's real, not a typo, and exactly why the suffix match
    # below is case-sensitive.
    _MASTERFILELIST = (
        "11279827 66b03e2efd7d51dabf916b1666910053 "
        "http://data.gdeltproject.org/gdeltv2/20150218233000.gkg.csv.zip\n"
        "318084 bb27f78ba45f69a17ea6ed7755e9f8ff "
        "http://data.gdeltproject.org/gdeltv2/20150218230000.mentions.CSV.zip\n"
        "58762123 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa "
        "http://data.gdeltproject.org/gdeltv2/20150218233000.export.CSV.zip\n"
        "\n"
        "not a valid masterfilelist line\n"
    )

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    def test_filters_to_the_requested_dataset_only(self, monkeypatch):
        monkeypatch.setattr(
            scraper.requests, "get",
            lambda *a, **kw: self.FakeResponse(self._MASTERFILELIST),
        )

        files = scraper._collect_gdeltv2_links({}, "gdelt_gkg_v2")

        assert len(files) == 1
        assert files[0].url == (
            "http://data.gdeltproject.org/gdeltv2/20150218233000.gkg.csv.zip"
        )
        assert files[0].md5 == "66b03e2efd7d51dabf916b1666910053"
        assert files[0].size == 11279827

    def test_mentions_suffix_is_case_sensitive_and_distinct_from_gkg(self, monkeypatch):
        monkeypatch.setattr(
            scraper.requests, "get",
            lambda *a, **kw: self.FakeResponse(self._MASTERFILELIST),
        )

        files = scraper._collect_gdeltv2_links({}, "gdelt_mentions")

        assert len(files) == 1
        assert files[0].url.endswith("mentions.CSV.zip")
        assert files[0].size == 318084

    def test_malformed_lines_are_skipped_without_error(self, monkeypatch):
        monkeypatch.setattr(
            scraper.requests, "get",
            lambda *a, **kw: self.FakeResponse(self._MASTERFILELIST),
        )

        # Doesn't raise despite the blank line and the malformed line in
        # the fixture; both are silently skipped rather than crashing the
        # whole scrape over one bad line.
        files = scraper._collect_gdeltv2_links({}, "gdelt_gkg_v2")
        assert len(files) == 1


# ------------------------------------------------------------
# _collect_gdelt_gkg_v1_links: GKG 1.0 HTML index parsing
# ------------------------------------------------------------
class TestCollectGdeltGkgV1Links:
    """
    NOTE: this HTML fixture is synthetic, built to match Events' confirmed
    index markup (see TestCollectLinksRequests), not a live capture of
    data.gdeltproject.org/gkg/index.html, since this sandbox can't reach
    that domain (see _collect_gdelt_gkg_v1_links's docstring). These tests
    prove the parsing logic is correct *if* the real page shares that
    markup, which is inferred, not yet directly confirmed.
    """

    _INDEX_HTML = (
        '<LI><A HREF="20130401.gkg.csv.zip">20130401.gkg.csv.zip</A> '
        '(2.1MB) (MD5: be29fb979f2832a9cc3126352e27e0f6)\n'
        '<LI><A HREF="20130401.gkgcounts.csv.zip">20130401.gkgcounts.csv.zip</A> '
        '(0.3MB) (MD5: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)\n'
        '<LI><A HREF="20130401.translation.gkg.csv.zip">20130401.translation.gkg.csv.zip</A> '
        '(1.8MB) (MD5: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb)\n'
        '<LI><A HREF="index.html">index.html</A>\n'
    )

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    def test_filters_to_the_main_gkg_file_only(self, monkeypatch):
        monkeypatch.setattr(
            scraper.requests, "get",
            lambda *a, **kw: self.FakeResponse(self._INDEX_HTML),
        )

        files = scraper._collect_gdelt_gkg_v1_links({}, "gdelt_gkg_v1")

        assert len(files) == 1
        assert files[0].url == "http://data.gdeltproject.org/gkg/20130401.gkg.csv.zip"
        assert files[0].md5 == "be29fb979f2832a9cc3126352e27e0f6"

    def test_filters_to_the_counts_file_only(self, monkeypatch):
        monkeypatch.setattr(
            scraper.requests, "get",
            lambda *a, **kw: self.FakeResponse(self._INDEX_HTML),
        )

        files = scraper._collect_gdelt_gkg_v1_links({}, "gdelt_gkg_v1_counts")

        assert len(files) == 1
        assert files[0].url == "http://data.gdeltproject.org/gkg/20130401.gkgcounts.csv.zip"
        assert files[0].md5 == "a" * 32

    def test_translation_mirror_is_excluded_from_both(self, monkeypatch):
        monkeypatch.setattr(
            scraper.requests, "get",
            lambda *a, **kw: self.FakeResponse(self._INDEX_HTML),
        )

        main_urls = {f.url for f in scraper._collect_gdelt_gkg_v1_links({}, "gdelt_gkg_v1")}
        assert not any("translation" in url for url in main_urls)


# ------------------------------------------------------------
# _warn_if_large_scrape
# ------------------------------------------------------------
class TestWarnIfLargeScrape:
    def test_no_warning_below_threshold(self, caplog):
        files = [GdeltFile(url=f"http://x/{i}.gkg.csv.zip") for i in range(10)]
        with caplog.at_level(logging.WARNING):
            _warn_if_large_scrape(files, "gdelt_gkg_v2")
        assert caplog.text == ""

    def test_warns_above_threshold_with_size_estimate(self, caplog):
        files = [
            GdeltFile(url=f"http://x/{i}.gkg.csv.zip", size=1_000_000)
            for i in range(6_000)
        ]
        with caplog.at_level(logging.WARNING):
            _warn_if_large_scrape(files, "gdelt_gkg_v2")

        assert "6,000 files" in caplog.text
        assert "GB total" in caplog.text
        assert "every 15 minutes" in caplog.text

    def test_warns_above_threshold_even_without_known_sizes(self, caplog):
        files = [GdeltFile(url=f"http://x/{i}.gkg.csv.zip") for i in range(6_000)]
        with caplog.at_level(logging.WARNING):
            _warn_if_large_scrape(files, "gdelt_gkg_v2")

        assert "6,000 files" in caplog.text
        assert "GB total" not in caplog.text  # no size data to estimate from

    def test_daily_dataset_warning_omits_the_15_minute_cadence_claim(self, caplog):
        # GKG 1.0 is daily, the same cadence as Events; the 15-minute framing
        # is specific to GKG 2.1/Mentions and would be misleading here.
        files = [GdeltFile(url=f"http://x/{i}.gkg.csv.zip") for i in range(6_000)]
        with caplog.at_level(logging.WARNING):
            _warn_if_large_scrape(files, "gdelt_gkg_v1")

        assert "6,000 files" in caplog.text
        assert "every 15 minutes" not in caplog.text


# ------------------------------------------------------------
# collect_gdelt_links dispatcher
# ------------------------------------------------------------
class TestCollectGdeltLinksDispatch:
    def test_defaults_to_requests(self, monkeypatch):
        monkeypatch.setattr(scraper, "_collect_gdelt_links_requests", lambda cfg: "requests-result")
        monkeypatch.setattr(scraper, "_collect_gdelt_links_selenium", lambda cfg: "selenium-result")
        assert collect_gdelt_links({}) == "requests-result"

    def test_selenium_when_configured(self, monkeypatch):
        monkeypatch.setattr(scraper, "_collect_gdelt_links_requests", lambda cfg: "requests-result")
        monkeypatch.setattr(scraper, "_collect_gdelt_links_selenium", lambda cfg: "selenium-result")
        cfg = {"scraping": {"method": "selenium"}}
        assert collect_gdelt_links(cfg) == "selenium-result"

    def test_gkg_v2_dispatches_to_masterfilelist_collector(self, monkeypatch):
        monkeypatch.setattr(scraper, "_collect_gdeltv2_links", lambda cfg, ds: f"v2-result-{ds}")
        assert collect_gdelt_links({}, dataset="gdelt_gkg_v2") == "v2-result-gdelt_gkg_v2"

    def test_mentions_dispatches_to_masterfilelist_collector(self, monkeypatch):
        monkeypatch.setattr(scraper, "_collect_gdeltv2_links", lambda cfg, ds: f"v2-result-{ds}")
        assert collect_gdelt_links({}, dataset="gdelt_mentions") == "v2-result-gdelt_mentions"

    def test_gkg_v1_dispatches_to_html_index_collector(self, monkeypatch):
        monkeypatch.setattr(
            scraper, "_collect_gdelt_gkg_v1_links", lambda cfg, ds: f"v1-result-{ds}"
        )
        assert collect_gdelt_links({}, dataset="gdelt_gkg_v1") == "v1-result-gdelt_gkg_v1"

    def test_gkg_v1_counts_dispatches_to_html_index_collector(self, monkeypatch):
        monkeypatch.setattr(
            scraper, "_collect_gdelt_gkg_v1_links", lambda cfg, ds: f"v1-result-{ds}"
        )
        assert (
            collect_gdelt_links({}, dataset="gdelt_gkg_v1_counts")
            == "v1-result-gdelt_gkg_v1_counts"
        )

    def test_unimplemented_dataset_raises_clearly_instead_of_scraping_events(self):
        # Before this guard existed, an unrecognized dataset would silently
        # fall through to the Events HTML-scraping path: a correctness
        # bug, not just a missing feature. Every real dataset is now wired
        # up, so exercise the guard with a name that will never exist.
        with pytest.raises(NotImplementedError, match="gdelt_nonexistent"):
            collect_gdelt_links({}, dataset="gdelt_nonexistent")

    def test_unknown_method_falls_back_to_requests(self, monkeypatch):
        monkeypatch.setattr(scraper, "_collect_gdelt_links_requests", lambda cfg: "requests-result")
        cfg = {"scraping": {"method": "carrier-pigeon"}}
        assert collect_gdelt_links(cfg) == "requests-result"


# ------------------------------------------------------------
# _date_parser_for: picks the right filename convention per dataset
# ------------------------------------------------------------
class TestDateParserFor:
    def test_events_uses_the_daily_monthly_yearly_parser(self):
        assert scraper._date_parser_for("gdelt_event") is parse_file_date

    def test_gkg_v2_and_mentions_use_the_15_minute_batch_parser(self):
        assert scraper._date_parser_for("gdelt_gkg_v2") is parse_gdeltv2_file_date
        assert scraper._date_parser_for("gdelt_mentions") is parse_gdeltv2_file_date

    def test_gkg_v1_and_counts_use_the_daily_gkg_v1_parser(self):
        assert scraper._date_parser_for("gdelt_gkg_v1") is parse_gdelt_gkg_v1_file_date
        assert scraper._date_parser_for("gdelt_gkg_v1_counts") is parse_gdelt_gkg_v1_file_date


# ------------------------------------------------------------
# _download_one: retry + MD5 verification
# ------------------------------------------------------------
class FakeResponse:
    def __init__(self, content: bytes, ok: bool = True):
        self._content = content
        self._ok = ok

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if not self._ok:
            raise scraper.requests.HTTPError("simulated HTTP error")

    def iter_content(self, chunk_size=8192):
        for i in range(0, len(self._content), chunk_size):
            yield self._content[i : i + chunk_size]


class FakeSession:
    """Returns a fresh FakeResponse per .get() call; records URLs requested."""

    def __init__(self, content: bytes = b"payload", ok: bool = True):
        self.content = content
        self.ok = ok
        self.calls = []

    def get(self, url, stream=True, timeout=None):
        self.calls.append(url)
        return FakeResponse(self.content, self.ok)


class TestDownloadOne:
    def test_success_with_matching_md5(self, tmp_path):
        content = b"hello gdelt"
        md5 = hashlib.md5(content).hexdigest()
        file = GdeltFile(url="http://x/20200101.export.CSV.zip", md5=md5)

        status, filename = _download_one(
            file, str(tmp_path), retries=3, timeout=5,
            session=FakeSession(content),  # pyright: ignore[reportArgumentType]
        )

        assert status == "success"
        assert filename == "20200101.export.CSV.zip"
        assert (tmp_path / filename).read_bytes() == content

    def test_success_without_md5_skips_verification(self, tmp_path):
        content = b"no checksum known for this file"
        file = GdeltFile(url="http://x/20200101.export.CSV.zip", md5=None)

        status, filename = _download_one(
            file, str(tmp_path), retries=1, timeout=5,
            session=FakeSession(content),  # pyright: ignore[reportArgumentType]
        )

        assert status == "success"
        assert (tmp_path / filename).read_bytes() == content

    def test_md5_mismatch_fails_after_exhausting_retries(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scraper.time, "sleep", lambda s: None)
        content = b"hello gdelt"
        file = GdeltFile(url="http://x/20200101.export.CSV.zip", md5="0" * 32)

        status, filename = _download_one(
            file, str(tmp_path), retries=3, timeout=5,
            session=FakeSession(content),  # pyright: ignore[reportArgumentType]
        )

        assert status == "failed"
        assert not (tmp_path / filename).exists()
        assert not (tmp_path / (filename + ".tmp")).exists()

    def test_warns_and_replaces_leftover_tmp_from_interrupted_run(self, tmp_path, caplog):
        content = b"hello gdelt"
        filename = "20200101.export.CSV.zip"
        (tmp_path / (filename + ".tmp")).write_bytes(b"partial garbage from a killed run")
        file = GdeltFile(url=f"http://x/{filename}", md5=None)

        with caplog.at_level(logging.WARNING):
            status, filename = _download_one(
                file, str(tmp_path), retries=1, timeout=5,
                session=FakeSession(content),  # pyright: ignore[reportArgumentType]
            )

        assert status == "success"
        assert "leftover incomplete download" in caplog.text
        assert (tmp_path / filename).read_bytes() == content
        assert not (tmp_path / (filename + ".tmp")).exists()

    def test_skips_already_downloaded_file(self, tmp_path):
        existing = tmp_path / "20200101.export.CSV.zip"
        existing.write_bytes(b"already here")
        file = GdeltFile(url="http://x/20200101.export.CSV.zip", md5="irrelevant")
        session = FakeSession()

        status, filename = _download_one(
            file, str(tmp_path), retries=3, timeout=5,
            session=session,  # pyright: ignore[reportArgumentType]
        )

        assert status == "skipped"
        assert session.calls == []  # no network call needed

    def test_http_error_retries_then_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(scraper.time, "sleep", lambda s: None)
        file = GdeltFile(url="http://x/20200101.export.CSV.zip", md5=None)
        session = FakeSession(ok=False)

        status, filename = _download_one(
            file, str(tmp_path), retries=2, timeout=5,
            session=session,  # pyright: ignore[reportArgumentType]
        )

        assert status == "failed"
        assert len(session.calls) == 2  # one attempt per retry


# ------------------------------------------------------------
# download_gdelt_files: aggregation over the (mocked) per-file results
# ------------------------------------------------------------
class TestDownloadGdeltFiles:
    def test_aggregates_success_skipped_failed_counts(self, monkeypatch, tmp_path):
        files = [GdeltFile(url=f"http://x/{i}.export.CSV.zip") for i in range(5)]
        outcomes = {
            "http://x/0.export.CSV.zip": "success",
            "http://x/1.export.CSV.zip": "success",
            "http://x/2.export.CSV.zip": "skipped",
            "http://x/3.export.CSV.zip": "failed",
            "http://x/4.export.CSV.zip": "success",
        }

        def fake_download_one(file, download_dir, retries, timeout, session):
            return outcomes[file.url], file.url.split("/")[-1]

        monkeypatch.setattr(scraper, "_download_one", fake_download_one)

        config = {
            "paths": {"downloaded_data_directory": str(tmp_path)},
            "scraping": {"retries": 3, "timeout": 5, "max_workers": 2},
        }
        result = download_gdelt_files(files, config)

        assert result["success"] == 3
        assert result["skipped"] == 1
        assert result["failed"] == ["3.export.CSV.zip"]

    def test_non_events_dataset_reads_its_own_prefixed_download_directory(
        self, monkeypatch, tmp_path
    ):
        gkg_dir = tmp_path / "gkg_raw"
        file = GdeltFile(url="http://x/20150218233000.gkg.csv.zip")

        def fake_download_one(file, download_dir, retries, timeout, session):
            assert download_dir == str(gkg_dir)
            return "success", "20150218233000.gkg.csv.zip"

        monkeypatch.setattr(scraper, "_download_one", fake_download_one)

        config = {
            "paths": {"gkg_v2_downloaded_data_directory": str(gkg_dir)},
            "scraping": {"retries": 3, "timeout": 5, "max_workers": 2},
        }
        result = download_gdelt_files([file], config, dataset="gdelt_gkg_v2")

        assert result["success"] == 1
