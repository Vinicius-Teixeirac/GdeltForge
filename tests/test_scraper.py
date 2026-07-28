import hashlib
import logging
from datetime import date

import pytest

import gdeltforge.scraping.scraper as scraper
from gdeltforge.scraping.scraper import (
    GdeltFile,
    _download_one,
    _is_gdelt_dataset_file,
    collect_gdelt_links,
    download_gdelt_files,
    filter_urls_by_date,
    parse_file_date,
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

    def test_unknown_method_falls_back_to_requests(self, monkeypatch):
        monkeypatch.setattr(scraper, "_collect_gdelt_links_requests", lambda cfg: "requests-result")
        cfg = {"scraping": {"method": "carrier-pigeon"}}
        assert collect_gdelt_links(cfg) == "requests-result"


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
