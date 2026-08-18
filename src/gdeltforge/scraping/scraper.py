"""
scraper.py

Utilities for scraping GDELT data: the Events daily/monthly/yearly archive
(https://data.gdeltproject.org/events/), the GKG 2.1 / Mentions 15-minute
real-time feed (https://data.gdeltproject.org/gdeltv2/), and the legacy
GKG 1.0 daily archive (https://data.gdeltproject.org/gkg/), which predates
the 15-minute architecture and still publishes daily for compatibility.

Two discovery mechanisms, not three: Events and GKG 1.0 both scrape an
HTML directory listing (same GCS-bucket-backed static index, and both
paths hit the identical TLS cert mismatch documented below, strong
evidence they're the same underlying serving infrastructure, just with a
different base URL and filename shape); GKG 2.1/Mentions are looked up in
a single master file list covering every 15-minute batch published since
Feb 2015.

Provides:
    - collect_gdelt_links: retrieves all downloadable file links for a dataset
    - parse_file_date / parse_gdeltv2_file_date / parse_gdelt_gkg_v1_file_date:
      extract the date period a filename covers
    - date_parser_for: picks the right parser above for a given dataset
    - filter_urls_by_date: narrows a GdeltFile list to a [start_date, end_date]
      window (scrape, working from a remote listing)
    - filter_paths_by_date: same window, for local file paths (convert/filter,
      working against a directory already on disk)
    - download_gdelt_files: downloads the files returned by `collect_gdelt_links`
    - run_scraping_pipeline: high-level interface that runs the full scraping workflow
"""

import hashlib
import logging
import os
import re
import time
from calendar import monthrange
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TypedDict, TypeVar
from urllib.parse import urljoin

import requests
import requests.adapters
import urllib3
from tqdm import tqdm

from gdeltforge.utils.config import dataset_path_key
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

GDELT_EVENTS_URL = "https://data.gdeltproject.org/events/"

GDELT_V2_BASE_URL = "http://data.gdeltproject.org/gdeltv2/"
GDELT_V2_MASTERFILELIST_URL = GDELT_V2_BASE_URL + "masterfilelist.txt"

# Filename suffix identifying each dataset within the shared 15-minute
# batch structure. Mentions follows Events' "CSV" capitalization; GKG uses
# its own lowercase "csv", confirmed against real masterfilelist.txt
# lines (GDELT's capitalization isn't consistent across families):
#   ...gdeltv2/20150218233000.gkg.csv.zip
#   ...gdeltv2/20150218230000.mentions.CSV.zip
_GDELT_V2_SUFFIXES = {
    "gdelt_gkg_v2": ".gkg.csv.zip",
    "gdelt_mentions": ".mentions.CSV.zip",
}

GDELT_GKG_V1_BASE_URL = "http://data.gdeltproject.org/gkg/"

# GKG 1.0 predates GKG 2.1/Mentions' 15-minute masterfilelist.txt entirely
# and is instead an HTML directory listing like Events, one daily file per
# dataset. Suffixes confirmed against linwoodc3/gdeltPyR's _urlBuilder
# (a real, working GDELT client) and cross-checked against GDELT's own
# documentation: base + "gkg/" + YYYYMMDD + suffix.
_GDELT_GKG_V1_SUFFIXES = {
    "gdelt_gkg_v1": ".gkg.csv.zip",
    "gdelt_gkg_v1_counts": ".gkgcounts.csv.zip",
}

_LARGE_SCRAPE_WARNING_THRESHOLD = 5_000

# data.gdeltproject.org serves this page from a GCS bucket whose TLS cert
# only covers *.googleapis.com, so the hostname never matches. All
# collection methods below deliberately skip certificate verification for
# this one known site; downloads themselves happen over plain http.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_MD5_RE = re.compile(r'MD5:\s*([0-9a-fA-F]{32})')


@dataclass(frozen=True)
class GdeltFile:
    """A downloadable GDELT file, with its expected MD5 and size when known."""
    url: str
    md5: str | None = None
    size: int | None = None


class DownloadResult(TypedDict):
    success: int
    skipped: int
    failed: list[str]


# ------------------------------------------------------------
# STEP 1: Collect GDELT links
# ------------------------------------------------------------
def _is_gdelt_dataset_file(filename: str) -> bool:
    if not filename.endswith(".zip"):
        return False
    is_daily = filename.endswith(".export.CSV.zip")
    is_monthly = len(filename) == 10 and filename[:-4].isdigit()
    is_yearly = len(filename) == 8 and filename[:-4].isdigit()
    return is_daily or is_monthly or is_yearly


def _scrape_html_index(
    index_url: str, config: dict, file_predicate: Callable[[str], bool]
) -> list[GdeltFile]:
    """
    Fetches a GDELT HTML directory listing with a plain HTTP GET and parses
    the anchor hrefs (and, when present, the "(MD5: ...)" hash that follows
    each entry) out of the page.

    Shared by every GDELT dataset served this way (Events, GKG 1.0): a
    static, server-rendered Apache-style index, not JS-rendered, so no
    browser is required. `file_predicate` decides which filenames in the
    listing belong to the caller's dataset.
    """
    timeout = config.get("scraping", {}).get("timeout", 30)

    response = requests.get(index_url, timeout=timeout, verify=False)
    response.raise_for_status()

    lines = response.text.splitlines()
    href_matches = [
        (line, m.group(1))
        for line in lines
        for m in [re.search(r'href="([^"]+)"', line, flags=re.IGNORECASE)]
        if m
    ]
    logger.info(f"Found {len(href_matches)} total links.")

    files = []
    for line, href in href_matches:
        filename = href.split("/")[-1]
        if not file_predicate(filename):
            continue

        full_url = urljoin(index_url, href).replace("https://", "http://", 1)
        md5_match = _MD5_RE.search(line)
        files.append(GdeltFile(url=full_url, md5=md5_match.group(1).lower() if md5_match else None))

    return files


def _collect_gdelt_links_requests(config: dict) -> list[GdeltFile]:
    """
    Fetches the GDELT events directory listing. This is the default and
    recommended method: no Chrome/ChromeDriver dependency, no version
    mismatches, and roughly an order of magnitude faster than Selenium.
    """
    logger.info("Collecting GDELT links using requests...")
    files = _scrape_html_index(GDELT_EVENTS_URL, config, _is_gdelt_dataset_file)
    logger.info(f"Identified {len(files)} GDELT dataset URLs.")
    return files


def _is_gdelt_gkg_v1_file(filename: str, suffix: str) -> bool:
    """
    True for exactly YYYYMMDD<suffix>: an 8-digit date immediately followed
    by the dataset's suffix. Excludes GDELT's "translation" mirror files
    (e.g. 20130401.translation.gkg.csv.zip), which end with the same suffix
    but aren't in scope here: a plain .endswith(suffix) would wrongly
    match them.
    """
    return filename[:8].isdigit() and filename[8:] == suffix


def _collect_gdelt_gkg_v1_links(config: dict, dataset: str) -> list[GdeltFile]:
    """
    Fetches the GKG 1.0 HTML directory listing at data.gdeltproject.org/gkg/
    and filters it down to the URLs for one dataset: the main GKG 1.0 file
    or its separate, narrower Counts file (see module docstring).

    NOTE: the markup match with Events' index page (same href/MD5 parsing
    via _scrape_html_index) is inferred from both paths sharing the same
    GCS-bucket TLS cert mismatch (see the module-level urllib3 comment
    above). It's not yet directly confirmed against a live fetch of this
    specific page, since this sandbox can't reach the domain at all.
    Confirm against a real response before relying on this for a large
    production scrape.
    """
    suffix = _GDELT_GKG_V1_SUFFIXES[dataset]
    logger.info(f"Collecting GKG 1.0 links for {dataset}...")

    files = _scrape_html_index(
        GDELT_GKG_V1_BASE_URL, config, lambda filename: _is_gdelt_gkg_v1_file(filename, suffix)
    )

    logger.info(f"Identified {len(files)} {dataset} URLs.")
    return files


def _collect_gdeltv2_links(config: dict, dataset: str) -> list[GdeltFile]:
    """
    Fetches GDELT's v2 master file list (every export/mentions/gkg file
    published since Feb 2015, one per 15-minute batch) and filters it down
    to the URLs for one dataset.

    This is a fundamentally different discovery mechanism from
    _collect_gdelt_links_requests's HTML-directory-listing scrape of the
    Events archive: one flat "size md5 url" text file that already covers
    every dataset in this family, rather than a page to parse per dataset.
    """
    suffix = _GDELT_V2_SUFFIXES[dataset]
    timeout = config.get("scraping", {}).get("timeout", 30)

    logger.info(f"Fetching GDELT v2 master file list for {dataset}...")
    response = requests.get(GDELT_V2_MASTERFILELIST_URL, timeout=timeout, verify=False)
    response.raise_for_status()

    files = []
    for line in response.text.splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue

        size_str, md5, url = parts
        if not url.endswith(suffix):
            continue

        try:
            size = int(size_str)
        except ValueError:
            size = None

        files.append(GdeltFile(url=url, md5=md5.lower(), size=size))

    logger.info(f"Identified {len(files)} {dataset} URLs.")
    return files


def _warn_if_large_scrape(files: list[GdeltFile], dataset: str) -> None:
    """
    GKG 2.1 and Mentions publish every 15 minutes (~96 files/day) rather
    than Events'/GKG 1.0's 1/day, so a multi-year range can silently imply
    hundreds of thousands of small downloads. Warn, don't block: the
    download's own progress bar makes it easy to Ctrl+C once the scale is
    visible anyway.
    """
    if len(files) <= _LARGE_SCRAPE_WARNING_THRESHOLD:
        return

    total_bytes = sum(f.size for f in files if f.size is not None)
    size_note = f" (~{total_bytes / 1e9:.1f} GB total)" if total_bytes else ""
    cadence_note = (
        f"{dataset} publishes every 15 minutes, not daily like Events; "
        if dataset in _GDELT_V2_SUFFIXES else ""
    )
    logger.warning(
        f"About to download {len(files):,} files{size_note}. {cadence_note}"
        f"Consider narrowing --start-date/--end-date, or raising "
        f"scraping.max_workers in settings.yaml for many-small-files throughput."
    )


def _build_driver(config: dict):
    """
    Build a Chrome WebDriver instance.

    Resolution order:
      1. scraping.chromedriver_path in settings.yaml (manual override, no network)
      2. webdriver-manager auto-download (requires internet on first run)

    If the network is blocked (e.g. corporate/university firewall), download
    ChromeDriver manually from https://googlechromelabs.github.io/chrome-for-testing/
    and set scraping.chromedriver_path in config/settings.yaml.
    """
    try:
        # selenium/webdriver-manager are an optional extra (uv pip install
        # '.[selenium]'), not installed by default -- pyright checks against
        # an env without them, hence the ignores below.
        from selenium import webdriver  # pyright: ignore[reportMissingImports]
        from selenium.webdriver.chrome.service import (  # pyright: ignore[reportMissingImports]
            Service,
        )
        from webdriver_manager.chrome import (  # pyright: ignore[reportMissingImports]
            ChromeDriverManager,
        )
    except ImportError as e:
        raise RuntimeError(
            "scraping.method is set to 'selenium' but the selenium/webdriver-manager "
            "packages are not installed. Install them with:\n"
            "    uv pip install '.[selenium]'\n"
            "or switch scraping.method to 'requests' in config/settings.yaml "
            "(the default, and faster in almost all cases)."
        ) from e

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--ignore-certificate-errors")

    manual_path = config.get("scraping", {}).get("chromedriver_path")

    if manual_path:
        logger.info(f"Using ChromeDriver from config: {manual_path}")
        return webdriver.Chrome(service=Service(manual_path), options=chrome_options)

    logger.info("No chromedriver_path set, attempting auto-download via webdriver-manager...")
    try:
        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options,
        )
    except Exception as e:
        raise RuntimeError(
            f"Could not initialize Chrome WebDriver: {e}\n\n"
            "The automatic ChromeDriver download failed (network may be blocked).\n"
            "To fix:\n"
            "  1. Download ChromeDriver matching your Chrome version from:\n"
            "     https://googlechromelabs.github.io/chrome-for-testing/\n"
            "  2. Set scraping.chromedriver_path in config/settings.yaml:\n"
            "       chromedriver_path: C:/path/to/chromedriver.exe\n"
            "  3. Make sure Google Chrome is installed: https://www.google.com/chrome/"
        ) from e


def _collect_gdelt_links_selenium(config: dict) -> list[GdeltFile]:
    """
    Uses Selenium to extract all GDELT .zip file URLs from the events page.

    Kept as an opt-in fallback (scraping.method: selenium) in case the page
    ever becomes JS-rendered, or as a manual comparison against the
    requests-based method. Requires Chrome + a matching ChromeDriver.
    """
    # _build_driver() raises a helpful RuntimeError if selenium isn't
    # installed; call it first so that error (not a raw ModuleNotFoundError)
    # is what surfaces.
    driver = _build_driver(config)

    from selenium.webdriver.common.by import By  # pyright: ignore[reportMissingImports]
    from selenium.webdriver.support import (  # pyright: ignore[reportMissingImports]
        expected_conditions as EC,
    )
    from selenium.webdriver.support.ui import WebDriverWait  # pyright: ignore[reportMissingImports]

    logger.info("Collecting GDELT links using Selenium...")

    files = []

    try:
        driver.get(GDELT_EVENTS_URL)
        time.sleep(2)

        # Try to bypass certificate warning if present
        try:
            proceed_btn = driver.find_element(
                By.XPATH, "//a[contains(text(), 'Proceed') or contains(text(), 'Advanced')]"
            )
            proceed_btn.click()
            time.sleep(2)
            logger.info("Security warning bypassed.")
        except Exception:
            pass  # no warning -- good

        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
        )
        anchors = driver.find_elements(By.TAG_NAME, "a")
        logger.info(f"Found {len(anchors)} total links.")

        for tag in anchors:
            href = tag.get_attribute("href")
            if not href:
                continue

            filename = href.split("/")[-1]
            if not _is_gdelt_dataset_file(filename):
                continue

            md5 = None
            try:
                # The MD5 hash sits as plain text next to the <a> tag inside
                # the same <li>; read the parent element's text to find it.
                li_text = tag.find_element(By.XPATH, "..").text
                md5_match = _MD5_RE.search(li_text)
                if md5_match:
                    md5 = md5_match.group(1).lower()
            except Exception:
                pass

            files.append(GdeltFile(url=href.replace("https://", "http://", 1), md5=md5))

        logger.info(f"Identified {len(files)} GDELT dataset URLs.")

    finally:
        driver.quit()

    return files


def collect_gdelt_links(config: dict, dataset: str = "gdelt_event") -> list[GdeltFile]:
    """
    Retrieves all downloadable GDELT file URLs for `dataset`.

    Events dispatches to the method configured in scraping.method
    ("requests", default, or "selenium"). GKG 2.1 and Mentions use the v2
    master file list instead (see _collect_gdeltv2_links), and GKG 1.0 uses
    its own HTML index (see _collect_gdelt_gkg_v1_links): explicit
    per-dataset dispatch, not a fallback, so an unimplemented dataset fails
    clearly instead of silently scraping Events data.
    """
    if dataset == "gdelt_event":
        method = config.get("scraping", {}).get("method", "requests").lower()

        if method == "selenium":
            return _collect_gdelt_links_selenium(config)

        if method != "requests":
            logger.warning(f"Unknown scraping.method '{method}', falling back to 'requests'.")

        return _collect_gdelt_links_requests(config)

    if dataset in _GDELT_V2_SUFFIXES:
        return _collect_gdeltv2_links(config, dataset)

    if dataset in _GDELT_GKG_V1_SUFFIXES:
        return _collect_gdelt_gkg_v1_links(config, dataset)

    raise NotImplementedError(f"Scraping for dataset {dataset!r} isn't implemented yet.")


# ------------------------------------------------------------
# DATE-RANGE FILTERING
# ------------------------------------------------------------
def parse_file_date(filename: str) -> tuple[date | None, date | None]:
    """
    Return (period_start, period_end) for the time window a GDELT file covers.

    File naming conventions:
      - Daily   YYYYMMDD.export.CSV.zip  -> single day
      - Monthly YYYYMM.zip               -> full calendar month
      - Yearly  YYYY.zip                 -> full calendar year

    Also matches the equivalent already-converted Parquet names
    (YYYYMMDD.export.parquet, YYYYMM.parquet, YYYY.parquet): convert only
    ever sees the raw .zip form, but filter operates on converted output,
    which keeps the source file's stem and swaps the extension.

    Returns (None, None) when the date cannot be determined.
    """
    # Daily: 20150218.export.CSV.zip (raw) or 20150218.export.parquet (converted)
    if filename.endswith(".export.CSV.zip") or filename.endswith(".export.parquet"):
        raw = filename[:8]
        if raw.isdigit() and len(raw) == 8:
            try:
                d = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
                return d, d
            except ValueError:
                pass
        return None, None

    # Monthly: YYYYMM.zip (10 chars) or YYYYMM.parquet (14 chars)
    if filename[:6].isdigit() and (
        (len(filename) == 10 and filename.endswith(".zip"))
        or (len(filename) == 14 and filename.endswith(".parquet"))
    ):
        try:
            year, month = int(filename[:4]), int(filename[4:6])
            start = date(year, month, 1)
            end = date(year, month, monthrange(year, month)[1])
            return start, end
        except ValueError:
            return None, None

    # Yearly: YYYY.zip (8 chars) or YYYY.parquet (12 chars)
    if filename[:4].isdigit() and (
        (len(filename) == 8 and filename.endswith(".zip"))
        or (len(filename) == 12 and filename.endswith(".parquet"))
    ):
        try:
            year = int(filename[:4])
            return date(year, 1, 1), date(year, 12, 31)
        except ValueError:
            return None, None

    return None, None


def parse_gdeltv2_file_date(filename: str) -> tuple[date | None, date | None]:
    """
    Return (period_start, period_end) for a GKG 2.1 / Mentions filename,
    e.g. 20150218233000.gkg.csv.zip or 20150218230000.mentions.CSV.zip --
    a 14-digit YYYYMMDDHHMMSS timestamp identifying one 15-minute batch.

    --start-date/--end-date stay whole-day, matching the Events CLI's
    existing granularity, so both start and end truncate to that batch's
    calendar date; the time portion is only used to validate the filename
    isn't garbage, not returned.

    Returns (None, None) when the date cannot be determined.
    """
    raw = filename[:14]
    if not (raw.isdigit() and len(raw) == 14):
        return None, None

    try:
        d = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        hour, minute, second = int(raw[8:10]), int(raw[10:12]), int(raw[12:14])
        if not (0 <= hour < 24 and 0 <= minute < 60 and 0 <= second < 60):
            return None, None
        return d, d
    except ValueError:
        return None, None


def parse_gdelt_gkg_v1_file_date(filename: str) -> tuple[date | None, date | None]:
    """
    Return (period_start, period_end) for a GKG 1.0 filename, e.g.
    20130401.gkg.csv.zip or 20130401.gkgcounts.csv.zip: an 8-digit
    YYYYMMDD date, one calendar day per file (Events' own daily cadence,
    unlike GKG 2.1/Mentions' 15-minute batches).

    Returns (None, None) when the date cannot be determined.
    """
    raw = filename[:8]
    if not (raw.isdigit() and len(raw) == 8):
        return None, None

    try:
        d = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        return d, d
    except ValueError:
        return None, None


def filter_urls_by_date(
    files: list[GdeltFile],
    start_date: date | None,
    end_date: date | None,
    date_parser: Callable[[str], tuple[date | None, date | None]] = parse_file_date,
) -> list[GdeltFile]:
    """
    Keep only files whose period overlaps [start_date, end_date].

    Either bound may be None (open-ended). If both are None the full list
    is returned unchanged so existing behaviour is preserved. date_parser
    lets callers plug in a different filename convention (see
    parse_gdeltv2_file_date) instead of Events' daily/monthly/yearly one.
    """
    if start_date is None and end_date is None:
        return files

    kept = []
    skipped = 0

    for file in files:
        filename = file.url.split("/")[-1]
        file_start, file_end = date_parser(filename)

        if file_start is None or file_end is None:
            logger.debug(f"Could not parse date from {filename}, skipping.")
            skipped += 1
            continue

        # Overlap: file period must intersect [start_date, end_date]
        if start_date and file_end < start_date:
            continue
        if end_date and file_start > end_date:
            continue

        kept.append(file)

    logger.info(
        f"Date filter [{start_date} - {end_date}]: "
        f"{len(kept)} URLs kept, {len(files) - len(kept) - skipped} excluded, "
        f"{skipped} skipped (unparseable filename)."
    )

    return kept


_PathT = TypeVar("_PathT", str, Path)


def filter_paths_by_date(
    paths: list[_PathT],
    start_date: date | None,
    end_date: date | None,
    date_parser: Callable[[str], tuple[date | None, date | None]],
) -> list[_PathT]:
    """
    Keep local file paths (convert/filter, working against a directory the
    user already fully controls) whose filename's period overlaps
    [start_date, end_date]. Same overlap semantics as filter_urls_by_date,
    for plain path strings instead of GdeltFile objects from a remote
    listing.

    Unlike filter_urls_by_date, a path whose date can't be parsed is KEPT
    rather than excluded: filter_urls_by_date skips what it can't parse
    because a remote listing entry like that is usually garbage (an md5
    checksum listing, an index page); a local directory can also hold
    pre-daily historical archives (1979.parquet, 200601.parquet) that
    carry no single day to filter by, and silently dropping those just
    because --start-date/--end-date was set would be a surprising side
    effect, not the intended scope of this flag.

    Either bound may be None (open-ended). If both are None the full list
    is returned unchanged so existing behaviour is preserved.
    """
    if start_date is None and end_date is None:
        return paths

    kept = []
    excluded = 0
    unparseable = 0

    for path in paths:
        filename = Path(path).name
        file_start, file_end = date_parser(filename)

        if file_start is None or file_end is None:
            kept.append(path)
            unparseable += 1
            continue

        if start_date and file_end < start_date:
            excluded += 1
            continue
        if end_date and file_start > end_date:
            excluded += 1
            continue

        kept.append(path)

    logger.info(
        f"Date filter [{start_date} - {end_date}]: {len(kept)} file(s) kept "
        f"({unparseable} without a parseable date, kept regardless), "
        f"{excluded} excluded."
    )

    return kept


# ------------------------------------------------------------
# STEP 2: Download files concurrently, verifying MD5 when known
# ------------------------------------------------------------
def _download_one(
    file: GdeltFile,
    download_dir: str,
    retries: int,
    timeout: int,
    session: requests.Session,
    force: bool = False,
) -> tuple[str, str]:
    """
    Download a single file with retry and (when file.md5 is known) checksum
    verification. Returns (status, filename) with status one of
    "success" / "skipped" / "failed".

    force skips the existing-file check below, re-downloading and
    overwriting local_path even when it's already present.
    """
    filename = file.url.split("/")[-1]
    local_path = os.path.join(download_dir, filename)
    tmp_path = local_path + ".tmp"

    if not force and os.path.exists(local_path):
        return "skipped", filename

    if os.path.exists(tmp_path):
        logger.warning(
            f"Found a leftover incomplete download from a previous "
            f"interrupted run: {tmp_path}. Re-downloading."
        )

    for attempt in range(retries):
        try:
            logger.debug(f"Downloading {filename} (attempt {attempt + 1}/{retries})")
            md5_hash = hashlib.md5()

            with session.get(file.url, stream=True, timeout=timeout) as response:
                response.raise_for_status()

                with open(tmp_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            md5_hash.update(chunk)

            if file.md5 and md5_hash.hexdigest() != file.md5:
                raise ValueError(
                    f"MD5 mismatch: expected {file.md5}, got {md5_hash.hexdigest()}"
                )

            os.replace(tmp_path, local_path)
            return "success", filename

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {filename}: {e}")
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError as cleanup_error:
                # Seen in practice on Windows: a brief file lock (real-time
                # antivirus scanning a just-written .tmp file) can make
                # os.replace above raise, then this cleanup raise too. If
                # this propagated, it would abort the retry loop entirely
                # instead of just failing this one attempt, and on a large
                # scrape it takes the whole batch down with it (this is
                # exactly what happened downloading a real ~4,850-file GKG
                # 1.0 history: one transient lock crashed the entire run).
                logger.warning(f"Could not remove leftover {tmp_path}: {cleanup_error}")
            if attempt < retries - 1:
                time.sleep(1)

    logger.error(f"Failed to download after {retries} attempts: {filename}")
    return "failed", filename


def download_gdelt_files(
    files: list[GdeltFile], config: dict, dataset: str = "gdelt_event", force: bool = False
) -> DownloadResult:
    """
    Downloads all `files` concurrently using a bounded thread pool, verifying
    each download's MD5 against the hash GDELT published for it (when known).

    force is forwarded to _download_one, re-downloading files that already
    exist locally instead of skipping them.
    """
    download_dir = config["paths"][dataset_path_key(dataset, "downloaded_data_directory")]
    retries = config["scraping"]["retries"]
    timeout = config["scraping"]["timeout"]
    max_workers = config.get("scraping", {}).get("max_workers", 8)

    os.makedirs(download_dir, exist_ok=True)

    success = 0
    skipped = 0
    failed: list[str] = []

    logger.info(
        f"Starting download of {len(files)} file(s) into {download_dir} "
        f"({max_workers} concurrent workers)..."
    )

    with requests.Session() as session:
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=max_workers, pool_maxsize=max_workers
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _download_one, file, download_dir, retries, timeout, session, force
                ): file
                for file in files
            }
            for future in tqdm(
                as_completed(futures), total=len(futures),
                desc="Downloading GDELT files", unit="file",
            ):
                try:
                    status, filename = future.result()
                except Exception as e:
                    # Defense in depth against the same class of issue
                    # _download_one's own retry loop now guards against:
                    # one file's unexpected failure must not take the
                    # whole batch down, matching process_all_files'
                    # equivalent per-future try/except in converter.py.
                    filename = futures[future].url.split("/")[-1]
                    logger.error(f"Unexpected error downloading {filename}: {e}")
                    failed.append(filename)
                    continue

                if status == "success":
                    success += 1
                elif status == "skipped":
                    skipped += 1
                else:
                    failed.append(filename)

    logger.info(f"Download summary: {success} success, {skipped} skipped, {len(failed)} failed.")

    return {
        "success": success,
        "skipped": skipped,
        "failed": failed,
    }


def _dry_run_preview(files: list[GdeltFile], download_dir: str, force: bool) -> DownloadResult:
    """
    Mirrors _download_one's own existing-file check without making any
    network request, so --dry-run's counts describe exactly what a real
    run would do. "success" here means "would be downloaded", not "was
    downloaded"; no file is written or verified.
    """
    would_download: list[str] = []
    would_skip: list[str] = []
    for file in files:
        filename = file.url.split("/")[-1]
        local_path = os.path.join(download_dir, filename)
        if not force and os.path.exists(local_path):
            would_skip.append(filename)
        else:
            would_download.append(filename)

    logger.info(
        f"[dry run] Would download {len(would_download)} file(s), "
        f"skip {len(would_skip)} already-downloaded file(s)."
    )
    for filename in would_download:
        logger.debug(f"[dry run] Would download: {filename}")

    return {"success": len(would_download), "skipped": len(would_skip), "failed": []}


# ------------------------------------------------------------
# PIPELINE INTERFACE
# ------------------------------------------------------------
def date_parser_for(dataset: str) -> Callable[[str], tuple[date | None, date | None]]:
    if dataset in _GDELT_V2_SUFFIXES:
        return parse_gdeltv2_file_date
    if dataset in _GDELT_GKG_V1_SUFFIXES:
        return parse_gdelt_gkg_v1_file_date
    return parse_file_date


def run_scraping_pipeline(
    config: dict,
    start_date: date | None = None,
    end_date: date | None = None,
    dataset: str = "gdelt_event",
    verbose: bool = False,
    quiet: bool = False,
    force: bool = False,
    dry_run: bool = False,
) -> DownloadResult:
    """
    Complete scraping step: collect URLs -> (optionally) filter by date ->
    warn if huge -> download.

    verbose raises this module's own logger to DEBUG, revealing the
    per-attempt "Downloading {filename} (attempt N/M)" line that's
    DEBUG-level (invisible) by default. Nothing new to add here for it:
    convert/filter's own --verbose (see their run_ wrappers) exists to
    match this module's already-quiet-by-default shape, not the other
    way around. quiet is the inverse: raises the logger to WARNING,
    suppressing even the default setup/summary INFO lines. Mutually
    exclusive at the CLI; verbose wins if a caller passes both directly.
    Unlike convert/filter, a single setLevel call here is enough:
    download_gdelt_files uses a ThreadPoolExecutor, not a process pool,
    so every worker thread shares this same logger instance.

    force re-downloads files already present locally instead of skipping
    them. dry_run reports what would be downloaded/skipped without
    making any network request; it is checked after force so the preview
    reflects force's effect on which files would be skipped.
    """
    if verbose:
        logger.setLevel(logging.DEBUG)
    elif quiet:
        logger.setLevel(logging.WARNING)

    files = collect_gdelt_links(config, dataset=dataset)

    date_parser = date_parser_for(dataset)
    files = filter_urls_by_date(files, start_date, end_date, date_parser=date_parser)

    _warn_if_large_scrape(files, dataset)

    if dry_run:
        download_dir = config["paths"][dataset_path_key(dataset, "downloaded_data_directory")]
        return _dry_run_preview(files, download_dir, force)

    result = download_gdelt_files(files, config, dataset=dataset, force=force)
    return result


# ------------------------------------------------------------
# STANDALONE SCRIPT
# ------------------------------------------------------------
if __name__ == "__main__":
    from gdeltforge.utils.config import load_config

    logger.info("Running scraping pipeline as standalone script...")
    cfg = load_config()
    result = run_scraping_pipeline(cfg)
    logger.info(result)
