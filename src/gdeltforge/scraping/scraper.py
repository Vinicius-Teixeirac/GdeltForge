"""
scraper.py

Utilities for scraping GDELT event data (https://data.gdeltproject.org/events/).

Provides:
    - collect_gdelt_links: retrieves all downloadable file links from the GDELT events directory
    - parse_file_date: extracts the date period covered by a GDELT filename
    - filter_urls_by_date: narrows a URL list to a [start_date, end_date] window
    - download_gdelt_files: downloads the files returned by `collect_gdelt_links`
    - run_scraping_pipeline: high-level interface that runs the full scraping workflow
"""

import hashlib
import os
import re
import requests
import requests.adapters
import time
import urllib3
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin

from tqdm import tqdm

from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

GDELT_EVENTS_URL = "https://data.gdeltproject.org/events/"

# data.gdeltproject.org serves this page from a GCS bucket whose TLS cert
# only covers *.googleapis.com, so the hostname never matches. Both
# collection methods below deliberately skip certificate verification for
# this one known site; downloads themselves happen over plain http.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_MD5_RE = re.compile(r'MD5:\s*([0-9a-fA-F]{32})')


@dataclass(frozen=True)
class GdeltFile:
    """A downloadable GDELT file, with its expected MD5 when the index page provides one."""
    url: str
    md5: Optional[str] = None


# ------------------------------------------------------------
# STEP 1: Collect GDELT links
# ------------------------------------------------------------
def _is_gdelt_dataset_file(filename: str) -> bool:
    is_daily = filename.endswith(".export.CSV.zip")
    is_monthly = filename[:6].isdigit() and len(filename) == 10
    is_yearly = filename[:4].isdigit() and len(filename) == 8
    return is_daily or is_monthly or is_yearly


def _collect_gdelt_links_requests(config: dict) -> List[GdeltFile]:
    """
    Fetches the GDELT events directory listing with a plain HTTP GET and
    parses the anchor hrefs (and, when present, the "(MD5: ...)" hash that
    follows each entry) out of the HTML.

    The page is a static, server-rendered Apache-style index (not
    JS-rendered), so no browser is required. This is the default and
    recommended method: no Chrome/ChromeDriver dependency, no version
    mismatches, and roughly an order of magnitude faster than Selenium.
    """
    logger.info("Collecting GDELT links using requests...")
    timeout = config.get("scraping", {}).get("timeout", 30)

    response = requests.get(GDELT_EVENTS_URL, timeout=timeout, verify=False)
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
        if not _is_gdelt_dataset_file(filename):
            continue

        full_url = urljoin(GDELT_EVENTS_URL, href).replace("https://", "http://", 1)
        md5_match = _MD5_RE.search(line)
        files.append(GdeltFile(url=full_url, md5=md5_match.group(1).lower() if md5_match else None))

    logger.info(f"Identified {len(files)} GDELT dataset URLs.")

    return files


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
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
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


def _collect_gdelt_links_selenium(config: dict) -> List[GdeltFile]:
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

    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    logger.info("Collecting GDELT links using Selenium...")

    files = []

    try:
        driver.get(GDELT_EVENTS_URL)
        time.sleep(2)

        # Try to bypass certificate warning if present
        try:
            proceed_btn = driver.find_element(By.XPATH, "//a[contains(text(), 'Proceed') or contains(text(), 'Advanced')]")
            proceed_btn.click()
            time.sleep(2)
            logger.info("Security warning bypassed.")
        except Exception:
            pass  # no warning, good

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


def collect_gdelt_links(config: dict) -> List[GdeltFile]:
    """
    Retrieves all downloadable GDELT file URLs, dispatching to the method
    configured in scraping.method ("requests", default, or "selenium").
    """
    method = config.get("scraping", {}).get("method", "requests").lower()

    if method == "selenium":
        return _collect_gdelt_links_selenium(config)

    if method != "requests":
        logger.warning(f"Unknown scraping.method '{method}', falling back to 'requests'.")

    return _collect_gdelt_links_requests(config)


# ------------------------------------------------------------
# DATE-RANGE FILTERING
# ------------------------------------------------------------
def parse_file_date(filename: str) -> Tuple[Optional[date], Optional[date]]:
    """
    Return (period_start, period_end) for the time window a GDELT file covers.

    File naming conventions:
      - Daily   YYYYMMDD.export.CSV.zip  -> single day
      - Monthly YYYYMM.zip               -> full calendar month
      - Yearly  YYYY.zip                 -> full calendar year

    Returns (None, None) when the date cannot be determined.
    """
    # Daily: 20150218.export.CSV.zip
    if filename.endswith(".export.CSV.zip"):
        raw = filename[:8]
        if raw.isdigit() and len(raw) == 8:
            try:
                d = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
                return d, d
            except ValueError:
                pass
        return None, None

    # Monthly: YYYYMM.zip (10 chars)
    if filename[:6].isdigit() and len(filename) == 10 and filename.endswith(".zip"):
        try:
            year, month = int(filename[:4]), int(filename[4:6])
            start = date(year, month, 1)
            end = date(year, month, monthrange(year, month)[1])
            return start, end
        except ValueError:
            return None, None

    # Yearly: YYYY.zip (8 chars)
    if filename[:4].isdigit() and len(filename) == 8 and filename.endswith(".zip"):
        try:
            year = int(filename[:4])
            return date(year, 1, 1), date(year, 12, 31)
        except ValueError:
            return None, None

    return None, None


def filter_urls_by_date(
    files: List[GdeltFile],
    start_date: Optional[date],
    end_date: Optional[date],
) -> List[GdeltFile]:
    """
    Keep only files whose period overlaps [start_date, end_date].

    Either bound may be None (open-ended). If both are None the full list
    is returned unchanged so existing behaviour is preserved.
    """
    if start_date is None and end_date is None:
        return files

    kept = []
    skipped = 0

    for file in files:
        filename = file.url.split("/")[-1]
        file_start, file_end = parse_file_date(filename)

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


# ------------------------------------------------------------
# STEP 2: Download files concurrently, verifying MD5 when known
# ------------------------------------------------------------
def _download_one(
    file: GdeltFile,
    download_dir: str,
    retries: int,
    timeout: int,
    session: requests.Session,
) -> Tuple[str, str]:
    """
    Download a single file with retry and (when file.md5 is known) checksum
    verification. Returns (status, filename) with status one of
    "success" / "skipped" / "failed".
    """
    filename = file.url.split("/")[-1]
    local_path = os.path.join(download_dir, filename)
    tmp_path = local_path + ".tmp"

    if os.path.exists(local_path):
        return "skipped", filename

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
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            if attempt < retries - 1:
                time.sleep(1)

    logger.error(f"Failed to download after {retries} attempts: {filename}")
    return "failed", filename


def download_gdelt_files(files: List[GdeltFile], config: dict) -> Dict[str, List[str] | int]:
    """
    Downloads all `files` concurrently using a bounded thread pool, verifying
    each download's MD5 against the hash GDELT published for it (when known).
    """
    download_dir = config["paths"]["downloaded_data_directory"]
    retries = config["scraping"]["retries"]
    timeout = config["scraping"]["timeout"]
    max_workers = config.get("scraping", {}).get("max_workers", 8)

    os.makedirs(download_dir, exist_ok=True)

    success = 0
    skipped = 0
    failed: List[str] = []

    logger.info(
        f"Starting download of {len(files)} file(s) into {download_dir} "
        f"({max_workers} concurrent workers)..."
    )

    with requests.Session() as session:
        adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_download_one, file, download_dir, retries, timeout, session)
                for file in files
            ]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading GDELT files", unit="file"):
                status, filename = future.result()
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


# ------------------------------------------------------------
# PIPELINE INTERFACE
# ------------------------------------------------------------
def run_scraping_pipeline(
    config: dict,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[str, int | List[str]]:
    """
    Complete scraping step: collect URLs -> (optionally) filter by date -> download.
    Called from main.py.
    """
    files = collect_gdelt_links(config)
    files = filter_urls_by_date(files, start_date, end_date)
    result = download_gdelt_files(files, config)
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
