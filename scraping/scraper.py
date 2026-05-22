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

import os
import requests
import time
from calendar import monthrange
from datetime import date
from typing import List, Dict, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

from utils.logging import get_logger

logger = get_logger(__name__)


# ------------------------------------------------------------
# STEP 1: Collect GDELT links via Selenium
# ------------------------------------------------------------
def _build_driver(config: dict) -> webdriver.Chrome:
    """
    Build a Chrome WebDriver instance.

    Resolution order:
      1. scraping.chromedriver_path in settings.yaml (manual override, no network)
      2. webdriver-manager auto-download (requires internet on first run)

    If the network is blocked (e.g. corporate/university firewall), download
    ChromeDriver manually from https://googlechromelabs.github.io/chrome-for-testing/
    and set scraping.chromedriver_path in config/settings.yaml.
    """
    chrome_options = webdriver.ChromeOptions()
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


def collect_gdelt_links(config: dict) -> List[str]:
    """
    Uses Selenium to extract all GDELT .zip file URLs from the events page.
    """
    logger.info("Collecting GDELT links using Selenium...")

    driver = _build_driver(config)

    urls = []

    try:
        driver.get("https://data.gdeltproject.org/events/")
        time.sleep(2)

        # Try to bypass certificate warning if present
        try:
            proceed_btn = driver.find_element(By.XPATH, "//a[contains(text(), 'Proceed') or contains(text(), 'Advanced')]")
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
            if href:
                filename = href.split("/")[-1]
                is_daily = filename.endswith(".export.CSV.zip")
                is_monthly = (filename[:6].isdigit() and len(filename) == 10)
                is_yearly = (filename[:4].isdigit() and len(filename) == 8)
                if is_daily or is_monthly or is_yearly:
                    urls.append(href.replace("https://", "http://", 1))

        logger.info(f"Identified {len(urls)} GDELT dataset URLs.")

    finally:
        driver.quit()

    return urls


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
    urls: List[str],
    start_date: Optional[date],
    end_date: Optional[date],
) -> List[str]:
    """
    Keep only URLs whose file period overlaps [start_date, end_date].

    Either bound may be None (open-ended). If both are None the full list
    is returned unchanged so existing behaviour is preserved.
    """
    if start_date is None and end_date is None:
        return urls

    kept = []
    skipped = 0

    for url in urls:
        filename = url.split("/")[-1]
        file_start, file_end = parse_file_date(filename)

        if file_start is None:
            logger.debug(f"Could not parse date from {filename}, skipping.")
            skipped += 1
            continue

        # Overlap: file period must intersect [start_date, end_date]
        if start_date and file_end < start_date:
            continue
        if end_date and file_start > end_date:
            continue

        kept.append(url)

    logger.info(
        f"Date filter [{start_date} - {end_date}]: "
        f"{len(kept)} URLs kept, {len(urls) - len(kept) - skipped} excluded, "
        f"{skipped} skipped (unparseable filename)."
    )

    return kept


# ------------------------------------------------------------
# STEP 2: Download files using requests + tqdm
# ------------------------------------------------------------
def download_gdelt_files(urls: List[str], config: dict) -> Dict[str, List[str] | int]:
    """
    Downloads all files listed in `urls` using streaming requests with retry.
    """
    download_dir = config["paths"]["downloaded_data_directory"]
    retries = config["scraping"]["retries"]
    timeout = config["scraping"]["timeout"]

    os.makedirs(download_dir, exist_ok=True)

    session = requests.Session()

    success = 0
    skipped = 0
    failed = []

    logger.info(f"Starting download of {len(urls)} file(s) into {download_dir}...")

    for url in tqdm(urls, desc="Downloading GDELT files", unit="file"):
        filename = url.split("/")[-1]
        local_path = os.path.join(download_dir, filename)

        # Skip existing
        if os.path.exists(local_path):
            skipped += 1
            continue

        for attempt in range(retries):
            try:
                logger.debug(f"Downloading {filename} (attempt {attempt + 1}/{retries})")

                with session.get(url, stream=True, timeout=timeout) as response:
                    response.raise_for_status()

                    with open(local_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                success += 1
                break

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {filename}: {e}")
                time.sleep(1)

                if attempt == retries - 1:
                    logger.error(f"Failed to download after {retries} attempts: {filename}")
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
    urls = collect_gdelt_links(config)
    urls = filter_urls_by_date(urls, start_date, end_date)
    result = download_gdelt_files(urls, config)
    return result


# ------------------------------------------------------------
# STANDALONE SCRIPT
# ------------------------------------------------------------
if __name__ == "__main__":
    from utils.config import load_config

    logger.info("Running scraping pipeline as standalone script...")
    cfg = load_config()
    result = run_scraping_pipeline(cfg)
    logger.info(result)
