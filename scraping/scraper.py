"""
scraper.py

Utilities for scraping GDELT event data (https://data.gdeltproject.org/events/).

Provides:
    - collect_gdelt_links: retrieves all downloadable file links from the GDELT events directory
    - download_gdelt_files: downloads the files returned by `collect_gdelt_links`
    - run_scraping_pipeline: high-level interface that runs the full scraping workflow
"""

import os
import requests
import time
from typing import List, Dict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm

from utils.config import load_config
from utils.logging import get_logger

log = get_logger(__name__)


# ------------------------------------------------------------
# STEP 1: Collect GDELT links via Selenium
# ------------------------------------------------------------
def collect_gdelt_links() -> List[str]:
    """
    Uses Selenium to extract all GDELT .zip file URLs from the events page.
    """
    log.info("Collecting GDELT links using Selenium…")

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--ignore-certificate-errors")
    driver = webdriver.Chrome(options=chrome_options)

    urls = []

    try:
        driver.get("https://data.gdeltproject.org/events/")
        time.sleep(2)

        # Try to bypass certificate warning if present
        try:
            proceed_btn = driver.find_element(By.XPATH, "//a[contains(text(), 'Proceed') or contains(text(), 'Advanced')]")
            proceed_btn.click()
            time.sleep(2)
            log.info("Security warning bypassed.")
        except:
            pass  # no warning—good

        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
        )
        anchors = driver.find_elements(By.TAG_NAME, "a")
        log.info(f"Found {len(anchors)} total links.")

        for tag in anchors:
            href = tag.get_attribute("href")
            if href:
                filename = href.split("/")[-1]
                is_daily = filename.endswith(".export.CSV.zip")
                is_monthly = (filename[:6].isdigit() and len(filename) == 10)
                is_yearly = (filename[:4].isdigit() and len(filename) == 8)
                if is_daily or is_monthly or is_yearly:
                    urls.append(href.replace("https://", "http://", 1))

        log.info(f"Identified {len(urls)} GDELT dataset URLs.")

    finally:
        driver.quit()

    return urls


# ------------------------------------------------------------
# STEP 2: Download files using requests + tqdm
# ------------------------------------------------------------
def download_gdelt_files(urls: List[str]) -> Dict[str, List[str] | int]:
    """
    Downloads all files listed in `urls` using streaming requests with retry.
    """
    config = load_config()
    download_dir = config["paths"]["downloaded_data_directory"]
    retries = config["scraping"]["retries"]
    timeout = config["scraping"]["timeout"]

    os.makedirs(download_dir, exist_ok=True)

    session = requests.Session()

    success = 0
    skipped = 0
    failed = []

    log.info(f"Starting download into {download_dir}…")

    for url in tqdm(urls, desc="Downloading GDELT files", unit="file"):
        filename = url.split("/")[-1]
        local_path = os.path.join(download_dir, filename)

        # Skip existing
        if os.path.exists(local_path):
            skipped += 1
            continue

        for attempt in range(retries):
            try:
                log.debug(f"Downloading {filename} (attempt {attempt + 1}/{retries})")

                with session.get(url, stream=True, timeout=timeout) as response:
                    response.raise_for_status()

                    with open(local_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                success += 1
                break

            except Exception as e:
                log.warning(f"Attempt {attempt + 1} failed for {filename}: {e}")
                time.sleep(1)

                if attempt == retries - 1:
                    log.error(f"Failed to download after {retries} attempts: {filename}")
                    failed.append(filename)

    log.info(f"Download summary: {success} success, {skipped} skipped, {len(failed)} failed.")

    return {
        "success": success,
        "skipped": skipped,
        "failed": failed,
    }


# ------------------------------------------------------------
# PIPELINE INTERFACE
# ------------------------------------------------------------
def run_scraping_pipeline() -> Dict[str, int | List[str]]:
    """
    Complete scraping step: collect URLs → download all.
    Called from main.py.
    """
    urls = collect_gdelt_links()
    result = download_gdelt_files(urls)
    return result

# ------------------------------------------------------------
# STANDALONE SCRIPT
# ------------------------------------------------------------
if __name__ == "__main__":
    log.info("Running scraping pipeline as standalone script…")
    result = run_scraping_pipeline()
    log.info(result)
