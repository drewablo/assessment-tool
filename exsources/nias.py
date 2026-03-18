import subprocess
import sys

# Auto-install required packages
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

required = [("playwright", "playwright"), ("requests", "requests"), ("beautifulsoup4", "bs4")]
for install_name, import_name in required:
    try:
        __import__(import_name)
    except ImportError:
        print(f"Installing {install_name}...")
        install(install_name)

# Install Playwright browsers if needed
try:
    subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
except Exception:
    pass

import csv
import json
import re
import time
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

FILTER_URL = (
    "https://www.nais.org/school-directory"
    "?filter=83be3e94-760e-400b-8dd5-1af27d432435%2C"
    "ecaa182b-3edd-43f5-b658-1eccec430aa3%2C"
    "77470b45-677c-4e71-a93d-abb5c55daa02%2C"
    "a890f12c-a905-48b5-b8b9-82e11d812431%2C"
    "c2cc03a4-1786-46be-b989-a2032b1451d9%2C"
    "3178b735-fd89-4dcc-bad3-2f465b15cb78%2C"
    "f13592c4-5d05-49db-a14b-78069af15057"
)
BASE_URL = "https://www.nais.org"
OUTPUT_FILE = "nais_schools.csv"

# Retry-enabled requests session
session = requests.Session()
retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; research bot)"}


def get_all_school_ids():
    """Use Playwright to load the directory page and extract all school IDs from Redux store."""
    print("Loading directory page to extract all school IDs from Redux store...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(FILTER_URL, wait_until="networkidle", timeout=60000)
        # Wait for React to render school results
        page.wait_for_selector('[data-testid="result-item-0"]', timeout=30000)

        # Extract all schools from Redux store
        schools = page.evaluate("""
            () => {
                const listItem = document.querySelector('[data-testid="result-item-0"]');
                const fiberKey = Object.keys(listItem).find(k => k.startsWith('__reactFiber'));
                let node = listItem[fiberKey];
                let store = null;
                let count = 0;
                while (node && count < 50) {
                    if (node.memoizedProps && node.memoizedProps.store && node.memoizedProps.store.getState) {
                        store = node.memoizedProps.store;
                        break;
                    }
                    node = node.return;
                    count++;
                }
                if (!store) return [];
                const results = store.getState().Search.results;
                return results.map(r => ({
                    id: r.detailsPageUrl.split('/').pop(),
                    name: r.name,
                    city: r.city,
                    state: r.state,
                    url: r.detailsPageUrl
                }));
            }
        """)
        browser.close()
    print(f"Found {len(schools)} schools in Redux store.")
    return schools


def get_school_detail(school_id):
    """Fetch grade levels, enrollment size, school type from individual school page."""
    url = f"{BASE_URL}/school-directory/school/{school_id}"
    resp = session.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Also try to extract street address and ZIP from JSON-LD
    script_tag = soup.find("script", {"type": "application/ld+json"})
    street, zipcode, country = "", "", ""
    if script_tag:
        try:
            data = json.loads(script_tag.string)
            addr = data.get("address", {})
            street = addr.get("streetAddress", "")
            zipcode = addr.get("postalCode", "")
            country = addr.get("addressCountry", "")
        except Exception:
            pass

    def extract_field(label):
        pattern = (
            rf"{re.escape(label)}\s+(.+?)"
            r"(?=Religious Affiliation|School Type|Grade Levels|Enrollment Size"
            r"|Student body|Learning Styles|Extended Schedule|Language Programs"
            r"|Sports Programs|Other Characteristics|Contact the School|$)"
        )
        match = re.search(pattern, text)
        return match.group(1).strip() if match else ""

    return {
        "street": street,
        "zip": zipcode,
        "country": country,
        "grade_levels": extract_field("Grade Levels"),
        "enrollment_size": extract_field("Enrollment Size"),
        "school_type": extract_field("School Type"),
        "student_body": extract_field("Student body"),
    }


def main():
    # Step 1: Get all school IDs in one shot via Playwright
    schools = get_all_school_ids()

    if not schools:
        print("ERROR: No schools found. Check your internet connection.")
        return

    # Step 2: Visit each school detail page
    print(f"\nFetching details for {len(schools)} schools...")

    fieldnames = [
        "id", "name", "city", "state", "street", "zip", "country",
        "url", "grade_levels", "enrollment_size", "school_type", "student_body"
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, school in enumerate(schools):
            print(f"  [{i+1}/{len(schools)}] {school['name']}...")
            try:
                detail = get_school_detail(school["id"])
                school.update(detail)
            except Exception as e:
                print(f"    ERROR: {e}")
                school.update({
                    "street": "", "zip": "", "country": "",
                    "grade_levels": "", "enrollment_size": "",
                    "school_type": "", "student_body": ""
                })
            writer.writerow(school)
            f.flush()
            time.sleep(0.4)

    print(f"\nDone! Results saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

"""
Key improvements over the previous version:**

1. **No more pagination problem** — Playwright loads the page once and pulls all 1,933 school IDs directly from the Redux store in memory, skipping 194 pages of clicking entirely.
2. **Street address and ZIP now included** — extracted from the JSON-LD on each school's detail page.
3. **Playwright handles JavaScript rendering** — no more relying on static HTML that misses JS-loaded content.
4. **Still uses fast `requests` for the 1,933 individual school detail pages** — Playwright is only used for the one directory page load.

**To install Playwright's browser**, the script does it automatically, but if you hit issues you can also run manually:
```
py -m playwright install chromium
"""