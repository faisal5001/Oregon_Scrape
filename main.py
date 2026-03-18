import csv
import os
import re
import sys
import logging
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError

# ==============================
# CONFIG
# ==============================
BASE_OUT = "output"
DEBUG_DIR = "debug"
HEADLESS = True
MAX_DEPTH = 6
SCROLL_DELAY = 1000
LAZY_LOAD_RETRIES = 15
MAX_RELOADS = 5

os.makedirs(BASE_OUT, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

ROOTS = [
    ("Facilities > Electricity", "electric", "https://apps.cer-rec.gc.ca/REGDOCS/Item/View/90548"),
    ("Facilities > Gas", "gas", "https://apps.cer-rec.gc.ca/REGDOCS/Item/View/90550"),
    ("Facilities > Oil", "oil", "https://apps.cer-rec.gc.ca/REGDOCS/Item/View/90552"),
    ("Exports & Imports > Electricity", "electric_export", "https://apps.cer-rec.gc.ca/REGDOCS/Item/View/94151"),
    ("Exports & Imports > Gas", "gas_export", "https://apps.cer-rec.gc.ca/REGDOCS/Item/View/94153"),
    ("Exports & Imports > Oil", "oil_export", "https://apps.cer-rec.gc.ca/REGDOCS/Item/View/94154"),
]

CSV_FIELDS = [
    "Stage", "Utility", "Root", "Level",
    "Title", "Company_Name", "Date", "Submitter",
    "Docket_No", "Page_Type", "PDF_Link",
    "Page_URL", "Breadcrumb"
]

ALL_ROWS = []
VISITED = set()

# ==============================
# LOGGING
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s"
)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# ==============================
# LIVE CSV WRITER
# ==============================
class LiveCSVWriter:
    def __init__(self, path):
        self.path = path
        self.file = open(path, "w", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.file, fieldnames=CSV_FIELDS)
        self.writer.writeheader()
        self.file.flush()

    def write(self, row):
        self.writer.writerow(row)
        self.file.flush()

    def close(self):
        try:
            self.file.flush()
            self.file.close()
        except:
            pass

# ==============================
# HELPERS
# ==============================
def normalize(url):
    return url.split("#")[0]

def is_pdf(url):
    return "/REGDOCS/File/Download/" in url

def extract_docket(text):
    if not text:
        return None
    m = re.search(r"\b[A-Z]\d{4,6}(?:-\d+)?\b", text)
    return m.group(0) if m else None

def save_debug(page, name):
    safe = re.sub(r"[^\w\-_.]", "_", name)
    try:
        with open(os.path.join(DEBUG_DIR, f"{safe}.html"), "w", encoding="utf-8") as f:
            f.write(page.content())
        page.screenshot(path=os.path.join(DEBUG_DIR, f"{safe}.png"), full_page=True)
    except:
        pass

# ==============================
# SAFE PAGE LOAD
# ==============================
def safe_goto(page, url):
    for i in range(MAX_RELOADS):
        try:
            page.goto(url, timeout=120000, wait_until="networkidle")
            page.wait_for_timeout(SCROLL_DELAY)
            return True
        except TimeoutError:
            logging.warning(f"Timeout loading {url}, retry {i+1}")
            save_debug(page, f"TIMEOUT_{i}_{url.rsplit('/',1)[-1]}")
    return False

def lazy_scroll(page):
    for _ in range(LAZY_LOAD_RETRIES):
        page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        page.wait_for_timeout(SCROLL_DELAY)

def wait_for_dataset(page):
    for _ in range(LAZY_LOAD_RETRIES):
        rows = page.query_selector_all("#divSearchResults table tr")
        if rows:
            return True
        lazy_scroll(page)
        page.wait_for_timeout(1000)
    return False

# ==============================
# SET MAX RECORDS (PAGINATION)
# ==============================
def set_max_records(page):
    """Set records per page to 200"""
    try:
        select = page.query_selector("#selNumberOfRecords")
        if select:
            select.select_option("200")
            page.wait_for_timeout(2000)
    except Exception as e:
        logging.warning(f"Failed to set max records: {e}")

# ==============================
# EXTRACT ALL LINKS WITH PAGINATION
# ==============================
def extract_all_links(page, base_url):
    """
    Extract all rows using 'Next 200 Results' pagination
    Handles broken last-page pagination (e.g. 530 records)
    """
    all_records = []
    seen_urls = set()

    set_max_records(page)

    previous_first_href = None

    while True:
        page.wait_for_selector("#divSearchResults table", timeout=15000)
        lazy_scroll(page)

        rows = page.query_selector_all(
            "#divSearchResults table tr a[href]"
        )

        if not rows:
            logging.info("No rows found — stopping pagination")
            break

        current_first_href = rows[0].get_attribute("href")

        for a in rows:
            title = a.inner_text().strip()
            href = a.get_attribute("href")
            if not href:
                continue

            full_url = normalize(urljoin(base_url, href))

            if full_url in seen_urls:
                continue

            seen_urls.add(full_url)
            all_records.append((title, full_url))

        logging.info(
            f"Pagination progress: {len(all_records)} records collected"
        )

        next_btn = page.query_selector("a.next-page")
        if not next_btn:
            logging.info("Pagination stopped: no Next button")
            break

        next_btn.click()

        try:
            page.wait_for_function(
                """
                (prevHref) => {
                    const first = document.querySelector(
                        '#divSearchResults table tr a[href]'
                    );
                    return first && first.getAttribute('href') !== prevHref;
                }
                """,
                arg=current_first_href,
                timeout=12000
            )
        except TimeoutError:
            logging.info(
                "Pagination exhausted: content did not change"
            )
            break

        previous_first_href = current_first_href

    return all_records



# ==============================
# CORE CRAWLER
# ==============================
def crawl_level(context, url, stage, level, root, utility, breadcrumb, writer):
    url = normalize(url)
    if url in VISITED:
        return
    VISITED.add(url)

    page = context.new_page()
    if not safe_goto(page, url):
        page.close()
        return

    if not wait_for_dataset(page):
        save_debug(page, f"NO_DATA_L{level}")
        page.close()
        return

    # Extract all links including pagination
    records = extract_all_links(page, url)
    logging.info(f"[STAGE {stage}] [LEVEL {level}] {url} | Links found (including subfolders): {len(records)}")

    for title, next_url in records:
        page_type = "PDF" if is_pdf(next_url) else "FOLDER"
        row = {
            "Stage": stage,
            "Utility": utility,
            "Root": root,
            "Level": level,
            "Title": title,
            "Company_Name": title,
            "Date": None,
            "Submitter": None,
            "Docket_No": extract_docket(title),
            "Page_Type": page_type,
            "PDF_Link": next_url if page_type == "PDF" else None,
            "Page_URL": next_url,
            "Breadcrumb": breadcrumb + " > " + title
        }

        writer.write(row)
        ALL_ROWS.append(row)

        # Recursive crawl for folders
        if page_type == "FOLDER" and level < MAX_DEPTH:
            crawl_level(
                context,
                next_url,
                stage,
                level + 1,
                root,
                utility,
                row["Breadcrumb"],
                writer
            )

    page.close()

# ==============================
# MAIN
# ==============================
def main():
    arg = sys.argv[1].lower() if len(sys.argv) > 1 else "all"
    stages = range(1, 7) if arg == "all" else [int(arg.replace("stage", ""))]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()

        for stage in stages:
            root, utility, url = ROOTS[stage - 1]
            logging.info(f"===== STAGE {stage} START ({root}) =====")

            csv_path = os.path.join(BASE_OUT, f"stage{stage}_live.csv")
            writer = LiveCSVWriter(csv_path)

            prev_count = len(ALL_ROWS)
            pdf_count_before = sum(1 for r in ALL_ROWS if r["Page_Type"] == "PDF")
            folder_count_before = sum(1 for r in ALL_ROWS if r["Page_Type"] == "FOLDER")

            # Crawl Level 1 (including all subfolders)
            crawl_level(
                context=context,
                url=url,
                stage=stage,
                level=1,
                root=root,
                utility=utility,
                breadcrumb=root,
                writer=writer
            )

            # Level 1 summary (including all subfolders)
            level1_total = len(ALL_ROWS) - prev_count
            level1_pdfs = sum(1 for r in ALL_ROWS if r["Page_Type"] == "PDF") - pdf_count_before
            level1_folders = sum(1 for r in ALL_ROWS if r["Page_Type"] == "FOLDER") - folder_count_before
            logging.info(f"Level 1 summary (including subfolders) | Total: {level1_total} | PDFs: {level1_pdfs} | Folders: {level1_folders}")

            writer.close()

        browser.close()

    # Save combined CSV
    combined = os.path.join(BASE_OUT, "combined_all_stages.csv")
    with open(combined, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(ALL_ROWS)

    logging.info("✅ ALL STAGES COMPLETED | Total links: {} | PDFs: {} | Folders: {}".format(
        len(ALL_ROWS),
        sum(1 for r in ALL_ROWS if r["Page_Type"] == "PDF"),
        sum(1 for r in ALL_ROWS if r["Page_Type"] == "FOLDER")
    ))

# ==============================
if __name__ == "__main__":
    main()
