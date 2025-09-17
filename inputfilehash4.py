# main_ci.py
import os
import time
import csv
from multiprocessing import Pool, cpu_count
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader

BASE_DIR = os.getenv("GITHUB_WORKSPACE", os.getcwd())
DOWNLOAD_DIR = "/tmp/Downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

INPUT_CSV = "indigo_input.csv"
OUTPUT_CSV = "output4.csv"
BATCH_SIZE = 500

def log_debug(msg):
    with open("/tmp/debug_log.txt", "a") as f:
        f.write(f"{msg}\n")
    print(msg)


def process_invoice(row):
    filehash = row["filehash"]
    link = row["assetlink"]

    row_download_dir = os.path.join(DOWNLOAD_DIR, filehash)
    os.makedirs(row_download_dir, exist_ok=True)

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": row_download_dir,
        "download.prompt_for_download": False,
        "directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=chrome_options)

    log_debug(f"[START] {filehash}")
    try:
        existing_files = set(os.listdir(row_download_dir))
        driver.get(link)
        time.sleep(5)

        try:
            download_button = driver.find_element(By.TAG_NAME, "a")
            download_button.click()
        except:
            log_debug(f"[ERROR] {filehash} - Download Button Not Found")
            return [filehash, "", "Download Button Not Found"]

        # Wait for download
        timeout = 70
        new_file = None
        for _ in range(timeout):
            current_files = set(os.listdir(row_download_dir))
            diff = current_files - existing_files
            if diff:
                candidate_file = list(diff)[0]
                candidate_path = os.path.join(row_download_dir, candidate_file)
                if not candidate_file.endswith(".crdownload") and not candidate_file.startswith(".com.google.Chrome"):
                    new_file = candidate_path
                    time.sleep(3)
                    break
            time.sleep(1)

        if not new_file:
            log_debug(f"[ERROR] {filehash} - Download Timeout")
            return [filehash, "", "Download Timeout"]

        # Process file
        filename = os.path.basename(new_file)
        status = "Unknown"
        try:
            if new_file.lower().endswith(".html"):
                with open(new_file, "r", encoding="utf-8") as f:
                    content = f.read()
                soup = BeautifulSoup(content, "html.parser")
                page_text = soup.get_text(separator=" ", strip=True)
                status = "No Invoice Found" if "No Invoice" in page_text else "Invoice Present"
            elif new_file.lower().endswith(".pdf"):
                reader = PdfReader(new_file)
                text_content = "".join([page.extract_text() or "" for page in reader.pages])
                status = "No Invoice Found" if "No Invoice" in text_content else "Invoice Present"
            else:
                status = "Unsupported File Type"
        except Exception as e:
            status = f"Error: {e}"

        log_debug(f"[DONE] {filehash} - {status}")

        # Cleanup
        try:
            os.remove(new_file)
        except:
            pass

        return [filehash, filename, status]

    finally:
        driver.quit()


def debug_wrapper(args):
    index, row, total_rows = args
    log_debug(f"Processing row {index}/{total_rows} -> filehash: {row['filehash']}")
    result = process_invoice(row)
    log_debug(f"Completed row {index}/{total_rows} -> status: {result[2]}")
    return result


def chunk_list(lst, n):
    """Yield successive n-sized chunks from lst"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if __name__ == "__main__":
    with open(INPUT_CSV, 'r') as f:
        reader = list(csv.DictReader(f))

    START_ROW = 160000
    reader = reader[START_ROW:]

    total_rows = len(reader)
    log_debug(f"Rows after slicing: {total_rows}")

    output_exists = os.path.exists(OUTPUT_CSV)

    with open(OUTPUT_CSV, 'a', newline='') as f_out:
        writer = csv.writer(f_out)
        if not output_exists:
            writer.writerow(["filehash", "filename", "status"])  # Write header only once

        for batch_index, batch_rows in enumerate(chunk_list(reader, BATCH_SIZE), start=1):
            log_debug(f"Starting batch {batch_index} with {len(batch_rows)} rows")

            indexed_rows = [(i + 1, row, total_rows) for i, row in enumerate(batch_rows)]
            pool_size = min(cpu_count(), 8)

            with Pool(pool_size) as pool:
                result_iterator = pool.imap_unordered(debug_wrapper, indexed_rows)

                for result in result_iterator:
                    writer.writerow(result)
                    f_out.flush()  # Ensure data is written immediately
                    log_debug(f"[WRITTEN] {result[0]} -> {result[2]}")

            log_debug(f"Completed batch {batch_index}")

    log_debug("Processing complete.")