import os
import time
import pickle
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from chromedriver_py import binary_path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
from utils import Config

# Constants
timeout_secs = 5
if not os.path.exists(Config.html_dir):
    os.makedirs(Config.html_dir)
have = set(os.listdir(Config.html_dir))
ua = UserAgent()
db = pickle.load(open(Config.db_path, "rb"))
driver_path = binary_path


def create_driver():
    options = Options()
    user_agent = ua.random
    print(f"Using User-Agent: {user_agent}")
    options.add_argument(f"--user-agent={user_agent}")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    service = Service(driver_path)
    return webdriver.Chrome(service=service, options=options)


def fetch_html(task):
    pid, j = task
    texs = [x["href"] for x in j["links"] if x["type"] == "application/pdf"]
    assert len(texs) == 1
    html_url = texs[0].replace("/pdf/", "/html/") #.replace("/arxiv.org/", "/ar5iv.org/")
    basename = html_url.split("/")[-1] + ".html"
    fname = os.path.join(Config.html_dir, basename)

    if basename in have:
        print(f"{fname} exists, skipping")
        return 1

    try:
        print(f"Task {pid} started on thread {threading.current_thread().name}")
        driver = create_driver()
        driver.get(html_url)

        # Detect redirection
        current_url = driver.current_url
        if "arxiv.org/abs/" in current_url:
            print(f"Redirection detected: {current_url}")
            redirected_id = current_url.split("/")[-1]
            redirected_html_url = f"https://arxiv.org/html/{redirected_id}"
            fname = os.path.join(Config.html_dir, f"{redirected_id}.html")
            driver.quit()
            driver = create_driver()
            driver.get(redirected_html_url)

        # Save the page source
        with open(fname, "w", encoding="utf-8") as fp:
            fp.write(driver.page_source)

        # Random delay to mimic human behavior
        time.sleep(random.uniform(1, 5))
        return 1
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(60)  # Wait to recover if flagged
        return 0
    finally:
        driver.quit()


def main():
    tasks = list(db.items())
    total = len(tasks)
    print(f"Total tasks: {total}")

    # Adjust max workers dynamically
    import multiprocessing
    max_workers = min(16, multiprocessing.cpu_count() * 3) #check with lscpu
    print(f"Using max_workers={max_workers}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(fetch_html, tasks))

    numok = sum(results)
    print(f"Final number of papers downloaded okay: {numok}/{total}")


if __name__ == "__main__":
    main()