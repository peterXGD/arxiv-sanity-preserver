import os
import time
import pickle
import shutil
import random
from chromedriver_py import binary_path  # This provides the path to chromedriver
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent

from utils import Config

# from proxymanager import ProxyManager

# Constants
timeout_secs = 5  # Max time to wait for a page load
if not os.path.exists(Config.html_dir):
  os.makedirs(Config.html_dir)  # Ensure output directory exists
have = set(os.listdir(Config.html_dir))  # Get list of all HTML files already downloaded

# Setup UserAgent
ua = UserAgent()  # Fake UserAgent generator
# proxy_manager = ProxyManager()  # Automatically fetches free proxies

# Initialize counters
numok = 0
numtot = 0

# Load database
db = pickle.load(open(Config.db_path, "rb"))

# Configure Chrome WebDriver
driver_path = binary_path  # Path to the chromedriver executable


# Function to create a new WebDriver instance with a random user-agent
def create_driver():
  options = Options()
  # Generate a random user-agent
  user_agent = ua.random
  print(f"Using User-Agent: {user_agent}")
  options.add_argument(f"--user-agent={user_agent}")
  options.add_argument("--headless")  # Run in headless mode (no GUI)
  options.add_argument("--disable-gpu")
  options.add_argument("--no-sandbox")
  service = Service(driver_path)

  # proxy = proxy_manager.get_next()  # Get a working proxy
  # options.add_argument(f"--proxy-server={proxy}")

  return webdriver.Chrome(service=service, options=options)


# Main loop to process HTML files
for pid, j in db.items():
  texs = [x["href"] for x in j["links"] if x["type"] == "application/pdf"]
  assert len(texs) == 1
  html_url = texs[0].replace("/pdf/", "/html/")
  basename = html_url.split("/")[-1] + ".html"
  fname = os.path.join(Config.html_dir, basename)

  numtot += 1
  try:
    if basename not in have:
      print(f"Fetching {html_url} into {fname}")

      # Create a new driver instance for each request to apply a new user-agent
      driver = create_driver()

      # Navigate to the URL
      driver.get(html_url)
      time.sleep(timeout_secs)  # Allow the page to load fully

      # Save the page source as an HTML file
      with open(fname, "w", encoding="utf-8") as fp:
        fp.write(driver.page_source)

      # Quit the driver after use
      driver.quit()

      time.sleep(0.05 + random.uniform(0, 0.1))  # Throttle requests
    else:
      print(f"{fname} exists, skipping")

    numok += 1
  except Exception as e:
    print("Error downloading: ", html_url)
    print(e)

  print(f"{numok}/{numtot} of {len(db)} downloaded ok.")

print(f"Final number of papers downloaded okay: {numok}/{len(db)}")



