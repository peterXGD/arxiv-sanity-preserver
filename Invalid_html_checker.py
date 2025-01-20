import os
import logging
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# Set up logging configuration
logging.basicConfig(filename='broken_html.log', level=logging.WARNING,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def check_html_file(file_path):
    """Check if the HTML file is well-formed and log any errors."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
            # Parse the HTML content with BeautifulSoup
            soup = BeautifulSoup(html_content, 'lxml')
            # Check if the document was parsed correctly (will raise error if malformed)
            if soup.prettify() != html_content:
                logging.warning(f"Malformed HTML in file: {file_path}")
    except Exception as e:
        # Log any exceptions or errors reading/parsing the file
        logging.error(f"Error reading or parsing {file_path}: {e}")
    print(f"Completed checking: {file_path}")

def check_html_in_folder(folder_path):
    """Check all HTML files in the given folder for broken HTML using parallel processing."""
    # Collect all HTML files from the folder
    html_files = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path) if filename.endswith(".html")]

    # Use ThreadPoolExecutor to process HTML files in parallel
    with ThreadPoolExecutor(max_workers=40) as executor:
        # Map check_html_file to all files
        executor.map(check_html_file, html_files)

# Specify the folder containing your HTML files
folder_path = '/Users/lansaber/Arxiv_downloader/data/html_no_references'

# Run the check on all HTML files in the folder
check_html_in_folder(folder_path)
