import os
import asyncio
from aiofiles import open as aio_open
from bs4 import BeautifulSoup

# Directories
html_folder = "./data/html"
output_folder = "./data/html_no_references/"

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

# Maximum number of concurrent file operations
MAX_CONCURRENT_FILES = 32  # Adjust this number based on your system limits

async def process_html_file(file_path, output_path, semaphore):
    """Process a single HTML file asynchronously with a semaphore."""
    async with semaphore:
        async with aio_open(file_path, "r", encoding="utf-8") as html_file:
            content = await html_file.read()
            soup = BeautifulSoup(content, "html.parser")

        # Remove elements with 'ltx_bibblock'
        bib_blocks = soup.find_all("span", {"class": "ltx_bibblock"})
        for block in bib_blocks:
            block.decompose()

        # Remove elements with 'ltx_bibitem'
        bib_items = soup.find_all("li", {"class": "ltx_bibitem"})
        for item in bib_items:
            item.decompose()

        # Remove elements with 'ltx_bibliography'
        bibliography_sections = soup.find_all("section", {"class": "ltx_bibliography"})
        for section in bibliography_sections:
            section.decompose()

        print(f"Removed {len(bib_blocks)} 'ltx_bibblock' elements, {len(bib_items)} 'ltx_bibitem' elements and {len(bibliography_sections)} 'ltx_bibliography' class from {file_path}")

        # Write the modified HTML to the output file
        async with aio_open(output_path, "w", encoding="utf-8") as output_file:
            await output_file.write(str(soup))

async def process_html_files(html_folder, output_folder, semaphore):
    """Walk through the folder and process all HTML files asynchronously."""
    tasks = []

    for root, _, files in os.walk(html_folder):
        for file in files:
            if file.endswith(".html"):
                html_file_path = os.path.join(root, file)
                output_file_path = os.path.join(output_folder, file)
                tasks.append(process_html_file(html_file_path, output_file_path, semaphore))

    await asyncio.gather(*tasks)

async def main():
    # Semaphore to limit the number of concurrent file processing tasks
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_FILES)
    await process_html_files(html_folder, output_folder, semaphore)
    print("Processing completed.")

if __name__ == "__main__":
    asyncio.run(main())
