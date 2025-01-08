import os
import asyncio
from aiofiles import open as aio_open
from bs4 import BeautifulSoup

# Directories
html_folder = "./data/html"
output_folder = "./data/html_no_references/"

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)


async def process_html_file(file_path, output_path):
    """Process a single HTML file asynchronously."""
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

    print(f"Removed {len(bib_blocks)} 'ltx_bibblock' elements and {len(bib_items)} 'ltx_bibitem' elements from {file_path}")

    # Write the modified HTML to the output file
    async with aio_open(output_path, "w", encoding="utf-8") as output_file:
        await output_file.write(str(soup))


async def process_html_files(html_folder, output_folder):
    """Walk through the folder and process all HTML files asynchronously."""
    tasks = []

    for root, _, files in os.walk(html_folder):
        for file in files:
            if file.endswith(".html"):
                html_file_path = os.path.join(root, file)
                output_file_path = os.path.join(output_folder, file)
                tasks.append(process_html_file(html_file_path, output_file_path))

    await asyncio.gather(*tasks)


async def main():
    await process_html_files(html_folder, output_folder)
    print("Processing completed.")


if __name__ == "__main__":
    asyncio.run(main())
