import os
from bs4 import BeautifulSoup

# Directories
html_folder = "./data/html"
output_folder = "./data/html_no_references/"

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

for root, dirs, files in os.walk(html_folder):
    for file in files:
        if file.endswith(".html"):
            html_file_path = os.path.join(root, file)

            # Read the HTML content
            with open(html_file_path, "r", encoding="utf-8") as html_file:
                soup = BeautifulSoup(html_file, "html.parser")

            # Remove all elements with 'ltx_bibblock'
            bib_blocks = soup.find_all("span", {"class": "ltx_bibblock"})
            for block in bib_blocks:
                block.decompose()

            # Remove all elements with 'ltx_bibitem'
            bib_items = soup.find_all("li", {"class": "ltx_bibitem"})
            for item in bib_items:
                item.decompose()

            bibliography_sections = soup.find_all("section", {"class": "ltx_bibliography"})
            for section in bibliography_sections:
                section.decompose()

            print(
                f"Removed {len(bib_blocks)} 'ltx_bibblock' elements, {len(bib_items)} 'ltx_bibitem' elements, "
                f"and {len(bibliography_sections)} 'ltx_bibliography' sections from {html_file_path}"
            )

            # Save the modified HTML
            output_file_path = os.path.join(output_folder, file)
            with open(output_file_path, "w", encoding="utf-8") as output_file:
                output_file.write(str(soup))

print("Processing completed.")

