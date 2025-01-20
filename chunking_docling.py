import os
from transformers import AutoTokenizer
from docling.chunking import HybridChunker
from docling.document_converter import DocumentConverter
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

# Folder containing the HTML files
DOC_FOLDER = "./data/html_processed"
OUTPUT_FOLDER = "./data/chunked_dataset_with_name"
MODEL_ID = "meta-llama/Llama-3.3-70B-Instruct"
MAX_TOKENS = 2000

# Ensure output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Initialize Tokenizer and Chunker
tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, use_auth_token="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")
chunker = HybridChunker(tokenizer=tokenizer, max_tokens=MAX_TOKENS)


def process_file(file_name):
    """
    Processes a single file and saves the results to a Parquet file.

    Args:
        file_name (str): Name of the HTML file to process.

    Returns:
        str: Path to the saved Parquet file, or None if processing fails.
    """
    if not file_name.endswith(".html"):
        return None

    doc_path = os.path.join(DOC_FOLDER, file_name)
    output_file = os.path.join(OUTPUT_FOLDER, f"{file_name}.parquet")

    try:
        # Convert the document
        doc = DocumentConverter().convert(source=doc_path).document
        chunk_iter = chunker.chunk(dl_doc=doc)
        chunks = list(chunk_iter)
        file_name_without_extension = os.path.splitext(file_name)[0]

        # Prepare data for the current file
        data = {
            "chunk_id": [],
            "chunk_text": [],
            "chunk_text_tokens": [],
            "serialized_text": [],
            "serialized_text_tokens": [],
            "filename": []
        }

        # Process and store chunks
        for i, chunk in enumerate(chunks):
            chunk_text = chunk.text
            serialized_text = chunker.serialize(chunk=chunk)

            # Count tokens
            chunk_text_tokens = len(tokenizer.tokenize(chunk_text, max_length=None))
            serialized_text_tokens = len(tokenizer.tokenize(serialized_text, max_length=None))

            # Add to data dictionary
            data["chunk_id"].append(i)
            data["chunk_text"].append(chunk_text)
            data["chunk_text_tokens"].append(chunk_text_tokens)
            data["serialized_text"].append(serialized_text)
            data["serialized_text_tokens"].append(serialized_text_tokens)
            data["filename"].append(file_name_without_extension)

        # Convert to a pandas DataFrame
        df = pd.DataFrame(data)

        # Save the dataset to Parquet
        df.to_parquet(output_file, index=False)
        print(f"Processed and saved {file_name} successfully to {output_file}.")
        return output_file

    except Exception as e:
        print(f"Failed to process {file_name}: {e}")
        return None


def main():
    # Get the list of HTML files
    html_files = [file_name for file_name in os.listdir(DOC_FOLDER) if file_name.endswith(".html")]

    # Process files in parallel
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_file, html_files))

    # Filter out failed files
    parquet_files = [result for result in results if result is not None]

    # Combine all Parquet files into one
    if parquet_files:
        final_dataset = pd.concat((pd.read_parquet(f) for f in parquet_files), ignore_index=True)

        # Save the final dataset
        final_dataset_path = "./data/chunked_dataset_with_name.parquet"
        final_dataset.to_parquet(final_dataset_path, index=False)
        print(f"Final dataset saved to {final_dataset_path}")

        # # Delete intermediate Parquet files
        # for file_path in parquet_files:
        #     os.remove(file_path)
        #     print(f"Deleted intermediate Parquet file: {file_path}")

    else:
        print("No datasets were created. Please check the input folder and files.")


if __name__ == "__main__":
    main()


