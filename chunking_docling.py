# from transformers import AutoTokenizer
# from docling.chunking import HybridChunker
# from docling.document_converter import DocumentConverter
# from datasets import Dataset
#
# # Document source and model details
# DOC_SOURCE = "/Users/lansaber/Arxiv_downloader/data/html_no_references/2009.13586v6.html"
# model_id = "meta-llama/Llama-3.3-70B-Instruct"
# MAX_TOKENS = 2000
#
# # Tokenizer and Chunker setup
# tokenizer = AutoTokenizer.from_pretrained(model_id, use_auth_token="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")
# chunker = HybridChunker(tokenizer=tokenizer, max_tokens=MAX_TOKENS)
#
# # Convert the document
# doc = DocumentConverter().convert(source=DOC_SOURCE).document
# chunk_iter = chunker.chunk(dl_doc=doc)
# chunks = list(chunk_iter)
#
# # Prepare data for Hugging Face Dataset
# data = {
#     "chunk_id": [],
#     "chunk_text": [],
#     "chunk_text_tokens": [],
#     "serialized_text": [],
#     "serialized_text_tokens": [],
# }
#
# # Process and store chunks
# for i, chunk in enumerate(chunks):
#     chunk_text = chunk.text
#     serialized_text = chunker.serialize(chunk=chunk)
#
#     # Count tokens
#     chunk_text_tokens = len(tokenizer.tokenize(chunk_text, max_length=None))
#     serialized_text_tokens = len(tokenizer.tokenize(serialized_text, max_length=None))
#
#     # Add to data dictionary
#     data["chunk_id"].append(i)
#     data["chunk_text"].append(chunk_text)
#     data["chunk_text_tokens"].append(chunk_text_tokens)
#     data["serialized_text"].append(serialized_text)
#     data["serialized_text_tokens"].append(serialized_text_tokens)
#
# # Convert to Hugging Face Dataset
# hf_dataset = Dataset.from_dict(data)
#
# # Save the dataset locally (optional)
# hf_dataset.save_to_disk("./chunked_dataset")
#
# # Print dataset preview
# print(hf_dataset)
4
import os
from transformers import AutoTokenizer
# from docling.chunking import HybridChunker
from docling.chunking import HierarchicalChunker
from docling.document_converter import DocumentConverter
from datasets import Dataset, concatenate_datasets

# Folder containing the HTML files
DOC_FOLDER = "/Users/lansaber/Arxiv_downloader/data/html_no_references"
OUTPUT_FOLDER = "./chunked_dataset"
MODEL_ID = "meta-llama/Llama-3.3-70B-Instruct"
MAX_TOKENS = 2000

# Initialize Tokenizer and Chunker
tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, use_auth_token="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")
# chunker = HybridChunker(tokenizer=tokenizer, max_tokens=MAX_TOKENS)
chunker = HierarchicalChunker(tokenizer=tokenizer, max_tokens=MAX_TOKENS)
# Data structure to hold all datasets
all_datasets = []

# Iterate through all HTML files in the folder
for file_name in os.listdir(DOC_FOLDER):
    if file_name.endswith(".html"):
        doc_path = os.path.join(DOC_FOLDER, file_name)

        try:
            # Convert the document
            doc = DocumentConverter().convert(source=doc_path).document
            chunk_iter = chunker.chunk(dl_doc=doc)
            chunks = list(chunk_iter)

            # Prepare data for the current file
            data = {
                "chunk_id": [],
                "chunk_text": [],
                "chunk_text_tokens": [],
                "serialized_text": [],
                "serialized_text_tokens": [],
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

            # Convert to Hugging Face Dataset and append to the list
            hf_dataset = Dataset.from_dict(data)
            all_datasets.append(hf_dataset)
            print(f"Processed {file_name} successfully.")

        except Exception as e:
            print(f"Failed to process {file_name}: {e}")

# Concatenate all datasets into one
if all_datasets:
    final_dataset = concatenate_datasets(all_datasets)

    # Save the dataset locally
    final_dataset.save_to_disk(OUTPUT_FOLDER)

    # Print dataset preview
    print(final_dataset)
else:
    print("No datasets were created. Please check the input folder and files.")
