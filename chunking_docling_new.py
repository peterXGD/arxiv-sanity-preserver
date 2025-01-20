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
    file_name_without_extension = os.path.splitext(file_name)[0]

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
            "filename": [],  # Add filename column
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
            data["filename"].append(file_name_without_extension)  # Add filename value

        # Convert to a pandas DataFrame
        df = pd.DataFrame(data)

        # Save the dataset to Parquet
        df.to_parquet(output_file, index=False)
        print(f"Processed and saved {file_name} successfully to {output_file}.")
        return output_file

    except Exception as e:
        print(f"Failed to process {file_name}: {e}")
        return None
