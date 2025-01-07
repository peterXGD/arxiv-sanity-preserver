from datasets import Dataset, DatasetDict

# Load the .arrow file
arrow_dataset = Dataset.load_from_disk("//Users/lansaber/Arxiv_downloader/chunked_dataset")

# Save the dataset as .parquet
arrow_dataset.to_parquet("/Users/lansaber/Arxiv_downloader/chunked_dataset_strict.parquet")

print("Dataset saved as Parquet!")
