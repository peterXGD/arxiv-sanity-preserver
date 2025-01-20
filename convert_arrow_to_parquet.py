from datasets import Dataset, DatasetDict

# Load the .arrow file
#arrow_dataset = Dataset.load_from_disk("/Users/lansaber/Arxiv_downloader/chunked_dataset")
parquet_dataset = Dataset.from_parquet("/Users/lansaber/Arxiv_downloader/data/2006.02768v1.html.parquet")
# Save the dataset as .parquet
print("number of rows is", len(parquet_dataset) ," rows")
print(next(iter(parquet_dataset)))
# arrow_dataset.to_parquet("/Users/lansaber/Arxiv_downloader/chunked_dataset_strict.parquet")

print("Dataset saved as Parquet!")
