import pandas as pd
import os

# Directories for shards
output_folder = "/Users/lansaber/Arxiv_downloader/chunked_dataset"

# Helper function to load all shards of a split
def load_all_shards(split_name, folder):
    # Find all shard files for the given split
    shard_files = [
        os.path.join(folder, f) for f in os.listdir(folder) if f.startswith(f"{split_name}_shard")
    ]
    # Load and concatenate all shards into a single DataFrame
    return pd.concat([pd.read_parquet(file) for file in shard_files], ignore_index=True)

# Load train, validation, and test shards
train_df = load_all_shards("train", output_folder)
valid_df = load_all_shards("valid", output_folder)
test_df = load_all_shards("test", output_folder)

# Check for duplicates between splits
train_valid_overlap = pd.merge(train_df, valid_df, how="inner")
train_test_overlap = pd.merge(train_df, test_df, how="inner")
valid_test_overlap = pd.merge(valid_df, test_df, how="inner")

# Report results
if not train_valid_overlap.empty:
    print(f"Duplicate entries found between Train and Validation:\n{train_valid_overlap}")
else:
    print("No duplicates between Train and Validation.")

if not train_test_overlap.empty:
    print(f"Duplicate entries found between Train and Test:\n{train_test_overlap}")
else:
    print("No duplicates between Train and Test.")

if not valid_test_overlap.empty:
    print(f"Duplicate entries found between Validation and Test:\n{valid_test_overlap}")
else:
    print("No duplicates between Validation and Test.")
