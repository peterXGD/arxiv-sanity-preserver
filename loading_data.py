from datasets import load_from_disk

dataset = load_from_disk("/Users/lansaber/Arxiv_downloader/chunked_dataset/")

print(dataset[:1])  # Access specific column
