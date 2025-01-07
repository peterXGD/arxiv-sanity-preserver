from transformers import AutoTokenizer
from docling.chunking import HybridChunker
from docling.document_converter import DocumentConverter

DOC_SOURCE = "/Users/lansaber/Arxiv_downloader/data/html_no_references/2009.13586v6.html"

doc = DocumentConverter().convert(source=DOC_SOURCE).document
model_id = "meta-llama/Llama-3.3-70B-Instruct"
#EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
MAX_TOKENS = 2000

tokenizer = AutoTokenizer.from_pretrained(model_id,
                                          use_auth_token="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"
                                          )

chunker = HybridChunker(
    tokenizer=tokenizer,  # can also just pass model name instead of tokenizer instance
    max_tokens=MAX_TOKENS,  # optional, by default derived from tokenizer
    # merge_peers=True,  # optional, defaults to True
)
chunk_iter = chunker.chunk(dl_doc=doc)
chunks = list(chunk_iter)

for i, chunk in enumerate(chunks):
    print(f"=== {i} ===")
    txt_tokens = len(tokenizer.tokenize(chunk.text, max_length=None))
    print(f"chunk.text ({txt_tokens} tokens):\n{repr(chunk.text)}")

    ser_txt = chunker.serialize(chunk=chunk)
    ser_tokens = len(tokenizer.tokenize(ser_txt, max_length=None))
    print(f"chunker.serialize(chunk) ({ser_tokens} tokens):\n{repr(ser_txt)}")

    print()