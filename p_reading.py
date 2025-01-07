import pickle

file_path = "./db.p"

with open(file_path, "rb") as f:
    data = pickle.load(f)

print(data)