import os

def delete_small_files(directory, size_limit_kb=20):
    """
    Deletes files smaller than a specified size in a given directory.

    Parameters:
        directory (str): Path to the directory.
        size_limit_kb (int): Size limit in KB. Files smaller than this size will be deleted.
    """
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            # Get file size in KB
            file_size_kb = os.path.getsize(file_path) / 1024
            if file_size_kb < size_limit_kb:
                print(f"Deleting {file_path} ({file_size_kb:.2f} KB)")
                os.remove(file_path)

# Replace 'your_directory_path' with the path to your directory
directory_path = 'your_directory_path'
delete_small_files(directory_path)
