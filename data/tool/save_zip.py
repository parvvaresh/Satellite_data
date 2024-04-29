import zipfile
import os

def create_zip(directory, zip_filename):
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for foldername, subfolders, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(foldername, filename)
                zipf.write(filepath, os.path.relpath(filepath, directory))

# Example usage:
directory_to_zip = '/path/to/directory'
zip_filename = 'archive.zip'
create_zip(directory_to_zip, zip_filename)
