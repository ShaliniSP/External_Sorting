import os

# Get a list of all files in the current directory
files = os.listdir()

# Iterate over the files and delete those starting with "temp"
for file in files:
    if file.startswith("temp"):
        os.remove(file)