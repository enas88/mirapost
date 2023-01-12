import os

# Set the directory where the files are located
directory = 'allDataset/re_tables-0003'

# Set the number of files per folder
files_per_folder = 100

# Get a list of all the files in the directory
files = os.listdir(directory)

# Calculate the number of folders needed
num_folders = len(files) // files_per_folder
if len(files) % files_per_folder != 0:
    num_folders += 1

# Create the folders
for i in range(num_folders):
    folder_name = f're_tables-0003_{i+1}'
    os.mkdir(os.path.join(directory, folder_name))

# Move the files into the appropriate folders
for i, file in enumerate(files):
    folder_index = i // files_per_folder
    folder_name = f're_tables-0003_{folder_index+1}'
    os.rename(os.path.join(directory, file), os.path.join(directory, folder_name, file))
