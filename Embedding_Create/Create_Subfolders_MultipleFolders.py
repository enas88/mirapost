import os

# Set the parent directory where the folders are located
parent_directory = 'csvFiles'

# Set the number of files per folder
files_per_folder = 100

# Get a list of all the items in the parent directory
directories = os.listdir(parent_directory)


# Loop through the directories
for directory in directories:

  # Get a list of all the files in the directory
  files = os.listdir(parent_directory + "/" + directory)

  # Calculate the number of folders needed
  num_folders = len(files) // files_per_folder

  if len(files) % files_per_folder != 0:
      num_folders += 1

  # Create the folders
  for i in range(num_folders):
      folder_name = f'{directory}_{i+1}'
      os.mkdir(os.path.join(parent_directory, directory, folder_name))

  # Move the files into the appropriate folders
  for i, file in enumerate(files):
      folder_index = i // files_per_folder
      folder_name = f'{directory}_{folder_index+1}'
      os.rename(os.path.join(parent_directory, directory, file), os.path.join(parent_directory, directory, folder_name, file))
