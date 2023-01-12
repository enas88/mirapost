import os

# Set the base directory where the subdirs are located
base_dir = 'csvFiles'

# Set the name of the new directory where the subsubdirs will be moved
new_dir_name = 'BenchmarkData'

# Create the new directory
os.mkdir(new_dir_name)

# Iterate over the subdirs in the base directory
for subdir in os.listdir(base_dir):
    # Check if the subdir is a directory
    if os.path.isdir(os.path.join(base_dir, subdir)):
        # Iterate over the subsubdirs in the subdir
        for subsubdir in os.listdir(os.path.join(base_dir, subdir)):
            # Check if the subsubdir is a directory
            if os.path.isdir(os.path.join(base_dir, subdir, subsubdir)):
                # Move the subsubdir to the new directory
                os.rename(os.path.join(base_dir, subdir, subsubdir), os.path.join(new_dir_name, subsubdir))
