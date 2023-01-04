import os
from os.path import isfile, join
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import torch

import pg8000

symmetric_embedder = SentenceTransformer('all-mpnet-base-v2')

# Connect to the database
conn = pg8000.connect(database="miracolousdb", user="nosa", password="nosaroses88", host="localhost", port=5432)

# Create a cursor
cur = conn.cursor()

files = [f for f in os.listdir("../data/") if (isfile(join("../data/", f)) and f.endswith('.csv'))]
print("found files")
print(files)

for file in files:
	df = pd.read_csv( "../data/" + file, encoding = "ISO-8859-1", on_bad_lines = "skip", sep = ";")
	cell_values = df.columns.tolist()

	# cell_values = column_names
	[cell_values.extend([x for x in row]) for row in df.values]

	em = symmetric_embedder.encode(cell_values, convert_to_tensor = True,\
                                                        show_progress_bar = False,\
                                                        batch_size = 128).tolist()
	
	postgresql_entries_for_file = [(file, cell_values[i], em[i]) for i in range(len(cell_values))]

	cur.executemany("INSERT INTO embeddingsTable (tablenames, cellvalues, embeddings) VALUES (%s, %s, %s)",\
		 												postgresql_entries_for_file)
	
	print(file + " done")
# Commit the changes
conn.commit()
#
## Close the cursor and connection
cur.close()
conn.close()