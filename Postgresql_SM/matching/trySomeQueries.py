import os
from os.path import isfile, join
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import torch

from prettytable import PrettyTable

import pg8000


# Set the batch size for the fetchmany() method
batch_size = 1100

print("Program started")
symmetric_embedder = SentenceTransformer('all-mpnet-base-v2')
print("Sentence TRanformers is built")



# Connect to the database
conn = pg8000.connect(database="miracolousdb", user="nosa", password="nosaroses88", host="localhost", port=5432)
print("Connection to database has been established")


# Create a cursor
cur = conn.cursor()

# Select all the data from the table
cur.execute("SELECT tablenames, cellvalues, embeddings FROM embeddingsTable")


# Fetch all the rows
databaseresults = []

batch = cur.fetchmany(batch_size)
while batch:
	databaseresults.extend(batch)
	batch = cur.fetchmany(batch_size)

databaseresults.extend(batch)
print("Data had been fetched")


# Extract the items from the nested list and create three separate lists
tablenames, cellvalues, embeddings_lists = zip(*databaseresults)

# Convert the elements in the third list to torch tensors
embeddings_tensors = [torch.tensor(em).unsqueeze(0) for em in embeddings_lists]

# Concatenate the tensors
embeddings_all = torch.cat(embeddings_tensors, dim = 0)
print("Embeddings are ready")

query = "[Bolivia|Bolivia]"
query_embedding = symmetric_embedder.encode(query, convert_to_tensor = True)
print("Encoding query is done")



top_k = min(20, len(embeddings_all))
cos_scores = util.cos_sim(query_embedding, embeddings_all)[0]
top_results = torch.topk(cos_scores, k = 3, sorted = True) #len(embeddings_all), sorted = True)
print("Results foi")

toBePrintedResults = []
for score, idx in zip(top_results[0], top_results[1]):
	toBePrintedResults.append([tablenames[idx], cellvalues[idx], "{:.4f}".format(score)])
print("============================")
print(f"your query is: {query}")
table = [['Filename', 'Cell Value', 'Score']]
for i in toBePrintedResults:
	table.append([i[0], i[1], i[2]])#"{:.4f}".format(i[2])])
tab = PrettyTable(table[0])
tab.add_rows(table[1:])
print(tab)


## Close the cursor and connection
cur.close()
conn.close()

