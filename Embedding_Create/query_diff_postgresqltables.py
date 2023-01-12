import os
from os.path import isfile, join
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import torch

from prettytable import PrettyTable

import pg8000

import threading

class myThread (threading.Thread):
	
	def __init__(self, table_name, query_embedding, lock, benchmark, top_similarities_results):
		"""Threading to speed the embedding"""
		threading.Thread.__init__(self)
		self.table_name = table_name
		self.query_embedding = query_embedding
		self.lock = lock
		
		self.benchmark = benchmark
		self.top_similarities_results = top_similarities_results
	def run(self):
		query_search(self.table_name, self.query_embedding, self.lock,  self.benchmark, self.top_similarities_results)



def query_search(table_name, query_embedding, lock, benchmark, top_similarities_results):

	connf = pg8000.connect(database="enas_db", user="enas", password="khwaileh", host="snowwhite", port=5432)
	curf = connf.cursor()

	print('Thread started')
	# Select all the data from the table
	curf.execute(f"SELECT tablenames, cellvalues, embeddings FROM {table_name}")
	print('executing SELECT done')
	# Set the batch size for the fetchmany() method
	batch_size = 1100

	# Fetch all the rows
	databaseresults = []

	batch = curf.fetchmany(batch_size)
	while batch:
		databaseresults.extend(batch)
		batch = curf.fetchmany(batch_size)
	
	databaseresults.extend(batch)
	print("Data had been fetched")


	# Extract the items from the nested list and create three separate lists
	tablenames, cellvalues, embeddings_lists = zip(*databaseresults)

	# Convert the elements in the third list to torch tensors
	embeddings_tensors = [torch.tensor(em).unsqueeze(0) for em in embeddings_lists]

	# Concatenate the tensors
	embeddings_all = torch.cat(embeddings_tensors, dim = 0)
	print("Embeddings are ready")
	cos_scores = util.cos_sim(query_embedding, embeddings_all)[0]

	filtered_scores = [s for sim, s in zip(cos_scores, cellvalues) if sim >= benchmark]
	lock.acquire()
	try:
		top_similarities_results.extend(filtered_scores)
	finally:
		lock.release()
	curf.close()
	connf.close()

	'''
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
	'''

def main():
	lock = threading.Lock()
	top_similarities_results = []
	benchmark = 0.7
	print("Program started")
	symmetric_embedder = SentenceTransformer('all-mpnet-base-v2')
	print("Sentence TRanformers is built")
	# Connect to the database
	conn = pg8000.connect(database="enas_db", user="enas", password="khwaileh", host="snowwhite", port=5432)
	print("Connection to database has been established")
	
	# Create a cursor
	cur = conn.cursor()
	
	# Get the list of table names
	cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
	table_names = [table_name[0] for table_name in cur.fetchall()]
	
	query = "Constituency abolished"
	query_embedding = symmetric_embedder.encode(query, convert_to_tensor = True)
	print("Encoding query is done")

	for i in range(0, len(table_names), 50):
		group = table_names[i:i+50]
		for j, table_name in enumerate(group):
			variable_name = f"thread_{j}"
			locals()[variable_name] = myThread(table_name, query_embedding, lock, benchmark, top_similarities_results)
			locals()[variable_name].start()
		for j, table_name in enumerate(group):
			locals()[variable_name].join()

	print(top_similarities_results)
	cur.close()
	conn.close()

if __name__ == "__main__":
	main()