import os
from os.path import isfile, join
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import torch
import json
import threading

from prettytable import PrettyTable

import pg8000


'''
myThread used to create multiple threads it takes 7 parameters
'''
class myThread (threading.Thread):
	
	def __init__(self, symmetric_embedder, lock, sub_directory_name, tables, cellsall, embeddingsall):
		"""Threading to speed the embedding"""
		threading.Thread.__init__(self)
		self.symmetric_embedder = symmetric_embedder
		self.lock = lock
		self.sub_directory_name = sub_directory_name
		self.tables = tables
		self.cellsall = cellsall
		self.embeddingsall = embeddingsall	

	def run(self):
		createEmbeddings(self.symmetric_embedder, self.lock, self.sub_directory_name, self.tables, self.cellsall, self.embeddingsall)


def createEmbeddings(symmetric_embedder, lock, sub_directory_name, tables, cellsall, embeddingsall):
	'''
	Generate the embeddings using the S-BERT model and all-mpnet-base-v2 method 
	'''
	foldertables = []
	foldercells = []
	folderembeds = []
	print("start a new thread")
	i = 0

	files = [f for f in os.listdir(sub_directory_name) if (isfile(join(sub_directory_name, f)) and f.endswith('.csv'))]
	print('found all files')
	for file in files:
		df = pd.read_csv(sub_directory_name +"/" + file, encoding = "ISO-8859-1", on_bad_lines = "skip", sep = ";")
		# cloumn names are taken as any other cell values in the dataframe
		cell_values = df.columns.tolist()

		# cell_values are the same as column_names
		[cell_values.extend([str(x) for x in row]) for row in df.values]

		# The cell values are turned to set in order to remove the redundant cell values from each table
		cell_values = list(set(cell_values))

		# define em which is the embedder and set its parameters
		em = symmetric_embedder.encode(cell_values, convert_to_tensor = True,\
														show_progress_bar = True,\
														batch_size = 128).tolist()

		foldertables.extend([file] * len(file))
		foldercells.extend(cell_values)
		folderembeds.extend(em)
		if i == 100:
			print(f'100 files done')
			i = 0
		else:
			i += 1
	print('start locking')	
	lock.acquire()
	try:
		tables.extend(foldertables)
		cellsall.extend(foldercells)
		embeddingsall.extend(folderembeds)
	finally:
		lock.release()
	print('end locking')	

def main():

	lock = threading.Lock()
	
	tables = []
	cellsall = []
	embeddingsall = []

	#read the dataset path
	datasetpath = f"{os.getcwd()}/csvFiles/"

	#Define the embedder type to start embeddings the csv files
	symmetric_embedder = SentenceTransformer('all-mpnet-base-v2')

	print("Embedding has been built!")

	# Read the dataset directories and enter each one of them
	directories = [f for f in os.listdir(datasetpath)]
	#print("I have found these directories!")
	#print(directories)

	# Write the progress inside a json file, in case of crashing this file contains all the folders content that had been embedded already
	#with open("progresstracking/alreadyEmbedded.json") as fo:
	#	alreadyEmbedded = json.load(fo)

	#todosdirectories = [x for x in directories if x not in alreadyEmbedded]
	#print("Let's start some threads!")

	'''Adjust threads number'''
	for i in range(0, len(directories), 50):
		group = directories[i:i+50]
		for j, directory_name in enumerate(group):
			variable_name = f"thread_{j}"
			locals()[variable_name] = myThread(symmetric_embedder, lock, datasetpath+directory_name, tables, cellsall, embeddingsall)
			locals()[variable_name].start()
			#print(f"{locals()[variable_name]} has been started!")
		
		for j, directory_name in enumerate(group):
			locals()[variable_name].join()
			#print(f"{locals()[variable_name]} Ended!")

	query = "Hymns To The Stone"
	query_embedding = symmetric_embedder.encode(query, convert_to_tensor = True)

	cos_scores = util.cos_sim(query_embedding, embeddingsall)[0]
	top_results = torch.topk(cos_scores, k = 3, sorted = True) #len(embeddings_all), sorted = True)
	print("Results foi")
	toBePrintedResults = []
	for score, idx in zip(top_results[0], top_results[1]):
		toBePrintedResults.append([tables[idx], cellsall[idx], "{:.4f}".format(score)])
	print("============================")
	print(f"your query is: {query}")
	table = [['Filename', 'Cell Value', 'Score']]
	for i in toBePrintedResults:
		table.append([i[0], i[1], i[2]])#"{:.4f}".format(i[2])])
	tab = PrettyTable(table[0])
	tab.add_rows(table[1:])
	print(tab)

if __name__ == "__main__":
	main()