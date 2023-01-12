import os
from os.path import isfile, join
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import torch
import json
import threading

import pg8000


'''
myThread used to create multiple threads it takes 7 parameters
'''
class myThread (threading.Thread):
	
	def __init__(self, symmetric_embedder, lock, sub_directory_name, alreadyEmbedded):
		"""Threading to speed the embedding"""
		threading.Thread.__init__(self)
		self.symmetric_embedder = symmetric_embedder
		self.lock = lock
		self.sub_directory_name = sub_directory_name
		self.alreadyEmbedded = alreadyEmbedded
	def run(self):
		createEmbeddings(self.symmetric_embedder, self.lock, self.sub_directory_name, self.alreadyEmbedded)

'''
Generate the embeddings using the S-BERT model and all-mpnet-base-v2 method 
'''
def createEmbeddings(symmetric_embedder, lock, sub_directory_name, alreadyEmbedded):
	print("start a new thread")
	conn = pg8000.connect(database="enas_db", user="enas", password="khwaileh", host="snowwhite", port=5432)

	# Create a cursor
	cur = conn.cursor()
	#print("Connection to database is established!")
	last_dir = os.path.basename(sub_directory_name)
	tbname = last_dir.replace('-', '_')
	cur.execute(f"CREATE TABLE {tbname} (tablenames varchar, cellvalues varchar, embeddings real[])")
	
	print('commited')
	files = [f for f in os.listdir(sub_directory_name) if (isfile(join(sub_directory_name, f)) and f.endswith('.csv'))]

	#Test to make sure the files do ex
	#print("found files")
	#print(len(files))

	# Start the embedding procedure after saving the csv file inside a pandas dataframe
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
														show_progress_bar = False,\
														batch_size = 128).tolist()

			
		# Save the embeddings along with their cell values inside the postgresql table, 
		postgresql_entries_for_file = [(file, cell_values[i], em[i]) for i in range(len(cell_values))]

		# The table in the database has three columns, the tables name, the cell values, and the third column is the cell values embeddings
		cur.executemany(f"INSERT INTO {tbname} (tablenames, cellvalues, embeddings) VALUES (%s, %s, %s)",\
														postgresql_entries_for_file)
		# Notification for each folder containing the tables to make sure it is done embeddings, 1653 folders each has 1000 table
		#print(file + " done")
	#print('Done embedding all files here')	
	# Commit the changes
	conn.commit()

	# Close the cursor and connection
	#close the cursor and the connection with snowwhite server
	cur.close()
	conn.close()
	#print("It was very cold, so I closed the door!!")
	lock.acquire()
	try:
		
		#Save the files status in the json files as a log 
		alreadyEmbedded.append(last_dir)
		# write the status log in the alreadyEmbeeded json file
		with open("progresstracking/alreadyEmbedded.json", "w") as fileobject:
			json.dump(alreadyEmbedded, fileobject)
			#print(f"Log file has been updated! {last_dir} is done!")		
	finally:
		lock.release()
	#print('Thread about to close')

def main():

	lock = threading.Lock()
	# Connect to the database
	

	#read the dataset path
	datasetpath = f"{os.getcwd()}/BenchmarkData/"

	#Define the embedder type to start embeddings the csv files
	symmetric_embedder = SentenceTransformer('all-mpnet-base-v2')

	#print("Embedding has been built!")

	# Read the dataset directories and enter each one of them
	directories = [f for f in os.listdir(datasetpath)]
	#print("I have found these directories!")
	#print(directories)

	# Write the progress inside a json file, in case of crashing this file contains all the folders content that had been embedded already
	with open("progresstracking/alreadyEmbedded.json") as fo:
		alreadyEmbedded = json.load(fo)

	todosdirectories = [x for x in directories if x not in alreadyEmbedded]
	#print("Let's start some threads!")

	'''Adjust threads number'''
	for i in range(0, len(todosdirectories), 18):
		group = todosdirectories[i:i+18]
		for j, directory_name in enumerate(group):
			variable_name = f"thread_{j}"
			locals()[variable_name] = myThread(symmetric_embedder, lock, datasetpath+directory_name, alreadyEmbedded)
			locals()[variable_name].start()
			#print(f"{locals()[variable_name]} has been started!")
		
		for j, directory_name in enumerate(group):			
			locals()[variable_name].join()
			print(f"{locals()[variable_name]} Ended!")


if __name__ == "__main__":
	main()