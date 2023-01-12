import json
import pandas as pd
import os
from os import listdir
from os.path import isfile, join

def read_json(filename: str) -> dict:

	try:
		with open(filename, "r") as f:
			data = json.loads(f.read())
	except:
		raise Exception(f"Reading {filename} file encountered an error")

	return data

def main():
	# Read the JSON file as python dictionary
	data = read_json(filename="re_tables-0001.json")

	# Normalize the nested python dict
	# new_data = normalize_json(data=data)

	# Pretty print the new dict object
	print("New dict:",data)# new_data)

	# Generate the desired CSV data
	csv_data = generate_csv_data(data=data)#new_data)

	# Save the generated CSV data to a CSV file
	write_to_file(data=csv_data, filepath="data.csv")


if __name__ == '__main__':

	files = [f for f in listdir("tables_redi2_1") if isfile(join("tables_redi2_1", f))]
	i = 0

	for file in files:
		i+=1
		print(i)
		data = read_json("tables_redi2_1/"+file)
		os.mkdir('csvFiles/'+ file[:-5])

	
		for tablename, value in data.items():
			csv_content = {} 
			column_numbering = 0
			for column_name in value["title"]:
				csv_content[str(column_numbering) + "_" + column_name] = []
				for column_content in value["data"]:
					csv_content[str(column_numbering) + "_" + column_name].append(column_content[column_numbering])
				column_numbering += 1
	
				df = pd.DataFrame(csv_content)
			df.to_csv("csvFiles/"+file[:-5]+"/"+tablename + ".csv", index = False, sep= ";")
			