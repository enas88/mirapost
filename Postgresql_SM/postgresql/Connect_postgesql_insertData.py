import torch
import pg8000

# Connect to the database
conn = pg8000.connect(database="miracolousdb", user="nosa", password="nosaroses88", host="localhost", port=5432)

# Create a cursor
cur = conn.cursor()

# Insert some data
col1_value = "TestTable"
col2_value = "Testcolumnname"
col3_value = "TestCellvalue"
col4_value = torch.tensor([1.0, 2.0, 3.0])

# Convert the torch vector to a list of floats
col4_list = col4_value.tolist()

# Insert the data into the table
cur.execute("INSERT INTO embeddingsTable (tablenames, columnnames, cellvalues, embeddings) VALUES (%s, %s, %s, %s)", (col1_value, col2_value, col3_value, col4_list))

# Commit the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
