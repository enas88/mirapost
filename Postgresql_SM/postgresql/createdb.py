import torch
import pg8000

# Connect to the database
conn = pg8000.connect(database="miracolousdb", user="nosa", password="nosaroses88", host="localhost", port=5432)

# Create a cursor
cur = conn.cursor()

# Create the table
cur.execute("CREATE TABLE embeddingsTable (tablenames varchar, columnnames varchar, cellvalues varchar, embeddings real[])")

# Commit the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()