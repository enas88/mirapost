import torch
import pg8000

# Connect to the database
conn = pg8000.connect(database="miracolousdb", user="nosa", password="nosaroses88", host="localhost", port=5432)

# Create a cursor
cur = conn.cursor()

# Select all the data from the table
cur.execute("SELECT tablenames FROM embeddingsTable")

# Fetch all the rows
rows = cur.fetchall()

# Print the rows
for row in rows:
    print(type(row))
    exit()


"""
# This code was used to delete the testdata

# Delete rows where the tablenames column is 'TestTable'
cur.execute("ALTER TABLE  embeddingsTable DROP COLUMN columnnames")
"""
# Commit the changes
# conn.commit()

# Close the cursor and connection
cur.close()
conn.close()