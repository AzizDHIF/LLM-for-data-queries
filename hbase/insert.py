import pandas as pd
import happybase
import time
import uuid
import os
time.sleep(10)  # wait for HBase

connection = happybase.Connection(host="localhost", port=9090)
connection.open()
#print(connection.tables())

movies_table = connection.table("movies")

movies = pd.read_csv("movies_updated.csv")

for _, row in movies.iterrows():
    movies_table.put(
        f"movie_{str(uuid.uuid4())}",  # unique row key
        {
            b'info:title': str(row['name']).encode(),
            b'info:genres': str(row['genre']).encode(),
            b'info:year': str(row['year']).encode(),
            b'info:director': str(row['director']).encode(),
            b'info:ratings': str(row['score']).encode()
        }
    )

print("Movies loaded ✔")
