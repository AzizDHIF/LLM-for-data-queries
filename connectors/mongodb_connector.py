# connectors/mongodb_connector.py

import pandas as pd
from pymongo import MongoClient

class DataLoader:
    def __init__(self, host="localhost", port=27017, username="admin", password="secret",
                 database="sample_mflix", collection="movies"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.collection = collection
        self.client = None
        self.db = None
        self.col = None

    def connect(self):
        """Connecte au MongoDB Docker"""
        self.client = MongoClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            authSource="admin"  # correspond à MONGO_INITDB_ROOT_USERNAME/MONGO_INITDB_ROOT_PASSWORD
        )
        self.db = self.client[self.database]
        self.col = self.db[self.collection]

    def init_data(self):
        """Récupère toutes les données sous forme de DataFrame"""
        if not self.col:
            self.connect()
        data = list(self.col.find())
        # Aplatir imdb si nécessaire
        for d in data:
            if "imdb" in d:
                d["imdb.rating"] = d["imdb"].get("rating")
                d["imdb.votes"] = d["imdb"].get("votes")
                del d["imdb"]
        df = pd.DataFrame(data)
        return df
