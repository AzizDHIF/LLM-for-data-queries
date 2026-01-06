# connector.py
import json

class MockMongoConnector:
    def __init__(self, path="data/mongo_amazon.json"):
        with open(path, "r") as f:
            self.data = json.load(f)

    def execute_query(self, query_func):
        """
        query_func : fonction Python qui simule la query MongoDB sur self.data
        """
        return query_func(self.data)
