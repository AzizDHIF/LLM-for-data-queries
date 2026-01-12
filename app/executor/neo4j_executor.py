from py2neo import Graph

class Neo4jExecutor:
    def __init__(self, uri, user, password):
        self.graph = Graph(uri, auth=(user, password))

    def run_query(self, cypher_query):
        result = self.graph.run(cypher_query).data()
        return result
