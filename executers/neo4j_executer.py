from py2neo import Graph
from utils.neo4j_llm_utils import detect_query_type
class Neo4jExecutor:
    def __init__(self, uri, user, password):
        self.graph = Graph(uri, auth=(user, password))

    def run_query(self, cypher_query: str):
        query_type = detect_query_type(cypher_query)

        if query_type == "write":
            self.graph.run(cypher_query)
            return {"Database updated successfully"}


        

        if query_type == "read":
            result = self.graph.run(cypher_query).data()
            return {"result": result}