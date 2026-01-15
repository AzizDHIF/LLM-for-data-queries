from py2neo import Graph

class Neo4jSchemaExtractor:
    def __init__(self, uri, user, password):
        self.graph = Graph(uri, auth=(user, password))

    def get_labels(self):
        labels_data = self.graph.run("CALL db.labels()").data()
        return [l["label"] for l in labels_data]

    def get_relationship_types(self):
        rels_data = self.graph.run("CALL db.relationshipTypes()").data()
        return [r["relationshipType"] for r in rels_data]

    def get_properties(self):
        props = {}
        labels = self.get_labels()
        for label in labels:
            keys_data = self.graph.run(f"""
                MATCH (n:{label})
                UNWIND keys(n) AS key
                RETURN DISTINCT key
            """).data()
            props[label] = [k["key"] for k in keys_data]
        return props

    def extract_schema(self):
        return {
            "labels": self.get_labels(),
            "relationships": self.get_relationship_types(),
            "properties": self.get_properties()
        }

if __name__ == "__main__":
    extractor = Neo4jSchemaExtractor("bolt://localhost:7687", "neo4j", "Azizdhif30032001")
    schema = extractor.extract_schema()
    print(schema)
