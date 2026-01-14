# app/llm/neo4j_llm.py

import yaml
from py2neo import Graph
from utils.neo4j_llm_utils import detect_query_type, parse_llm_output
from google import genai
from connectors.api import load_gemini_config

# =========================
# 0️⃣ Charger la config Gemini depuis YAML
# =========================
config = load_gemini_config()
API_KEY = config.get("api_key")
MODEL = config.get("model", "gemini-2.5-pro")

# ------------------------------------------
# 1️⃣ Neo4j Executor : exécuter les requêtes Cypher
# ------------------------------------------
class Neo4jExecutor:
    def __init__(self, uri, user, password):
        self.graph = Graph(uri, auth=(user, password))

    def run_query(self, cypher_query: str):
        query_type = detect_query_type(cypher_query)

        if query_type == "write":
            self.graph.run(cypher_query)
            return {"status": "Database updated successfully"}

        if query_type == "read":
            result = self.graph.run(cypher_query).data()
            return {"result": result}

# ------------------------------------------
# 2️⃣ Neo4j Schema Extractor : extraire le schéma de la DB
# ------------------------------------------
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

# ------------------------------------------
# 3️⃣ Gemini Client : génère les requêtes Cypher depuis NL
# ------------------------------------------
def build_read_prompt(nl_query: str, schema: dict) -> str:
    return f"""
You are a Neo4j Cypher expert.
Use only the following schema:

Labels: {schema['labels']}
Relationships: {schema['relationships']}
Properties: {schema['properties']}

Translate the following natural language request into a valid Cypher query:

\"{nl_query}\"

Generate ONLY the Cypher query. Do NOT add 'cypher', 'Cypher:', or any extra words.
Example: MATCH (m:Movie) RETURN m.title
"""

def build_write_prompt(nl_query: str, schema: dict) -> str:
    return f"""
You are an expert Neo4j Cypher assistant.

The database schema is:

Labels: {schema['labels']}
Relationships: {schema['relationships']}
Properties: {schema['properties']}

Your task is to translate the following natural language request into a Cypher WRITE query.

You MUST answer in ONE of the following formats:

1) If all the details about the write query are present and safe:
format="CYPHER:
<cypher query>"

2) If any detail is missing:
format="QUESTION:
<question to ask the user>"

Rules:

- Do NOT assume missing values
- Do NOT generate Cypher if information is missing
- Do NOT add explanations
- You MUST start your answer with "QUESTION:" or "CYPHER:"

Natural language request:
"{nl_query}"
"""

class GeminiClient:
    """
    Wrapper pour le LLM Gemini : génère des requêtes Cypher à partir de NL.
    """
    def __init__(self, api_key=None, model=None):
        if api_key is None:
            api_key = API_KEY  # utilise la clé du YAML
        if api_key is None:
            raise ValueError("Vous devez fournir une clé Gemini API ou configurer config/gemini.yaml")
        self.client = genai.Client(api_key=api_key)
        self.model = model or MODEL

    def generate_cypher(self, nl_query, schema):
        query_type = detect_query_type(nl_query)

        if query_type == "write":
            prompt = build_write_prompt(nl_query, schema)
            response = self.client.models.generate_content(model=self.model, contents=prompt)
            return parse_llm_output(response.text)

        else:
            prompt = build_read_prompt(nl_query, schema)
            response = self.client.models.generate_content(model=self.model, contents=prompt)
            return response.text

# ------------------------------------------
# 4️⃣ Test rapide
# ------------------------------------------
if __name__ == "__main__":
    # Extraire le schéma
    extractor = Neo4jSchemaExtractor("bolt://localhost:7687", "neo4j", "password")
    schema = extractor.extract_schema()
    print("Schema Neo4j:", schema)

    # Tester le LLM
    gemini_client = GeminiClient()  # utilise directement la clé YAML
    nl_query = "Find all movies released after 2000"
    cypher = gemini_client.generate_cypher(nl_query, schema)
    print("NL Query:", nl_query)
    print("Generated Cypher:", cypher)
