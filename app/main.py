import yaml
from app.schema.schema_extractor import Neo4jSchemaExtractor
from app.llm.gemini_client import GeminiClient
from app.translator.nl_to_cypher import NLToCypherTranslator
from app.executor.neo4j_executor import Neo4jExecutor
import json

# --- Charger la config ---
with open("config/neo4j.yaml") as f:
    neo4j_cfg = yaml.safe_load(f)

with open("config/gemini.yaml") as f:
    gemini_cfg = yaml.safe_load(f)

# --- Extraire le schema ---
extractor = Neo4jSchemaExtractor(
    neo4j_cfg["uri"], neo4j_cfg["user"], neo4j_cfg["password"]
)
schema = extractor.extract_schema()

# --- Initialiser LLM ---
gemini_client = GeminiClient(api_key=gemini_cfg["api_key"], model=gemini_cfg["model"])
translator = NLToCypherTranslator(gemini_client)

# --- Initialiser Neo4j Executor ---
executor = Neo4jExecutor(
    neo4j_cfg["uri"], neo4j_cfg["user"], neo4j_cfg["password"]
)

# --- Charger les requêtes NL ---
with open("data/queries/nl_queries.json") as f:
    nl_queries = json.load(f)

# --- Pipeline ---
for nl_query in nl_queries:
    print(f"\nNL Query: {nl_query}")
    cypher_query = translator.translate(nl_query, schema)
    print(f"Generated Cypher: {cypher_query}")
    try:
        result = executor.run_query(cypher_query)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Execution Error: {e}")
