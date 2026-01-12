import streamlit as st
import yaml
from app.schema.schema_extractor import Neo4jSchemaExtractor
from app.llm.gemini_client import GeminiClient
from app.executor.neo4j_executor import Neo4jExecutor
import sys
import os


# --- Charger la config Neo4j ---
with open("config/neo4j.yaml") as f:
    neo4j_cfg = yaml.safe_load(f)

# --- Charger la config Gemini ---
with open("config/gemini.yaml") as f:
    gemini_cfg = yaml.safe_load(f)

# --- Initialiser Neo4j et extraire le schéma ---
extractor = Neo4jSchemaExtractor(
    neo4j_cfg["uri"], neo4j_cfg["user"], neo4j_cfg["password"]
)
schema = extractor.extract_schema()
executor = Neo4jExecutor(
    neo4j_cfg["uri"], neo4j_cfg["user"], neo4j_cfg["password"]
)

# --- Initialiser Gemini LLM ---
gemini_client = GeminiClient(api_key=gemini_cfg["api_key"], model=gemini_cfg["model"])

# --- Interface Streamlit ---
nl_query = st.text_input("Enter your natural language query:", "")

if st.button("Generate Cypher"):
    if nl_query.strip() == "":
        st.warning("Please enter a query first!")
    else:
        # Générer Cypher
        cypher_query = gemini_client.generate_cypher(nl_query, schema)
        st.subheader("Generated Cypher")
        st.code(cypher_query, language="cypher")

        # Exécuter le Cypher
        try:
            result = executor.run_query(cypher_query)
            st.subheader("Result")
            st.write(result)
        except Exception as e:
            st.error(f"Execution Error: {e}")
