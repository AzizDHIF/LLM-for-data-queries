import streamlit as st
import yaml
from app.schema.schema_extractor import Neo4jSchemaExtractor
from app.llm.gemini_client import GeminiClient
from app.executor.neo4j_executor import Neo4jExecutor
import sys
import os
from  app.llm.llm_utils import detect_query_type

# --- Charger les configs ---
with open("config/neo4j.yaml") as f:
    neo4j_cfg = yaml.safe_load(f)

with open("config/gemini.yaml") as f:
    gemini_cfg = yaml.safe_load(f)

# --- Initialiser Neo4j ---
extractor = Neo4jSchemaExtractor(
    neo4j_cfg["uri"],
    neo4j_cfg["user"],
    neo4j_cfg["password"]
)
schema = extractor.extract_schema()

executor = Neo4jExecutor(
    neo4j_cfg["uri"],
    neo4j_cfg["user"],
    neo4j_cfg["password"]
)

# --- Initialiser Gemini ---
gemini_client = GeminiClient(api_key=gemini_cfg["api_key"], model=gemini_cfg["model"])

# --- Interface Streamlit ---
st.title("NL → Cypher with Gemini + Neo4j")

nl_query = st.text_input("Enter your natural language query:")

if st.button("Generate Cypher"):
    if not nl_query.strip():
        st.warning("Please enter a query first!")
    else:
        response = gemini_client.generate_cypher(nl_query, schema)

        
        if detect_query_type(nl_query)=="read":
            # Générer Cypher
            
            st.subheader("Generated Cypher")
            st.code(response, language="cypher")

            # Exécuter le Cypher
            try:
                result = executor.run_query(response)
                st.subheader("Result")
                st.write(result)
            except Exception as e:
                st.error(f"Execution Error: {e}")
        
        elif detect_query_type(nl_query)=="write":
            if response["type"] == "clarification":
                st.warning("More information is required")
                st.write(response["content"])
            elif response["type"] == "cypher":
                result = executor.run_query(response["content"])
                st.subheader("Result")
                st.write(response["content"])

            
            
        """
        # 🔴 Erreur LLM
        if response["type"] == "error":
            st.error("LLM Error")
            st.write(response["content"])


        # 🔵 Cypher généré
        elif response["type"] == "cypher":
            cypher_query = response["content"]

            st.subheader("Generated Cypher")
            st.code(cypher_query, language="cypher")

            # Exécution Neo4j
            try:
                result = executor.run_query(cypher_query)

                st.subheader("Result")

                if result["type"] == "write":
                    st.success(result["message"])

                elif result["type"] == "aggregation":
                    value = list(result["result"][0].values())[0]
                    st.metric("Result", value)

                else:
                    st.dataframe(result["result"])

            except Exception as e:
                st.error(f"Execution Error: {e}")"""
