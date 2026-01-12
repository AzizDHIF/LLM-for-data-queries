def build_cypher_prompt(nl_query, schema):
    """
    Construit un prompt pour le LLM Gemini à partir d'une requête NL et du schéma Neo4j.
    """

    prompt = f"""
You are a Neo4j Cypher expert.
Use only the following schema:

Labels: {schema['labels']}
Relationships: {schema['relationships']}
Properties: {schema['properties']}

Translate the following natural language request into a valid Cypher query:

\"{nl_query}\"

 Generate ONLY the Cypher query. Do NOT add 'cypher', 'Cypher:', or any extra words. 
Example: MATCH (m:Movie) RETURN m.title
.
"""
    return prompt
