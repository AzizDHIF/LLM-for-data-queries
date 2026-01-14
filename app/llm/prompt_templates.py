def build_read_prompt(nl_query: str, schema: str) -> str:
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
.
"""



def build_write_prompt(nl_query: str, schema: str) -> str:
    return f"""
You are an expert Neo4j Cypher assistant.

The database schema is:

Labels: {schema['labels']}
Relationships: {schema['relationships']}
Properties: {schema['properties']}

Your task is to translate the following natural language request into a Cypher WRITE query.


You MUST answer in ONE of the following formats:

1) If all the details about the write query are present and safe :
format="CYPHER:
<cypher query>"

2) If any detail  is missing:
format="QUESTION:
<question to ask the user>"

Rules:

- Do NOT assume missing values
- Do NOT generate Cypher if information is missing
- Do NOT add explanations
- You MUST start your answer with "QUESTION:" or "CYPHER:". Don't start with something else, don't even say hi.

Natural language request:
"{nl_query}"


"""
