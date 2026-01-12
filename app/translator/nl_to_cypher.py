from app.llm.gemini_client import GeminiClient

class NLToCypherTranslator:
    def __init__(self, gemini_client):
        self.gemini_client = gemini_client

    def translate(self, nl_query, schema):
        cypher_query = self.gemini_client.generate_cypher(nl_query, schema)
        return cypher_query
