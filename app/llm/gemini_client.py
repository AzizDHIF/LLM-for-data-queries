# app/llm/gemini_client.py

from google import genai
from app.llm.prompt_templates import build_cypher_prompt
from app.llm.llm_utils import clean_cypher_output

class GeminiClient:
    """
    Wrapper pour le LLM Gemini : génère des requêtes Cypher à partir de NL.
    Utilise la librairie officielle google-genai.
    """

    def __init__(self, api_key=None, model="gemini-2.5-flash"):
        """
        Initialise le client Gemini.
        :param api_key: Clé API Gemini (optionnelle si variable d'environnement GEMINI_API_KEY)
        :param model: Nom du modèle Gemini
        """
        if api_key is None:
            import os
            api_key = os.getenv("GEMINI_API_KEY")
            if api_key is None:
                raise ValueError("Vous devez fournir une clé Gemini API ou définir GEMINI_API_KEY.")
        
        self.client = genai.Client(api_key=api_key)
        self.model = model

    def generate_cypher(self, nl_query, schema):
        """
        Génère une requête Cypher à partir d'une NL query et d'un schéma Neo4j.
        :param nl_query: str
        :param schema: dict
        :return: str (requête Cypher)
        """
        # Construire le prompt avec le template
        prompt = build_cypher_prompt(nl_query, schema)

        # Appeler Gemini via la librairie officielle
        response = self.client.models.generate_content(
            model=self.model,
            contents=prompt
        )

        raw_output = response.text
        cypher_query = clean_cypher_output(raw_output)
        return cypher_query


# --- Test rapide ---
if __name__ == "__main__":
    from app.schema.schema_extractor import Neo4jSchemaExtractor

    # Config minimale pour test local
    extractor = Neo4jSchemaExtractor("bolt://localhost:7687", "neo4j", "password")
    schema = extractor.extract_schema()

    # Remplace "YOUR_GEMINI_API_KEY" par ta vraie clé
    gemini_client = GeminiClient(api_key="YOUR_GEMINI_API_KEY")

    nl_query = "Find all movies released after 2000"
    cypher = gemini_client.generate_cypher(nl_query, schema)

    print("NL Query:", nl_query)
    print("Generated Cypher:", cypher)
