# app/llm/gemini_client.py

from google import genai

from app.llm.prompt_templates import (
    build_read_prompt,
    build_write_prompt
)
from app.llm.llm_utils import (
    clean_cypher_output,
    detect_query_type,
    parse_llm_output

)
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
     
        query_type = detect_query_type(nl_query)

        if query_type == "write":

            prompt = build_write_prompt(nl_query, schema)
            response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
            
            )
            return parse_llm_output(response.text)

            
        else:
            prompt = build_read_prompt(nl_query, schema)
            response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
            
            )


            return response.text


       
        

        



# --- Test rapide ---
if __name__ == "__main__":




    

