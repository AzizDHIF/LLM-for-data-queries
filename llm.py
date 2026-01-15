# app/llm/gemini_client.py
import yaml
from google import genai
from extractor import RDF_DATA


def read_prompt_rdf(nl_query: str, ontology: str) -> str:
     
     return f"""#task:
     You are an expert in SPARQL. Generate a SPARQL query to answer the following natural language question using ONLY the ontology provided.
#Natural language query:
{nl_query}
#Rules:

- Never invent classes, properties, prefixes, individuals, or literals.
- Output ONLY a SPARQL query, no explanations, no comments, start directly with the query.
- the output you will give is a sparql ready to be executed , so make sure you output only a sparql query
- Always respect the ontology below
- the output should not start with the word sparql, it should start directly with the query
- no hallucinations

#Ontology:
{ontology}


"""

class GeminiClient:
    """
    Wrapper pour le LLM Gemini : génère des requêtes Cypher à partir de NL.
    Utilise la librairie officielle google-genai.
    """

    def __init__(self, api_key=None, model="gemini-2.5-pro"):
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





    def generate_rdf(self, nl_query, ontology):
     
            prompt=read_prompt_rdf(nl_query,ontology)

            
            response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,)

            execution=rdf_data.run_sparql_query(response.text)

            


            return response.text +f"\n   execution: {execution}"


       
        

        



# --- Test rapide ---
if __name__ == "__main__":
    rdf_data=RDF_DATA("http://localhost:3030/movies/sparql")
    with open("config/gemini.yaml") as f:
         gemini_cfg = yaml.safe_load(f)


    # Remplace "YOUR_GEMINI_API_KEY" par ta vraie clé
    gemini_client = GeminiClient(gemini_cfg["api_key"])

    nl_query = "how many actors?"
    sparql = gemini_client.generate_rdf(nl_query, rdf_data.extract_ontology_from_fuseki())

    print("NL Query:", nl_query)
    print("Generated Sparql:", sparql)
