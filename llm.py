# app/llm/gemini_client.py
import yaml
from google import genai
from extractor import RDF_DATA
def _display_user_friendly_results( result: dict) -> str:
    """
    Formate les résultats de manière compréhensible pour un utilisateur non technique.
    Retourne une chaîne de caractères formatée.
    """
    bindings = result.get('results', {}).get('bindings', [])
    
    if not bindings:
        return "ℹ️ Aucun résultat trouvé."
    
    # Cas spécial : comptage
    if len(bindings) == 1 and 'count' in bindings[0]:
        count = bindings[0]['count']['value']
        return f"📊 Nombre de résultats : {count}"
    
    # Affichage des résultats normaux
    output_lines = [f"📋 {len(bindings)} résultat(s) trouvé(s):\n"]
    
    for i, binding in enumerate(bindings, 1):
        values = [item['value'] for item in binding.values()]
        output_lines.append(f"  {i}. {' | '.join(values)}")
    
    return "\n".join(output_lines)

def read_prompt_rdf(nl_query: str, ontology: str) -> str:
     
     return f"""#task:
     You are an expert in SPARQL. Generate a SPARQL query to answer the following natural language question using ONLY the ontology provided.
#Natural language query:
{nl_query}
#Rules:
- Use only classes, properties, and namespaces explicitly defined in the provided RDF schema.
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

            execution=_display_user_friendly_results(rdf_data.run_sparql_query(response.text))

            


            return response.text +f"\n   Result of the query: {execution}"
    
    def generate_rdf_no_execution(self, nl_query, ontology):
     
            prompt=read_prompt_rdf(nl_query,ontology)

            
            response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,)
            return response.text 


       
        

        



# --- Test rapide ---
if __name__ == "__main__":
    rdf_data=RDF_DATA("http://localhost:3030/movies/sparql")
    with open("config/gemini.yaml") as f:
         gemini_cfg = yaml.safe_load(f)


    # Remplace "YOUR_GEMINI_API_KEY" par ta vraie clé
    gemini_client = GeminiClient(gemini_cfg["api_key"])

    nl_query = "Show all film titles with their release year."
    sparql = gemini_client.generate_rdf(nl_query, rdf_data.extract_ontology_from_fuseki())
    
    print("NL Query:", nl_query)
    print("Generated Sparql:", sparql)
