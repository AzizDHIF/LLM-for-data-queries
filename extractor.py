import requests
import os

class RDF_DATA:
    def __init__(self, endpoint):
        self.endpoint = endpoint
    def extract_ontology_from_fuseki(self) -> dict:
        with open("ontology.ttl","r") as file:
            result=file.readlines()

        return result

        



    def run_sparql_query(self, sparql_query: str) -> dict:
        """
        Exécute une requête SPARQL sur Fuseki.
        """
        headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": "NL2SPARQL-StudentProject"
        }
        
        print(f"\n🔍 Recherche en cours dans la base de données...")
        
        # Affichage optionnel de la requête technique (peut être commenté)
        if hasattr(self, 'debug_mode') and self.debug_mode:
            print(f"📝 Requête technique:\n{sparql_query}\n")
        
        try:
            response = requests.post(
                self.endpoint,
                data={"query": sparql_query},
                headers=headers,
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"❌ Erreur lors de la recherche")
                print(f"💡 Le serveur a renvoyé une erreur. Veuillez vérifier votre connexion.")
                raise Exception(f"Erreur de connexion au serveur (Code: {response.status_code})")
            
            print("✅ Recherche terminée avec succès\n")
            return response.json()
            
        except requests.exceptions.Timeout:
            print("⏱️ La recherche a pris trop de temps. Veuillez réessayer.")
            raise Exception("Délai d'attente dépassé")
        except requests.exceptions.ConnectionError:
            print("🔌 Impossible de se connecter au serveur de données.")
            raise Exception("Erreur de connexion")
        except Exception as e:
            print(f"❌ Une erreur est survenue: {str(e)}")
            raise

if __name__ == '__main__':
    sparql_query = """
PREFIX ex: <http://example.org/movies/>
SELECT ?film ?title WHERE {
  ?film a ex:Film ;
        ex:title ?title ;
        ex:actor ex:Leonardo_DiCaprio .
}
"""
    my_data = RDF_DATA("http://localhost:3030/movies/sparql")
    
    print("voici l'ontology: \n")
    print(my_data.extract_ontology_from_fuseki())
    
    print(f"\nvoici le résultat de l'execution de cette requête:\n{sparql_query}\n")
    print(my_data.run_sparql_query(sparql_query))