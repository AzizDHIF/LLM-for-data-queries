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
        
        print(f"\n📤 Sending SPARQL query to Fuseki...")
        print(f"📝 Query:\n{sparql_query}\n")
        
        response = requests.post(
            self.endpoint,
            data={"query": sparql_query},
            headers=headers
        )
        
        if response.status_code != 200:
            print(f"❌ HTTP Error {response.status_code}")
            print(f"📝 Details: {response.text}")
            raise Exception(f"SPARQL Error: {response.status_code} - {response.text}")
        
        print("✅ Query executed successfully\n")
        return response.json()

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