import csv
from llm import GeminiClient,_display_user_friendly_results,read_prompt_rdf
import yaml
from google import genai
from extractor import RDF_DATA
data = [
    ("List all films.",
     "SELECT ?film WHERE { ?film a ex:Film . }",
     ""),

    ("Show all film titles with their release year.",
     "SELECT ?title ?year WHERE { ?film a ex:Film ; ex:title ?title ; ex:year ?year . }",
     ""),

    ("Which films were directed by Christopher Nolan?",
     "SELECT ?title WHERE { ?film ex:director ex:Christopher_Nolan ; ex:title ?title . }",
     ""),

    ("Which films feature Leonardo DiCaprio?",
     "SELECT ?title WHERE { ?film ex:actor ex:Leonardo_DiCaprio ; ex:title ?title . }",
     ""),

    ("List films produced by Emma Thomas.",
     "SELECT ?title WHERE { ?film ex:producer ex:Emma_Thomas ; ex:title ?title . }",
     ""),

    ("Which films were released after 2010?",
     "SELECT ?title WHERE { ?film ex:title ?title ; ex:year ?year . FILTER(?year > 2010) }",
     ""),

    ("Which films have a rating higher than 8.5?",
     "SELECT ?title ?rating WHERE { ?film ex:title ?title ; ex:rating ?rating . FILTER(?rating > 8.5) }",
     ""),

    ("List all science fiction films.",
     "SELECT ?title WHERE { ?film ex:hasGenre ex:ScienceFiction ; ex:title ?title . }",
     ""),

    ("Which films belong to more than one genre?",
     """SELECT ?title (COUNT(?genre) AS ?nbGenres)
        WHERE { ?film ex:title ?title ; ex:hasGenre ?genre . }
        GROUP BY ?title
        HAVING(COUNT(?genre) > 1)""",
     ""),

    ("Which films were directed and produced by the same person?",
     "SELECT ?title WHERE { ?film ex:title ?title ; ex:director ?p ; ex:producer ?p . }",
     ""),

    ("Which films were produced in the USA?",
     "SELECT ?title WHERE { ?film ex:title ?title ; ex:hasCountry ex:USA . }",
     ""),

    ("Which films are longer than 150 minutes?",
     "SELECT ?title WHERE { ?film ex:title ?title ; ex:duration ?d . FILTER(?d > 150) }",
     ""),

    ("Which actors have appeared in more than one film?",
     """SELECT ?actor (COUNT(?film) AS ?count)
        WHERE { ?film ex:actor ?actor . }
        GROUP BY ?actor
        HAVING(COUNT(?film) > 1)""",
     ""),

    ("How many films are in the database?",
     "SELECT (COUNT(?film) AS ?count) WHERE { ?film a ex:Film . }",
     ""),

    ("List films released between 2000 and 2015.",
     "SELECT ?title WHERE { ?film ex:title ?title ; ex:year ?year . FILTER(?year >= 2000 && ?year <= 2015) }",
     ""),

    ("Which films starring Leonardo DiCaprio were released after 2000?",
     "SELECT ?title WHERE { ?film ex:title ?title ; ex:actor ex:Leonardo_DiCaprio ; ex:year ?year . FILTER(?year > 2000) }",
     ""),

    ("Which Christopher Nolan films have a rating above 8.7?",
     "SELECT ?title WHERE { ?film ex:title ?title ; ex:director ex:Christopher_Nolan ; ex:rating ?rating . FILTER(?rating > 8.7) }",
     ""),

    ("What is the average release year of all films?",
     "SELECT (AVG(?year) AS ?avgYear) WHERE { ?film ex:year ?year . }",
     ""),

    ("Which films were produced by someone different from the director?",
     "SELECT ?title WHERE { ?film ex:title ?title ; ex:director ?d ; ex:producer ?p . FILTER(?d != ?p) }",
     ""),

    ("What are the most common film genres?",
     "SELECT ?genre (COUNT(?film) AS ?count) WHERE { ?film ex:hasGenre ?genre . } GROUP BY ?genre ORDER BY DESC(?count)",
     "")
]




rdf_data=RDF_DATA("http://localhost:3030/movies/sparql")
with open("config/gemini.yaml") as f:
   gemini_cfg = yaml.safe_load(f)


# Remplace "YOUR_GEMINI_API_KEY" par ta vraie clé
gemini_client = GeminiClient(gemini_cfg["api_key"])


    
   



with open("nl_sparql_execution_dataset.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter=";")
    writer.writerow(["Natural language", "generated_SPARQL","the correct sparql","generated execution","correct execution"])
    for tuples in data:
      question=tuples[0]
      true_sparql=tuples[1]
      sparql = gemini_client.generate_rdf_no_execution(question, rdf_data.extract_ontology_from_fuseki())
      gen_execution=rdf_data.run_sparql_query(sparql)
      #correct_execution=rdf_data.run_sparql_query(true_sparql)
      
      writer.writerow([
         question,
         sparql,
         true_sparql,
         #gen_execution,
         #correct_execution
         
      ])

print("CSV file created: nl_sparql_execution_dataset.csv")



