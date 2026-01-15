import csv
from llm import GeminiClient,_display_user_friendly_results,read_prompt_rdf
import yaml
from google import genai
from extractor import RDF_DATA
data = [

("How many films are in the database?",
"""
PREFIX ex: <http://example.org/movies/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT (COUNT(?film) AS ?count)
WHERE {
  ?film rdf:type ex:Film .
}
"""),

("List all film titles.",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title
WHERE {
  ?film ex:title ?title .
}
"""),

("List all films with their release year.",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title ?year
WHERE {
  ?film ex:title ?title ;
        ex:year ?year .
}
"""),

("How many actors are there?",
"""
PREFIX ex: <http://example.org/movies/>

SELECT (COUNT(DISTINCT ?actor) AS ?count)
WHERE {
  ?film ex:actor ?actor .
}
"""),

("List all directors.",
"""
PREFIX ex: <http://example.org/movies/>

SELECT DISTINCT ?name
WHERE {
  ?film ex:director ?p .
  ?p ex:name ?name .
}
"""),

("List all films released after 2010.",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title
WHERE {
  ?film ex:title ?title ;
        ex:year ?year .
  FILTER(?year > 2010)
}
"""),

("List all films longer than 150 minutes.",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title
WHERE {
  ?film ex:title ?title ;
        ex:duration ?d .
  FILTER(?d > 150)
}
"""),

("How many films have a rating higher than 8.5?",
"""
PREFIX ex: <http://example.org/movies/>

SELECT (COUNT(?film) AS ?count)
WHERE {
  ?film ex:rating ?rating .
  FILTER(?rating > 8.5)
}
"""),

("List all films and their genres.",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title ?genre
WHERE {
  ?film ex:title ?title ;
        ex:hasGenre ?genre .
}
"""),

("List all films and their production countries.",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title ?country
WHERE {
  ?film ex:title ?title ;
        ex:hasCountry ?country .
}
"""),

("Which films were directed and produced by the same person?",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title
WHERE {
  ?film ex:title ?title ;
        ex:director ?p ;
        ex:producer ?p .
}
"""),

("Which actors appeared in more than one film?",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?actor (COUNT(?film) AS ?count)
WHERE {
  ?film ex:actor ?actor .
}
GROUP BY ?actor
HAVING(COUNT(?film) > 1)
"""),

("What is the average rating of all films?",
"""
PREFIX ex: <http://example.org/movies/>

SELECT (AVG(?rating) AS ?avgRating)
WHERE {
  ?film ex:rating ?rating .
}
"""),

("What is the average film duration?",
"""
PREFIX ex: <http://example.org/movies/>

SELECT (AVG(?d) AS ?avgDuration)
WHERE {
  ?film ex:duration ?d .
}
"""),

("List all people born before 1970.",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?name
WHERE {
  ?p ex:name ?name ;
     ex:birthYear ?year .
  FILTER(?year < 1970)
}
"""),

("Which films feature an actor named Leonardo DiCaprio?",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title
WHERE {
  ?film ex:title ?title ;
        ex:actor ?p .
  ?p ex:name "Leonardo DiCaprio" .
}
"""),

("Which films were directed by someone born after 1960?",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title
WHERE {
  ?film ex:title ?title ;
        ex:director ?d .
  ?d ex:birthYear ?year .
  FILTER(?year > 1960)
}
"""),

("List all genres with the number of films per genre.",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?genre (COUNT(?film) AS ?count)
WHERE {
  ?film ex:hasGenre ?genre .
}
GROUP BY ?genre
"""),

("What is the highest rated film?",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title ?rating
WHERE {
  ?film ex:title ?title ;
        ex:rating ?rating .
}
ORDER BY DESC(?rating)
LIMIT 1
"""),

("List films released between 2000 and 2015.",
"""
PREFIX ex: <http://example.org/movies/>

SELECT ?title
WHERE {
  ?film ex:title ?title ;
        ex:year ?year .
  FILTER(?year >= 2000 && ?year <= 2015)
}
""")

]





rdf_data=RDF_DATA("http://localhost:3030/movies/sparql")
with open("config/gemini.yaml") as f:
   gemini_cfg = yaml.safe_load(f)


# Remplace "YOUR_GEMINI_API_KEY" par ta vraie clé
gemini_client = GeminiClient(gemini_cfg["api_key"])


    
   



with open("nl_sparql_execution_dataset.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter=";")
    writer.writerow(["Natural language", "generated request","correct request","generated execution","correct execution"])
    for tuples in data:
      question=tuples[0]
      true_sparql=tuples[1]
      sparql = gemini_client.generate_rdf_no_execution(question, rdf_data.extract_ontology_from_fuseki())
      gen_execution=rdf_data.run_sparql_query(sparql)
      correct_execution=rdf_data.run_sparql_query(true_sparql)
      
      writer.writerow([
         question,
         sparql,
         true_sparql,
         gen_execution,
         correct_execution
         
      ])

print("CSV file created: nl_sparql_execution_dataset.csv")



