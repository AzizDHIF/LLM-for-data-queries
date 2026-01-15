
from google import genai


import sys
import os
import yaml
import csv 
from gemini_client import GeminiClient
from schema_extractor import Neo4jSchemaExtractor
from neo4j_executor import Neo4jExecutor

# --- Charger les configs ---
with open("config/neo4j.yaml") as f:
    neo4j_cfg = yaml.safe_load(f)

with open("config/gemini.yaml") as f:
    gemini_cfg = yaml.safe_load(f)

# --- Initialiser Neo4j ---
executor = Neo4jExecutor(
    neo4j_cfg["uri"],
    neo4j_cfg["user"],
    neo4j_cfg["password"]
)
extractor = Neo4jSchemaExtractor(
    neo4j_cfg["uri"],
    neo4j_cfg["user"],
    neo4j_cfg["password"]
)
schema = extractor.extract_schema()

# --- Initialiser Gemini ---
gemini_client = GeminiClient(api_key=gemini_cfg["api_key"], model=gemini_cfg["model"])

data= [
    # 1
    ("List all movies.",
     "MATCH (m:Movie) RETURN m.title"),

    # 2
    ("Find all people.",
     "MATCH (p:Person) RETURN p.name, p.born"),

    # 3
    ("Who acted in 'Inception'?",
     "MATCH (p:Person)-[:ACTED_IN]->(m:Movie {title: 'Inception'}) RETURN p.name"),

    # 4
    ("Which movies were directed by Christopher Nolan?",
     "MATCH (p:Person {name: 'Christopher Nolan'})-[:DIRECTED]->(m:Movie) RETURN m.title"),

    # 5
    ("Who produced the movie 'Interstellar'?",
     "MATCH (p:Person)-[:PRODUCED]->(m:Movie {title: 'Interstellar'}) RETURN p.name"),

    # 6
    ("List movies released after 2010.",
     "MATCH (m:Movie) WHERE m.released > 2010 RETURN m.title, m.released"),

    # 7
    ("Find movies written by 'Jonathan Nolan'.",
     "MATCH (p:Person {name: 'Jonathan Nolan'})-[:WROTE]->(m:Movie) RETURN m.title"),

    # 8
    ("Who follows Leonardo DiCaprio?",
     "MATCH (follower:Person)-[:FOLLOWS]->(p:Person {name: 'Leonardo DiCaprio'}) RETURN follower.name"),

    # 9
    ("Which movies has 'Leonardo DiCaprio' acted in?",
     "MATCH (p:Person {name: 'Leonardo DiCaprio'})-[:ACTED_IN]->(m:Movie) RETURN m.title, m.released"),

    # 10
    ("Find movies reviewed by 'Alice'.",
     "MATCH (p:Person {name: 'Alice'})-[:REVIEWED]->(m:Movie) RETURN m.title, m.released"),

    # 11
    ("Who directed and wrote 'Memento'?",
     "MATCH (p:Person)-[:DIRECTED]->(m:Movie {title: 'Memento'}), (p)-[:WROTE]->(m) RETURN p.name"),

    # 12
    ("List movies produced by 'Emma Thomas'.",
     "MATCH (p:Person {name: 'Emma Thomas'})-[:PRODUCED]->(m:Movie) RETURN m.title"),

    # 13
    ("Which movies did 'Christian Bale' act in after 2005?",
     "MATCH (p:Person {name: 'Christian Bale'})-[:ACTED_IN]->(m:Movie) WHERE m.released > 2005 RETURN m.title, m.released"),

    # 14
    ("Find all actors in movies directed by 'Christopher Nolan'.",
     "MATCH (d:Person {name:'Christopher Nolan'})-[:DIRECTED]->(m:Movie)<-[:ACTED_IN]-(a:Person) RETURN a.name, m.title"),

    # 15
    ("Who follows people that acted in 'Inception'?",
     "MATCH (f:Person)-[:FOLLOWS]->(p:Person)-[:ACTED_IN]->(m:Movie {title:'Inception'}) RETURN f.name, p.name"),

    # 16
    ("List all movies reviewed after 2015.",
     "MATCH (p:Person)-[:REVIEWED]->(m:Movie) WHERE m.released > 2015 RETURN m.title, p.name"),

    # 17
    ("Find movies acted by both 'Leonardo DiCaprio' and 'Joseph Gordon-Levitt'.",
     "MATCH (p1:Person {name:'Leonardo DiCaprio'})-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person {name:'Joseph Gordon-Levitt'}) RETURN m.title"),

    # 18
    ("Who produced movies written by 'Jonathan Nolan'?",
     "MATCH (writer:Person {name:'Jonathan Nolan'})-[:WROTE]->(m:Movie)<-[:PRODUCED]-(producer:Person) RETURN producer.name, m.title"),

    # 19
    ("Find all people who both acted and wrote a movie.",
     "MATCH (p:Person)-[:ACTED_IN]->(m:Movie), (p)-[:WROTE]->(m) RETURN p.name, m.title"),

    # 20
    ("List movies released between 2000 and 2010.",
     "MATCH (m:Movie) WHERE m.released >= 2000 AND m.released <= 2010 RETURN m.title, m.released")
]




with open("nl_neo4j_execution_dataset.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter=";")
    writer.writerow(["Natural language", "generated_neo4j","the correct neo4j","generated_execution","the correct execution"])
    for tuples in data:
      question=tuples[0]
      true_neo4j=tuples[1]
      neo4j = gemini_client.generate_cypher(question, schema)
      generated_execution=executor.run_query(neo4j)
      correct_execution=executor.run_query(true_neo4j)
      
      writer.writerow([
         question,
         neo4j,
         true_neo4j,
         generated_execution,
         correct_execution
         
      ])

print("CSV file created: nl_sparql_execution_dataset.csv")