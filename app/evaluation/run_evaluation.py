NL_QUERIES = [
    # READ
    "List all movies in the database.",
    "Find all movies released after 2010.",
    "Find movies released before 2000.",
    "Return the titles of all movies directed by Christopher Nolan.",
    "Find all actors who acted in the movie Inception.",
    "List movies in which Tom Hanks acted.",
    "Count how many movies are in the database.",
    "Find all movies released between 1995 and 2005.",
    "List all directors and the movies they directed.",
    "Find the 5 most recent movies in the database.",

    # WRITE
    "Add a movie titled Interstellar released in 2014.",
    "Create a movie called Dune released in 2021.",
    "Add an actor named Leonardo DiCaprio.",
    "Create a director named Denis Villeneuve.",
    "Delete the movie titled Titanic.",
    "Add a movie.",
    "Delete a movie.",
    "Update the release year of a movie.",
    "Add an actor to a movie.",
    "Change the title of a movie."
]

import csv
import yaml
from app.llm.gemini_client import GeminiClient
from app.schema.schema_extractor import Neo4jSchemaExtractor
from app.llm.llm_utils import detect_query_type

# ---------- Load configs ----------
with open("config/neo4j.yaml") as f:
    neo4j_cfg = yaml.safe_load(f)

with open("config/gemini.yaml") as f:
    gemini_cfg = yaml.safe_load(f)

# ---------- Init schema ----------
extractor = Neo4jSchemaExtractor(
    neo4j_cfg["uri"],
    neo4j_cfg["user"],
    neo4j_cfg["password"]
)
schema = extractor.extract_schema()

# ---------- Init LLM ----------
llm = GeminiClient(
    api_key=gemini_cfg["api_key"],
    model=gemini_cfg["model"]
)

# ---------- NL queries ----------
NL_QUERIES = [
    "List all movies in the database.",
    "Find all movies released after 2010.",
    "Find movies released before 2000.",
    "Return the titles of all movies directed by Christopher Nolan.",
    "Find all actors who acted in the movie Inception.",
    "List movies in which Tom Hanks acted.",
    "Count how many movies are in the database.",
    "Find all movies released between 1995 and 2005.",
    "List all directors and the movies they directed.",
    "Find the 5 most recent movies in the database.",
    "Add a movie titled Interstellar released in 2014.",
    "Create a movie called Dune released in 2021.",
    "Add an actor named Leonardo DiCaprio.",
    "Create a director named Denis Villeneuve.",
    "Delete the movie titled Titanic.",
    "Add a movie.",
    "Delete a movie.",
    "Update the release year of a movie.",
    "Add an actor to a movie.",
    "Change the title of a movie."
]

# ---------- Run evaluation ----------
output_file = "llm_results.csv"

with open(output_file, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["NL Query", "Query Type", "LLM Response Type", "LLM Output"])

    for nl in NL_QUERIES:
        qtype = detect_query_type(nl)

        response = llm.generate_cypher(nl, schema)

        # response est un dict normalisé
        writer.writerow([
            nl,
            qtype,
            response["type"],
            response["content"]
        ])

print(f"✅ Evaluation finished. Results saved to {output_file}")
