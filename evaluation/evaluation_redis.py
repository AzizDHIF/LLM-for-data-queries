import csv
import redis
import json
from llm.redis_llm import GeminiClient, execute_redis_command  # ton client Gemini adapté Redis
 # exécuteur Redis similaire à Neo4jExecutor

# --- Connexion à Redis ---
r = redis.Redis(host="localhost", port=6379, decode_responses=True)


# --- Initialiser Gemini ---
gemini_client = GeminiClient()  # Suppose qu'il prend api_key et model depuis config interne

# --- Dataset de test ---
data = [
    # 1
    ("Count all users.", {"command": "COUNT_KEYS", "pattern": "user:*"}),

    # 2
    ("Get user with id 1.", {"command": "HGETALL", "key_or_index": "user:1"}),

    # 3
    ("Get movie with id 10.", {"command": "HGETALL", "key_or_index": "movie:10"}),

    # 4
    ("Get actor with id 5.", {"command": "HGETALL", "key_or_index": "actor:5"}),

    # 5
    ("Add a new movie with title 'Interstellar'.", {
        "command": "HSET",
        "key_or_index": "movie:20",
        "fields": {"title": "Interstellar", "genre": "Sci-Fi", "rating": "8.6", "release_year": "2014"}
    }),

    # 6
    ("Delete movie with id 20.", {"command": "DEL", "key_or_index": "movie:20"}),

    # 7
    ("Search movies with rating above 8 in 2020.", {
        "command": "FT.SEARCH",
        "key_or_index": "movies_idx",
        "query": "@rating:[8 +inf] @release_year:[2020 +inf]",
        "limit": 50
    }),

    # 8
    ("Get all movies in 'Action' genre.", {
        "command": "FT.SEARCH",
        "key_or_index": "movies_idx",
        "query": "@genre:Action",
        "limit": 50
    }),

    # 9
    ("Count all movies.", {"command": "COUNT_KEYS", "pattern": "movie:*"}),

    # 10
    ("Add new user Alice.", {
        "command": "HSET",
        "key_or_index": "user:100",
        "fields": {"name": "Alice", "email": "alice@example.com"}
    }),

    # 11
    ("Get user Alice.", {"command": "HGETALL", "key_or_index": "user:100"}),

    # 12
    ("Delete user Alice.", {"command": "DEL", "key_or_index": "user:100"}),

    # 13
    ("Get all actors.", {"command": "FT.SEARCH", "key_or_index": "actors_idx", "query": "*", "limit": 50}),

    # 14
    ("Add actor 'Leonardo DiCaprio'.", {
        "command": "HSET",
        "key_or_index": "actor:50",
        "fields": {"name": "Leonardo DiCaprio", "dob": "1974-11-11"}
    }),

    # 15
    ("Delete actor 'Leonardo DiCaprio'.", {"command": "DEL", "key_or_index": "actor:50"})
]

# --- Générer CSV ---
with open("data/evaluation/nl_redis_execution_dataset.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter=";")
    writer.writerow(["Natural language", "generated command", "correct command", "generated execution", "correct execution"])

    for question, correct_cmd in data:
        # Génération via LLM
        generated_cmd = gemini_client.generate_redis_command(question)

        # Exécution
        generated_execution = execute_redis_command(generated_cmd)
        correct_execution = execute_redis_command(correct_cmd)

        writer.writerow([
            question,
            json.dumps(generated_cmd, ensure_ascii=False),
            json.dumps(correct_cmd, ensure_ascii=False),
            json.dumps(generated_execution, ensure_ascii=False),
            json.dumps(correct_execution, ensure_ascii=False)
        ])

print("CSV file created: data/evaluation/nl_redis_execution_dataset.csv")
