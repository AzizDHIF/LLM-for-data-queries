import json
from google import genai
import redis

import yaml
from google.genai import Client
from connectors.api import load_gemini_config
from utils.neo4j_llm_utils import detect_query_type


# =========================
# LOAD GEMINI CONFIG
# =========================
config = load_gemini_config()
API_KEY = config["api_key"]
MODEL = config.get("model", "gemini-2.5-pro")
client = Client(api_key=API_KEY)


# Connect to Redis Stack (RediSearch enabled)
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
# print(r.keys("*"))
indexes = r.execute_command("FT._LIST")
print("les indexes sont:", indexes)

SYSTEM_PROMPT = """
You are an AI assistant that translates NATURAL LANGUAGE into precise Redis commands.
Dataset:
- Movies stored as HASH with key movie:{id}
- Actors stored as HASH with key actor:{id}
- Users stored as HASH with key user:{id}
- Movies index: movies_idx (RediSearch)
- Actors index: actors_idx (RediSearch)

Allowed commands:
- HGETALL key (for HASH data types like movie:*, actor:*, user:*)
- GET key (for simple string values only)
- SET key value (for simple string values only)
- HSET key field value (for HASH data types)
- DEL key
- SMEMBERS key
- SADD key value
- FT.SEARCH index query limit

Rules:
1. Output ONLY valid JSON without any explanation or markdown formatting
2. Do NOT include ```json or ``` markers
3. IMPORTANT: For movie:*, actor:*, and user:* keys, ALWAYS use HGETALL (not GET) because they are stored as HASHes
4. Example JSON formats:

For getting a user/movie/actor (they are HASHes):
{
  "command": "HGETALL",
  "key_or_index": "user:1"
}

For getting a movie:
{
  "command": "HGETALL",
  "key_or_index": "movie:1"
}

For getting an actor:
{
  "command": "HGETALL",
  "key_or_index": "actor:1"
}

For adding a new movie:
{
  "command": "HSET",
  "key_or_index": "movie:10",
  "fields": {
    "title": "Sci-Fi Movie",
    "genre": "Action",
    "rating": "8.5",
    "release_year": "2025"
  }
}

For deleting:
{
  "command": "DEL",
  "key_or_index": "movie:10"
}

For searching:
{
  "command": "FT.SEARCH",
  "key_or_index": "movies_idx",
  "query": "@rating:[8 +inf] @release_year:[2020 +inf]",
  "limit": 50
}

5. Use HSET for new entries, DEL to remove, HGETALL to get all fields from HASHes, and FT.SEARCH for filtering.
6. CRITICAL: Return ONLY the JSON object, nothing else.
7. IMPORTANT:
To count users, movies, or actors:
- NEVER use GET user:count
- Instead, assume counting keys matching pattern (user:*)
- Return:
{
  "command": "COUNT_KEYS",
  "pattern": "user:*"
}
"""


def generate_redis_command(user_request: str) -> dict:
    try : 
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=user_request,
            config={
                    "system_instruction": SYSTEM_PROMPT,
                    "temperature": 0.1,
                    "response_mime_type": "application/json"  # Force JSON response
                }
            )
            
            # Extract text content from Gemini response
        content_text = response.text.strip()
            
            # Debug: Print what we received
        print(f"\n🔍 Debug - Raw response:\n{content_text}\n")
            
        if content_text.startswith("```json"):
                content_text = content_text.replace("```json", "").replace("```", "").strip()
        elif content_text.startswith("```"):
                content_text = content_text.replace("```", "").strip()
            
        return json.loads(content_text)
            
    except json.JSONDecodeError as e:
        print(f"❌ JSON Parse Error: {e}")
        print(f"Received content: {content_text}")
        raise
    except Exception as e:
        print(f"❌ API Error: {e}")
        raise

def execute_redis_command(cmd: dict):
    command = cmd["command"].upper()
    if command == "COUNT_KEYS":
        return {"count": len(r.keys(cmd["pattern"]))}
    elif command == "HGETALL":
        return r.hgetall(cmd["key_or_index"])
    elif command == "GET":
        return r.get(cmd["key_or_index"])
    elif command == "SET":
        return r.set(cmd["key_or_index"], cmd["value"])
    elif command == "HSET":
        fields = cmd.get("fields", {})
        return r.hset(cmd["key_or_index"], mapping=fields)
    elif command == "DEL":
        return r.delete(cmd["key_or_index"])
    elif command == "SMEMBERS":
        return list(r.smembers(cmd["key_or_index"]))
    elif command == "SADD":
        return r.sadd(cmd["key_or_index"], cmd["value"])
    elif command == "FT.SEARCH":
        limit = cmd.get("limit", 50)
        raw = r.execute_command(
            "FT.SEARCH",
            cmd["key_or_index"],
            cmd["query"],
            "LIMIT", 0, limit
        )
        # raw[0] = count
        results = []
        for i in range(1, len(raw), 2):
            doc_id = raw[i]
            fields = raw[i+1] if i+1 < len(raw) else []
            doc = {"id": doc_id}
            # transformer les champs en dictionnaire en vérifiant les paires
            for j in range(0, len(fields)-1, 2):
                doc[fields[j]] = fields[j+1]
            results.append(doc)
        return {"result": results, "count": raw[0]}
    else:
        raise ValueError(f"Unknown command type: {command}")

def normalize_redis_command(cmd):
    # Cas 1 : le LLM a retourné une liste
    if isinstance(cmd, list):
        for item in cmd:
            if isinstance(item, dict) and "example" in item:
                return item["example"]
        raise ValueError("No executable Redis command found in list")

    # Cas 2 : tout va bien
    if isinstance(cmd, dict) and "command" in cmd:
        return cmd

    raise ValueError("Invalid Redis command format")


def chat():
    print("\n🎬 Redis AI Assistant ")

    while True:
        user_input = input("User > ")
        if user_input.lower() == "exit":
            break

        try:
            cmd = generate_redis_command(user_input)
            results = execute_redis_command(cmd)

            print("\n🔹 Generated Redis Command:")
            print(json.dumps(cmd, indent=2))

            print("\n🔹 Result:")
            if isinstance(results, list):
                for r in results:
                    print(r)
            elif isinstance(results, dict):
                for k, v in results.items():
                    print(f"{k}: {v}")
            else:
                print(results)

            print("-"*50)

        except Exception as e:
            print("❌ Error:", e)

if __name__ == "__main__":
    chat()