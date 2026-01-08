import json
import google.generativeai as genai
import redis



genai.configure(api_key="PUT_YOUR_GEMINI_API_KEY_HERE")

# Connect to Redis Stack (RediSearch enabled)
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

SYSTEM_PROMPT = """
You are an AI assistant that translates NATURAL LANGUAGE into precise Redis commands.
Dataset:
- Movies stored as HASH with key movie:{id}
- Actors stored as HASH with key actor:{id}
- Movies index: movies_idx (RediSearch)
- Actors index: actors_idx (RediSearch)

Allowed commands:
- HGETALL key
- GET key
- SET key value
- HSET key field value
- DEL key
- SMEMBERS key
- SADD key value
- FT.SEARCH index query limit

Rules:
1. Output ONLY JSON
2. Example JSON formats:

# Add a new movie
{
  "command": "HSET",
  "key_or_index": "movie:10",
  "fields": {
    "title": "Sci-Fi Movie",
    "genre": "Action",
    "rating": 8.5,
    "release_year": 2025
  }
}

# Delete a key
{
  "command": "DEL",
  "key_or_index": "movie:10"
}

# Search
{
  "command": "FT.SEARCH",
  "key_or_index": "movies_idx",
  "query": "@rating:[8 +inf] @release_year:[2020 +inf]",
  "limit": 50
}

# Get all fields
{
  "command": "HGETALL",
  "key_or_index": "movie:10"
}

3. Use HSET for new entries, DEL to remove, HGETALL to get all fields, and FT.SEARCH for filtering.
4. Never output raw Redis syntax, only JSON.
"""


def generate_redis_command_gemini(user_request: str) -> dict:
    response = genai.chat(
        model="gemini-1.5",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_request}
        ]
    )
    content = response.candidates[0].content
    return json.loads(content)

def execute_redis_command(cmd: dict):
    command = cmd["command"].upper()

    if command == "HGETALL":
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
        count = raw[0]
        results = []
        for i in range(1, len(raw), 2):
            doc_id = raw[i]
            fields = raw[i+1]
            doc = {"id": doc_id}
            for j in range(0, len(fields), 2):
                doc[fields[j]] = fields[j+1]
            results.append(doc)
        return results
    else:
        raise ValueError(f"Unknown command type: {command}")

# =============================
# CHAT LOOP
# =============================

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
