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


# =========================
# PROMPT BUILDER
# =========================

def build_mongodb_prompt(nl_query: str) -> str:
    return (
        "You are a MongoDB expert.\n\n"
        "Generate ONLY a valid MongoDB shell query.\n"
        "NO explanation. NO markdown. NO comments.\n\n"

        "CRITICAL RULES:\n"
        "- Use STRICT MongoDB JSON syntax\n"
        "- ALL keys MUST be quoted\n"
        "- Use db.movies\n"
        "- Use ONLY: find, countDocuments, aggregate\n"
        "- Aggregation pipelines are ALLOWED\n"
        "- Allowed stages: $match, $group, $unwind, $avg, $sum, $sort\n"
        "- NEVER invent fields\n"
        "- If computing an average on imdb.rating, ALWAYS filter null values\n\n"

        "Schema:\n"
        "movies(\n"
        "  title: string,\n"
        "  year: int,\n"
        "  genres: array[string],\n"
        "  runtime: int,\n"
        "  imdb.rating: float,\n"
        "  imdb.votes: int\n"
        ")\n\n"

        "Examples:\n\n"

        "Question: Number of movies per year\n"
        "Output:\n"
        "db.movies.aggregate([\n"
        "  { \"$group\": { \"_id\": \"$year\", \"count\": { \"$sum\": 1 } } },\n"
        "  { \"$sort\": { \"_id\": 1 } }\n"
        "])\n\n"

        "Question: Average rating per genre\n"
        "Output:\n"
        "db.movies.aggregate([\n"
        "  { \"$match\": { \"imdb.rating\": { \"$ne\": null } } },\n"
        "  { \"$unwind\": \"$genres\" },\n"
        "  { \"$group\": { \"_id\": \"$genres\", \"avg_rating\": { \"$avg\": \"$imdb.rating\" } } },\n"
        "  { \"$sort\": { \"avg_rating\": -1 } }\n"
        "])\n\n"

        f"Question:\n\"{nl_query}\""
    )
    
    
def build_mongodb_write_prompt(nl_query: str) -> str:
    return f"""
You are an expert MONGODB assistant.

The database schema is:

 "Schema:\n"
        "movies(\n"
        "  title: string,\n"
        "  year: int,\n"
        "  genres: array[string],\n"
        "  runtime: int,\n"
        "  imdb.rating: float,\n"
        "  imdb.votes: int\n"
        

Your task is to translate the following natural language request into a Mongodb WRITE query.

You MUST answer in ONE of the following formats:

1) If all the details about the write query are present and safe:
format="MONGO:
<mongodb query>"

2) If any detail is missing:
format="QUESTION:
<question to ask the user>"

Rules:

- Do NOT assume missing values
- Do NOT generate MONGODB if information is missing
- Do NOT add explanations
- You MUST start your answer with "QUESTION:" or "MONGO:"

Natural language request:
"{nl_query}"
"""



# =========================
# GEMINI CALL
# =========================

def generate_mongodb_query(nl_query: str) -> str:
    
    client = Client(api_key=API_KEY)
    query_type = detect_query_type(nl_query)
    
    if query_type == "read":
        prompt = build_mongodb_prompt(nl_query)
        response = client.models.generate_content(model=MODEL,contents=prompt)
        return response.text.replace("```", "").strip()
    
    if query_type == "write":
        prompt = build_mongodb_write_prompt(nl_query)
        response = client.models.generate_content(model=MODEL,contents=prompt)
        return response.text.replace("```", "").strip()
    
    
    

    
