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

def build_hbase_prompt(nl_query: str) -> str:
    return (
        "You are an HBase expert.\n\n"
        "Generate ONLY a valid HBase shell query.\n"
        "NO explanation. NO markdown. NO comments.\n\n"

        "CRITICAL RULES:\n"
        "- Use HBase shell syntax for queries\n"
        "- For complex operations, use Scan with filters\n"
        "- Use table name: 'movies'\n"
        "- Columns are in format 'cf:column'\n"
        "- Column families: 'info' and 'ratings'\n"
        "- Row key format: name_year (e.g., 'Inception_2010')\n"
        "- Use ONLY: scan, get, count, delete, put\n"
        "- For filters, use: SingleColumnValueFilter, PrefixFilter, PageFilter, ColumnPrefixFilter\n"
        "- NEVER invent fields\n"
        "- Always specify column family explicitly\n\n"

        "Schema:\n"
        "Table: movies\n"
        "Row key: name_year\n"
        "Column Families and columns:\n"
        "  info:\n"
        "    genre, released, votes, director, writer, star, country, budget, gross, company, runtime\n"
        "  ratings:\n"
        "    rating, score\n\n"

        "Examples:\n\n"

        "Question: Get movies from year 2010\n"
        "Output:\n"
        "scan 'movies', {FILTER => \"PrefixFilter('_2010')\"}\n\n"

        "Question: Count movies with score above 8\n"
        "Output:\n"
        "scan 'movies', {FILTER => \"SingleColumnValueFilter('ratings', 'score', >=, 'binary:8')\"}\n\n"

        "Question: Get all movies directed by Christopher Nolan\n"
        "Output:\n"
        "scan 'movies', {FILTER => \"SingleColumnValueFilter('info', 'director', =, 'binary:Christopher Nolan')\"}\n\n"

        "Question: Get movie details for Inception\n"
        "Output:\n"
        "get 'movies', 'Inception_2010'\n\n"

        "Question: Get movies with rating higher than 8.5\n"
        "Output:\n"
        "scan 'movies', {FILTER => \"SingleColumnValueFilter('ratings', 'rating', >=, 'binary:8.5')\"}\n\n"

        "Question: Get name and genre of all movies\n"
        "Output:\n"
        "scan 'movies', {COLUMNS => ['info:genre']}\n\n"

        f"Question:\n\"{nl_query}\""
    )
    
    
def build_hbase_write_prompt(nl_query: str) -> str:
    return f"""
You are an expert HBase assistant.

The database schema is:

Table: movies
Row key: name_year
Column Families and columns:
  info:
    genre, released, votes, director, writer, star, country, budget, gross, company, runtime
  ratings:
    rating, score

Your task is to translate the following natural language request into an HBase WRITE operation.

You MUST answer in ONE of the following formats:

1) If all the details about the write query are present and safe:
format="HBASE:
<hbase operation>"

2) If any detail is missing:
format="QUESTION:
<question to ask the user>"

Rules:

- For PUT operations, use format: put 'table', 'rowkey', 'cf:column', 'value'
- For multiple PUTs, separate them with newlines
- For DELETE operations, use format: delete 'table', 'rowkey', 'cf:column'
- For DELETE entire row: deleteall 'table', 'rowkey'
- Do NOT assume missing values (especially year for row key)
- Row key MUST be in format: name_year
- Do NOT generate HBase if information is missing
- Do NOT add explanations
- You MUST start your answer with "QUESTION:" or "HBASE:"

Natural language request:
"{nl_query}"
"""



# =========================
# GEMINI CALL
# =========================

def generate_hbase_query(nl_query: str) -> str:
    
    client = Client(api_key=API_KEY)
    query_type = detect_query_type(nl_query)
    
    if query_type == "read":
        prompt = build_hbase_prompt(nl_query)
        response = client.models.generate_content(model=MODEL, contents=prompt)
        return response.text.replace("```", "").strip()
    
    if query_type == "write":
        prompt = build_hbase_write_prompt(nl_query)
        response = client.models.generate_content(model=MODEL, contents=prompt)
        return response.text.replace("```", "").strip()
    
    # Default fallback
    prompt = build_hbase_prompt(nl_query)
    response = client.models.generate_content(model=MODEL, contents=prompt)
    return response.text.replace("```", "").strip()