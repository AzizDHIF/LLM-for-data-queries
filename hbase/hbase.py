import happybase
import json
from google import genai

HBASE_HOST = "localhost"
HBASE_PORT = 9090
GEMINI_API_KEY = "AIzaSyCxn0IfMlu3EcZbMEm1EsbPPU9arQYyHcI"
TABLE_NAME = "movies"

# Open HBase connection
connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
connection.open()
table = connection.table(TABLE_NAME)

# Gemini client
client = genai.Client(api_key=GEMINI_API_KEY)


def translate_query(nl_query):

    system_prompt = f"""
You translate natural language queries into HBase scan parameters.

Table: {TABLE_NAME}
Column family: info
Available columns:
- info:title
- info:genres
- info:year
- info:director
- info:ratings

Instructions:
- Return ONLY valid JSON.
- The JSON must always have:
  - "columns": a list of fully qualified columns relevant to the query
  - "limit": an integer (maximum number of rows to return)
- If the query includes a condition (e.g., "movies rated above 7"), include a "filter" field:
  Example: 
    "filter": "SingleColumnValueFilter('info','ratings',>, 'binary:7')"
- Always use a scan (no get).
- Do NOT include any extra fields or comments.
- Use the following JSON as an example, adapt columns and filters based on the query:

Example:
{{
  "columns": ["info:title", "info:ratings"],
  "limit": 10,
  "filter": "SingleColumnValueFilter('info','ratings',>, 'binary:7')"
}}
"""


    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=nl_query,
        config={
            "system_instruction": system_prompt,
            "temperature": 0.1,
            "response_mime_type": "application/json"
        }
    )

    return json.loads(response.text)

def execute_query(query_plan):
    """
    Executes an HBase scan based on the query plan.
    """
    columns = [c.encode() for c in query_plan.get("columns", [])]
    limit = query_plan.get("limit", 10)
    filter_str = query_plan.get("filter")  # New: optional filter from plan

    rows = table.scan(columns=columns, limit=limit, filter=filter_str)
    return list(rows)



def run_query(user_query):
    """
    Given a natural language query, return both the query plan
    and a preview of the results.
    """
    query_plan = translate_query(user_query)
    results = execute_query(query_plan)

    # Preview first 5 rows
    preview = []
    for i, (row_key, data) in enumerate(results):
        if i >= 5:
            break
        row_dict = {col.decode(): val.decode() for col, val in data.items()}
        preview.append({"row_key": row_key.decode(), "data": row_dict})

    return {
        "query_plan": query_plan,
        "preview_results": preview
    }


# Main loop
print("\n🤖 HBase Natural Language Assistant")
print("Type a query or 'exit'\n")

while True:
    user_query = input("Query> ").strip()

    if user_query.lower() in ("exit", "quit"):
        break

    output = run_query(user_query)

    print("\nGenerated HBase plan:")
    print(json.dumps(output["query_plan"], indent=2))

    print("\nPreview of results:")
    if not output["preview_results"]:
        print("No results found")
    else:
        for row in output["preview_results"]:
            print(f"\nRow: {row['row_key']}")
            for col, val in row["data"].items():
                print(f"  {col} = {val}")

connection.close()
print("\n✓ Connection closed")
