import happybase
import json
from google import genai

HBASE_HOST = "localhost"
HBASE_PORT = 9090
GEMINI_API_KEY = "key"
TABLE_NAME = "movies"

# Open HBase connection
connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
connection.open()
table = connection.table(TABLE_NAME)

# Gemini client
client = genai.Client(api_key=GEMINI_API_KEY)


def translate_query(nl_query):

    system_prompt = f"""
You translate natural language queries into **HBase scan commands** in Python.

Table: {TABLE_NAME}
Column family: info
Available columns:
- info:title
- info:genres
- info:year
- info:director
- info:ratings

Instructions:
- Return ONLY a Python HBase scan statement using `table.scan`.
- Include relevant columns and a filter if the query has a condition.
- For numeric comparisons on ratings, use a filter like:
  "SingleColumnValueFilter('info', 'ratings', >=, 'binary:9')"
- For string comparisons on title, use:
  "SingleColumnValueFilter('info', 'title', =, 'binary:Metro')"
- Include a limit (e.g., limit=10).
- Example for a specific title: 
  table.scan(
      columns=[b'info:title', b'info:genres', b'info:year', b'info:director', b'info:ratings'],
      filter=b"SingleColumnValueFilter('info', 'title', =, 'binary:Metro')",
      limit=10
  )
- Do not include any extra explanation, markdown, or JSON formatting.
- Return ONLY the executable Python code.
- Filters must be bytes (prefix with b).
"""

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=nl_query,
        config={
            "system_instruction": system_prompt,
            "temperature": 0.1,
        }
    )

    query_str = response.text.strip()
    
    # Remove any markdown code blocks if present
    if query_str.startswith("```"):
        lines = query_str.split('\n')
        query_str = '\n'.join(lines[1:-1]) if len(lines) > 2 else query_str
    
    # Remove any JSON array formatting
    if query_str.startswith('[') and query_str.endswith(']'):
        try:
            parsed = json.loads(query_str)
            if isinstance(parsed, list) and len(parsed) > 0:
                query_str = parsed[0]
        except:
            pass
    
    return query_str
def execute_query(query_str):

    # Only provide `table` in eval namespace for safety
    local_env = {"table": table}

    # Evaluate the scan string and get results
    rows = eval(query_str, {}, local_env)

    # Convert to list
    return list(rows)


def run_query(user_query):
    query_str = translate_query(user_query)
    results = execute_query(query_str)

    # Preview first 5 rows
    preview = []
    for i, row in enumerate(results):
        if i >= 5:
            break

        # row should be a tuple (row_key, data)
        if not isinstance(row, tuple) or len(row) != 2:
            print(f"Warning: Unexpected row format: {row}")
            continue

        row_key, data = row
        
        # Check if data is actually a dictionary
        if not isinstance(data, dict):
            print(f"Warning: Data is not a dict, it's {type(data)}: {data}")
            continue

        row_dict = {}
        for col, val in data.items():
            try:
                col_str = col.decode() if isinstance(col, bytes) else str(col)
                val_str = val.decode() if isinstance(val, bytes) else str(val)
                row_dict[col_str] = val_str
            except Exception as e:
                print(f"Warning: Could not decode {col}:{val} - {e}")
                continue
        
        row_key_str = row_key.decode() if isinstance(row_key, bytes) else str(row_key)
        preview.append({"row_key": row_key_str, "data": row_dict})

    return {
        "query": query_str,
        "preview_results": preview
    }


while True:
    user_query = input("Query> ").strip()

    output = run_query(user_query)

    print("\nGenerated HBase query:")
    print(output["query"])
    if not output["preview_results"]:
        print("No results found")
    else:
        for row in output["preview_results"]:
            print(f"\nRow: {row['row_key']}")
            for col, val in row["data"].items():
                print(f"  {col} = {val}")

connection.close()
