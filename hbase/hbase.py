import happybase
import json
from google import genai


HBASE_HOST = "localhost"
HBASE_PORT = 9090
GEMINI_API_KEY = "AIzaSyCb1e6COHxCNrL8PorQMq2QFrx8--TXrb8"
TABLE_NAME = "movies"

connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
connection.open()
table = connection.table(TABLE_NAME)

client = genai.Client(api_key=GEMINI_API_KEY)



def translate_query(nl_query):
    """
    Translates a natural language query into
    simple HBase scan parameters.
    """

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

Return ONLY valid JSON with this structure:

{{
  "columns": ["info:title", "info:year"],
  "limit": 10
}}

Rules:
- Always use a scan
- Columns must be fully qualified
- limit is mandatory
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
    table = connection.table("movies")

    title = query_plan.get("title")
    columns = [c.encode() for c in query_plan.get("columns", [])]
    limit = query_plan.get("limit", 10)

    if title:
        filter_str = (
            "SingleColumnValueFilter("
            "'info','title',=,'binary:{}')"
        ).format(title)

        rows = table.scan(
            filter=filter_str,
            columns=columns,
            limit=limit
        )
    else:
        rows = table.scan(columns=columns, limit=limit)

    return list(rows)


def display_results(results):
    if not results:
        print("No results found")
        return

    for row_key, data in results:
        print(f"\nRow: {row_key.decode()}")
        for col, val in data.items():
            print(f"  {col.decode()} = {val.decode()}")


print("\n🤖 HBase Natural Language Assistant")
print("Type a query or 'exit'\n")

while True:
    user_query = input("Query> ").strip()

    if user_query.lower() in ("exit", "quit"):
        break

    query_plan = translate_query(user_query)
    print("\nGenerated HBase plan:", query_plan)

    results = execute_query(query_plan)
    display_results(results)
connection.close()
print("\n✓ Connection closed")
