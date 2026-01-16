import csv
import happybase
import yaml
from google import genai

from llm.hbase_llm import GeminiClient  # ton client Gemini adapté HBase
from executers.hbase_executer import HBaseExecutor  # un exécuteur HBase similaire à Neo4jExecutor

# --- Charger les configs ---
with open("config/hbase.yaml") as f:
    hbase_cfg = yaml.safe_load(f)

with open("config/gemini.yaml") as f:
    gemini_cfg = yaml.safe_load(f)

# --- Initialiser HBase ---
executor = HBaseExecutor(
    host=hbase_cfg["host"],
    port=hbase_cfg["port"],
    table_name=hbase_cfg["table"]
)

# --- Initialiser Gemini ---
gemini_client = GeminiClient(api_key=gemini_cfg["api_key"], model=gemini_cfg["model"])

# --- Dataset de test ---
data = [
    ("List all movies.", "table.scan(columns=[b'info:title'], limit=10)"),
    ("List movies released after 2010.", "table.scan(filter=b\"SingleColumnValueFilter('info','year',>, 'binary:2010')\", columns=[b'info:title',b'info:year'], limit=10)"),
    ("Movies with rating above 8.", "table.scan(filter=b\"SingleColumnValueFilter('info','ratings',>, 'binary:8')\", columns=[b'info:title',b'info:ratings'], limit=10)"),
    ("List movies in 'Action' genre.", "table.scan(filter=b\"SingleColumnValueFilter('info','genres',=, 'binary:Action')\", columns=[b'info:title',b'info:genres'], limit=10)"),
    ("Movies directed by Christopher Nolan.", "table.scan(filter=b\"SingleColumnValueFilter('info','director',=, 'binary:Christopher Nolan')\", columns=[b'info:title',b'info:director'], limit=10)"),
    ("List movies released between 2000 and 2010.", "table.scan(filter=b\"SingleColumnValueFilter('info','year',>=, 'binary:2000') AND SingleColumnValueFilter('info','year',<=, 'binary:2010')\", columns=[b'info:title',b'info:year'], limit=10)"),
    ("Top 5 highest rated movies.", "table.scan(columns=[b'info:title',b'info:ratings'], limit=5)"),
    ("Average rating of movies after 2015.", "table.scan(filter=b\"SingleColumnValueFilter('info','year',>, 'binary:2015')\", columns=[b'info:title',b'info:ratings'], limit=10)"),
    ("Movies with title containing 'Star'.", "table.scan(filter=b\"SingleColumnValueFilter('info','title',=, 'binary:Star')\", columns=[b'info:title'], limit=10)"),
    ("Number of movies per director.", "table.scan(columns=[b'info:director',b'info:title'], limit=10)"),
    ("List all genres.", "table.scan(columns=[b'info:genres'], limit=10)"),
    ("Movies with rating between 7 and 9.", "table.scan(filter=b\"SingleColumnValueFilter('info','ratings',>=, 'binary:7') AND SingleColumnValueFilter('info','ratings',<=, 'binary:9')\", columns=[b'info:title',b'info:ratings'], limit=10)"),
    ("Movies reviewed by Alice.", "table.scan(filter=b\"SingleColumnValueFilter('info','director',=, 'binary:Alice')\", columns=[b'info:title',b'info:director'], limit=10)"),
    ("Movies released in 2020.", "table.scan(filter=b\"SingleColumnValueFilter('info','year',=, 'binary:2020')\", columns=[b'info:title',b'info:year'], limit=10)"),
    ("Movies in 'Comedy' genre with rating above 6.", "table.scan(filter=b\"SingleColumnValueFilter('info','genres',=, 'binary:Comedy') AND SingleColumnValueFilter('info','ratings',>, 'binary:6')\", columns=[b'info:title',b'info:genres',b'info:ratings'], limit=10)")
]

# --- Générer CSV ---
with open("data/evaluation/nl_hbase_execution_dataset.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f, delimiter=";")
    writer.writerow(["Natural language", "generated request", "correct request", "generated execution", "correct execution"])

    for question, true_query in data:
        generated_query = gemini_client.generate_hbase_query(question)
        generated_execution = executor.run_query(generated_query)
        correct_execution = executor.run_query(true_query)

        writer.writerow([
            question,
            generated_query,
            true_query,
            generated_execution,
            correct_execution
        ])

print("CSV file created: data/evaluation/nl_hbase_execution_dataset.csv")
