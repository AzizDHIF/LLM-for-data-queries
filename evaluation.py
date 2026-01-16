import pandas as pd
import json
import re

# -------------------------------------------------
# Utils
# -------------------------------------------------

def normalize_query(q):
    return re.sub(r"\s+", " ", str(q).strip().lower())

def load_json_safe(x):
    try:
        return json.loads(x)
    except Exception:
        return None

def extract_fields(query):
    try:
        q = json.loads(query)
        fields = set()

        def walk(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    fields.add(k)
                    walk(v)
            elif isinstance(obj, list):
                for x in obj:
                    walk(x)

        walk(q)
        return fields
    except Exception:
        return set()

def extract_stages(query):
    try:
        q = json.loads(query)
        if isinstance(q, list):
            return set(stage for step in q for stage in step.keys())
        return set()
    except Exception:
        return set()

# -------------------------------------------------
# Query-based metrics
# -------------------------------------------------

def exact_match(pred, gold):
    return int(normalize_query(pred) == normalize_query(gold))

def query_stages_match(pred, gold):
    return int(extract_stages(pred) == extract_stages(gold))

def query_fields_coverage(pred, gold):
    gold_fields = extract_fields(gold)
    pred_fields = extract_fields(pred)
    if not gold_fields:
        return 1.0
    return len(gold_fields & pred_fields) / len(gold_fields)

# -------------------------------------------------
# Execution-based metrics
# -------------------------------------------------

def execution_accuracy(pred_res, gold_res):
    return int(pred_res == gold_res)

def execution_fields_match(pred_res, gold_res):
    if not pred_res or not gold_res:
        return 0

    def fields(res):
        f = set()
        for doc in res:
            f |= set(doc.keys())
        return f

    return int(fields(pred_res) == fields(gold_res))

def execution_value_match(pred_res, gold_res):
    if not pred_res or not gold_res:
        return 0
    return int(pred_res == gold_res)

# -------------------------------------------------
# Main processing
# -------------------------------------------------

def compute_metrics_csv(
    input_csv,
    output_csv="llm_metrics_results.csv"
):
    df = pd.read_csv(
        input_csv,
        sep=";",        # ✅ séparateur correct
        engine="python",
        nrows=20        # ✅ 20 requêtes uniquement
    )

    output_rows = []

    for _, row in df.iterrows():
        nl_query = row["Natural language"]

        pred_query = row["generated request"]
        gold_query = row["correct request"]

        pred_exec = load_json_safe(row["generated execution"])
        gold_exec = load_json_safe(row["correct execution"])

        output_rows.append({
            "Natural language": nl_query,
            "Metric_ExactMatch": exact_match(pred_query, gold_query),
            "Metric_QueryStagesMatch": query_stages_match(pred_query, gold_query),
            "Metric_QueryFieldsCoverage": query_fields_coverage(pred_query, gold_query),
            "Metric_ExecutionAccuracy": execution_accuracy(pred_exec, gold_exec),
            "Metric_ExecutionFieldsMatch": execution_fields_match(pred_exec, gold_exec),
            "Metric_ExecutionValueMatch": execution_value_match(pred_exec, gold_exec),
        })

    result_df = pd.DataFrame(output_rows)
    result_df.to_csv(output_csv, index=False,sep=";")

    return result_df

import pandas as pd

def moyenne_par_colonne(input_csv):
    """
    Calcule la moyenne de chaque colonne numérique d'un DataFrame.

    Args:
        df (pd.DataFrame): Le DataFrame dont on veut calculer les moyennes.

    Returns:
        pd.Series: Moyenne de chaque colonne.
    """
    df = pd.read_csv(
        input_csv,
        sep=";",        # ✅ séparateur correct
        engine="python",
        nrows=20        # ✅ 20 requêtes uniquement
    )
    return df.mean(numeric_only=True)  # ignore les colonnes non numériques





# -------------------------------------------------
# Example usage
# -------------------------------------------------

if __name__ == "__main__":
    compute_metrics_csv(
        input_csv="nl_sparql_execution_dataset.csv",
        output_csv="evaluation_sparql.csv"
    )
    print("moyenne des métriques pour sparql: \n")

    print(moyenne_par_colonne("evaluation_sparql.csv"))

    compute_metrics_csv(
        input_csv="nl_neo4j_execution_dataset.csv",
        output_csv="evaluation_neo4j.csv"
    )
    print("\n")
    print("moyenne des métriques pour neo4j: \n")
    print(moyenne_par_colonne("evaluation_neo4j.csv"))
