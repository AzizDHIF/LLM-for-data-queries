import os
import traceback
from flask import Flask, render_template, request, session, redirect
import json 
import yaml
# Import MongoDB Executor et LLM
from llm.mongodb_llm import generate_mongodb_query
from executers.mongodb_executer import MongoExecutor
from llm.neo4j_llm import Neo4jExecutor, Neo4jSchemaExtractor, GeminiClient
from utils.neo4j_llm_utils import detect_query_type
from connectors.api import load_gemini_config
from llm.redis_llm import generate_redis_command, execute_redis_command

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "secret-key-123")

# --- Initialisation MongoExecutor --- #
executor = MongoExecutor(
    host="localhost",
    port=27017,
    username="admin",
    password="secret",
    database="sample_mflix",
    collection="movies"
)

# --- Initialisation Neo4j --- #
neo4j_executor = Neo4jExecutor(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="password"
)

# Extraire le schéma pour le LLM
neo4j_schema_extractor = Neo4jSchemaExtractor(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="password"
)
neo4j_schema = neo4j_schema_extractor.extract_schema()

# LLM Gemini pour NL -> Cypher
gemini_cfg = load_gemini_config()
API_KEY = gemini_cfg.get("api_key")
MODEL = gemini_cfg.get("model", "gemini-2.5-pro")
gemini_client = GeminiClient(api_key=API_KEY, model=MODEL)

# ----- Fonctions pour la conversion en tableau ----
def json_to_table(results):
    headers = set()
    table_rows = []

    for r in results.get("data", []):
        if isinstance(r, dict):
            headers.update(r.keys())
            table_rows.append([r.get(h, "") for h in headers])

    return headers, table_rows

# --- Fonctions utils --- #
def preprocess_question(question: str) -> str:
    """Normalisation simple de la question"""
    return question.strip().lower()

def generate_response_text(results):
    """Texte simple de réponse pour l'interface"""
    if isinstance(results, list):
        return f"✅ {len(results)} résultat(s) trouvé(s)"
    elif results:
        return "✅ Résultat obtenu"
    return "❌ Aucun résultat trouvé"



# --- Routes --- #
@app.route('/', methods=['GET', 'POST'])
def index():
    # Sécurité : s'assurer que session['conversation'] est un dict
    if 'conversation' not in session or not isinstance(session['conversation'], dict):
        session['conversation'] = {}

    question = ""
    results = []
    response_text = ""
    mongo_query = ""
    cypher_query = ""
    redis_query= ""
    headers = []
    table_rows = []
    pretty_results = ""
    selected_db = "mongodb"

    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        selected_db = request.form.get('db_choice', 'mongodb')
        print("🔹 Base sélectionnée :", selected_db)

        # Initialiser conversation spécifique si n'existe pas
        if selected_db not in session['conversation']:
            session['conversation'][selected_db] = []

        if question:
            normalized_question = preprocess_question(question)
            session['conversation'][selected_db].append({'role': 'user', 'text': question})

            try:
                if selected_db == "mongodb":
                    response = generate_mongodb_query(normalized_question)
                    if not response:
                            print("⚠️ Requête vide, impossible d'exécuter")
                    else:
                        print("🔹 Requête à exécuter :", response)
                    query_type = detect_query_type(normalized_question)
                    
                    mongo_query = None

                    if query_type == "read":
                        if isinstance(response, str) and response.strip():
                            mongo_query = response
                        else:
                            response_text = "ℹ️ LLM n'a pas généré de requête MongoDB valide."
                            pretty_results = ""
                    else:  # write / insert / update / delete
                        if isinstance(response, dict):
                            if response.get("type") == "clarification":
                                response_text = f"ℹ️ {response.get('content', '')}"
                                pretty_results = response.get('content', '')
                            elif response.get("type") == "mongo":
                                mongo_query = response.get("content")
                            elif response.get("type") == "error":
                                response_text = f"❌ {response.get('content', '')}"
                                pretty_results = response.get("content", '')
                        elif isinstance(response, str):
                            mongo_query = response

                    if mongo_query:
                        if mongo_query.startswith("QUESTION:"):
                            response_text = mongo_query  # afficher la question au lieu d'exécuter
                            pretty_results = mongo_query
                            results = []
                        else:
                            results = executor.run_query(mongo_query)
                            from bson import json_util
                            pretty_results = json.dumps(results, indent=4, default=json_util.default, ensure_ascii=False)
                            response_text = generate_response_text(results)
                            headers, table_rows = json_to_table(results)

                        

                elif selected_db == "neo4j":
                    response = gemini_client.generate_cypher(normalized_question, neo4j_schema)
                    query_type = detect_query_type(normalized_question)

                    if query_type == "read":
                        # response est une string Cypher
                        cypher_query = response
                        results = neo4j_executor.run_query(cypher_query)
                        pretty_results = json.dumps(results, indent=4, ensure_ascii=False)
                        response_text = generate_response_text(results)
                        headers, table_rows = json_to_table({"data": results.get("result", [])})

                    elif query_type == "write":
                        # response peut être dict ou string
                        if isinstance(response, dict):
                            if response.get("type") == "clarification":
                                response_text = f"ℹ️ More information is required: {response.get('content')}"
                                pretty_results = response.get("content", "")
                            elif response.get("type") == "cypher":
                                cypher_query = response.get("content")
                                results = neo4j_executor.run_query(cypher_query)
                                pretty_results = json.dumps(results, indent=4, ensure_ascii=False)
                                response_text = "✅ Write executed successfully"
                                headers, table_rows = json_to_table({"data": results.get("result", [])})
                            elif response.get("type") == "error":
                                response_text = f"❌ LLM Error: {response.get('content')}"
                                pretty_results = response.get("content", "")
                        else:
                            # Si Gemini retourne directement du Cypher
                            cypher_query = response
                            results = neo4j_executor.run_query(cypher_query)
                            pretty_results = json.dumps(results, indent=4, ensure_ascii=False)
                            response_text = "✅ Write executed successfully"
                            headers, table_rows = json_to_table({"data": results.get("result", [])})

                elif selected_db == "redis":
                    response = generate_redis_command(normalized_question, neo4j_schema)
                    redis_query = response
                    results = execute_redis_command(redis_query)
                    pretty_results = json.dumps(results, indent=4, ensure_ascii=False)
                    response_text = generate_response_text(results)
                    headers, table_rows = json_to_table({"data": results.get("result", [])})
                   
                        

                else:
                    results = []
                    response_text = f"❌ Base {selected_db} sélectionnée : aucun résultat."
                    pretty_results = json.dumps(results, indent=4, ensure_ascii=False)

            except Exception as e:
                print("⚠️ Erreur complète :")
                traceback.print_exc()
                response_text = f"❌ Erreur lors de l'exécution : {e}"
                pretty_results = ""

            # Ajouter réponse assistant
            session['conversation'][selected_db].append({'role': 'assistant', 'text': response_text})
            # Limiter à 10 messages
            if len(session['conversation'][selected_db]) > 10:
                session['conversation'][selected_db] = session['conversation'][selected_db][-10:]
            session.modified = True

    return render_template(
        'index.html',
        question=question,
        conversation=session['conversation'].get(selected_db, []),
        results=results,
        response=response_text,
        mongo_query=mongo_query,
        neo4j_query=cypher_query,
        redis_query=redis_query,
        table_rows=table_rows,
        selected_db=selected_db,
        pretty_results=pretty_results,
    )


@app.route('/clear', methods=['GET'])
def clear_conversation():
    # Réinitialisation sécurisée
    session['conversation'] = {}
    session.modified = True
    return redirect('/')

@app.route('/health', methods=['GET'])
def health_check():
    return {
        'status': 'healthy',
        'mongodb_connected': executor is not None
    }

if __name__ == '__main__':
    print("🌐 Démarrage du serveur Flask sur http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)
