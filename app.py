import os
import traceback
from flask import Flask, render_template, request, session, redirect
import json 
import yaml
# Import MongoDB Executor et LLM
from llm.mongodb_llm import generate_mongodb_query
# from llm.hbase_llm import generate_hbase_query
from executers.rdf_executer import RDF_DATA
from executers.mongodb_executer import MongoExecutor
from llm.neo4j_llm import Neo4jExecutor, Neo4jSchemaExtractor, GeminiClient
from llm.rdf_llm import GeminiClientRDF
from utils.neo4j_llm_utils import detect_query_type
from connectors.api import load_gemini_config
from llm.redis_llm import generate_redis_command, execute_redis_command, normalize_redis_command
from executers.hbase_executer import HBaseExecutor
from app_all import *
from app_all import MultiDBManager
from llm.classifier_old import detect_database_language, analyze_query, detect_query_type1, normalize_nl_prefix
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

# Initialisation
hbase_executor = HBaseExecutor(host='localhost', port=9090)


# LLM Gemini pour NL -> Cypher
gemini_cfg = load_gemini_config()
API_KEY = gemini_cfg.get("api_key")
MODEL = gemini_cfg.get("model", "gemini-2.5-pro")
gemini_client = GeminiClient(api_key=API_KEY, model=MODEL)
gemini_client_rdf = GeminiClientRDF(gemini_cfg["api_key"])

rdf_data=RDF_DATA("http://localhost:3030/movies/sparql")

def redis_command_to_string(cmd):
    c = cmd["command"].upper()
    if c in ["GET", "DEL"]:
        return f"{c} {cmd['key_or_index']}"
    if c == "HGETALL":
        return f"HGETALL {cmd['key_or_index']}"
    if c == "FT.SEARCH":
        return f"FT.SEARCH {cmd['key_or_index']} \"{cmd['query']}\" LIMIT 0 {cmd.get('limit', 50)}"


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

def clean_cypher_output(cypher_input) -> str:
    """
    Nettoie la sortie LLM pour récupérer uniquement la requête Cypher.
    Accepte une string ou un dict {"type": "cypher", "content": "..."}
    """
    if not cypher_input:
        return ""

    # Si dict, récupérer la clé content
    if isinstance(cypher_input, dict):
        cypher_text = cypher_input.get("content", "")
    else:
        cypher_text = str(cypher_input)

    # Supprimer préfixe CYPHER:
    cypher_text = cypher_text.strip()
    if cypher_text.upper().startswith("CYPHER:"):
        cypher_text = cypher_text[len("CYPHER:"):].strip()

    # Supprimer retours à la ligne et espaces multiples
    cypher_text = " ".join(cypher_text.split())

    return cypher_text



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
    hbase_query=""
    rdf_query=""
    headers = []
    table_rows = []
    pretty_results = ""
    selected_db = "mongodb"
    analysis_explanation=""
    result_type=""
    query_type=""

    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        selected_db = request.form.get('db_choice', 'mongodb')
        print("🔹 Base sélectionnée :", selected_db)

        # Initialiser conversation spécifique si n'existe pas
        if selected_db not in session['conversation']:
            session['conversation'][selected_db] = []

        if question:
            normalized_question = preprocess_question(question)
            print("normalized question", normalized_question)
            session['conversation'][selected_db].append({'role': 'user', 'text': question})
            detect_type= detect_query_type1(normalized_question)
            print("datected type", detect_type)
            if detect_type =="convert_nosql":
                selected_db = detect_database_language(normalized_question)
                llm_response = analyze_query(normalize_nl_prefix(normalized_question))
                print(llm_response)

                if llm_response.get('status') == 'success':
                    explanation = llm_response.get('explanation', {})
                    detected_lang = llm_response.get('detected_language', 'unknown')

                    analysis_explanation = {
                        'language': detected_lang,
                        'objective': explanation.get('objective', 'N/A'),
                        'breakdown': explanation.get('breakdown', []),
                        'expected_result': explanation.get('expected_result', 'N/A'),
                        'optimization_tips': explanation.get('optimization_tips', []),
                        'human_readable': explanation.get('human_readable', 'N/A')
                    }

                    response_text = f"✅ Requête {detected_lang.upper()} analysée avec succès !"
                    result_type = 'convert_nosql'
                    results = []
                    metadata = {'language': detected_lang}

                    sql_query = llm_response.get('original_query', question)
                    redis_query = ""
                    hbase_query = ""
                    neo4j_query = ""
                    sparql_query = ""
                else:
                    error_msg = llm_response.get('message', 'Erreur inconnue')
                    response_text = f"❌ Erreur: {error_msg}"
                    result_type = 'error'
                    metadata = {'message': error_msg}

            
            else:
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

                        if not response:
                                print("⚠️ Requête vide, impossible d'exécuter")
                        else:
                            print("🔹 Requête à exécuter :", response)
                            query_type = detect_query_type(normalized_question)
                        
                        cypher_query = None

                        if query_type == "read":
                            # Lecture : le LLM doit générer du Cypher lisible
                            if isinstance(response, str) and response.strip():
                                cypher_query = response
                                results = neo4j_executor.run_query(cypher_query)
                                pretty_results = json.dumps(results, indent=4, ensure_ascii=False)
                                response_text = generate_response_text(results)
                            else:
                                response_text = "ℹ️ LLM n'a pas généré de requête Cypher valide."
                                pretty_results = ""
                                results = []

                        elif query_type == "write":
                            # Écriture : gérer dict et string brute
                            if isinstance(response, dict):
                                r_type = response.get("type")
                                content = response.get("content", "").strip()

                                if r_type == "clarification":
                                    response_text = f"ℹ️ More information is required: {content}"
                                    pretty_results = content
                                    results = []
                                    cypher_query = None

                                elif r_type == "cypher":
                                    cypher_query = clean_cypher_output(content)
                                    if cypher_query:
                                        results = neo4j_executor.run_query(cypher_query)
                                        pretty_results = json.dumps(results, indent=4, ensure_ascii=False)
                                        response_text = "✅ Write executed successfully"
                                    else:
                                        response_text = "⚠️ LLM n'a pas fourni de Cypher exécutable"
                                        results = []

                                elif r_type == "error":
                                    response_text = f"❌ LLM Error: {content}"
                                    pretty_results = content
                                    results = []

                                else:
                                    response_text = "⚠️ Type de réponse LLM inconnu pour Neo4j"
                                    results = []

                            elif isinstance(response, str):
                                # Si string brute, vérifier qu'elle ressemble à du Cypher
                                cypher_query = clean_cypher_output(response)
                                if cypher_query.upper().startswith(("MATCH", "CREATE", "MERGE", "RETURN")):
                                    results = neo4j_executor.run_query(cypher_query)
                                    pretty_results = json.dumps(results, indent=4, ensure_ascii=False)
                                    response_text = "✅ Write exécuté avec succès"
                                else:
                                    response_text = "⚠️ LLM n'a pas généré de Cypher exécutable"
                                    results = []

                        else:
                            response_text = "❌ Impossible de détecter le type de requête Neo4j"
                            results = []



                    elif selected_db == "redis":
                        response = generate_redis_command(normalized_question)
                        redis_query = normalize_redis_command(response)
                        print(redis_query)
                        results = execute_redis_command(redis_query)
                        pretty_results = json.dumps(results, indent=4, ensure_ascii=False)
                        response_text = generate_response_text(results)
                        redis_query = redis_command_to_string(redis_query)
                        
                        
                    elif selected_db =="hbase":
                        response = generate_hbase_query(normalized_question)
                        hbase_query=response
                        results = hbase_executor.run_query(response)
                        from bson import json_util
                        pretty_results = json.dumps(results, indent=4, default=json_util.default, ensure_ascii=False)
                        response_text = generate_response_text(results)
                        
                    elif selected_db =="rdf":
                        rdf_query= gemini_client_rdf.generate_rdf(normalized_question, rdf_data.extract_ontology_from_fuseki())
                    
                    elif selected_db == "all":
                        # Essayer de générer la requête MongoDB
                        mongo_response = generate_mongodb_query(normalized_question)
                        
                        # Initialiser un dict par défaut
                        mongo_query_dict = {
                            'type': 'select',
                            'filter': {},
                            'aggregation': None,
                            'group_by': None,
                            'sort': None,
                            'limit': 20,
                            'collection': 'movies'
                        }
                        
                        # Analyser la question pour remplir le dict
                        normalized_lower = normalized_question.lower()
                        
                        # Détecter le type de requête
                        if 'compter' in normalized_lower or 'nombre' in normalized_lower or 'count' in normalized_lower:
                            mongo_query_dict['type'] = 'count'
                        elif 'moyenne' in normalized_lower or 'average' in normalized_lower or 'avg' in normalized_lower:
                            mongo_query_dict['type'] = 'avg'
                            mongo_query_dict['aggregation'] = {'field': 'rating'}
                        elif 'somme' in normalized_lower or 'sum' in normalized_lower or 'total' in normalized_lower:
                            mongo_query_dict['type'] = 'sum'
                            mongo_query_dict['aggregation'] = {'field': 'rating'}
                        elif 'maximum' in normalized_lower or 'max' in normalized_lower or 'plus haut' in normalized_lower:
                            mongo_query_dict['type'] = 'max'
                            mongo_query_dict['aggregation'] = {'field': 'rating'}
                        elif 'minimum' in normalized_lower or 'min' in normalized_lower or 'plus bas' in normalized_lower:
                            mongo_query_dict['type'] = 'min'
                            mongo_query_dict['aggregation'] = {'field': 'rating'}
                        
                        # Détecter des filtres simples
                        if 'rating' in normalized_lower:
                            import re
                            rating_match = re.search(r'rating\s*[><=]?\s*(\d+\.?\d*)', normalized_lower)
                            if rating_match:
                                rating_value = float(rating_match.group(1))
                                if '>' in normalized_lower or 'supérieur' in normalized_lower:
                                    mongo_query_dict['filter']['rating'] = {'$gt': rating_value}
                                elif '<' in normalized_lower or 'inférieur' in normalized_lower:
                                    mongo_query_dict['filter']['rating'] = {'$lt': rating_value}
                                else:
                                    mongo_query_dict['filter']['rating'] = rating_value
                        
                        if 'année' in normalized_lower or 'year' in normalized_lower:
                            import re
                            year_match = re.search(r'(?:année|year)\s*[=:]?\s*(\d{4})', normalized_lower)
                            if year_match:
                                mongo_query_dict['filter']['year'] = int(year_match.group(1))
                        
                        if 'genre' in normalized_lower:
                            import re
                            genre_match = re.search(r'genre\s*(?:est|de|:)?\s*(\w+)', normalized_lower)
                            if genre_match:
                                mongo_query_dict['filter']['genre'] = genre_match.group(1)
                        
                        # Convertir en syntaxes des différentes bases
                        from app_all import (
                            convert_to_mongodb_syntax,
                            convert_to_redis_syntax,
                            convert_to_hbase_syntax,
                            convert_to_neo4j_syntax
                        )
                        
                        mongo_query = convert_to_mongodb_syntax(mongo_query_dict)
                        redis_query = convert_to_redis_syntax(mongo_query_dict)
                        hbase_query = convert_to_hbase_syntax(mongo_query_dict)
                        cypher_query = convert_to_neo4j_syntax(mongo_query_dict)
                        
                        # Exécuter sur MongoDB si vous voulez
                        try:
                            results = executor.run_query(mongo_query_dict)
                            if isinstance(results, list):
                                response_text = f"📊 Traductions générées pour {len(results)} résultats MongoDB"
                            else:
                                response_text = "📊 Traductions multi-bases générées"
                        except Exception as e:
                            results = []
                            response_text = "📊 Traductions multi-bases générées"
                        
                        # Ajouter à la session
                        if selected_db not in session['conversation']:
                            session['conversation'][selected_db] = []
                        
                        session['conversation'][selected_db].append({
                            'role': 'assistant', 
                            'text': response_text
                        })
                        
                        # Préparer les données pour le template
                        all_translations = {
                            'mongodb': mongo_query,
                            'redis': redis_query,
                            'hbase': hbase_query,
                            'neo4j': cypher_query,
                            'results_count': len(results) if isinstance(results, list) else 0
                        }
                    

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
        rdf_query=rdf_query,
        table_rows=table_rows,
        selected_db=selected_db,
        pretty_results=pretty_results,
        hbase_query=hbase_query,
        analysis_explanation=analysis_explanation,
        result_type=result_type
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
