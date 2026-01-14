import os
import json
from flask import Flask, render_template, request, session

# Importations MongoDB
from llm.mongodb_llm_old import (
    generate_mongodb_query,
    execute_mongodb_query
)

# Importations Classifier
from llm.classifier_old import (
    init_groq_client,
    format_explanation_output,
    analyze_query,
    explain_query_with_llm
)

# Importations Redis
from llm.redis_llm_old import (
    generate_redis_query,
    execute_redis_query,
    init_redis,
    check_redis_data
)

# Importation pour MongoDB DataLoader
from connectors.mongodb_connector import DataLoader

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "secret-key-123")
import spacy 
import re 

# Variables globales
df = None
redis_client = None

def normalize_text(text):
    """Normalise le texte pour gérer les pluriels et variations"""
    # Convertir en minuscules
    text = text.lower()
    
    # Liste des termes avec leurs variations
    term_variations = {
        'ratings': 'rating',
        'notes': 'rating',
        'évaluations': 'rating',
        'score': 'rating',
        'scores': 'rating',
        'produits': 'product',
        'articles': 'product',
        'items': 'product',
        'catégories': 'category',
        'prix': 'price',
        'tarifs': 'price',
        'coûts': 'price',
        'supérieur à': '>',
        'supérieure à': '>',
        'supérieurs à': '>',
        'supérieures à': '>',
        'plus grand que': '>',
        'plus grande que': '>',
        'plus grands que': '>',
        'plus grandes que': '>',
        'inférieur à': '<',
        'inférieure à': '<',
        'inférieurs à': '<',
        'inférieures à': '<',
        'plus petit que': '<',
        'plus petite que': '<',
        'plus petits que': '<',
        'plus petites que': '<',
        'égal à': '=',
        'égale à': '=',
        'égaux à': '=',
        'égales à': '='
    }
    
    # Remplacer les variations par leurs formes canoniques
    for variation, canonical in term_variations.items():
        text = re.sub(r'\b' + re.escape(variation) + r'\b', canonical, text)
    
    return text

def preprocess_question(question):
    """Prétraite la question avant de l'envoyer au système NLP"""
    # Normaliser le texte
    normalized = normalize_text(question)
    
    # Remplacer les opérateurs en français par des symboles
    replacements = {
        'supérieur': '>',
        'supérieure': '>',
        'inférieur': '<',
        'inférieure': '<',
        'égal': '=',
        'égale': '='
    }
    
    for fr_word, symbol in replacements.items():
        pattern = fr' {fr_word} à (\d+)'
        match = re.search(pattern, normalized)
        if match:
            number = match.group(1)
            normalized = normalized.replace(match.group(0), f' {symbol} {number}')
    
    return normalized

def convert_to_mongodb_syntax(query_dict):
    """Convertit la structure interne en syntaxe MongoDB réelle"""
    query_type = query_dict.get('type', 'select')
    filter_query = query_dict.get('filter', {})
    aggregation = query_dict.get('aggregation')
    group_by = query_dict.get('group_by')
    sort_spec = query_dict.get('sort')
    limit = query_dict.get('limit')
    
    mongodb_query = ""
    
    if query_type == 'count':
        if group_by:
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$group": {"_id": f"${group_by}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            pipeline = [stage for stage in pipeline if stage is not None]
            mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
        else:
            if filter_query:
                mongodb_query = f"db.products.countDocuments({json.dumps(filter_query, indent=2)})"
            else:
                mongodb_query = "db.products.countDocuments({})"
    
    elif query_type == 'avg' and aggregation:
        field = aggregation.get('field', 'rating')
        pipeline = [
            {"$match": filter_query} if filter_query else None,
            {"$group": {"_id": None if not group_by else f"${group_by}", 
                       "average": {"$avg": f"${field}"}}},
            {"$sort": {"average": -1}} if group_by else None
        ]
        pipeline = [stage for stage in pipeline if stage is not None]
        mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
    
    elif query_type == 'sum' and aggregation:
        field = aggregation.get('field', 'discounted_price')
        pipeline = [
            {"$match": filter_query} if filter_query else None,
            {"$group": {"_id": None if not group_by else f"${group_by}", 
                       "sum": {"$sum": f"${field}"}}},
            {"$sort": {"sum": -1}} if group_by else None
        ]
        pipeline = [stage for stage in pipeline if stage is not None]
        mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
    
    elif query_type in ['max', 'min']:
        field = aggregation.get('field', 'rating') if aggregation else 'rating'
        order = -1 if query_type == 'max' else 1
        if group_by:
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$sort": {field: order}},
                {"$group": {"_id": f"${group_by}", 
                           f"{query_type}_doc": {"$first": "$$ROOT"}}},
                {"$replaceRoot": {"newRoot": f"${query_type}_doc"}}
            ]
            pipeline = [stage for stage in pipeline if stage is not None]
            mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
        else:
            mongodb_query = f"db.products.find({json.dumps(filter_query, indent=2)}).sort({{\"{field}\": {order}}}).limit(1)"
    
    elif query_type == 'group':
        if not group_by:
            group_by = 'category'
        agg_field = aggregation.get('field', 'product_id') if aggregation else 'product_id'
        agg_op = aggregation.get('operation', 'count') if aggregation else 'count'
        
        if agg_op == 'count':
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$group": {"_id": f"${group_by}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
        elif agg_op == 'avg':
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$group": {"_id": f"${group_by}", "average": {"$avg": f"${agg_field}"}}},
                {"$sort": {"average": -1}}
            ]
        elif agg_op == 'sum':
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$group": {"_id": f"${group_by}", "sum": {"$sum": f"${agg_field}"}}},
                {"$sort": {"sum": -1}}
            ]
        
        pipeline = [stage for stage in pipeline if stage is not None]
        mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
    
    else:  # select
        query_parts = []
        if filter_query:
            query_parts.append(f"db.products.find({json.dumps(filter_query, indent=2)})")
        else:
            query_parts.append("db.products.find({})")
        
        if sort_spec:
            sort_field = sort_spec.get('field', 'rating')
            sort_order = sort_spec.get('order', -1)
            query_parts.append(f".sort({{\"{sort_field}\": {sort_order}}})")
        
        if limit:
            query_parts.append(f".limit({limit})")
        
        mongodb_query = "".join(query_parts)
    
    return mongodb_query


def convert_to_redis_syntax(query_dict):
    """Convertit en commandes Redis"""
    query_type = query_dict.get('type', 'select')
    filter_query = query_dict.get('filter', {})
    aggregation = query_dict.get('aggregation')
    limit = query_dict.get('limit', 20)
    
    redis_commands = []
    
    category_filter = None
    if filter_query.get('category', {}).get('$regex'):
        category_filter = filter_query['category']['$regex'].lower()
    
    if query_type == 'count':
        redis_commands.append("SCARD products:all" if not category_filter else f"SCARD category:{category_filter}")
    elif query_type in ['avg', 'sum']:
        field = aggregation.get('field', 'rating') if aggregation else 'rating'
        redis_commands.append("SMEMBERS products:all")
        redis_commands.append(f"# Pour chaque ID: HGET product:<id> {field}")
    elif query_type in ['max', 'min']:
        field = aggregation.get('field', 'rating') if aggregation else 'rating'
        if field == 'rating':
            redis_commands.append("ZREVRANGE products:by_rating 0 0" if query_type == 'max' else "ZRANGE products:by_rating 0 0")
        elif field == 'discounted_price':
            redis_commands.append("ZREVRANGE products:by_price 0 0" if query_type == 'max' else "ZRANGE products:by_price 0 0")
        redis_commands.append("HGETALL product:<id>")
    else:
        redis_commands.append("SMEMBERS products:all")
        redis_commands.append("HGETALL product:<id>")
    
    return "\n".join(redis_commands)


def convert_to_hbase_syntax(query_dict):
    """Convertit en commandes HBase"""
    query_type = query_dict.get('type', 'select')
    limit = query_dict.get('limit', 20)
    
    if query_type == 'count':
        return "count 'products'"
    elif query_type in ['avg', 'sum', 'max', 'min']:
        return "scan 'products', {COLUMNS => ['data:price', 'data:rating']}"
    else:
        return f"scan 'products', {{LIMIT => {limit}}}"


def convert_to_neo4j_syntax(query_dict):
    """Convertit en Cypher (Neo4j)"""
    query_type = query_dict.get('type', 'select')
    filter_query = query_dict.get('filter', {})
    aggregation = query_dict.get('aggregation')
    group_by = query_dict.get('group_by')
    limit = query_dict.get('limit', 20)
    
    cypher_parts = ["MATCH (p:Product)"]
    
    # WHERE clause
    where_conditions = []
    for key, value in filter_query.items():
        if isinstance(value, dict):
            if "$gt" in value:
                where_conditions.append(f"p.{key} > {value['$gt']}")
            if "$lt" in value:
                where_conditions.append(f"p.{key} < {value['$lt']}")
            if "$regex" in value:
                pattern = value["$regex"]
                where_conditions.append(f"p.{key} =~ '(?i).*{pattern}.*'")
        else:
            where_conditions.append(f"p.{key} = '{value}'")
    
    if where_conditions:
        cypher_parts.append("WHERE " + " AND ".join(where_conditions))
    
    # Agrégations
    if query_type == 'count':
        if group_by:
            cypher_parts.append(f"RETURN p.{group_by} as group, COUNT(p) as count")
            cypher_parts.append("ORDER BY count DESC")
        else:
            cypher_parts.append("RETURN COUNT(p) as count")
    elif query_type == 'avg' and aggregation:
        field = aggregation.get('field', 'rating')
        cypher_parts.append(f"RETURN AVG(p.{field}) as average")
    elif query_type == 'sum' and aggregation:
        field = aggregation.get('field', 'discounted_price')
        cypher_parts.append(f"RETURN SUM(p.{field}) as sum")
    else:  # select
        cypher_parts.append("RETURN p.product_name, p.category, p.rating, p.discounted_price")
        if limit:
            cypher_parts.append(f"LIMIT {limit}")
    
    return "\n".join(cypher_parts)


# Initialiser les systèmes au démarrage
print("=" * 50)
print("🚀 Initialisation de l'application multi-base...")
print("=" * 50)

# Initialiser MongoDB
# Initialiser MongoDB
print("\n📊 Initialisation MongoDB...")
loader = DataLoader()   # ← plus de path
df = loader.init_data()
print(f"✅ MongoDB: {len(df) if df is not None else 0} produits chargés")

# Initialiser Redis
print("\n🔴 Initialisation Redis...")
redis_client = init_redis()
redis_info = check_redis_data()
print(f"✅ Redis: {redis_info}")

# Initialiser le client Groq
print("\n🤖 Initialisation Groq...")
groq_available = init_groq_client()
print(f"✅ Groq: {'Disponible' if groq_available else 'Non disponible'}")

print("=" * 50)
print("✅ Application multi-base prête !")
print("=" * 50)


@app.route('/', methods=['GET', 'POST'])
def index():
    """Route principale de l'application avec mode conversationnel CRUD"""
    if 'conversation' not in session:
        session['conversation'] = []
    
    # Variables pour le template
    question = ""
    sql_query = ""
    redis_query = ""
    hbase_query = ""
    neo4j_query = ""
    sparql_query = ""
    response_text = ""
    results = []
    result_type = None
    metadata = {}
    analysis_explanation = None
    crud_prompt = None
    
    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        
        if question:
            print(f"\n📝 Nouvelle question: {question}")
            
            # 🔧 CORRECTION ICI : Prétraiter la question pour normaliser les termes
            normalized_question = preprocess_question(question)
            print(f"📝 Question normalisée: {normalized_question}")
            
            session['conversation'].append({
                'role': 'user', 
                'text': question
            })
            
            # 🔧 MODIFICATION : Utiliser la question normalisée pour générer la requête
            mongo_query_dict = generate_mongodb_query(normalized_question)
            query_type = mongo_query_dict.get('type')
            
            print(f"🔍 Type de requête: {query_type}")
            
            # 🆕 GESTION CRUD INCOMPLET (CHAMPS MANQUANTS)
            if query_type == 'crud_incomplete':
                operation = mongo_query_dict.get('operation')
                missing_fields = mongo_query_dict.get('missing_fields', [])
                prompt_message = mongo_query_dict.get('prompt', '')
                
                print(f"⚠️ CRUD incomplet - Champs manquants: {missing_fields}")
                
                response_text = prompt_message
                result_type = 'crud_prompt'
                
                # Afficher un exemple de requête
                if operation == 'create':
                    sql_query = "db.products.insertOne({...})"
                    redis_query = "HSET products:new_id ..."
                elif operation == 'update':
                    sql_query = "db.products.updateOne({...}, {$set: {...}})"
                    redis_query = "HSET products:id ..."
                elif operation == 'delete':
                    sql_query = "db.products.deleteOne({...})"
                    redis_query = "DEL products:id"
                
                hbase_query = "# En attente de données complètes"
                neo4j_query = "# En attente de données complètes"
                sparql_query = ""
                
                results = []
                metadata = {
                    'operation': operation,
                    'missing_fields': missing_fields
                }
            
            # 🆕 GESTION CRUD INVALIDE
            elif query_type == 'crud_invalid':
                error_message = mongo_query_dict.get('error', 'Erreur de validation')
                
                print(f"❌ CRUD invalide: {error_message}")
                
                response_text = error_message
                result_type = 'error'
                metadata = {'message': error_message}
            
            # GESTION CRUD COMPLET ET VALIDE
            elif query_type in ['create', 'update', 'delete']:
                print(f"✅ Opération CRUD complète: {query_type}")
                
                # Récupérer toutes les requêtes générées
                all_queries = mongo_query_dict.get('queries', {})
                
                sql_query = all_queries.get('mongodb', '')
                redis_query = all_queries.get('redis', '')
                hbase_query = all_queries.get('hbase', '')
                neo4j_query = all_queries.get('neo4j', '')
                sparql_query = all_queries.get('web_semantique', '')
                
                # Exécuter l'opération sur MongoDB
                result_type, results, metadata = execute_mongodb_query(mongo_query_dict)
                response_text = generate_response_text(result_type, results, metadata, question)
            
            # GESTION ANALYSE DE REQUÊTE
            elif query_type == 'convert_nosql':
                print("🔄 Mode analyse de requête détecté!")
                
                analysis = mongo_query_dict.get('analysis')
                
                if not analysis:
                    # 🔧 MODIFICATION : Analyser la question normalisée
                    analysis = analyze_query(normalized_question)
                
                if analysis.get('status') == 'success':
                    explanation = analysis.get('explanation', {})
                    detected_lang = analysis.get('detected_language', 'unknown')
                    
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
                    
                    sql_query = analysis.get('original_query', question)
                    redis_query = ""
                    hbase_query = ""
                    neo4j_query = ""
                    sparql_query = ""
                else:
                    error_msg = analysis.get('message', 'Erreur inconnue')
                    response_text = f"❌ Erreur: {error_msg}"
                    result_type = 'error'
                    metadata = {'message': error_msg}
            
            # TRAITEMENT NORMAL
            else:
                print("🔄 Mode requête normale")
                
                sql_query = convert_to_mongodb_syntax(mongo_query_dict)
                redis_query = convert_to_redis_syntax(mongo_query_dict)
                hbase_query = convert_to_hbase_syntax(mongo_query_dict)
                neo4j_query = convert_to_neo4j_syntax(mongo_query_dict)
                sparql_query = ""
                
                result_type, results, metadata = execute_mongodb_query(mongo_query_dict)
                response_text = generate_response_text(result_type, results, metadata, question)
            
            # Ajouter la réponse à l'historique
            session['conversation'].append({
                'role': 'assistant', 
                'text': response_text
            })
            
            if len(session['conversation']) > 10:
                session['conversation'] = session['conversation'][-10:]
            
            session.modified = True
    
    return render_template(
        'index.html',
        question=question,
        conversation=session.get('conversation', []),
        results=results,
        result_type=result_type,
        metadata=metadata,
        sql_query=sql_query,
        redis_query=redis_query,
        hbase_query=hbase_query,
        neo4j_query=neo4j_query,
        sparql_query=sparql_query,
        response=response_text,
        analysis_explanation=analysis_explanation
    )


def generate_response_text(result_type, results, metadata, question):
    """Génère le texte de réponse selon le type de résultat"""
    
    # RÉPONSES CRUD
    if result_type == 'create':
        doc_id = metadata.get('id', 'N/A')
        count = metadata.get('count', 1)
        return f"✅ {count} document créé avec succès ! ID: {doc_id}"
    
    elif result_type == 'update':
        count = metadata.get('count', 0)
        fields = ', '.join(metadata.get('fields_updated', []))
        return f"✅ {count} document(s) mis à jour (champs: {fields})"
    
    elif result_type == 'delete':
        count = metadata.get('count', 0)
        return f"✅ {count} document(s) supprimé(s)"
    
    # 🆕 RÉPONSE POUR PROMPT CONVERSATIONNEL
    elif result_type == 'crud_prompt':
        operation = metadata.get('operation', 'opération')
        return f"💬 Informations requises pour {operation}"
    
    # RÉPONSES NORMALES (inchangées)
    if result_type == 'error':
        return f"❌ Erreur: {metadata.get('message', 'Erreur inconnue')}"
    
    elif result_type == 'count':
        if 'groups' in metadata:
            return f"📊 {metadata['groups']} groupes (total: {metadata['total']} produits)"
        else:
            return f"✅ Nombre total: {metadata.get('total', 0)}"
    
    elif result_type == 'avg':
        if results and 'average' in results[0]:
            field = results[0].get('field', 'rating')
            avg = results[0]['average']
            return f"📈 Moyenne {field}: {avg:.2f}"
        return "📈 Moyenne calculée"
    
    elif result_type == 'sum':
        if results and 'sum' in results[0]:
            field = results[0].get('field', 'price')
            total = results[0]['sum']
            return f"➕ Somme {field}: {total:.2f}"
        return "➕ Somme calculée"
    
    elif result_type == 'max':
        if results and 'value' in results[0]:
            product = results[0].get('product', 'produit')
            value = results[0]['value']
            return f"🔝 Maximum: {value} ({product})"
        return "🔝 Valeur maximale trouvée"
    
    elif result_type == 'min':
        if results and 'value' in results[0]:
            product = results[0].get('product', 'produit')
            value = results[0]['value']
            return f"⬇️ Minimum: {value} ({product})"
        return "⬇️ Valeur minimale trouvée"
    
    elif result_type == 'group':
        groups = metadata.get('groups', 0)
        return f"📊 Groupement: {groups} groupes"
    
    elif result_type == 'select':
        if results:
            total = metadata.get('count', len(results))
            return f"✅ {total} produit(s) trouvé(s)"
        else:
            return "❌ Aucun produit trouvé"
    
    return "✅ Requête exécutée"


@app.route('/clear', methods=['GET'])
def clear_conversation():
    """Efface l'historique de conversation"""
    session['conversation'] = []
    session.modified = True
    from flask import redirect
    return redirect('/')


@app.route('/health', methods=['GET'])
def health_check():
    """Vérifie l'état de l'application"""
    status = {
        'status': 'healthy',
        'mongodb_loaded': df is not None and not df.empty,
        'redis_connected': redis_client is not None,
        'mongodb_count': len(df) if df is not None else 0
    }
    return status


if __name__ == '__main__':
    print("\n🌐 Démarrage du serveur Flask...")
    print("👉 http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)