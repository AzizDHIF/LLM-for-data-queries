import os
import json
from flask import Flask, render_template, request, session

# Importations MongoDB
from llm.mongodb_llm import (
    generate_mongodb_query,
    execute_mongodb_query,
 # 🆕 AJOUTER CETTE LIGNE
     # 🆕 AJOUTER CETTE LIGNE
     
)
from llm.classifier import (
    init_groq_client,
    format_explanation_output,
    analyze_query,
    explain_query_with_llm)

# Importations Redis
from llm.redis_llm import (
    generate_redis_query,
    execute_redis_query,
    init_redis,
    check_redis_data
)

# Importation pour MongoDB DataLoader
from connectors.mongodb_connector import DataLoader

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "secret-key-123")

# Variables globales
df = None  # Pour MongoDB
redis_client = None  # Pour Redis



def safe_nunique(series):
    """
    Calcule le nombre de valeurs uniques de manière sécurisée,
    même pour les séries contenant des listes ou autres types non hachables.
    """
    try:
        # Essayer la méthode normale
        return int(series.nunique())
    except TypeError:
        try:
            # Essayer avec conversion en string
            return int(series.astype(str).nunique())
        except:
            return "N/A"
    except Exception:
        return "N/A"

def safe_top_values(series, n=5):
    """
    Récupère les top valeurs de manière sécurisée
    """
    try:
        value_counts = series.value_counts().head(n)
        top_vals = {}
        for k, v in value_counts.items():
            # Convertir en string si nécessaire
            key_str = str(k) if not isinstance(k, (str, int, float)) else k
            top_vals[key_str] = int(v)
        return top_vals
    except TypeError:
        try:
            # Essayer avec conversion en string
            str_series = series.astype(str)
            value_counts = str_series.value_counts().head(n)
            return value_counts.to_dict()
        except:
            return "N/A"
    except Exception:
        return "N/A"
    
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
            # Agrégation avec groupement
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$group": {"_id": f"${group_by}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            pipeline = [stage for stage in pipeline if stage is not None]
            mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
        else:
            # Simple count
            if filter_query:
                mongodb_query = f"db.products.countDocuments({json.dumps(filter_query, indent=2)})"
            else:
                mongodb_query = "db.products.countDocuments({})"
    
    elif query_type == 'avg' and aggregation:
        field = aggregation.get('field', 'rating')
        if group_by:
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$group": {"_id": f"${group_by}", "average": {"$avg": f"${field}"}}},
                {"$sort": {"average": -1}}
            ]
            pipeline = [stage for stage in pipeline if stage is not None]
            mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
        else:
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$group": {"_id": None, "average": {"$avg": f"${field}"}}}
            ]
            pipeline = [stage for stage in pipeline if stage is not None]
            mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
    
    elif query_type == 'sum' and aggregation:
        field = aggregation.get('field', 'discounted_price')
        if group_by:
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$group": {"_id": f"${group_by}", "sum": {"$sum": f"${field}"}}},
                {"$sort": {"sum": -1}}
            ]
            pipeline = [stage for stage in pipeline if stage is not None]
            mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
        else:
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$group": {"_id": None, "sum": {"$sum": f"${field}"}}}
            ]
            pipeline = [stage for stage in pipeline if stage is not None]
            mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
    
    elif query_type == 'max' and aggregation:
        field = aggregation.get('field', 'rating')
        if group_by:
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$sort": {field: -1}},
                {"$group": {"_id": f"${group_by}", "max_doc": {"$first": "$ROOT"}}},
                {"$replaceRoot": {"newRoot": "$max_doc"}}
            ]
            pipeline = [stage for stage in pipeline if stage is not None]
            mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
        else:
            mongodb_query = f"db.products.find({json.dumps(filter_query, indent=2)}).sort({{\"{field}\": -1}}).limit(1)"
    
    elif query_type == 'min' and aggregation:
        field = aggregation.get('field', 'discounted_price')
        if group_by:
            pipeline = [
                {"$match": filter_query} if filter_query else None,
                {"$sort": {field: 1}},
                {"$group": {"_id": f"${group_by}", "min_doc": {"$first": "$ROOT"}}},
                {"$replaceRoot": {"newRoot": "$min_doc"}}
            ]
            pipeline = [stage for stage in pipeline if stage is not None]
            mongodb_query = f"db.products.aggregate({json.dumps(pipeline, indent=2)})"
        else:
            mongodb_query = f"db.products.find({json.dumps(filter_query, indent=2)}).sort({{\"{field}\": 1}}).limit(1)"
    
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
    query_type = query_dict.get('type', 'select')
    filter_query = query_dict.get('filter', {})
    aggregation = query_dict.get('aggregation')
    group_by = query_dict.get('group_by')
    sort_spec = query_dict.get('sort')
    limit = query_dict.get('limit', 20)
    
    redis_commands = []
    
    category_filter = None
    if filter_query.get('category', {}).get('$regex'):
        category_filter = filter_query['category']['$regex'].lower()
    
    if query_type == 'count':
        if group_by == 'category':
            redis_commands.append("KEYS category:*")
            redis_commands.append("# Pour chaque catégorie: SCARD category:<nom>")
        elif category_filter:
            redis_commands.append(f"SCARD category:{category_filter}")
        else:
            redis_commands.append("SCARD products:all")
    
    elif query_type in ['avg', 'sum']:
        field = aggregation.get('field') if aggregation else 'rating'
        if category_filter:
            redis_commands.append(f"SMEMBERS category:{category_filter}")
        else:
            redis_commands.append("SMEMBERS products:all")
        redis_commands.append(f"# Pour chaque ID: HGET product:<id> {field} (calcul côté client)")
    
    elif query_type in ['max', 'min']:
        field = aggregation.get('field') if aggregation else 'rating'
        if field == 'rating':
            if category_filter:
                redis_commands.append(f"SMEMBERS category:{category_filter}")
                redis_commands.append(f"# Pour chaque ID: HGET product:<id> rating (trouver max/min côté client)")
            else:
                if query_type == 'max':
                    redis_commands.append("ZREVRANGE products:by_rating 0 0 WITHSCORES")
                else:
                    redis_commands.append("ZRANGE products:by_rating 0 0 WITHSCORES")
                redis_commands.append("HGETALL product:<id>")
        elif field == 'discounted_price':
            if category_filter:
                redis_commands.append(f"SMEMBERS category:{category_filter}")
                redis_commands.append(f"# Pour chaque ID: HGET product:<id> discounted_price (trouver max/min côté client)")
            else:
                if query_type == 'max':
                    redis_commands.append("ZREVRANGE products:by_price 0 0 WITHSCORES")
                else:
                    redis_commands.append("ZRANGE products:by_price 0 0 WITHSCORES")
                redis_commands.append("HGETALL product:<id>")
        else:
            redis_commands.append(f"# Nécessite ZSET products:by_{field}")
    
    elif query_type == 'group':
        if group_by == 'category':
            if category_filter:
                redis_commands.append(f"SCARD category:{category_filter}")
            else:
                redis_commands.append("KEYS category:*")
                redis_commands.append("# Pour chaque catégorie: SCARD category:<nom>")
        else:
            redis_commands.append(f"# Groupement par {group_by} nécessite indexation")
    
    else:  # select / search
        if category_filter:
            redis_commands.append(f"SMEMBERS category:{category_filter}")
        else:
            redis_commands.append("SMEMBERS products:all")
        
        if sort_spec:
            sort_field = sort_spec.get('field')
            sort_order = sort_spec.get('order', 'desc')
            if sort_field == 'rating':
                if sort_order == 'desc':
                    redis_commands.append("ZREVRANGE products:by_rating 0 -1")
                else:
                    redis_commands.append("ZRANGE products:by_rating 0 -1")
            elif sort_field == 'discounted_price':
                if sort_order == 'desc':
                    redis_commands.append("ZREVRANGE products:by_price 0 -1")
                else:
                    redis_commands.append("ZRANGE products:by_price 0 -1")
            else:
                redis_commands.append(f"# Tri par {sort_field} nécessite ZSET pré-calculé")
        
        if limit:
            redis_commands.append(f"# Limiter à {limit} résultats (côté client)")
        
        redis_commands.append("HGETALL product:<id>")
    
    return "\n".join(redis_commands)


def convert_to_hbase_syntax(query_dict):
    """Convertit en commandes HBase"""
    query_type = query_dict.get('type', 'select')
    filter_query = query_dict.get('filter', {})
    aggregation = query_dict.get('aggregation')
    group_by = query_dict.get('group_by')
    limit = query_dict.get('limit', 20)
    
    hbase_commands = []
    
    if query_type == 'count':
        # hbase_commands.append("# Compter avec MapReduce ou Coprocessor")
        hbase_commands.append("count 'products'")
        if filter_query:
            # hbase_commands.append("# Avec filtre (Scan + Filter):")
            hbase_commands.append("scan 'products', {FILTER => \"ValueFilter(=,'binary:match')\"}")
    
    elif query_type in ['avg', 'sum', 'max', 'min']:
        # hbase_commands.append("# HBase nécessite MapReduce pour agrégations")
        field = aggregation.get('field', 'rating') if aggregation else 'rating'
        # hbase_commands.append(f"# Pour {field}, utiliser Coprocessor Endpoint")
        hbase_commands.append("scan 'products', {COLUMNS => ['data:price', 'data:rating']}")
    
    elif query_type == 'group':
        # hbase_commands.append("# Groupement nécessite MapReduce")
        group_field = group_by if group_by else 'category'
        # hbase_commands.append(f"# Job MapReduce avec clé = {group_field}")
        hbase_commands.append("scan 'products'")
    
    else:  # select
        hbase_commands.append("# Scanner la table")
        if limit:
            hbase_commands.append(f"scan 'products', {{LIMIT => {limit}}}")
        else:
            hbase_commands.append("scan 'products'")
        
        if filter_query:
            # Construire le filtre
            filter_str = ""
            for key, value in filter_query.items():
                if isinstance(value, dict):
                    if "$gt" in value:
                        filter_str = f"SingleColumnValueFilter('data','{key}',>=,'binary:{value['$gt']}')"
            
            if filter_str:
                hbase_commands.append(f"# Avec filtre :")
                hbase_commands.append(f"scan 'products', {{FILTER => \"{filter_str}\"}}")
    
    return "\n".join(hbase_commands)

def convert_to_neo4j_syntax(query_dict):
    """Convertit en Cypher (Neo4j)"""
    query_type = query_dict.get('type', 'select')
    filter_query = query_dict.get('filter', {})
    aggregation = query_dict.get('aggregation')
    group_by = query_dict.get('group_by')
    sort_spec = query_dict.get('sort')
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
        if group_by:
            cypher_parts.append(f"RETURN p.{group_by} as group, AVG(p.{field}) as average")
            cypher_parts.append("ORDER BY average DESC")
        else:
            cypher_parts.append(f"RETURN AVG(p.{field}) as average")
    
    elif query_type == 'sum' and aggregation:
        field = aggregation.get('field', 'discounted_price')
        if group_by:
            cypher_parts.append(f"RETURN p.{group_by} as group, SUM(p.{field}) as sum")
            cypher_parts.append("ORDER BY sum DESC")
        else:
            cypher_parts.append(f"RETURN SUM(p.{field}) as sum")
    
    elif query_type == 'max' and aggregation:
        field = aggregation.get('field', 'rating')
        cypher_parts.append(f"RETURN p ORDER BY p.{field} DESC LIMIT 1")
    
    elif query_type == 'min' and aggregation:
        field = aggregation.get('field', 'discounted_price')
        cypher_parts.append(f"RETURN p ORDER BY p.{field} ASC LIMIT 1")
    
    elif query_type == 'group':
        if not group_by:
            group_by = 'category'
        cypher_parts.append(f"RETURN p.{group_by} as group, COUNT(p) as count")
        cypher_parts.append("ORDER BY count DESC")
    
    else:  # select
        cypher_parts.append("RETURN p.product_name, p.category, p.rating, p.discounted_price")
        
        if sort_spec:
            sort_field = sort_spec.get('field', 'rating')
            sort_order = "DESC" if sort_spec.get('order', -1) == -1 else "ASC"
            cypher_parts.append(f"ORDER BY p.{sort_field} {sort_order}")
        
        if limit:
            cypher_parts.append(f"LIMIT {limit}")
    
    return "\n".join(cypher_parts)

# Initialiser les systèmes au démarrage
print("=" * 50)
print("🚀 Initialisation de l'application multi-base...")
print("=" * 50)

# Initialiser MongoDB
print("\n📊 Initialisation MongoDB...")
loader = DataLoader(path="data/mongo_amazon.json")
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



# 2. MODIFIER LA ROUTE PRINCIPALE @app.route('/')
@app.route('/', methods=['GET', 'POST'])
def index():
    """
    Route principale de l'application.
    """
    # Initialiser la session pour la conversation
    if 'conversation' not in session:
        session['conversation'] = []
    
    # Variables pour le template
    question = ""
    sql_query = ""  # MongoDB
    redis_query = ""
    hbase_query = ""
    neo4j_query = ""
    response_text = ""
    results = []
    result_type = None
    metadata = {}
    analysis_explanation = None  # 🆕 NOUVELLE VARIABLE
    
    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        
        if question:
            print(f"\n📝 Nouvelle question: {question}")
            
            # Ajouter la question à l'historique de conversation
            session['conversation'].append({
                'role': 'user', 
                'text': question
            })
            
            # Générer la requête MongoDB structurée (base de référence)
            mongo_query_dict = generate_mongodb_query(question)
            print(f"🔍 Requête MongoDB générée: {json.dumps(mongo_query_dict, indent=2, ensure_ascii=False)}")
            
            # 🆕 GESTION DES DIFFÉRENTS TYPES DE REQUÊTES
            query_type = mongo_query_dict.get('type')
            
            if query_type == 'convert_nosql':
                print("🔄 Mode analyse de requête détecté!")
                
                # Récupérer l'analyse depuis mongo_query_dict
                analysis = mongo_query_dict.get('analysis')
                
                # Si l'analyse n'est pas présente, l'appeler directement
                if not analysis:
                    print("⚠️ Analyse manquante, appel direct d'analyze_query()")
                    analysis = analyze_query(question)
                
                print(f"📊 Statut de l'analyse: {analysis.get('status')}")
                
                # Vérifier le statut de l'analyse
                if analysis.get('status') == 'success':
                    explanation = analysis.get('explanation', {})
                    detected_lang = analysis.get('detected_language', 'unknown')
                    
                    print(f"✅ Langue détectée: {detected_lang}")
                    print(f"📝 Objectif: {explanation.get('objective', 'N/A')[:50]}...")
                    
                    # Stocker l'analyse pour l'affichage
                    analysis_explanation = {
                        'language': detected_lang,
                        'objective': explanation.get('objective', 'N/A'),
                        'breakdown': explanation.get('breakdown', []),
                        'expected_result': explanation.get('expected_result', 'N/A'),
                        'optimization_tips': explanation.get('optimization_tips', []),
                        'human_readable': explanation.get('human_readable', 'N/A')
                    }
                    
                    # Réponse textuelle
                    response_text = f"✅ Requête {detected_lang.upper()} analysée avec succès !"
                    
                    # Pas de résultats tabulaires, seulement l'analyse
                    result_type = 'convert_nosql'
                    results = []
                    metadata = {'language': detected_lang}
                    
                    # La "requête" affichée est celle analysée
                    sql_query = analysis.get('original_query', question)
                    redis_query = ""
                    hbase_query = ""
                    neo4j_query = ""
                    
                    print("✅ Analyse préparée pour l'affichage")
                else:
                    # Erreur d'analyse
                    error_msg = analysis.get('message', 'Erreur inconnue')
                    print(f"❌ Erreur d'analyse: {error_msg}")
                    response_text = f"❌ Erreur lors de l'analyse: {error_msg}"
                    result_type = 'error'
                    metadata = {'message': error_msg}
            
            elif query_type == 'schema':
                print("📋 Mode schéma détecté!")
                
                # Exécuter la requête pour obtenir le schéma
                result_type, results, metadata = execute_mongodb_query(mongo_query_dict)
                
                # Convertir en différentes syntaxes
                sql_query = json.dumps(mongo_query_dict, indent=2)
                redis_query = "# Redis ne gère pas les schémas de la même manière"
                hbase_query = "# HBase: describe 'products'"
                neo4j_query = "CALL db.schema.visualization()"
                
                # Générer la réponse textuelle pour le schéma
                schema_data = mongo_query_dict.get('schema', {})
                if schema_data:
                    response_text = f"📋 Schéma détecté : {len(schema_data)} colonnes"
                else:
                    response_text = "ℹ️ Informations sur la structure de la base"
            
            else:
                # 🔄 TRAITEMENT NORMAL (génération de requête)
                print("🔄 Mode génération de requête normal")
                
                # Convertir en différentes syntaxes
                sql_query = convert_to_mongodb_syntax(mongo_query_dict)
                redis_query = convert_to_redis_syntax(mongo_query_dict)
                hbase_query = convert_to_hbase_syntax(mongo_query_dict)
                neo4j_query = convert_to_neo4j_syntax(mongo_query_dict)
                
                # Exécuter la requête sur MongoDB (résultats principaux)
                result_type, results, metadata = execute_mongodb_query(mongo_query_dict)
                
                print(f"📊 Résultats MongoDB: {result_type}, {len(results)} éléments")
                
                # Générer la réponse textuelle (SANS passer query_dict)
                response_text = generate_response_text(result_type, results, metadata, question)
                analysis = explain_query_with_llm(
                    query=sql_query,          # 👈 la requête MongoDB générée
                    db_language="mongodb"
                )
            
            # Ajouter la réponse à l'historique de conversation
            session['conversation'].append({
                'role': 'assistant', 
                'text': response_text
            })
            
            # Limiter la taille de l'historique de conversation
            if len(session['conversation']) > 10:
                session['conversation'] = session['conversation'][-10:]
            
            # Marquer la session comme modifiée
            session.modified = True
    
    # Rendu du template
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
        response=response_text,
        analysis_explanation=analysis_explanation  # 🆕 NOUVELLE VARIABLE
    )

# 3. AJOUTER UNE NOUVELLE ROUTE POUR L'ANALYSE DIRECTE (OPTIONNEL)
@app.route('/analyze', methods=['POST'])
def analyze_nosql_query():
    """
    Route dédiée pour analyser une requête NoSQL
    """
    data = request.get_json()
    query = data.get('query', '')
    
    if not query:
        return {'error': 'Requête requise'}, 400
    
    # Analyser la requête
    analysis = analyze_query(query)
    
    return {
        'status': analysis.get('status'),
        'language': analysis.get('detected_language'),
        'explanation': analysis.get('explanation'),
        'formatted': format_explanation_output(analysis)
    }

def generate_response_text(result_type, results, metadata, question,query_dict=None):
    """Génère le texte de réponse selon le type de résultat"""
    if result_type == 'error':
        return f"❌ Erreur lors de l'exécution: {metadata.get('message', 'Erreur inconnue')}"
    
    elif result_type == 'count':
        if 'groups' in metadata:
            return f"📊 Résultats groupés: {metadata['groups']} groupes trouvés (total: {metadata['total']} produits)"
        else:
            return f"✅ Nombre total de produits: {metadata.get('total', 0)}"
    
    elif result_type == 'avg':
        if results and 'average' in results[0]:
            field = results[0].get('field', 'rating')
            avg_value = results[0]['average']
            count = metadata.get('total_count', metadata.get('count', 0))
            return f"📈 Moyenne {field}: {avg_value:.2f} (basée sur {count} produits)"
        return "📈 Moyenne calculée"
    
    elif result_type == 'sum':
        if results and 'sum' in results[0]:
            field = results[0].get('field', 'discounted_price')
            sum_value = results[0]['sum']
            count = metadata.get('total_count', metadata.get('count', 0))
            return f"➕ Somme {field}: {sum_value:.2f} (basée sur {count} produits)"
        return "➕ Somme calculée"
    
    elif result_type == 'max':
        if results and 'value' in results[0]:
            product = results[0].get('product', 'produit')
            value = results[0]['value']
            field = results[0].get('field', 'valeur')
            return f"🔝 {field} maximum: {value} ({product})"
        return "🔝 Valeur maximale trouvée"
    
    elif result_type == 'min':
        if results and 'value' in results[0]:
            product = results[0].get('product', 'produit')
            value = results[0]['value']
            field = results[0].get('field', 'valeur')
            return f"⬇️ {field} minimum: {value} ({product})"
        return "⬇️ Valeur minimale trouvée"
    
    elif result_type == 'group':
        displayed = metadata.get('displayed_groups', metadata.get('groups', 0))
        total = metadata.get('total_groups', displayed)
        count = metadata.get('total_count', 0)
        group_by = metadata.get('group_by', 'catégorie')
        return f"📊 Groupement par {group_by}: {displayed} groupes affichés (total: {total} groupes, {count} produits)"
    
    elif result_type == 'general_info':
        return "ℹ️ Informations générales sur la base de données"
    
    elif query_dict and query_dict.get('type') == 'schema':
        print("🔍 Analyse de schéma détectée !")

        schema = query_dict.get('schema', {})
        
        # Préparer l'analyse pour l'affichage
        analysis_explanation = {
            'language': 'schema',
            'objective': 'Affichage du schéma de la base',
            'breakdown': [],
            'expected_result': f'{len(schema)} champs détectés',
            'optimization_tips': [],
            'human_readable': schema
        }
        
        response_text = f"📊 Schéma détecté : {len(schema)} champs"
        
        # Pas de résultats tabulaires, seulement l'analyse
        result_type = 'schema'
        results = []
        metadata = {'columns': list(schema.keys())}
        
        # Afficher quand même la "requête" générée pour référence
        sql_query = json.dumps(schema, indent=2)
        redis_query = ""
        hbase_query = ""
        neo4j_query = ""

    elif result_type == 'data_profile':
        rows = metadata.get('num_rows', 0)
        cols = metadata.get('num_columns', 0)
        return f"📊 Profil des données : {rows} lignes, {cols} colonnes"
    
    elif result_type == 'columns':
        count = metadata.get('count', 0)
        return f"📝 Colonnes disponibles : {count} colonnes"
    
    elif result_type == 'select':
        if results:
            total = metadata.get('total_matching', metadata.get('count', len(results)))
            displayed = metadata.get('display_count', len(results))
            
            if metadata.get('has_more'):
                return f"✅ {total} produit(s) trouvé(s) - affichage des {displayed} premiers"
            else:
                return f"✅ {total} produit(s) trouvé(s)"
        else:
            return "❌ Aucun produit trouvé pour votre recherche."
    
    return "✅ Requête exécutée avec succès"

@app.route('/clear', methods=['GET'])
def clear_conversation():
    """
    Route pour effacer l'historique de conversation.
    """
    session['conversation'] = []
    session.modified = True
    from flask import redirect
    return redirect('/')

@app.route('/health', methods=['GET'])
def health_check():
    """
    Route pour vérifier l'état de l'application.
    """
    from llm.mongodb_llm import groq_available as mongo_groq
    from llm.redis_llm import groq_available as redis_groq
    
    status = {
        'status': 'healthy',
        'mongodb_loaded': df is not None and not df.empty,
        'redis_connected': redis_client is not None,
        'groq_available': mongo_groq or redis_groq,
        'mongodb_count': len(df) if df is not None else 0,
        'redis_info': redis_info if redis_info else 'Non disponible'
    }
    return status

@app.route('/compare/<database>', methods=['POST'])
def compare_database(database):
    """
    Route pour exécuter une requête sur une base spécifique.
    """
    data = request.get_json()
    question = data.get('question', '')
    
    if not question:
        return {'error': 'Question requise'}, 400
    
    # Générer la requête MongoDB comme référence
    mongo_query_dict = generate_mongodb_query(question)
    
    if database == 'mongodb':
        result_type, results, metadata = execute_mongodb_query(mongo_query_dict)
        response_text = generate_response_text(result_type, results, metadata, question)
        mongodb_query_str = convert_to_mongodb_syntax(mongo_query_dict)
    elif database == 'redis':
        # Convertir pour Redis si nécessaire
        redis_query_dict = generate_redis_query(question)
        result_type, results, metadata = execute_redis_query(redis_query_dict)
    else:
        return {'error': f'Base de données non supportée: {database}'}, 400
    
    return {
        'database': database,
        'result_type': result_type,
        'results': results[:10],  # Limiter pour la réponse
        'metadata': metadata
    }

if __name__ == '__main__':
    # Démarrer l'application Flask
    print("\n🌐 Démarrage du serveur Flask...")
    print("👉 Accédez à l'application sur: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)