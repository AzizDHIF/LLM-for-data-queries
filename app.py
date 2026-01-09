import os
import json
from flask import Flask, render_template, request, session
from llm import (
    init_data, 
    init_groq_client, 
    generate_mongodb_query, 
    execute_mongodb_query
)

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "secret-key-123")

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
        
        # IMPORTANT: Si on groupe par le même champ qu'on agrège, faire un count à la place
        if agg_field == group_by and agg_op != 'count':
            agg_op = 'count'
            agg_field = 'product_id'
        
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
        query_parts.append(f"db.products.find({json.dumps(filter_query, indent=2)})")
        
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
    group_by = query_dict.get('group_by')
    limit = query_dict.get('limit', 20)
    
    redis_commands = []
    
    # Redis est key-value, donc on simule avec des structures
    if query_type == 'count':
        if group_by:
            redis_commands.append(f"# Grouper et compter par {group_by}")
            redis_commands.append(f"# Pour chaque produit:")
            redis_commands.append(f"SADD products:{{{group_by}}} <product_id>")
            redis_commands.append(f"# Puis compter:")
            redis_commands.append(f"SCARD products:{{{group_by}}}")
        else:
            redis_commands.append("# Compter tous les produits")
            redis_commands.append("SCARD products:all")
    
    elif query_type in ['avg', 'sum']:
        redis_commands.append(f"# Redis n'a pas d'agrégations natives")
        redis_commands.append(f"# Solution: utiliser Lua script ou RedisTimeSeries")
        redis_commands.append(f"EVAL \"local sum=0; local count=0; \" 0")
    
    elif query_type in ['max', 'min']:
        field = aggregation.get('field', 'rating') if aggregation else 'rating'
        if query_type == 'max':
            redis_commands.append(f"# Max {field} avec Sorted Set")
            redis_commands.append(f"ZREVRANGE products:by_{field} 0 0 WITHSCORES")
        else:
            redis_commands.append(f"# Min {field} avec Sorted Set")
            redis_commands.append(f"ZRANGE products:by_{field} 0 0 WITHSCORES")
    
    else:  # select
        redis_commands.append("# Récupérer les produits")
        redis_commands.append(f"SMEMBERS products:all")
        redis_commands.append("# Puis pour chaque ID:")
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
        hbase_commands.append("# Compter avec MapReduce ou Coprocessor")
        hbase_commands.append("count 'products'")
        if filter_query:
            hbase_commands.append("# Avec filtre (Scan + Filter):")
            hbase_commands.append("scan 'products', {FILTER => \"ValueFilter(=,'binary:match')\"}")
    
    elif query_type in ['avg', 'sum', 'max', 'min']:
        hbase_commands.append("# HBase nécessite MapReduce pour agrégations")
        hbase_commands.append("# Ou utiliser Coprocessor Endpoint")
        hbase_commands.append("scan 'products', {COLUMNS => ['data:price', 'data:rating']}")
    
    elif query_type == 'group':
        hbase_commands.append("# Groupement nécessite MapReduce")
        hbase_commands.append(f"# Job MapReduce avec clé = {group_by}")
        hbase_commands.append("scan 'products'")
    
    else:  # select
        hbase_commands.append("# Scanner la table")
        if limit:
            hbase_commands.append(f"scan 'products', {{LIMIT => {limit}}}")
        else:
            hbase_commands.append("scan 'products'")
        
        if filter_query:
            hbase_commands.append("# Avec filtres:")
            hbase_commands.append("scan 'products', {FILTER => \"SingleColumnValueFilter('data','rating',>=,'binary:4')\"}")
    
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

# Initialiser les données et le client Groq au démarrage
print("=" * 50)
print("🚀 Initialisation de l'application...")
print("=" * 50)

# Initialiser les données
init_data()

# Initialiser le client Groq
init_groq_client()

print("=" * 50)
print("✅ Application prête !")
print("=" * 50)

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
        query_parts.append(f"db.products.find({json.dumps(filter_query, indent=2)})")
        
        if sort_spec:
            sort_field = sort_spec.get('field', 'rating')
            sort_order = sort_spec.get('order', -1)
            query_parts.append(f".sort({{\"{sort_field}\": {sort_order}}})")
        
        if limit:
            query_parts.append(f".limit({limit})")
        
        mongodb_query = "".join(query_parts)
    
    return mongodb_query

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
    mongo_query = {}
    response_text = ""
    results = []
    result_type = None
    metadata = {}
    
    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        
        if question:
            print(f"\n📝 Nouvelle question: {question}")
            
            # Ajouter la question à l'historique de conversation
            session['conversation'].append({
                'role': 'user', 
                'text': question
            })
            
            # Générer la requête MongoDB structurée
            mongo_query_dict = generate_mongodb_query(question)
            print(f"🔍 Requête générée: {json.dumps(mongo_query_dict, indent=2, ensure_ascii=False)}")
            
            # Convertir en vraie syntaxe MongoDB pour l'affichage
            mongo_query = convert_to_mongodb_syntax(mongo_query_dict)
            
            # Exécuter la requête
            result_type, results, metadata = execute_mongodb_query(mongo_query_dict)
            print(f"📊 Type de résultat: {result_type}, Données: {len(results)} éléments")
            
            # Générer la réponse textuelle selon le type de résultat
            if result_type == 'error':
                response_text = f"❌ Erreur lors de l'exécution: {metadata.get('message', 'Erreur inconnue')}"
            
            elif result_type == 'count':
                if 'groups' in metadata:
                    response_text = f"📊 Résultats groupés : {metadata['groups']} catégories trouvées (total: {metadata['total']} produits)"
                else:
                    response_text = f"✅ Nombre total de produits trouvés : {metadata['total']}"
            
            elif result_type == 'avg':
                if 'overall_avg' in metadata:
                    response_text = f"📈 Moyenne calculée : {metadata['overall_avg']} (basée sur {metadata.get('count', 0)} produits)"
                else:
                    response_text = f"📈 Moyenne de {results[0]['field']} : {results[0]['average']}"
            
            elif result_type == 'sum':
                if 'total_sum' in metadata:
                    response_text = f"➕ Somme totale : {metadata['total_sum']} (basée sur {metadata.get('count', 0)} produits)"
                else:
                    response_text = f"➕ Somme de {results[0]['field']} : {results[0]['sum']}"
            
            elif result_type == 'max':
                response_text = f"🔝 Valeur maximale trouvée"
            
            elif result_type == 'min':
                response_text = f"⬇️ Valeur minimale trouvée"
            
            elif result_type == 'group':
                response_text = f"📊 Résultats groupés par {metadata.get('group_by', 'catégorie')} : {metadata['groups']} groupes"
                
            elif result_type == 'general_info':
                response_text = "ℹ️ Informations générales\n"
                # for k, v in metadata.items():
                #     response_text += f"{k} : {v}\n"
            
            elif result_type == 'select':
                if results:
                    count_msg = f"{metadata['count']} produit(s) trouvé(s)"
                    if metadata.get('limited'):
                        count_msg += " (résultats limités)"
                    response_text = f"✅ {count_msg}"
                else:
                    response_text = "❌ Aucun produit trouvé pour votre recherche."
            
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
    
    # Rendre le template avec les données
    return render_template(
        'index.html',
        question=question,
        conversation=session.get('conversation', []),
        results=results,
        result_type=result_type,
        metadata=metadata,
        sql_query=mongo_query,
        response=response_text
    )

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
    from llm import df, groq_available
    
    status = {
        'status': 'healthy',
        'data_loaded': not df.empty if df is not None else False,
        'groq_available': groq_available,
        'data_count': len(df) if df is not None else 0
    }
    return status

if __name__ == '__main__':
    # Démarrer l'application Flask
    print("\n🌐 Démarrage du serveur Flask...")
    print("👉 Accédez à l'application sur: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)