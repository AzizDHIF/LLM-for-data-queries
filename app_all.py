# multi_db_manager.py
import os
import json
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import traceback
from executers.mongodb_executer import MongoExecutor
executor = MongoExecutor(
        host="localhost",
        port=27017,
        username="admin",
        password="secret",
        database="sample_mflix",
        collection="movies"
        )
# Importations pour toutes les bases
try:
    from llm.mongodb_llm import (
        generate_mongodb_query,
    )
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    print("⚠️ MongoDB module not available")

try:
    from llm.redis_llm_old import (
        generate_redis_query,
        execute_redis_query,
        init_redis
    )
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("⚠️ Redis module not available")

try:
    from llm.hbase_llm import (
        generate_hbase_query,
        HBaseExecutor
    )
    HBASE_AVAILABLE = True
except ImportError:
    HBASE_AVAILABLE = False
    print("⚠️ HBase module not available")

try:
    from llm.neo4j_llm import (
        generate_neo4j_query,
        Neo4jExecutor
    )
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    print("⚠️ Neo4j module not available")

# ============================================================================
# FONCTIONS DE CONVERSION
# ============================================================================

def convert_to_mongodb_syntax(query_dict):
    """Convertit la structure interne en syntaxe MongoDB réelle"""
    query_type = query_dict.get('type', 'select')
    filter_query = query_dict.get('filter', {})
    aggregation = query_dict.get('aggregation')
    group_by = query_dict.get('group_by')
    sort_spec = query_dict.get('sort')
    limit = query_dict.get('limit')
    collection = query_dict.get('collection', 'movies')
    
    mongodb_query = ""
    
    # Construction des filtres MongoDB
    mongo_filter = {}
    for key, value in filter_query.items():
        if isinstance(value, dict):
            if "$regex" in value:
                pattern = value["$regex"]
                mongo_filter[key] = {"$regex": pattern, "$options": "i"}
            elif "$gt" in value:
                mongo_filter[key] = {"$gt": value["$gt"]}
            elif "$gte" in value:
                mongo_filter[key] = {"$gte": value["$gte"]}
            elif "$lt" in value:
                mongo_filter[key] = {"$lt": value["$lt"]}
            elif "$lte" in value:
                mongo_filter[key] = {"$lte": value["$lte"]}
            elif "$in" in value:
                mongo_filter[key] = {"$in": value["$in"]}
            elif "$ne" in value:
                mongo_filter[key] = {"$ne": value["$ne"]}
        else:
            mongo_filter[key] = value
    
    if query_type == 'count':
        if group_by:
            pipeline = [
                {"$match": mongo_filter} if mongo_filter else {"$match": {}},
                {"$group": {"_id": f"${group_by}", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]
            mongodb_query = f"db.{collection}.aggregate({json.dumps(pipeline, indent=2)})"
        else:
            if mongo_filter:
                mongodb_query = f"db.{collection}.countDocuments({json.dumps(mongo_filter, indent=2)})"
            else:
                mongodb_query = f"db.{collection}.countDocuments()"
    
    elif query_type == 'avg' and aggregation:
        field = aggregation.get('field', 'rating')
        pipeline = [
            {"$match": mongo_filter} if mongo_filter else {"$match": {}},
            {"$match": {field: {"$ne": None}}} if field else {"$match": {}},
            {"$group": {"_id": None if not group_by else f"${group_by}", 
                       "average": {"$avg": f"${field}"}}}
        ]
        if group_by:
            pipeline.append({"$sort": {"average": -1}})
        mongodb_query = f"db.{collection}.aggregate({json.dumps(pipeline, indent=2)})"
    
    elif query_type == 'sum' and aggregation:
        field = aggregation.get('field', 'rating')
        pipeline = [
            {"$match": mongo_filter} if mongo_filter else {"$match": {}},
            {"$group": {"_id": None if not group_by else f"${group_by}", 
                       "sum": {"$sum": f"${field}"}}}
        ]
        if group_by:
            pipeline.append({"$sort": {"sum": -1}})
        mongodb_query = f"db.{collection}.aggregate({json.dumps(pipeline, indent=2)})"
    
    elif query_type in ['max', 'min']:
        field = aggregation.get('field', 'rating') if aggregation else 'rating'
        order = -1 if query_type == 'max' else 1
        if group_by:
            pipeline = [
                {"$match": mongo_filter} if mongo_filter else {"$match": {}},
                {"$sort": {field: order}},
                {"$group": {"_id": f"${group_by}", 
                           f"{query_type}_value": {"$first": f"${field}"},
                           f"{query_type}_doc": {"$first": "$$ROOT"}}},
                {"$project": {"_id": 1, f"{query_type}_value": 1, "doc": f"${query_type}_doc"}}
            ]
            mongodb_query = f"db.{collection}.aggregate({json.dumps(pipeline, indent=2)})"
        else:
            sort_query = f"db.{collection}.find({json.dumps(mongo_filter, indent=2)}).sort({{'{field}': {order}}}).limit(1)"
            mongodb_query = sort_query
    
    elif query_type == 'group':
        if not group_by:
            group_by = 'category'
        agg_field = aggregation.get('field', 'rating') if aggregation else 'rating'
        agg_op = aggregation.get('operation', 'count') if aggregation else 'count'
        
        if agg_op == 'count':
            pipeline = [
                {"$match": mongo_filter} if mongo_filter else {"$match": {}},
                {"$group": {"_id": f"${group_by}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
        elif agg_op == 'avg':
            pipeline = [
                {"$match": mongo_filter} if mongo_filter else {"$match": {}},
                {"$group": {"_id": f"${group_by}", "average": {"$avg": f"${agg_field}"}}},
                {"$sort": {"average": -1}}
            ]
        elif agg_op == 'sum':
            pipeline = [
                {"$match": mongo_filter} if mongo_filter else {"$match": {}},
                {"$group": {"_id": f"${group_by}", "sum": {"$sum": f"${agg_field}"}}},
                {"$sort": {"sum": -1}}
            ]
        elif agg_op == 'max':
            pipeline = [
                {"$match": mongo_filter} if mongo_filter else {"$match": {}},
                {"$group": {"_id": f"${group_by}", "max": {"$max": f"${agg_field}"}}},
                {"$sort": {"max": -1}}
            ]
        elif agg_op == 'min':
            pipeline = [
                {"$match": mongo_filter} if mongo_filter else {"$match": {}},
                {"$group": {"_id": f"${group_by}", "min": {"$min": f"${agg_field}"}}},
                {"$sort": {"min": 1}}
            ]
        
        mongodb_query = f"db.{collection}.aggregate({json.dumps(pipeline, indent=2)})"
    
    else:  # select
        query_parts = [f"db.{collection}.find({json.dumps(mongo_filter, indent=2)})"]
        
        if sort_spec:
            sort_field = sort_spec.get('field', 'rating')
            sort_order = sort_spec.get('order', -1)
            query_parts.append(f".sort({{'{sort_field}': {sort_order}}})")
        
        if limit:
            query_parts.append(f".limit({limit})")
        
        if query_dict.get('projection'):
            projection = query_dict.get('projection')
            query_parts.insert(1, f".project({json.dumps(projection, indent=2)})")
        
        mongodb_query = "".join(query_parts)
    
    return mongodb_query


def convert_to_redis_syntax(query_dict):
    """Convertit en commandes Redis"""
    query_type = query_dict.get('type', 'select')
    filter_query = query_dict.get('filter', {})
    aggregation = query_dict.get('aggregation')
    group_by = query_dict.get('group_by')
    sort_spec = query_dict.get('sort')
    limit = query_dict.get('limit', 20)
    
    redis_commands = []
    
    # Extraction des filtres
    category_filter = None
    rating_filter = None
    price_filter = None
    
    for key, value in filter_query.items():
        if key == 'category' and isinstance(value, dict) and '$regex' in value:
            category_filter = value['$regex']
        elif key == 'rating' and isinstance(value, dict):
            if '$gt' in value:
                rating_filter = (value['$gt'], '>')
            elif '$lt' in value:
                rating_filter = (value['$lt'], '<')
            elif '$gte' in value:
                rating_filter = (value['$gte'], '>=')
            elif '$lte' in value:
                rating_filter = (value['$lte'], '<=')
        elif key == 'discounted_price' and isinstance(value, dict):
            if '$gt' in value:
                price_filter = (value['$gt'], '>')
            elif '$lt' in value:
                price_filter = (value['$lt'], '<')
    
    # Construction des commandes Redis
    if query_type == 'count':
        if not category_filter and not rating_filter:
            redis_commands.append("SCARD products:all")
            redis_commands.append("# Count all products")
        elif category_filter:
            redis_commands.append(f"SCARD category:{category_filter}")
            redis_commands.append(f"# Count products in category: {category_filter}")
        else:
            redis_commands.append("SCARD products:all")
            redis_commands.append("# Need to filter manually for rating/price")
    
    elif query_type in ['avg', 'sum']:
        field = aggregation.get('field', 'rating') if aggregation else 'rating'
        
        if category_filter:
            redis_commands.append(f"SMEMBERS category:{category_filter}")
            redis_commands.append(f"# Get all product IDs in category: {category_filter}")
        else:
            redis_commands.append("SMEMBERS products:all")
            redis_commands.append("# Get all product IDs")
        
        redis_commands.append(f"# For each ID: HGET product:<id> {field}")
        redis_commands.append("# Then calculate average/sum manually")
        
        if rating_filter:
            val, op = rating_filter
            redis_commands.append(f"# Filter where rating {op} {val}")
    
    elif query_type in ['max', 'min']:
        field = aggregation.get('field', 'rating') if aggregation else 'rating'
        
        if field == 'rating':
            if query_type == 'max':
                if category_filter:
                    redis_commands.append(f"ZREVRANGE category:{category_filter}:by_rating 0 0 WITHSCORES")
                    redis_commands.append(f"# Max rating in category {category_filter}")
                else:
                    redis_commands.append("ZREVRANGE products:by_rating 0 0 WITHSCORES")
                    redis_commands.append("# Max rating overall")
            else:  # min
                if category_filter:
                    redis_commands.append(f"ZRANGE category:{category_filter}:by_rating 0 0 WITHSCORES")
                    redis_commands.append(f"# Min rating in category {category_filter}")
                else:
                    redis_commands.append("ZRANGE products:by_rating 0 0 WITHSCORES")
                    redis_commands.append("# Min rating overall")
        
        elif field == 'discounted_price':
            if query_type == 'max':
                if category_filter:
                    redis_commands.append(f"ZREVRANGE category:{category_filter}:by_price 0 0 WITHSCORES")
                    redis_commands.append(f"# Max price in category {category_filter}")
                else:
                    redis_commands.append("ZREVRANGE products:by_price 0 0 WITHSCORES")
                    redis_commands.append("# Max price overall")
            else:  # min
                if category_filter:
                    redis_commands.append(f"ZRANGE category:{category_filter}:by_price 0 0 WITHSCORES")
                    redis_commands.append(f"# Min price in category {category_filter}")
                else:
                    redis_commands.append("ZRANGE products:by_price 0 0 WITHSCORES")
                    redis_commands.append("# Min price overall")
        
        redis_commands.append("HGETALL product:<id>")
        redis_commands.append("# Get full product details")
    
    elif query_type == 'group':
        field = group_by or 'category'
        agg_op = aggregation.get('operation', 'count') if aggregation else 'count'
        agg_field = aggregation.get('field', 'rating') if aggregation else 'rating'
        
        if field == 'category':
            redis_commands.append("SMEMBERS categories:all")
            redis_commands.append("# Get all categories")
            redis_commands.append(f"# For each category:")
            redis_commands.append(f"#   SCARD category:<category_name>  # Count")
            
            if agg_op == 'avg':
                redis_commands.append(f"#   For each product in category: HGET product:<id> {agg_field}")
                redis_commands.append(f"#   Calculate average {agg_field}")
            elif agg_op == 'sum':
                redis_commands.append(f"#   For each product in category: HGET product:<id> {agg_field}")
                redis_commands.append(f"#   Calculate sum {agg_field}")
    
    else:  # select
        if category_filter:
            redis_commands.append(f"SMEMBERS category:{category_filter}")
            redis_commands.append(f"# Get products in category: {category_filter}")
        else:
            redis_commands.append("SMEMBERS products:all")
            redis_commands.append("# Get all products")
        
        if limit:
            redis_commands.append(f"# Limit to {limit} products")
        
        redis_commands.append("# For each ID:")
        redis_commands.append("#   HGETALL product:<id>")
        
        if rating_filter:
            val, op = rating_filter
            redis_commands.append(f"#   Filter where rating {op} {val}")
        
        if price_filter:
            val, op = price_filter
            redis_commands.append(f"#   Filter where price {op} {val}")
        
        if sort_spec:
            sort_field = sort_spec.get('field', 'rating')
            sort_order = sort_spec.get('order', -1)
            if sort_field == 'rating':
                if sort_order == -1:  # DESC
                    redis_commands.append("#   Sort by ZREVRANGE products:by_rating")
                else:  # ASC
                    redis_commands.append("#   Sort by ZRANGE products:by_rating")
            elif sort_field == 'discounted_price':
                if sort_order == -1:  # DESC
                    redis_commands.append("#   Sort by ZREVRANGE products:by_price")
                else:  # ASC
                    redis_commands.append("#   Sort by ZRANGE products:by_price")
    
    return "\n".join(redis_commands)


# Remplacer la fonction convert_to_hbase_syntax par cette version corrigée :

def convert_to_hbase_syntax(query_dict):
    """Convertit en commandes HBase"""
    query_type = query_dict.get('type', 'select')
    filter_query = query_dict.get('filter', {})
    aggregation = query_dict.get('aggregation')
    group_by = query_dict.get('group_by')
    sort_spec = query_dict.get('sort')
    limit = query_dict.get('limit', 20)
    collection = query_dict.get('collection', 'movies')
    
    # Construction des filtres HBase
    hbase_filters = []
    
    for key, value in filter_query.items():
        if isinstance(value, dict):
            if "$regex" in value:
                pattern = value["$regex"]
                # CORRECTION : Utiliser une variable intermédiaire
                filter_str = f"SingleColumnValueFilter('info', '{key}', =, 'regexstring:.*{pattern}.*')"
                hbase_filters.append(filter_str)
            elif "$gt" in value:
                gt_value = value["$gt"]
                filter_str = f"SingleColumnValueFilter('info', '{key}', >=, 'binary:{gt_value}')"
                hbase_filters.append(filter_str)
            elif "$gte" in value:
                gte_value = value["$gte"]
                filter_str = f"SingleColumnValueFilter('info', '{key}', >=, 'binary:{gte_value}')"
                hbase_filters.append(filter_str)
            elif "$lt" in value:
                lt_value = value["$lt"]
                filter_str = f"SingleColumnValueFilter('info', '{key}', <=, 'binary:{lt_value}')"
                hbase_filters.append(filter_str)
            elif "$lte" in value:
                lte_value = value["$lte"]
                filter_str = f"SingleColumnValueFilter('info', '{key}', <=, 'binary:{lte_value}')"
                hbase_filters.append(filter_str)
        else:
            filter_str = f"SingleColumnValueFilter('info', '{key}', =, 'binary:{value}')"
            hbase_filters.append(filter_str)
    
    # Construction de la requête HBase
    if query_type == 'count':
        if hbase_filters:
            filter_str = ", ".join(hbase_filters)
            return f"scan '{collection}', {{FILTER => \"{filter_str}\"}}"
        else:
            return f"count '{collection}'"
    
    elif query_type in ['avg', 'sum', 'max', 'min']:
        field = aggregation.get('field', 'rating') if aggregation else 'rating'
        
        # Déterminer la column family
        if field in ['rating', 'score']:
            cf = 'ratings'
        elif field in ['budget', 'gross']:
            cf = 'financial'
        else:
            cf = 'info'
        
        columns = [f"{cf}:{field}"]
        
        if group_by:
            columns.append(f"info:{group_by}")
        
        columns_str = "', '".join(columns)
        
        if hbase_filters:
            filter_str = ", ".join(hbase_filters)
            return f"scan '{collection}', {{COLUMNS => ['{columns_str}'], FILTER => \"{filter_str}\"}}"
        else:
            return f"scan '{collection}', {{COLUMNS => ['{columns_str}']}}"
    
    elif query_type == 'group':
        field = group_by or 'category'
        agg_op = aggregation.get('operation', 'count') if aggregation else 'count'
        agg_field = aggregation.get('field', 'rating') if aggregation else 'rating'
        
        # Déterminer la column family pour le champ d'agrégation
        if agg_field in ['rating', 'score']:
            agg_cf = 'ratings'
        elif agg_field in ['budget', 'gross']:
            agg_cf = 'financial'
        else:
            agg_cf = 'info'
        
        columns = [f"info:{field}", f"{agg_cf}:{agg_field}"]
        columns_str = "', '".join(columns)
        
        if hbase_filters:
            filter_str = ", ".join(hbase_filters)
            return f"scan '{collection}', {{COLUMNS => ['{columns_str}'], FILTER => \"{filter_str}\"}}"
        else:
            return f"scan '{collection}', {{COLUMNS => ['{columns_str}']}}"
    
    else:  # select
        # Déterminer les colonnes à récupérer
        columns = []
        
        # Colonnes par défaut pour les films
        default_columns = ['name', 'genre', 'year', 'rating', 'director']
        
        # Ajouter les colonnes basées sur le schéma HBase
        for col in default_columns:
            if col in ['rating', 'score']:
                columns.append(f"ratings:{col}")
            elif col in ['budget', 'gross']:
                columns.append(f"financial:{col}")
            else:
                columns.append(f"info:{col}")
        
        columns_str = "', '".join(columns)
        
        scan_parts = [f"scan '{collection}', {{COLUMNS => ['{columns_str}']"]
        
        if hbase_filters:
            filter_str = ", ".join(hbase_filters)
            scan_parts.append(f", FILTER => \"{filter_str}\"")
        
        if limit:
            scan_parts.append(f", LIMIT => {limit}")
        
        scan_parts.append("}")
        
        return "".join(scan_parts)


def convert_to_neo4j_syntax(query_dict):
    """Convertit en Cypher (Neo4j)"""
    query_type = query_dict.get('type', 'select')
    filter_query = query_dict.get('filter', {})
    aggregation = query_dict.get('aggregation')
    group_by = query_dict.get('group_by')
    sort_spec = query_dict.get('sort')
    limit = query_dict.get('limit', 20)
    
    cypher_parts = ["MATCH (m:Movie)"]
    
    # Construction de la clause WHERE
    where_conditions = []
    
    for key, value in filter_query.items():
        # Nettoyer les noms de propriétés
        prop_name = key.replace('.', '_')
        
        if isinstance(value, dict):
            if "$regex" in value:
                pattern = value["$regex"]
                where_conditions.append(f"m.{prop_name} =~ '(?i).*{pattern}.*'")
            elif "$gt" in value:
                where_conditions.append(f"m.{prop_name} > {value['$gt']}")
            elif "$gte" in value:
                where_conditions.append(f"m.{prop_name} >= {value['$gte']}")
            elif "$lt" in value:
                where_conditions.append(f"m.{prop_name} < {value['$lt']}")
            elif "$lte" in value:
                where_conditions.append(f"m.{prop_name} <= {value['$lte']}")
            elif "$in" in value:
                values = ', '.join([f"'{v}'" for v in value["$in"]])
                where_conditions.append(f"m.{prop_name} IN [{values}]")
            elif "$ne" in value:
                where_conditions.append(f"m.{prop_name} <> {repr(value['$ne'])}")
        else:
            if isinstance(value, str):
                where_conditions.append(f"m.{prop_name} = '{value}'")
            else:
                where_conditions.append(f"m.{prop_name} = {value}")
    
    if where_conditions:
        cypher_parts.append("WHERE " + " AND ".join(where_conditions))
    
    # Construction de la clause RETURN
    if query_type == 'count':
        if group_by:
            prop_name = group_by.replace('.', '_')
            cypher_parts.append(f"RETURN m.{prop_name} as group, COUNT(m) as count")
            cypher_parts.append("ORDER BY count DESC")
        else:
            cypher_parts.append("RETURN COUNT(m) as count")
    
    elif query_type == 'avg' and aggregation:
        field = aggregation.get('field', 'rating')
        prop_name = field.replace('.', '_')
        
        if group_by:
            group_prop = group_by.replace('.', '_')
            cypher_parts.append(f"RETURN m.{group_prop} as group, AVG(m.{prop_name}) as average")
            cypher_parts.append("ORDER BY average DESC")
        else:
            cypher_parts.append(f"RETURN AVG(m.{prop_name}) as average")
    
    elif query_type == 'sum' and aggregation:
        field = aggregation.get('field', 'rating')
        prop_name = field.replace('.', '_')
        
        if group_by:
            group_prop = group_by.replace('.', '_')
            cypher_parts.append(f"RETURN m.{group_prop} as group, SUM(m.{prop_name}) as total")
            cypher_parts.append("ORDER BY total DESC")
        else:
            cypher_parts.append(f"RETURN SUM(m.{prop_name}) as total")
    
    elif query_type in ['max', 'min']:
        field = aggregation.get('field', 'rating') if aggregation else 'rating'
        prop_name = field.replace('.', '_')
        
        if query_type == 'max':
            order_dir = "DESC"
            func = "MAX"
        else:
            order_dir = "ASC"
            func = "MIN"
        
        if group_by:
            group_prop = group_by.replace('.', '_')
            cypher_parts.append(f"RETURN m.{group_prop} as group, {func}(m.{prop_name}) as {query_type}_value")
            cypher_parts.append(f"ORDER BY {query_type}_value {order_dir}")
        else:
            cypher_parts.append(f"RETURN m.name, m.{prop_name}")
            cypher_parts.append(f"ORDER BY m.{prop_name} {order_dir}")
            cypher_parts.append("LIMIT 1")
    
    elif query_type == 'group':
        field = group_by or 'genre'
        agg_field = aggregation.get('field', 'rating') if aggregation else 'rating'
        agg_op = aggregation.get('operation', 'count') if aggregation else 'count'
        
        field_prop = field.replace('.', '_')
        agg_prop = agg_field.replace('.', '_')
        
        if agg_op == 'count':
            cypher_parts.append(f"RETURN m.{field_prop} as group, COUNT(m) as count")
            cypher_parts.append("ORDER BY count DESC")
        elif agg_op == 'avg':
            cypher_parts.append(f"RETURN m.{field_prop} as group, AVG(m.{agg_prop}) as average")
            cypher_parts.append("ORDER BY average DESC")
        elif agg_op == 'sum':
            cypher_parts.append(f"RETURN m.{field_prop} as group, SUM(m.{agg_prop}) as total")
            cypher_parts.append("ORDER BY total DESC")
        elif agg_op == 'max':
            cypher_parts.append(f"RETURN m.{field_prop} as group, MAX(m.{agg_prop}) as max_value")
            cypher_parts.append("ORDER BY max_value DESC")
        elif agg_op == 'min':
            cypher_parts.append(f"RETURN m.{field_prop} as group, MIN(m.{agg_prop}) as min_value")
            cypher_parts.append("ORDER BY min_value ASC")
    
    else:  # select
        # Sélection des propriétés par défaut pour les films
        return_fields = ["m.name", "m.genre", "m.year", "m.rating", "m.director"]
        cypher_parts.append(f"RETURN {', '.join(return_fields)}")
        
        if sort_spec:
            sort_field = sort_spec.get('field', 'rating')
            sort_order = sort_spec.get('order', -1)
            order_dir = "DESC" if sort_order == -1 else "ASC"
            sort_prop = sort_field.replace('.', '_')
            cypher_parts.append(f"ORDER BY m.{sort_prop} {order_dir}")
        
        if limit:
            cypher_parts.append(f"LIMIT {limit}")
    
    return "\n".join(cypher_parts)


# ============================================================================
# CLASSE PRINCIPALE MULTI-DB MANAGER
# ============================================================================

class MultiDBManager:
    """Gestionnaire centralisé pour toutes les bases de données"""
    
    def __init__(self):
        self.db_clients = {}
        self.initialized = False
        self.df = None
        self.redis_client = None
        self.hbase_executor = None
        self.neo4j_executor = None
    
    def init_all_databases(self):
        """Initialise toutes les connexions aux bases"""
        try:
            print("=" * 50)
            print("🚀 Initialisation MULTI-BASE (ALL mode)...")
            print("=" * 50)
            
            # MongoDB
            if MONGODB_AVAILABLE:
                print("\n📊 Initialisation MongoDB...")
                try:
                    from connectors.mongodb_connector import DataLoader
                    loader = DataLoader()
                    self.df = loader.init_data()
                    print(f"✅ MongoDB: {len(self.df) if self.df is not None else 0} produits chargés")
                except Exception as e:
                    print(f"❌ MongoDB Erreur: {e}")
            else:
                print("📊 MongoDB: Module non disponible")
            
            # Redis
            if REDIS_AVAILABLE:
                print("\n🔴 Initialisation Redis...")
                try:
                    self.redis_client = init_redis()
                    if self.redis_client:
                        self.db_clients['redis'] = self.redis_client
                        print("✅ Redis: Connecté")
                    else:
                        print("⚠️ Redis: Non disponible")
                except Exception as e:
                    print(f"❌ Redis Erreur: {e}")
            else:
                print("🔴 Redis: Module non disponible")
            
            # HBase
            if HBASE_AVAILABLE:
                print("\n🔶 Initialisation HBase...")
                try:
                    self.hbase_executor = HBaseExecutor(host='localhost', port=9090)
                    self.db_clients['hbase'] = self.hbase_executor
                    print("✅ HBase: Connecté")
                except Exception as e:
                    print(f"❌ HBase Erreur: {e}")
                    self.db_clients['hbase'] = None
            else:
                print("🔶 HBase: Module non disponible")
            
            # Neo4j
            if NEO4J_AVAILABLE:
                print("\n🌀 Initialisation Neo4j...")
                try:
                    self.neo4j_executor = Neo4jExecutor(
                        uri="bolt://localhost:7687",
                        user="neo4j",
                        password="password"  # À changer selon votre configuration
                    )
                    self.db_clients['neo4j'] = self.neo4j_executor
                    print("✅ Neo4j: Connecté")
                except Exception as e:
                    print(f"❌ Neo4j Erreur: {e}")
                    self.db_clients['neo4j'] = None
            else:
                print("🌀 Neo4j: Module non disponible")
            
            self.initialized = True
            print("=" * 50)
            print("✅ Toutes les bases initialisées !")
            print("=" * 50)
            
        except Exception as e:
            print(f"❌ Erreur lors de l'initialisation: {e}")
            print(traceback.format_exc())
            self.initialized = False
    
    def generate_all_queries(self, question: str) -> Dict[str, str]:
        """Génère les requêtes pour toutes les bases"""
        queries = {}
        
        # MongoDB
        if MONGODB_AVAILABLE:
            try:
                mongo_dict = generate_mongodb_query(question)
                queries['mongodb'] = convert_to_mongodb_syntax(mongo_dict)
            except Exception as e:
                queries['mongodb'] = f"Erreur MongoDB: {e}"
        else:
            queries['mongodb'] = "MongoDB non disponible"
        
        # Redis
        if REDIS_AVAILABLE:
            try:
                redis_query = generate_redis_query(question)
                queries['redis'] = redis_query
            except Exception as e:
                queries['redis'] = f"Erreur Redis: {e}"
        else:
            queries['redis'] = "Redis non disponible"
        
        # HBase
        if HBASE_AVAILABLE:
            try:
                hbase_query = generate_hbase_query(question)
                queries['hbase'] = hbase_query
            except Exception as e:
                queries['hbase'] = f"Erreur HBase: {e}"
        else:
            queries['hbase'] = "HBase non disponible"
        
        # Neo4j
        if NEO4J_AVAILABLE:
            try:
                neo4j_query = generate_neo4j_query(question)
                queries['neo4j'] = neo4j_query
            except Exception as e:
                queries['neo4j'] = f"Erreur Neo4j: {e}"
        else:
            queries['neo4j'] = "Neo4j non disponible"
        
        return queries
    

    
    def execute_all_queries(self, queries: Dict[str, str]) -> Dict[str, Any]:
        """Exécute les requêtes sur toutes les bases"""
        results = {}
        
        # MongoDB
        if 'mongodb' in queries and queries['mongodb'] and MONGODB_AVAILABLE:
            try:
                if not queries['mongodb'].startswith("Erreur"):
                    # Générer le dictionnaire MongoDB
                    mongo_dict = generate_mongodb_query(queries['mongodb'])
                    results = executor.run_query(mongo_dict)
                    results['mongodb'] = {
                        'status': 'success',
                    }
                else:
                    results['mongodb'] = {'status': 'error', 'message': queries['mongodb']}
            except Exception as e:
                results['mongodb'] = {'status': 'error', 'message': str(e)}
        
        # Redis
        if 'redis' in queries and queries['redis'] and self.db_clients.get('redis') and REDIS_AVAILABLE:
            try:
                if not queries['redis'].startswith("Erreur"):
                    redis_results = execute_redis_query(queries['redis'], self.redis_client)
                    results['redis'] = {
                        'status': 'success',
                        'results': redis_results
                    }
                else:
                    results['redis'] = {'status': 'error', 'message': queries['redis']}
            except Exception as e:
                results['redis'] = {'status': 'error', 'message': str(e)}
        
        # HBase
        if 'hbase' in queries and queries['hbase'] and self.db_clients.get('hbase') and HBASE_AVAILABLE:
            try:
                if not queries['hbase'].startswith("Erreur"):
                    hbase_results = self.hbase_executor.run_query(queries['hbase'])
                    results['hbase'] = {
                        'status': 'success',
                        'results': hbase_results
                    }
                else:
                    results['hbase'] = {'status': 'error', 'message': queries['hbase']}
            except Exception as e:
                results['hbase'] = {'status': 'error', 'message': str(e)}
        
        # Neo4j
        if 'neo4j' in queries and queries['neo4j'] and self.db_clients.get('neo4j') and NEO4J_AVAILABLE:
            try:
                if not queries['neo4j'].startswith("Erreur"):
                    neo4j_results = self.neo4j_executor.run_query(queries['neo4j'])
                    results['neo4j'] = {
                        'status': 'success',
                        'results': neo4j_results
                    }
                else:
                    results['neo4j'] = {'status': 'error', 'message': queries['neo4j']}
            except Exception as e:
                results['neo4j'] = {'status': 'error', 'message': str(e)}
        
        return results
    
    def format_all_results(self, results: Dict[str, Any], queries: Dict[str, str]) -> Dict[str, Any]:
        """Formate les résultats pour l'affichage"""
        formatted = {
            'databases': [],
            'success_count': 0,
            'error_count': 0,
            'total_time': datetime.now().strftime("%H:%M:%S")
        }
        
        db_order = ['mongodb', 'redis', 'hbase', 'neo4j']
        
        for db in db_order:
            if db in results:
                result = results[db]
                query = queries.get(db, '')
                
                db_info = {
                    'name': db,
                    'display_name': self._get_db_display_name(db),
                    'query': query,
                    'status': result.get('status', 'unknown'),
                    'icon': self._get_db_icon(db)
                }
                
                if db_info['status'] == 'success':
                    db_info['badge_class'] = 'bg-success'
                    formatted['success_count'] += 1
                    
                    # Extraire les données pertinentes
                    result_data = result.get('results', {})
                    if db == 'mongodb':
                        data_list = result_data if isinstance(result_data, list) else []
                        metadata = result.get('metadata', {})
                        db_info['data'] = {
                            'count': len(data_list),
                            'sample': data_list[:5],  # 5 premiers résultats
                            'metadata': metadata
                        }
                    else:
                        db_info['data'] = result_data
                
                else:
                    db_info['badge_class'] = 'bg-danger'
                    db_info['error'] = result.get('message', 'Erreur inconnue')
                    formatted['error_count'] += 1
                
                formatted['databases'].append(db_info)
            else:
                # Base de données non disponible
                formatted['databases'].append({
                    'name': db,
                    'display_name': self._get_db_display_name(db),
                    'query': 'Non disponible',
                    'status': 'unavailable',
                    'icon': self._get_db_icon(db),
                    'badge_class': 'bg-secondary',
                    'error': 'Module non chargé'
                })
                formatted['error_count'] += 1
        
        return formatted
    
    def _get_db_display_name(self, db_name: str) -> str:
        """Retourne le nom d'affichage de la base"""
        names = {
            'mongodb': 'MongoDB',
            'redis': 'Redis',
            'hbase': 'HBase',
            'neo4j': 'Neo4j'
        }
        return names.get(db_name, db_name)
    
    def _get_db_icon(self, db_name: str) -> str:
        """Retourne l'icône de la base"""
        icons = {
            'mongodb': '📊',
            'redis': '🔴',
            'hbase': '🔶',
            'neo4j': '🌀'
        }
        return icons.get(db_name, '📝')
    
    def get_database_status(self) -> Dict[str, Any]:
        """Retourne le statut de toutes les bases"""
        status = {
            'mongodb': MONGODB_AVAILABLE and self.df is not None,
            'redis': REDIS_AVAILABLE and self.db_clients.get('redis') is not None,
            'hbase': HBASE_AVAILABLE and self.db_clients.get('hbase') is not None,
            'neo4j': NEO4J_AVAILABLE and self.db_clients.get('neo4j') is not None,
            'initialized': self.initialized
        }
        return status
    
    def clear_all(self):
        """Réinitialise toutes les connexions"""
        self.db_clients = {}
        self.initialized = False
        self.df = None
        self.redis_client = None
        self.hbase_executor = None
        self.neo4j_executor = None
        print("🧹 Toutes les connexions ont été réinitialisées")


# Instance globale
multi_db_manager = MultiDBManager()