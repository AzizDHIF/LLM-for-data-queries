import json
import pandas as pd
from groq import Groq
import os
import re
import redis
from typing import Dict, List, Any, Tuple
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Variables globales
r = None
client = None
groq_available = False

# Connexion Redis
def init_redis():
    """Initialise la connexion Redis"""
    global r
    try:
        r = redis.Redis(
            host="localhost",
            port=6379,
            db=0,
            decode_responses=True
        )
        # Tester la connexion
        r.ping()
        print("✅ Connexion Redis établie")
        return r
    except Exception as e:
        print(f"❌ Erreur de connexion Redis: {e}")
        return None

# Initialiser Redis
init_redis()

def init_groq_client():
    """Initialise le client Groq"""
    global client, groq_available
    
    try:
        api_key = os.getenv("GROQ_API_KEY")
        
        if not api_key or api_key == "votre_clé_api_groq_ici":
            print("⚠️ GROQ_API_KEY non configurée ou invalide")
            client = None
            groq_available = False
            return client, groq_available
            
        client = Groq(api_key=api_key)
        print("✅ Client Groq initialisé")
        groq_available = True
    except Exception as e:
        print(f"❌ Erreur client Groq : {e}")
        client = None
        groq_available = False
    
    return client, groq_available

def detect_query_type(question: str) -> str:
    """Détecte le type de requête demandée pour Redis"""
    question_lower = question.lower()
    
    # Requêtes générales sur la base
    if any(word in question_lower for word in ['colonnes', 'types', 'noms des colonnes', 'nombre de lignes', 'plage', 'range', 'résumé']):
        return 'general_info'
    
    # Vérifier d'abord les groupements explicites
    if any(word in question_lower for word in ['grouper', 'group by', 'par catégorie', 'par type', 'par prix', 'by price']):
        return 'group'

    # Agrégations
    if any(word in question_lower for word in ['moyenne', 'moyen', 'average', 'avg']):
        return 'avg'
    
    if any(word in question_lower for word in ['combien', 'nombre', 'count', 'total de produits', 'compter']):
        return 'count'
    
    if any(word in question_lower for word in ['somme', 'sum', 'total des', 'addition']):
        return 'sum'
    
    if any(word in question_lower for word in ['maximum', 'max', 'plus élevé', 'meilleur', 'le plus cher', 'plus cher']):
        return 'max'
    
    if any(word in question_lower for word in ['minimum', 'min', 'plus bas', 'moins cher', 'le moins cher', 'meilleur prix']):
        return 'min'
    
    # Recherche de produits
    if any(word in question_lower for word in ['produit', 'produits', 'trouver', 'chercher', 'liste']):
        return 'search'
    
    # Requête de sélection standard
    return 'select'

def generate_redis_query(question: str) -> Dict[str, Any]:
    """
    Génère une requête Redis structurée avec le type d'opération
    """
    question_lower = question.lower().strip()
    query_type = detect_query_type(question)
    
    result = {
        'type': query_type,
        'filter': {},
        'aggregation': None,
        'group_by': None,
        'sort': None,
        'limit': None,
        'redis_commands': []
    }

    # --- Traitement des requêtes générales ---
    if query_type == 'general_info':
        # Pour Redis, on peut compter les clés
        try:
            if r:
                total_products = r.scard("products:all") if r.exists("products:all") else 0
                result['info'] = {
                    'total_products': total_products,
                    'redis_ready': r is not None
                }
        except:
            result['info'] = {'redis_ready': False}
        return result

    # --- Règles de base pour Redis ---
    
    # "Tous les produits"
    if "tous les produits" in question_lower:
        result['redis_commands'] = ["SMEMBERS products:all", "HGETALL product:<id> pour chaque produit"]
        print(f"🔍 Règle: Tous les produits (type: {query_type})")
        return result
    
    # Recherche par rating
    rating_match = re.search(r'rating\s*[>:≥]\s*(\d+(?:\.\d+)?)', question_lower)
    if rating_match:
        rating_value = float(rating_match.group(1))
        result['filter'] = {"rating": {"$gt": rating_value}}
        result['redis_commands'] = [
            f"# Filtrer les produits par rating > {rating_value}",
            "# Nécessite un Sorted Set ou un scan de tous les produits"
        ]
        print(f"🔍 Règle: Rating > {rating_value} (type: {query_type})")
        return result
    
    # Recherche par catégorie
    if "electronics" in question_lower or "électronique" in question_lower:
        result['filter'] = {"category": {"$regex": "electronics", "$options": "i"}}
        result['redis_commands'] = [
            "# Filtrer par catégorie 'electronics'",
            "# Utiliser des Sets par catégorie si disponible"
        ]
        print(f"🔍 Règle: Catégorie Electronics (type: {query_type})")
        return result
    
    if "câble" in question_lower or "cable" in question_lower:
        result['filter'] = {"category": {"$regex": "cable", "$options": "i"}}
        result['redis_commands'] = [
            "# Filtrer par catégorie 'cable'",
            "# Utiliser des Sets par catégorie si disponible"
        ]
        print(f"🔍 Règle: Catégorie Cable (type: {query_type})")
        return result
    
    # Top N produits
    top_match = re.search(r'top\s+(\d+)\s+produits.*mieux\s+notés', question_lower)
    if top_match:
        limit_value = int(top_match.group(1))
        result['sort'] = {"field": "rating", "order": -1}
        result['limit'] = limit_value
        result['redis_commands'] = [
            f"# Top {limit_value} produits mieux notés",
            "ZREVRANGE products:by_rating 0 {limit_value-1} WITHSCORES"
        ]
        print(f"🔍 Règle: Top {limit_value} produits mieux notés")
        return result
    
    # Nombre de produits par catégorie
    if 'nombre de produits par catégorie' in question_lower or 'produits par catégorie' in question_lower:
        result['type'] = 'group'
        result['group_by'] = 'category'
        result['aggregation'] = {'field': 'product_id', 'operation': 'count'}
        result['redis_commands'] = [
            "# Nombre de produits par catégorie",
            "# Nécessite des Sets par catégorie:",
            "# Pour chaque catégorie: SCARD category:<nom_categorie>"
        ]
        print(f"🔍 Règle: Nombre de produits par catégorie")
        return result
    
    # Utiliser le LLM pour les cas complexes
    if not groq_available:
        return result
    
    try:
        prompt = f"""
Tu es un expert en conversion de langage naturel vers Redis.

Question: "{question}"

Structure des données Redis:
- Chaque produit est stocké dans un HASH: product:<id>
- Tous les IDs de produit sont dans un SET: products:all
- Produits triés par rating: ZSET products:by_rating
- Produits triés par prix: ZSET products:by_price
- Produits par catégorie: SET category:<nom_categorie>

Champs disponibles dans chaque HASH product:<id>:
- product_id: string
- product_name: string
- category: string
- discounted_price: number
- actual_price: number
- discount_percentage: string
- rating: number (0-5)
- rating_count: string
- about_product: string

Type de requête détecté: {query_type}

Génère UNIQUEMENT un objet JSON avec cette structure:
{{
  "type": "{query_type}",
  "filter": {{}},
  "aggregation": {{"field": "nom_champ", "operation": "avg|sum|max|min"}},
  "group_by": "nom_champ",
  "sort": {{"field": "nom_champ", "order": "asc|desc"}},
  "limit": nombre,
  "redis_commands": ["commande1", "commande2", ...]
}}

Exemples:
1. "Combien de produits avec rating > 4?" 
   -> {{"type": "count", "filter": {{"rating": {{"$gt": 4}}}}, 
       "redis_commands": ["# Filtrer par rating > 4", "ZCOUNT products:by_rating 4 5"]}}

2. "Prix moyen des câbles"
   -> {{"type": "avg", "filter": {{"category": {{"$regex": "cable", "$options": "i"}}}}, 
       "aggregation": {{"field": "discounted_price", "operation": "avg"}},
       "redis_commands": ["# Prix moyen des câbles", "# Nécessite script Lua ou traitement client"]}}

3. "Nombre de produits par catégorie"
   -> {{"type": "group", "aggregation": {{"field": "product_id", "operation": "count"}}, 
       "group_by": "category",
       "redis_commands": ["# Compter par catégorie", "KEYS category:*", "SCARD <chaque_catégorie>"]}}

4. "Top 10 produits les mieux notés"
   -> {{"type": "select", "sort": {{"field": "rating", "order": "desc"}}, "limit": 10,
       "redis_commands": ["ZREVRANGE products:by_rating 0 9 WITHSCORES", 
                         "HGETALL product:<id> pour chaque résultat"]}}

Réponds UNIQUEMENT avec le JSON, sans explication.
"""
        
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "Tu retournes uniquement du JSON valide, pas d'explications ni de markdown."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=300
        )
        
        query_str = response.choices[0].message.content.strip()
        query_str = query_str.replace('```json', '').replace('```', '').replace('`', '').strip()
        
        try:
            parsed_query = json.loads(query_str)
            print(f"✅ Requête Redis LLM générée: {query_str[:150]}...")
            return parsed_query
        except json.JSONDecodeError:
            print(f"⚠️ Requête LLM invalide, retour au résultat par défaut")
            return result
            
    except Exception as e:
        print(f"❌ Erreur LLM: {e}")
        return result

def get_all_product_ids():
    """Récupère tous les IDs de produits depuis Redis"""
    if not r:
        return []
    
    try:
        # Vérifier si le set "products:all" existe
        if r.exists("products:all"):
            return list(r.smembers("products:all"))
        else:
            # Sinon, chercher toutes les clés product:*
            keys = r.keys("product:*")
            # Filtrer pour n'avoir que les IDs de produit (pas les reviews)
            product_ids = []
            for key in keys:
                if key.startswith("product:") and ":reviews" not in key:
                    # Extraire l'ID du produit
                    product_id = key.split(":")[1]
                    if product_id not in product_ids:
                        product_ids.append(product_id)
            return product_ids
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des IDs: {e}")
        return []

def get_product_details(product_id: str) -> Dict:
    """Récupère les détails d'un produit depuis Redis"""
    if not r:
        return {}
    
    try:
        product_key = f"product:{product_id}"
        if r.exists(product_key):
            product_data = r.hgetall(product_key)
            # Convertir les valeurs numériques
            if 'discounted_price' in product_data:
                try:
                    product_data['discounted_price'] = float(product_data['discounted_price'])
                except:
                    pass
            if 'rating' in product_data:
                try:
                    product_data['rating'] = float(product_data['rating'])
                except:
                    pass
            if 'actual_price' in product_data:
                try:
                    product_data['actual_price'] = float(product_data['actual_price'])
                except:
                    pass
            return product_data
        return {}
    except Exception as e:
        print(f"❌ Erreur lors de la récupération du produit {product_id}: {e}")
        return {}

def apply_redis_filter(product_ids: List[str], filter_query: Dict) -> List[str]:
    """Applique un filtre sur les produits Redis"""
    if not filter_query or not product_ids:
        return product_ids
    
    filtered_ids = []
    
    for product_id in product_ids:
        product_data = get_product_details(product_id)
        if not product_data:
            continue
        
        match = True
        for key, value in filter_query.items():
            if key in product_data:
                if isinstance(value, dict):
                    if "$gt" in value:
                        try:
                            product_val = float(product_data[key])
                            if not (product_val > value["$gt"]):
                                match = False
                                break
                        except:
                            match = False
                            break
                    elif "$lt" in value:
                        try:
                            product_val = float(product_data[key])
                            if not (product_val < value["$lt"]):
                                match = False
                                break
                        except:
                            match = False
                            break
                    elif "$regex" in value:
                        pattern = value["$regex"]
                        if not re.search(pattern, str(product_data[key]), re.IGNORECASE):
                            match = False
                            break
                else:
                    if str(product_data[key]) != str(value):
                        match = False
                        break
        
        if match:
            filtered_ids.append(product_id)
    
    return filtered_ids

def execute_redis_query(query_dict: Dict[str, Any]) -> Tuple[str, List[Dict], Dict]:
    """
    Exécute une requête Redis structurée
    Retourne: (type_de_résultat, données, métadonnées)
    """
    global r
    
    if not r:
        return 'error', [], {'message': 'Connexion Redis non établie'}
    
    try:
        query_type = query_dict.get('type', 'select')
        filter_query = query_dict.get('filter', {})
        aggregation = query_dict.get('aggregation')
        group_by = query_dict.get('group_by')
        sort_spec = query_dict.get('sort')
        limit = query_dict.get('limit')
        
        # Récupérer tous les IDs de produits
        all_product_ids = get_all_product_ids()
        
        # Appliquer le filtre
        filtered_ids = apply_redis_filter(all_product_ids, filter_query)
        print(f"✅ Filtrage: {len(filtered_ids)} produits trouvés")
        
        # Récupérer les données des produits filtrés
        products_data = []
        for product_id in filtered_ids:
            product_data = get_product_details(product_id)
            if product_data:
                products_data.append(product_data)
        
        # Convertir en DataFrame pour faciliter les opérations
        if products_data:
            df = pd.DataFrame(products_data)
        else:
            df = pd.DataFrame()
        
        # Traiter selon le type de requête
        if query_type == 'count':
            if group_by:
                # Compter par groupe
                if not df.empty and group_by in df.columns:
                    result = df.groupby(group_by).size().reset_index(name='count')
                    result = result.sort_values('count', ascending=False)
                    data = [{'group': row[group_by], 'count': row['count']} for _, row in result.iterrows()]
                    metadata = {'total': len(filtered_ids), 'groups': len(result)}
                else:
                    data = []
                    metadata = {'total': len(filtered_ids), 'groups': 0}
            else:
                # Compte total
                count = len(filtered_ids)
                data = [{'count': count}]
                metadata = {'total': count}
            return 'count', data, metadata
        
        elif query_type == 'avg' and aggregation:
            field = aggregation.get('field', 'rating')
            if df.empty or field not in df.columns:
                return 'error', [], {'message': f'Champ {field} non trouvé ou aucune donnée'}
            
            if group_by:
                result = df.groupby(group_by)[field].mean().reset_index()
                result.columns = ['group', 'average']
                result = result.sort_values('average', ascending=False)
                data = [{'group': row['group'], 'average': round(row['average'], 2)} for _, row in result.iterrows()]
                metadata = {'field': field, 'overall_avg': round(df[field].mean(), 2), 'total_count': len(df)}
            else:
                avg_value = df[field].mean()
                data = [{'field': field, 'average': round(avg_value, 2)}]
                metadata = {'total_count': len(df)}
            return 'avg', data, metadata
        
        elif query_type == 'sum' and aggregation:
            field = aggregation.get('field', 'discounted_price')
            if df.empty or field not in df.columns:
                return 'error', [], {'message': f'Champ {field} non trouvé ou aucune donnée'}
            
            if group_by:
                result = df.groupby(group_by)[field].sum().reset_index()
                result.columns = ['group', 'sum']
                result = result.sort_values('sum', ascending=False)
                data = [{'group': row['group'], 'sum': round(row['sum'], 2)} for _, row in result.iterrows()]
                metadata = {'field': field, 'total_sum': round(df[field].sum(), 2), 'total_count': len(df)}
            else:
                sum_value = df[field].sum()
                data = [{'field': field, 'sum': round(sum_value, 2)}]
                metadata = {'total_count': len(df)}
            return 'sum', data, metadata
        
        elif query_type == 'max' and aggregation:
            field = aggregation.get('field', 'rating')
            if df.empty or field not in df.columns:
                return 'error', [], {'message': f'Champ {field} non trouvé ou aucune donnée'}
            
            if group_by:
                result = df.loc[df.groupby(group_by)[field].idxmax()]
                data = []
                for _, row in result.iterrows():
                    data.append({
                        'group': row[group_by],
                        'product': str(row.get('product_name', 'N/A'))[:60],
                        'value': round(row[field], 2)
                    })
                metadata = {'field': field, 'total_count': len(df)}
            else:
                max_row = df.loc[df[field].idxmax()]
                data = [{
                    'product': str(max_row.get('product_name', 'N/A'))[:80],
                    'field': field,
                    'value': round(max_row[field], 2)
                }]
                metadata = {'total_count': len(df)}
            return 'max', data, metadata
        
        elif query_type == 'min' and aggregation:
            field = aggregation.get('field', 'discounted_price')
            if df.empty or field not in df.columns:
                return 'error', [], {'message': f'Champ {field} non trouvé ou aucune donnée'}
            
            if group_by:
                result = df.loc[df.groupby(group_by)[field].idxmin()]
                data = []
                for _, row in result.iterrows():
                    data.append({
                        'group': row[group_by],
                        'product': str(row.get('product_name', 'N/A'))[:60],
                        'value': round(row[field], 2)
                    })
                metadata = {'field': field, 'total_count': len(df)}
            else:
                min_row = df.loc[df[field].idxmin()]
                data = [{
                    'product': str(min_row.get('product_name', 'N/A'))[:80],
                    'field': field,
                    'value': round(min_row[field], 2)
                }]
                metadata = {'total_count': len(df)}
            return 'min', data, metadata
        
        elif query_type == 'group':
            # Groupement avec agrégation
            if not group_by:
                group_by = 'category'
            
            agg_field = aggregation.get('field', 'product_id') if aggregation else 'product_id'
            agg_op = aggregation.get('operation', 'count') if aggregation else 'count'
            
            if df.empty:
                return 'group', [], {'group_by': group_by, 'displayed_groups': 0, 'total_groups': 0, 'total_count': 0}
            
            if agg_field == group_by and agg_op != 'count':
                agg_op = 'count'
                agg_field = 'product_id'
            
            if agg_op == 'count':
                result = df.groupby(group_by).size().reset_index(name='count')
                result.columns = ['group', 'count']
            elif agg_op == 'avg':
                result = df.groupby(group_by)[agg_field].mean().reset_index()
                result.columns = ['group', 'average']
            elif agg_op == 'sum':
                result = df.groupby(group_by)[agg_field].sum().reset_index()
                result.columns = ['group', 'sum']
            
            total_groups = len(result)
            result_display = result.sort_values(result.columns[-1], ascending=False).head(20)
            data = result_display.to_dict('records')
            
            metadata = {
                'group_by': group_by, 
                'displayed_groups': len(result_display),
                'total_groups': total_groups,
                'total_count': len(df)
            }
            return 'group', data, metadata
        
        elif query_type == 'general_info':
            total_products = len(all_product_ids)
            return 'general_info', {'total_products': total_products, 'redis_ready': r is not None}, {'total_count': total_products}
        
        else:  # select ou search
            # Trier si spécifié
            if sort_spec and not df.empty:
                sort_field = sort_spec.get('field', 'rating')
                sort_order = sort_spec.get('order', 'desc')
                if sort_field in df.columns:
                    ascending = (sort_order == 'asc')
                    df = df.sort_values(by=sort_field, ascending=ascending)
            
            # Sauvegarder le total avant de limiter
            total_matching = len(df)
            
            # Limiter les résultats pour l'affichage
            if limit:
                display_df = df.head(limit)
            else:
                display_df = df.head(20)
            
            # Formater les résultats
            results = []
            for _, row in display_df.iterrows():
                product = {
                    'product_id': str(row.get('product_id', 'N/A')),
                    'product_name': str(row.get('product_name', 'N/A'))[:80] + ("..." if len(str(row.get('product_name', ''))) > 80 else ""),
                    'category': str(row.get('category', 'N/A'))[:40],
                    'rating': f"⭐{row['rating']:.1f}" if 'rating' in row and pd.notna(row['rating']) else "N/A",
                    'discounted_price': f"₹{row['discounted_price']:.2f}" if 'discounted_price' in row and pd.notna(row['discounted_price']) else "N/A"
                }
                results.append(product)
            
            metadata = {
                'display_count': len(display_df),
                'total_matching': total_matching,
                'limit_applied': limit is not None or total_matching > len(display_df),
                'has_more': total_matching > len(display_df)
            }
            
            if limit:
                metadata['requested_limit'] = limit
            
            return 'select', results, metadata
        
    except Exception as e:
        print(f"❌ Erreur d'exécution Redis: {e}")
        import traceback
        traceback.print_exc()
        return 'error', [], {'message': str(e)}

# Fonction utilitaire pour vérifier les données Redis
def check_redis_data():
    """Vérifie les données disponibles dans Redis"""
    if not r:
        return "Redis non connecté"
    
    try:
        total_products = len(get_all_product_ids())
        
        # Compter les catégories uniques
        categories = set()
        for product_id in get_all_product_ids():
            product_data = get_product_details(product_id)
            if 'category' in product_data:
                categories.add(product_data['category'])
        
        return {
            'total_products': total_products,
            'unique_categories': len(categories),
            'categories_examples': list(categories)[:5] if categories else []
        }
    except Exception as e:
        return f"Erreur: {e}"

# Initialiser le client Groq
init_groq_client()

# Vérifier les données Redis au démarrage
print("\n🔍 Vérification des données Redis...")
redis_info = check_redis_data()
print(f"📊 Info Redis: {redis_info}")



# Redis
def explore_schema_redis(redis_client) -> dict:
    result = {
        "database": "redis",
        "num_keys": 0,
        "schema": {},
        "profile": {}
    }

    keys = redis_client.keys('*')
    result["num_keys"] = len(keys)

    for key in keys:
        key_type = redis_client.type(key).decode() if isinstance(redis_client.type(key), bytes) else redis_client.type(key)
        info = {"type": key_type}
        result["schema"][key] = info

        # Profiling pour HASH ou SET/ZSET
        if key_type in ["hash", "set", "zset"]:
            try:
                if key_type == "hash":
                    fields = list(redis_client.hkeys(key))
                    info["fields"] = [f.decode() if isinstance(f, bytes) else f for f in fields]
                    # valeurs numériques approximatives
                    numeric_values = []
                    for f in fields:
                        val = redis_client.hget(key, f)
                        try:
                            numeric_values.append(float(val))
                        except:
                            continue
                    if numeric_values:
                        info["min"] = min(numeric_values)
                        info["max"] = max(numeric_values)
                        info["mean"] = sum(numeric_values)/len(numeric_values)
                elif key_type in ["set", "zset"]:
                    info["cardinality"] = redis_client.scard(key) if key_type == "set" else redis_client.zcard(key)
            except:
                pass

    result["profile"] = result["schema"]
    return result
