import json
import re
import pandas as pd
from typing import Dict, List, Any, Tuple
from connectors.mongodb_connector import DataLoader
from dotenv import load_dotenv
from pandas.api.types import is_numeric_dtype

# Importer les fonctions du classifier
from .classifier_old import (
    detect_query_type, 
    analyze_query,
    extract_crud_params,
    generate_crud_queries,
    detect_missing_crud_fields,  
    generate_crud_prompt,         
    validate_crud_data            
)

# Charger les variables d'environnement
load_dotenv()

# Variables globales
df = None
client = None
groq_available = False

# ✅ Initialisation correcte du DataLoader (via YAML)
loader = DataLoader()  
df = loader.init_data()

print(df.head())
print(df.dtypes)



def clean_price_column(dataframe):
    """Nettoie la colonne des prix pour permettre un tri numérique"""
    if 'discounted_price' in dataframe.columns:
        # Créer une copie pour éviter les warnings
        df_copy = dataframe.copy()
        
        # Fonction pour nettoyer les prix
        def clean_price(price):
            if pd.isna(price):
                return 0.0
            # Convertir en string
            price_str = str(price)
            # Supprimer les symboles de devise et caractères non numériques
            price_str = re.sub(r'[^\d.]', '', price_str)
            # Gérer les cas où il n'y a pas de point décimal
            if '.' not in price_str:
                price_str = price_str + '.00'
            try:
                return float(price_str)
            except:
                return 0.0
        
        # Appliquer le nettoyage
        df_copy['discounted_price_clean'] = df_copy['discounted_price'].apply(clean_price)
        return df_copy
    return dataframe


def semantic_query_analysis(question: str) -> Dict[str, Any]:
    """
    Analyse sémantique de la question pour comprendre l'intention
    Utilise le LLM pour une compréhension plus intelligente
    """
    try:
        from groq import Groq
        client = Groq()
        
        prompt = f"""
Tu es un expert en analyse sémantique de requêtes pour bases de données.
Analyse cette question et détermine l'intention de l'utilisateur.

Question: "{question}"

Schéma de la collection "products":
- product_id: string (identifiant unique)
- product_name: string (nom du produit)
- category: string (catégorie, format: "Category|SubCategory")
- discounted_price: number (prix après réduction en roupies)
- actual_price: number (prix original)
- discount_percentage: number (pourcentage de réduction)
- rating: number (note de 0 à 5)
- rating_count: number (nombre d'avis)
- about_product: string (description)

INTENTIONS POSSIBLES:
1. TOP_PRODUCTS - Recherche des meilleurs produits (ex: "top 10 produits les mieux notés")
2. EXTREME_PRICE - Produit le plus/moins cher (ex: "produit le moins cher", "le plus coûteux")
3. BY_CATEGORY - Analyse par catégorie (ex: "produit le plus cher par catégorie")
4. TEXT_SEARCH - Recherche textuelle (ex: "produits dont le nom contient 'TV'")
5. COUNT - Nombre de produits (ex: "combien de produits")
6. AVERAGE - Moyenne (ex: "prix moyen")
7. SUM - Somme (ex: "total des prix")
8. FILTER - Filtrage simple (ex: "produits avec rating > 4")
9. LIST_ALL - Liste tous les produits (ex: "tous les produits")
10. SCHEMA_INFO - Informations sur le schéma

Retourne UNIQUEMENT un objet JSON avec cette structure:
{{
  "intention": "TOP_PRODUCTS|EXTREME_PRICE|BY_CATEGORY|TEXT_SEARCH|COUNT|AVERAGE|SUM|FILTER|LIST_ALL|SCHEMA_INFO",
  "details": {{
    "field": "rating|discounted_price|category|product_name|...",
    "operation": "max|min|avg|sum|count|sort|group_by|contains",
    "value": valeur_numerique_si_filtre,
    "text_search": mot_clé_si_recherche_textuelle,
    "limit": nombre_si_limité,
    "order": "asc|desc" (ascendant/descendant)
  }},
  "confidence": 0.0 à 1.0 (confiance dans l'analyse)
}}

Exemples:
Question: "Top 10 produits les mieux notés"
Réponse: {{"intention": "TOP_PRODUCTS", "details": {{"field": "rating", "operation": "sort", "limit": 10, "order": "desc"}}, "confidence": 0.95}}

Question: "produit le moins cher"
Réponse: {{"intention": "EXTREME_PRICE", "details": {{"field": "discounted_price", "operation": "min", "limit": 1, "order": "asc"}}, "confidence": 0.98}}

Question: "me plus cher" (faute de frappe pour "le plus cher")
Réponse: {{"intention": "EXTREME_PRICE", "details": {{"field": "discounted_price", "operation": "max", "limit": 1, "order": "desc"}}, "confidence": 0.85}}

Question: "produits dont le nom contient 'TV'"
Réponse: {{"intention": "TEXT_SEARCH", "details": {{"field": "product_name", "operation": "contains", "text_search": "TV"}}, "confidence": 0.92}}
"""
        
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "Tu retournes uniquement du JSON valide. Pas d'explications."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=300
        )
        
        result_str = response.choices[0].message.content.strip()
        result_str = result_str.replace('```json', '').replace('```', '').strip()
        
        analysis = json.loads(result_str)
        print(f"🔍 Analyse sémantique: {analysis}")
        return analysis
            
    except Exception as e:
        print(f"❌ Erreur analyse sémantique: {e}")
        return {
            "intention": "UNKNOWN",
            "details": {},
            "confidence": 0.0
        }


def generate_mongodb_query(question: str) -> Dict[str, Any]:
    """
    Génère une requête MongoDB structurée à partir d'une question en langage naturel
    Utilise l'analyse sémantique intelligente
    """
    question_lower = question.lower().strip()
    query_type = detect_query_type(question)
    if query_type == 'count':
        # Recherche de texte dans le nom
        text_search_patterns = [
            r'nom.*contient\s+["\']([^"\']+)["\']',
            r'name.*contains\s+["\']([^"\']+)["\']',
            r'avec\s+["\']([^"\']+)["\'].*dans.*nom'
        ]
        
        for pattern in text_search_patterns:
            match = re.search(pattern, question_lower)
            if match:
                keyword = match.group(1).strip()
                return {
                    'type': 'count',
                    'filter': {'product_name': {'$regex': keyword, '$options': 'i'}},
                    'aggregation': None,
                    'group_by': None,
                    'sort': None,
                    'limit': None
                }
            
    # 🆕 GESTION DES OPÉRATIONS CRUD AVEC MODE CONVERSATIONNEL
    if query_type in ['create', 'update', 'delete']:
        print(f"🆕 Opération CRUD détectée: {query_type.upper()}")
        
        # Extraire les paramètres depuis la question
        params = extract_crud_params(question, query_type)
        
        # 🔍 VÉRIFIER LES CHAMPS MANQUANTS
        missing_fields = detect_missing_crud_fields(query_type, params)
        
        if missing_fields:
            print(f"⚠️ Champs manquants détectés: {missing_fields}")
            
            # Générer un prompt conversationnel
            prompt_message = generate_crud_prompt(query_type, missing_fields)
            
            return {
                'type': 'crud_incomplete',
                'operation': query_type,
                'params': params,
                'missing_fields': missing_fields,
                'prompt': prompt_message
            }
        
        # ✅ VALIDER LES DONNÉES
        is_valid, error_message = validate_crud_data(query_type, params)
        
        if not is_valid:
            print(f"❌ Validation échouée: {error_message}")
            return {
                'type': 'crud_invalid',
                'operation': query_type,
                'error': error_message,
                'params': params
            }
        
        # ✅ DONNÉES COMPLÈTES ET VALIDES - Générer les requêtes
        print("✅ Données complètes et valides")
        all_queries = generate_crud_queries(query_type, params)
        
        return {
            'type': query_type,
            'operation': query_type,
            'params': params,
            'queries': all_queries,
            'mongodb_query': all_queries.get('mongodb', '')
        }

    # GESTION DU TYPE convert_nosql
    if query_type == "convert_nosql":
        print("🔄 Détection d'une requête NoSQL à analyser...")
        
        # Appeler analyze_query pour obtenir l'analyse complète
        analysis = analyze_query(question)
        
        return {
            'type': 'convert_nosql',
            'analysis': analysis,
            'original_query': question
        }

    # ANALYSE SÉMANTIQUE INTELLIGENTE
    semantic_analysis = semantic_query_analysis(question)
    intention = semantic_analysis.get('intention', 'UNKNOWN')
    details = semantic_analysis.get('details', {})
    confidence = semantic_analysis.get('confidence', 0.0)
    
    print(f"🧠 Intention détectée: {intention} (confiance: {confidence})")
    
    # Traitement basé sur l'intention sémantique
    if intention == "TOP_PRODUCTS":
        field = details.get('field', 'rating')
        limit = details.get('limit', 10)
        order = details.get('order', 'desc')
        
        sort_field = f'{field}_clean' if field == 'discounted_price' else field
        sort_order = -1 if order == 'desc' else 1
        
        return {
            'type': 'select',
            'filter': {},
            'aggregation': None,
            'group_by': None,
            'sort': {'field': sort_field, 'order': sort_order},
            'limit': limit
        }
    
    elif intention == "EXTREME_PRICE":
        field = details.get('field', 'discounted_price')
        operation = details.get('operation', 'min')
        limit = details.get('limit', 1)
        
        if operation == 'min':
            query_type = 'min'
            sort_order = 1
        else:  # max
            query_type = 'max'
            sort_order = -1
        
        sort_field = f'{field}_clean' if field == 'discounted_price' else field
        
        return {
            'type': query_type,
            'aggregation': {'field': field},
            'sort': {'field': sort_field, 'order': sort_order},
            'limit': limit,
            'filter': {}
        }
    
    elif intention == "BY_CATEGORY":
        field = details.get('field', 'discounted_price')
        operation = details.get('operation', 'max')
        
        if operation == 'min':
            query_type = 'min'
        else:  # max
            query_type = 'max'
        
        return {
            'type': query_type,
            'aggregation': {'field': field},
            'group_by': 'category',
            'filter': {}
        }
    
    elif intention == "TEXT_SEARCH":
        field = details.get('field', 'product_name')
        text_search = details.get('text_search', '')
        
        if text_search:
            return {
                'type': 'select',
                'filter': {field: {"$regex": text_search, "$options": "i"}},
                'aggregation': None,
                'group_by': None,
                'sort': None,
                'limit': None
            }
    
    elif intention == "COUNT":
        return {
            'type': 'count',
            'filter': {},
            'aggregation': None,
            'group_by': None,
            'sort': None,
            'limit': None
        }
    
    elif intention == "AVERAGE":
        field = details.get('field', 'rating')
        return {
            'type': 'avg',
            'filter': {},
            'aggregation': {'field': field},
            'group_by': None,
            'sort': None,
            'limit': None
        }
    
    elif intention == "SUM":
        field = details.get('field', 'discounted_price')
        return {
            'type': 'sum',
            'filter': {},
            'aggregation': {'field': field},
            'group_by': None,
            'sort': None,
            'limit': None
        }
    
    elif intention == "FILTER":
        field = details.get('field', 'rating')
        operation = details.get('operation', 'gt')
        value = details.get('value', 4)
        
        filter_query = {}
        if operation == 'gt':
            filter_query[field] = {"$gt": value}
        elif operation == 'lt':
            filter_query[field] = {"$lt": value}
        elif operation == 'eq':
            filter_query[field] = value
        
        return {
            'type': 'select',
            'filter': filter_query,
            'aggregation': None,
            'group_by': None,
            'sort': None,
            'limit': None
        }
    
    elif intention == "LIST_ALL":
        return {
            'type': 'select',
            'filter': {},
            'aggregation': None,
            'group_by': None,
            'sort': None,
            'limit': 20  # Limiter par défaut
        }
    
    elif intention == "SCHEMA_INFO":
        return {
            'type': 'schema',
            'filter': {},
            'aggregation': None,
            'group_by': None,
            'sort': None,
            'limit': None
        }

    # Fallback: utiliser le prompt LLM original
    if not groq_available:
        return {
            'type': query_type,
            'filter': {},
            'aggregation': None,
            'group_by': None,
            'sort': None,
            'limit': None
        }
    
    try:
        from groq import Groq
        client = Groq()
        
        prompt = f"""
Tu es un expert en conversion de langage naturel vers MongoDB.

Question: "{question}"

Analyse sémantique détectée: {intention} (confiance: {confidence})
Détails: {details}

Schéma de la collection "products":
- product_id: string
- product_name: string
- category: string (format: "Category|SubCategory")
- discounted_price: number (en roupies)
- actual_price: number
- discount_percentage: number
- rating: number (0-5)
- rating_count: number
- about_product: string

Génère UNIQUEMENT un objet JSON avec cette structure:
{{
  "type": "select|count|avg|sum|max|min|group|schema",
  "filter": {{}},
  "aggregation": {{"field": "nom_champ", "operation": "avg|sum|max|min"}},
  "group_by": "nom_champ",
  "sort": {{"field": "nom_champ", "order": 1|-1}},
  "limit": nombre
}}

Instructions importantes:
1. Pour les top produits: utiliser "sort" avec order: -1 (desc) et spécifier un "limit"
2. Pour le produit le plus/moins cher: type "max" ou "min" avec field "discounted_price", limit: 1
3. Pour les requêtes par catégorie: ajouter "group_by": "category"
4. Pour les recherches textuelles: utiliser $regex dans filter

Réponds UNIQUEMENT avec le JSON, sans explication.
"""
        
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "Tu retournes uniquement du JSON valide."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=300
        )
        
        query_str = response.choices[0].message.content.strip()
        query_str = query_str.replace('```json', '').replace('```', '').strip()
        
        parsed_query = json.loads(query_str)
        print(f"✅ Requête LLM générée: {query_str[:150]}...")
        return parsed_query
            
    except Exception as e:
        print(f"❌ Erreur LLM: {e}")
        return {
            'type': query_type,
            'filter': {},
            'aggregation': None,
            'group_by': None,
            'sort': None,
            'limit': None
        }


def apply_filter(dataframe: pd.DataFrame, query: dict) -> pd.DataFrame:
    """Applique un filtre MongoDB-style sur un DataFrame"""
    if not query:
        return dataframe.copy()
    
    mask = pd.Series([True] * len(dataframe), index=dataframe.index)
    
    for key, value in query.items():
        if key == "$and":
            for sub_query in value:
                sub_mask = apply_filter(dataframe, sub_query).index
                mask = mask & dataframe.index.isin(sub_mask)
        elif key == "$or":
            or_mask = pd.Series([False] * len(dataframe), index=dataframe.index)
            for sub_query in value:
                sub_mask = apply_filter(dataframe, sub_query).index
                or_mask = or_mask | dataframe.index.isin(sub_mask)
            mask = mask & or_mask
        elif isinstance(value, dict):
            if "$gt" in value:
                mask = mask & (dataframe[key] > value["$gt"])
            if "$gte" in value:
                mask = mask & (dataframe[key] >= value["$gte"])
            if "$lt" in value:
                mask = mask & (dataframe[key] < value["$lt"])
            if "$lte" in value:
                mask = mask & (dataframe[key] <= value["$lte"])
            if "$regex" in value:
                pattern = value["$regex"]
                case = False if ("$options" in value and "i" in value["$options"]) else True
                mask = mask & dataframe[key].astype(str).str.contains(pattern, case=case, na=False, regex=True)
            if "$ne" in value:
                mask = mask & (dataframe[key] != value["$ne"])
        else:
            mask = mask & (dataframe[key] == value)
    
    return dataframe[mask]


def execute_crud_operation(query_dict: Dict[str, Any]) -> Tuple[str, List[Dict], Dict]:
    """
    Exécute une opération CRUD sur le DataFrame
    """
    global df
    
    operation = query_dict.get('operation')
    params = query_dict.get('params', {})
    
    try:
        if operation == 'create':
            # Ajouter une nouvelle ligne au DataFrame
            new_data = params.get('data', {})
            
            # Générer un ID si pas présent
            if '_id' not in new_data and 'id' not in new_data:
                new_data['_id'] = f"prod_{len(df) + 1}"
            
            # Créer un DataFrame avec la nouvelle ligne
            new_row = pd.DataFrame([new_data])
            
            # Ajouter au DataFrame global
            df = pd.concat([df, new_row], ignore_index=True)
            
            print(f"✅ Nouveau document créé: {new_data.get('_id', 'N/A')}")
            
            return 'create', [new_data], {
                'message': 'Document créé avec succès',
                'count': 1,
                'id': new_data.get('_id', new_data.get('id'))
            }
        
        elif operation == 'update':
            # Mettre à jour des lignes existantes
            filter_q = params.get('filter', {})
            fields_to_update = params.get('fields_to_update', {})
            
            # Appliquer le filtre
            mask = pd.Series([True] * len(df))
            for key, value in filter_q.items():
                if key in df.columns:
                    mask = mask & (df[key] == value)
            
            # Mettre à jour les champs
            for key, value in fields_to_update.items():
                if key in df.columns:
                    df.loc[mask, key] = value
            
            # Récupérer les documents mis à jour
            updated_docs = df[mask].to_dict('records')
            
            print(f"✅ {len(updated_docs)} document(s) mis à jour")
            
            return 'update', updated_docs, {
                'message': f'{len(updated_docs)} document(s) mis à jour',
                'count': len(updated_docs),
                'fields_updated': list(fields_to_update.keys())
            }
        
        elif operation == 'delete':
            # Supprimer des lignes
            filter_q = params.get('filter', {})
            
            # Appliquer le filtre
            mask = pd.Series([True] * len(df))
            for key, value in filter_q.items():
                if key in df.columns:
                    if isinstance(value, dict):
                        if '$gt' in value:
                            mask = mask & (df[key] > value['$gt'])
                        elif '$lt' in value:
                            mask = mask & (df[key] < value['$lt'])
                    else:
                        mask = mask & (df[key] == value)
            
            # Récupérer les documents à supprimer
            deleted_docs = df[mask].to_dict('records')
            
            # Supprimer du DataFrame
            df = df[~mask].reset_index(drop=True)
            
            print(f"✅ {len(deleted_docs)} document(s) supprimé(s)")
            
            return 'delete', deleted_docs, {
                'message': f'{len(deleted_docs)} document(s) supprimé(s)',
                'count': len(deleted_docs)
            }
        
        else:
            return 'error', [], {'message': f'Opération inconnue: {operation}'}
    
    except Exception as e:
        print(f"❌ Erreur CRUD: {e}")
        import traceback
        traceback.print_exc()
        return 'error', [], {'message': str(e)}


def execute_mongodb_query(query_dict: Dict[str, Any]) -> Tuple[str, List[Dict], Dict]:
    """
    Exécute une requête MongoDB structurée sur le DataFrame
    Retourne: (type_de_résultat, données, métadonnées)
    """
    global df
    
    if df.empty:
        print("⚠️ DataFrame vide")
        return 'error', [], {'message': 'Aucune donnée disponible'}
    
    try:
        query_type = query_dict.get('type', 'select')
        
        # EXÉCUTION DES OPÉRATIONS CRUD
        if query_type in ['create', 'update', 'delete']:
            return execute_crud_operation(query_dict)
        
        filter_query = query_dict.get('filter', {})
        aggregation = query_dict.get('aggregation')
        group_by = query_dict.get('group_by')
        sort_spec = query_dict.get('sort')
        limit = query_dict.get('limit')
        
        # Nettoyer la colonne des prix
        cleaned_df = clean_price_column(df)
        
        # Appliquer le filtre
        filtered_df = apply_filter(cleaned_df, filter_query)
        print(f"✅ Filtrage: {len(filtered_df)} produits")
        
        # Gestion spécifique pour les requêtes avec group_by
        if query_type in ['max', 'min'] and group_by:
            field = aggregation.get('field', 'discounted_price') if aggregation else 'discounted_price'
            
            if field == 'discounted_price' and 'discounted_price_clean' in filtered_df.columns:
                # Utiliser la colonne nettoyée pour les prix
                result = filtered_df.loc[filtered_df.groupby(group_by)['discounted_price_clean'].idxmax() if query_type == 'max' 
                                       else filtered_df.groupby(group_by)['discounted_price_clean'].idxmin()]
                data = []
                for _, row in result.iterrows():
                    data.append({
                        'group': row[group_by],
                        'product': str(row.get('product_name', 'N/A'))[:60],
                        'value': row.get('discounted_price', 'N/A'),
                        'category': str(row.get('category', 'N/A')).split('|')[-1][:40],
                        'rating': f"⭐{row['rating']:.1f}" if pd.notna(row.get('rating')) else "N/A"
                    })
                metadata = {'field': field, 'groups': len(result)}
                return query_type, data, metadata
            else:
                # Pour les autres champs
                result = filtered_df.loc[filtered_df.groupby(group_by)[field].idxmax() if query_type == 'max' 
                                       else filtered_df.groupby(group_by)[field].idxmin()]
                data = []
                for _, row in result.iterrows():
                    data.append({
                        'group': row[group_by],
                        'product': str(row.get('product_name', 'N/A'))[:60],
                        'value': round(row[field], 2) if pd.api.types.is_numeric_dtype(type(row[field])) else row[field]
                    })
                metadata = {'field': field, 'groups': len(result)}
                return query_type, data, metadata
        
        # Traiter selon le type de requête
        if query_type == 'count':
            if group_by:
                result = filtered_df.groupby(group_by).size().reset_index(name='count')
                result = result.sort_values('count', ascending=False)
                data = [{'group': row[group_by], 'count': row['count']} for _, row in result.iterrows()]
                metadata = {'total': len(filtered_df), 'groups': len(result)}
            else:
                count = len(filtered_df)
                data = [{'count': count}]
                metadata = {'total': count}
            return 'count', data, metadata
        
        elif query_type == 'avg' and aggregation:
            field = aggregation.get('field', 'rating')
            if group_by:
                result = filtered_df.groupby(group_by)[field].mean().reset_index()
                result.columns = ['group', 'average']
                result = result.sort_values('average', ascending=False)
                data = [{'group': row['group'], 'average': round(row['average'], 2)} for _, row in result.iterrows()]
                metadata = {'field': field, 'overall_avg': round(filtered_df[field].mean(), 2)}
            else:
                avg_value = filtered_df[field].mean()
                data = [{'field': field, 'average': round(avg_value, 2)}]
                metadata = {'count': len(filtered_df)}
            return 'avg', data, metadata
        
        elif query_type == 'sum' and aggregation:
            field = aggregation.get('field', 'discounted_price')
            if group_by:
                if field == 'discounted_price' and 'discounted_price_clean' in filtered_df.columns:
                    result = filtered_df.groupby(group_by)['discounted_price_clean'].sum().reset_index()
                else:
                    result = filtered_df.groupby(group_by)[field].sum().reset_index()
                result.columns = ['group', 'sum']
                result = result.sort_values('sum', ascending=False)
                data = [{'group': row['group'], 'sum': round(row['sum'], 2)} for _, row in result.iterrows()]
                metadata = {'field': field, 'total_sum': round(filtered_df[field].sum(), 2) if field != 'discounted_price' else round(filtered_df['discounted_price_clean'].sum(), 2)}
            else:
                sum_value = filtered_df[field].sum() if field != 'discounted_price' else filtered_df['discounted_price_clean'].sum()
                data = [{'field': field, 'sum': round(sum_value, 2)}]
                metadata = {'count': len(filtered_df)}
            return 'sum', data, metadata
        
        elif query_type == 'max' and aggregation:
            field = aggregation.get('field', 'rating')
            if group_by:
                result = filtered_df.loc[filtered_df.groupby(group_by)[field].idxmax()]
                data = []
                for _, row in result.iterrows():
                    data.append({
                        'group': row[group_by],
                        'product': str(row.get('product_name', 'N/A'))[:60],
                        'value': round(row[field], 2)
                    })
                metadata = {'field': field}
            else:
                max_row = filtered_df.loc[filtered_df[field].idxmax()]
                data = [{
                    'product': str(max_row.get('product_name', 'N/A'))[:80],
                    'field': field,
                    'value': round(max_row[field], 2)
                }]
                metadata = {}
            return 'max', data, metadata
        
        elif query_type == 'min' and aggregation:
            field = aggregation.get('field', 'discounted_price')
            if group_by:
                result = filtered_df.loc[filtered_df.groupby(group_by)[field].idxmin()]
                data = []
                for _, row in result.iterrows():
                    data.append({
                        'group': row[group_by],
                        'product': str(row.get('product_name', 'N/A'))[:60],
                        'value': round(row[field], 2)
                    })
                metadata = {'field': field}
            else:
                if field == 'discounted_price' and 'discounted_price_clean' in filtered_df.columns:
                    min_idx = filtered_df['discounted_price_clean'].idxmin()
                    min_row = filtered_df.loc[min_idx]
                    
                    data = [{
                        'product': str(min_row.get('product_name', 'N/A'))[:80],
                        'field': 'discounted_price',
                        'value': min_row.get('discounted_price', 'N/A'),
                        'category': str(min_row.get('category', 'N/A')).split('|')[-1][:40],
                        'rating': f"⭐{min_row['rating']:.1f}" if pd.notna(min_row.get('rating')) else "N/A"
                    }]
                    metadata = {'count': 1}
                else:
                    min_row = filtered_df.loc[filtered_df[field].idxmin()]
                    data = [{
                        'product': str(min_row.get('product_name', 'N/A'))[:80],
                        'field': field,
                        'value': round(min_row[field], 2)
                    }]
                    metadata = {}
                return 'min', data, metadata
        
        elif query_type == 'group':
            if not group_by:
                group_by = 'category'
            
            agg_field = aggregation.get('field', 'product_id') if aggregation else 'product_id'
            agg_op = aggregation.get('operation', 'count') if aggregation else 'count'
            
            if agg_field == group_by and agg_op != 'count':
                agg_op = 'count'
                agg_field = 'product_id'
            
            if agg_op == 'count':
                result = filtered_df.groupby(group_by).size().reset_index(name='count')
                result.columns = ['group', 'count']
            elif agg_op == 'avg':
                result = filtered_df.groupby(group_by)[agg_field].mean().reset_index()
                result.columns = ['group', 'average']
            elif agg_op == 'sum':
                if agg_field == 'discounted_price' and 'discounted_price_clean' in filtered_df.columns:
                    result = filtered_df.groupby(group_by)['discounted_price_clean'].sum().reset_index()
                else:
                    result = filtered_df.groupby(group_by)[agg_field].sum().reset_index()
                result.columns = ['group', 'sum']
            
            result = result.sort_values(result.columns[-1], ascending=False).head(20)
            data = result.to_dict('records')
            metadata = {'group_by': group_by, 'groups': len(result)}
            return 'group', data, metadata
        
        elif query_type == "schema":
            schema_data = query_dict.get('schema', {})
            if schema_data:
                results_list = []
                for col, info in schema_data.items():
                    results_list.append({
                        'column': col,
                        'type': info.get('type', 'N/A'),
                        'non_null': info.get('non_null', 0),
                        'null': info.get('null', 0),
                        'completeness': f"{(info.get('non_null', 0) / len(df) * 100):.1f}%" if len(df) > 0 else "0%"
                    })
                return 'schema', results_list, {
                    'count': len(df), 
                    'schema': schema_data,
                    'columns': list(schema_data.keys()),
                    'num_columns': len(schema_data)
                }
        
        elif query_type == "data_profile":
            profile_data = query_dict.get('profile', {})
            return 'data_profile', [], {
                'count': len(df),
                'profile': profile_data,
                'num_rows': profile_data.get('num_rows', len(df)),
                'num_columns': profile_data.get('num_columns', len(df.columns))
            }
        
        elif query_type == "columns":
            columns = query_dict.get('columns', list(df.columns))
            return 'columns', [], {
                'columns': columns,
                'count': len(columns)
            }
        
        else:  # select
            # Si un tri est spécifié, l'appliquer
            if sort_spec:
                sort_field = sort_spec.get('field', 'rating')
                sort_order = sort_spec.get('order', -1)
                
                if sort_field == 'discounted_price' and 'discounted_price_clean' in filtered_df.columns:
                    filtered_df = filtered_df.sort_values(
                        by='discounted_price_clean', 
                        ascending=(sort_order == 1)
                    )
                else:
                    filtered_df = filtered_df.sort_values(
                        by=sort_field, 
                        ascending=(sort_order == 1)
                    )
            
            # Appliquer la limite
            if limit:
                filtered_df = filtered_df.head(limit)
            else:
                filtered_df = filtered_df.head(20)
            
            # Préparer les résultats
            results = []
            for _, row in filtered_df.iterrows():
                product = {
                    'product_name': str(row.get('product_name', 'N/A'))[:80],
                    'category': str(row.get('category', 'N/A')).split('|')[-1][:40],
                    'rating': f"⭐{row['rating']:.1f}" if pd.notna(row.get('rating')) else "N/A",
                    'discounted_price': f"₹{row['discounted_price']:.2f}" if pd.notna(row.get('discounted_price')) else "N/A"
                }
                results.append(product)
            
            metadata = {'count': len(filtered_df), 'limited': limit is not None}
            return 'select', results, metadata
        
    except Exception as e:
        print(f"❌ Erreur d'exécution: {e}")
        import traceback
        traceback.print_exc()
        return 'error', [], {'message': str(e)}