import json
import pandas as pd
from groq import Groq
import os
import re
from typing import Dict, List, Any, Tuple

# Variables globales
df = None
client = None
groq_available = False

def init_data():
    """Initialise les données depuis le fichier JSON"""
    global df
    
    try:
        with open("data/mongo_amazon.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"✅ Données chargées : {len(data)} produits")
    except FileNotFoundError:
        print("❌ Erreur : Fichier 'data/mongo_amazon.json' non trouvé")
        data = []
    except json.JSONDecodeError:
        print("❌ Erreur : Format JSON invalide")
        data = []
    
    if not data:
        print("⚠️ Aucune donnée chargée, création d'un DataFrame vide")
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(data)
        print(f"✅ DataFrame créé avec {len(df)} lignes et {len(df.columns)} colonnes")
    
    if not df.empty:
        # Nettoyer rating
        if 'rating' in df.columns:
            df['rating'] = pd.to_numeric(df['rating'].astype(str).str.replace(',', '', regex=False).fillna('0'), errors='coerce')
        
        # Nettoyer les prix
        for price_col in ['discounted_price', 'actual_price']:
            if price_col in df.columns:
                df[price_col] = pd.to_numeric(
                    df[price_col].astype(str).str.replace(r'[^\d.]', '', regex=True).fillna('0'),
                    errors='coerce'
                )
        
        print("✅ Colonnes numériques nettoyées")
    
    return df

def init_groq_client():
    """Initialise le client Groq"""
    global client, groq_available
    
    try:
        api_key = os.getenv("GROQ_API_KEY") 
        client = Groq(api_key=api_key)
        print("✅ Client Groq initialisé")
        groq_available = True
    except Exception as e:
        print(f"❌ Erreur client Groq : {e}")
        client = None
        groq_available = False
    
    return client, groq_available

def detect_query_type(question: str) -> str:
    """Détecte le type de requête demandée"""
    question_lower = question.lower()
    
    # Requêtes générales sur la base
    if any(word in question_lower for word in ['colonnes', 'types', 'noms des colonnes', 'nombre de lignes', 'plage', 'range', 'résumé']):
        return 'general_info'
    
    # Vérifier d'abord les groupements explicites
    if any(word in question_lower for word in ['grouper', 'group by', 'par catégorie', 'par type', 'par prix', 'by price']):
        return 'group'

    # Agrégations - plus strictes pour détecter avant les autres
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
    
    # Requête de sélection standard
    return 'select'

def generate_mongodb_query(question: str) -> Dict[str, Any]:
    """
    Génère une requête MongoDB structurée avec le type d'opération,
    et gère aussi les requêtes 'general_info' sur le DataFrame.
    """
    question_lower = question.lower().strip()
    query_type = detect_query_type(question)
    
    result = {
        'type': query_type,
        'filter': {},
        'aggregation': None,
        'group_by': None,
        'sort': None,
        'limit': None
    }

    # --- Nouveau : traitement des requêtes générales ---
    if query_type == 'general_info':
        from pandas.api.types import is_numeric_dtype
        info = {}

        # Colonnes
        if 'colonnes' in question_lower or 'noms des colonnes' in question_lower:
            info['columns'] = list(df.columns)

        # Types
        if 'types' in question_lower or 'type des colonnes' in question_lower:
            info['dtypes'] = df.dtypes.apply(lambda x: str(x)).to_dict()

        # Nombre de lignes
        if 'nombre de lignes' in question_lower or 'combien de lignes' in question_lower:
            info['num_rows'] = len(df)

        # Plage/statistiques des champs numériques
        numeric_cols = [c for c in df.columns if is_numeric_dtype(df[c])]
        for col in numeric_cols:
            if col in question_lower or 'plage' in question_lower or 'range' in question_lower:
                info[col] = {
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'mean': df[col].mean(),
                    'std': df[col].std()
                }

        result['info'] = info
        return result

    # --- Cas simples avec règles existantes ---
    if "tous les produits" in question_lower or (query_type == 'select' and len(question_lower.split()) <= 3):
        print(f"🔍 Règle: Tous les produits (type: {query_type})")
        return result
    
    # Rating patterns
    rating_match = re.search(r'rating\s*[>:≥]\s*(\d+(?:\.\d+)?)', question_lower)
    if rating_match:
        rating_value = float(rating_match.group(1))
        result['filter'] = {"rating": {"$gt": rating_value}}
        print(f"🔍 Règle: Rating > {rating_value} (type: {query_type})")
        return result
    
    rating_match_lt = re.search(r'rating\s*[<≤]\s*(\d+(?:\.\d+)?)', question_lower)
    if rating_match_lt:
        rating_value = float(rating_match_lt.group(1))
        result['filter'] = {"rating": {"$lt": rating_value}}
        print(f"🔍 Règle: Rating < {rating_value} (type: {query_type})")
        return result
    
    # Prix patterns
    price_match = re.search(r'(?:prix|price)\s*[<]\s*(\d+)', question_lower)
    if price_match:
        price_value = float(price_match.group(1))
        result['filter'] = {"discounted_price": {"$lt": price_value}}
        print(f"🔍 Règle: Prix < {price_value} (type: {query_type})")
        return result
    
    # Catégories
    if "electronics" in question_lower or "électronique" in question_lower:
        result['filter'] = {"category": {"$regex": "electronics", "$options": "i"}}
        if query_type in ['avg', 'sum', 'max', 'min']:
            result['aggregation'] = {'field': 'discounted_price', 'operation': query_type}
        print(f"🔍 Règle: Catégorie Electronics (type: {query_type})")
        return result
    
    if "câble" in question_lower or "cable" in question_lower:
        result['filter'] = {"category": {"$regex": "cable", "$options": "i"}}
        if query_type in ['avg', 'sum', 'max', 'min']:
            if 'prix' in question_lower or 'price' in question_lower:
                result['aggregation'] = {'field': 'discounted_price', 'operation': query_type}
            elif 'rating' in question_lower or 'note' in question_lower:
                result['aggregation'] = {'field': 'rating', 'operation': query_type}
            else:
                result['aggregation'] = {'field': 'discounted_price', 'operation': query_type}
        print(f"🔍 Règle: Catégorie Cable (type: {query_type})")
        return result
    
    # Utiliser le LLM pour les cas complexes
    if not groq_available:
        return result
    
    try:
        prompt = f"""
Tu es un expert en conversion de langage naturel vers MongoDB.

Question: "{question}"

Schéma de la collection:
- product_id: string
- product_name: string
- category: string (format: "Category|SubCategory")
- discounted_price: number (en roupies)
- actual_price: number
- discount_percentage: number
- rating: number (0-5)
- rating_count: number
- about_product: string

Type de requête détecté: {query_type}

Génère UNIQUEMENT un objet JSON avec cette structure:
{{
  "type": "{query_type}",
  "filter": {{}},
  "aggregation": {{"field": "nom_champ", "operation": "avg|sum|max|min"}},
  "group_by": "nom_champ",
  "sort": {{"field": "nom_champ", "order": 1|-1}},
  "limit": nombre
}}

Exemples:
1. "Combien de produits avec rating > 4?" 
   -> {{"type": "count", "filter": {{"rating": {{"$gt": 4}}}}, "aggregation": null, "group_by": null}}

2. "Prix moyen des câbles"
   -> {{"type": "avg", "filter": {{"category": {{"$regex": "cable", "$options": "i"}}}}, "aggregation": {{"field": "discounted_price", "operation": "avg"}}, "group_by": null}}

3. "Nombre de produits par catégorie"
   -> {{"type": "group", "filter": {{}}, "aggregation": {{"field": "product_id", "operation": "count"}}, "group_by": "category"}}

4. "Top 10 produits les mieux notés"
   -> {{"type": "select", "filter": {{}}, "sort": {{"field": "rating", "order": -1}}, "limit": 10}}

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
            print(f"✅ Requête LLM générée: {query_str[:150]}...")
            return parsed_query
        except json.JSONDecodeError:
            print(f"⚠️ Requête LLM invalide, retour au résultat par défaut")
            return result
            
    except Exception as e:
        print(f"❌ Erreur LLM: {e}")
        return result

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
        filter_query = query_dict.get('filter', {})
        aggregation = query_dict.get('aggregation')
        group_by = query_dict.get('group_by')
        sort_spec = query_dict.get('sort')
        limit = query_dict.get('limit')
        
        # Appliquer le filtre
        filtered_df = apply_filter(df, filter_query)
        print(f"✅ Filtrage: {len(filtered_df)} produits")
        
        # Traiter selon le type de requête
        if query_type == 'count':
            if group_by:
                # Compter par groupe
                result = filtered_df.groupby(group_by).size().reset_index(name='count')
                result = result.sort_values('count', ascending=False)
                data = [{'group': row[group_by], 'count': row['count']} for _, row in result.iterrows()]
                metadata = {'total': len(filtered_df), 'groups': len(result)}
            else:
                # Compte total
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
                result = filtered_df.groupby(group_by)[field].sum().reset_index()
                result.columns = ['group', 'sum']
                result = result.sort_values('sum', ascending=False)
                data = [{'group': row['group'], 'sum': round(row['sum'], 2)} for _, row in result.iterrows()]
                metadata = {'field': field, 'total_sum': round(filtered_df[field].sum(), 2)}
            else:
                sum_value = filtered_df[field].sum()
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
                min_row = filtered_df.loc[filtered_df[field].idxmin()]
                data = [{
                    'product': str(min_row.get('product_name', 'N/A'))[:80],
                    'field': field,
                    'value': round(min_row[field], 2)
                }]
                metadata = {}
            return 'min', data, metadata
        
        elif query_type == 'group':
            # Groupement avec agrégation
            if not group_by:
                group_by = 'category'
            
            agg_field = aggregation.get('field', 'product_id') if aggregation else 'product_id'
            agg_op = aggregation.get('operation', 'count') if aggregation else 'count'
            
            # IMPORTANT: Si on groupe par le même champ qu'on agrège, faire un count à la place
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
                result = filtered_df.groupby(group_by)[agg_field].sum().reset_index()
                result.columns = ['group', 'sum']
            
            result = result.sort_values(result.columns[-1], ascending=False).head(20)
            data = result.to_dict('records')
            metadata = {'group_by': group_by, 'groups': len(result)}
            return 'group', data, metadata
        
        elif query_dict.get('type') == 'general_info':
            return 'general_info', query_dict.get('info', {}), {'count': len(df), 'columns': list(df.columns)}
        
        else:  # select
            # Trier si spécifié
            if sort_spec:
                sort_field = sort_spec.get('field', 'rating')
                sort_order = sort_spec.get('order', -1)
                filtered_df = filtered_df.sort_values(
                    by=sort_field, 
                    ascending=(sort_order == 1)
                )
            
            # Limiter les résultats
            if limit:
                filtered_df = filtered_df.head(limit)
            else:
                filtered_df = filtered_df.head(20)
            
            # Formater les résultats
            results = []
            for _, row in filtered_df.iterrows():
                product = {
                    'product_name': str(row.get('product_name', 'N/A'))[:80] + ("..." if len(str(row.get('product_name', ''))) > 80 else ""),
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