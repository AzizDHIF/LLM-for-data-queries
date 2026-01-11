import json
import re
import pandas as pd
import torch
from typing import Dict, List, Any, Tuple
from connectors.mongodb_connector import *
from dotenv import load_dotenv
from collections import Counter
from pandas.api.types import is_numeric_dtype
from .classifier import detect_query_type, analyze_query

# Charger les variables d'environnement
load_dotenv()

# Variables globales
df = None
client = None
groq_available = False

# Créer une instance de DataLoader
loader = DataLoader(path="data/mongo_amazon.json")
df = loader.init_data()

print(df.head())
print(df.dtypes)




def generate_mongodb_query(question: str) -> Dict[str, Any]:
    """
    Génère une requête MongoDB structurée à partir d'une question en langage naturel
    OU analyse une requête NoSQL existante
    """
    question_lower = question.lower().strip()
    query_type = detect_query_type(question)

    # 🆕 GESTION DU TYPE convert_nosql
    if query_type == "convert_nosql":
        print("🔄 Détection d'une requête NoSQL à analyser...")
        
        # Appeler analyze_query pour obtenir l'analyse complète
        analysis = analyze_query(question)
        
        # 🔧 CORRECTION IMPORTANTE : Retourner l'analyse complète
        return {
            'type': 'convert_nosql',
            'analysis': analysis,  # ✅ Inclure l'analyse complète
            'original_query': question
        }

    # Traitement normal pour les autres types
    result = {
        'type': query_type,
        'filter': {},
        'aggregation': None,
        'group_by': None,
        'sort': None,
        'limit': None
    }

    # SCHÉMA
    if query_type == "schema":
        schema = {}
        for col in df.columns:
            schema[col] = {
                "type": str(df[col].dtype),
                "non_null": int(df[col].notna().sum()),
                "null": int(df[col].isna().sum())
            }
        result["schema"] = schema
        return result

    # =========================
    # 1️⃣ SCHÉMA (types des champs)
    # =========================
    if query_type == "schema":
        schema = {}
        for col in df.columns:
            schema[col] = {
                "type": str(df[col].dtype),
                "non_null": int(df[col].notna().sum()),
                "null": int(df[col].isna().sum())
            }

        result["schema"] = schema
        return result

    # =========================
    # 2️⃣ COLONNES UNIQUEMENT
    # =========================
    if query_type == "columns":
        result["columns"] = list(df.columns)
        return result

    # =========================
    # 3️⃣ PROFIL DES DONNÉES
    # =========================
    if query_type == "data_profile":
        

        profile = {
            "num_rows": len(df),
            "num_columns": len(df.columns),
            "columns": {}
        }

        for col in df.columns:
            col_info = {
                "type": str(df[col].dtype),
                "missing": int(df[col].isna().sum())
            }

            if is_numeric_dtype(df[col]):
                col_info.update({
                    "min": float(df[col].min()),
                    "max": float(df[col].max()),
                    "mean": float(df[col].mean()),
                    "std": float(df[col].std())
                })
            else:
                # Catégoriel / texte
                col_info.update({
                    "unique_values": int(df[col].nunique()),
                    "top_values": df[col].value_counts().head(5).to_dict()
                })

            profile["columns"][col] = col_info

        result["profile"] = profile
        return result

    # =========================
    # 4️⃣ CAS SIMPLES
    # =========================
    if "tous les produits" in question_lower or (
        query_type == 'select' and len(question_lower.split()) <= 3
    ):
        print(f"🔍 Règle: Tous les produits (type: {query_type})")
        return result

    # =========================
    # 5️⃣ FILTRES (rating, prix)
    # =========================
    rating_match = re.search(r'rating\s*[>:≥]\s*(\d+(?:\.\d+)?)', question_lower)
    if rating_match:
        rating_value = float(rating_match.group(1))
        result['filter'] = {"rating": {"$gt": rating_value}}
        return result

    price_match = re.search(r'(?:prix|price)\s*[<]\s*(\d+)', question_lower)
    if price_match:
        price_value = float(price_match.group(1))
        result['filter'] = {"discounted_price": {"$lt": price_value}}
        return result

    # =========================
    # 6️⃣ CATÉGORIES
    # =========================
    if "câble" in question_lower or "cable" in question_lower:
        result['filter'] = {"category": {"$regex": "cable", "$options": "i"}}
        if query_type in ['avg', 'sum', 'max', 'min']:
            result['aggregation'] = {
                'field': 'discounted_price',
                'operation': query_type
            }
        return result

    # =========================
    # 7️⃣ LLM (fallback)
    # =========================
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
# file: schema_profiler.py



def explore_schema_mongodb(df, query_type="schema"):
    if query_type == "columns":
        return "columns", list(df.columns), {"num_columns": len(df.columns)}

    profile = {}
    for col in df.columns:
        col_series = df[col]
        col_info = {"type": str(col_series.dtype), "missing": int(col_series.isna().sum())}

        if is_numeric_dtype(col_series):
            col_info.update({
                "min": float(col_series.min()) if not col_series.empty else None,
                "max": float(col_series.max()) if not col_series.empty else None,
                "mean": float(col_series.mean()) if not col_series.empty else None,
                "std": float(col_series.std()) if not col_series.empty else None,
                "unique_values": int(col_series.nunique(dropna=True))
            })
        else:
            # Gestion des listes ou objets non hashables
            try:
                col_info.update({
                    "unique_values": int(col_series.dropna().apply(lambda x: tuple(x) if isinstance(x, list) else x).nunique()),
                    "top_values": col_series.value_counts().head(5).to_dict()
                })
            except TypeError:
                col_info.update({
                    "unique_values": "N/A",
                    "top_values": "N/A"
                })

        profile[col] = col_info

    if query_type == "data_profile":
        return "data_profile", profile, {"num_rows": len(df), "num_columns": len(df.columns)}
    else:  # schema
        return "schema", profile, {"num_rows": len(df), "num_columns": len(df.columns)}





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
    
               # elif query_dict.get("type") in ["schema", "data_profile", "columns"]:
        # # explore_schema_mongodb doit retourner les 3 valeurs : type, results, metadata
        #     result_type, results, metadata = explore_schema_mongodb(df, query_type=query_dict.get("type"))
        #     return result_type, results, metadata
        
        elif query_type == "schema":
            print("🔍 Traitement du schéma")
            schema_data = query_dict.get('schema', {})
            if schema_data:
                # Convertir le schéma en format d'affichage
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
            else:
                # Fallback si pas de schéma dans query_dict
                schema = {}
                for col in df.columns:
                    schema[col] = {
                        "type": str(df[col].dtype),
                        "non_null": int(df[col].notna().sum()),
                        "null": int(df[col].isna().sum())
                    }
                results_list = []
                for col, info in schema.items():
                    results_list.append({
                        'column': col,
                        'type': info.get('type', 'N/A'),
                        'non_null': info.get('non_null', 0),
                        'null': info.get('null', 0),
                        'completeness': f"{(info.get('non_null', 0) / len(df) * 100):.1f}%" if len(df) > 0 else "0%"
                    })
                return 'schema', results_list, {
                    'count': len(df), 
                    'schema': schema,
                    'columns': list(schema.keys()),
                    'num_columns': len(schema)
                }
        
        elif query_type == "data_profile":
            print("📊 Traitement du profil des données")
            profile_data = query_dict.get('profile', {})
            if profile_data:
                return 'data_profile', [], {
                    'count': len(df),
                    'profile': profile_data,
                    'num_rows': profile_data.get('num_rows', len(df)),
                    'num_columns': profile_data.get('num_columns', len(df.columns))
                }
            else:
                # Générer un profil si pas présent dans query_dict
                profile = {
                    "num_rows": len(df),
                    "num_columns": len(df.columns),
                    "columns": {}
                }
                for col in df.columns:
                    col_info = {
                        "type": str(df[col].dtype),
                        "missing": int(df[col].isna().sum())
                    }
                    if is_numeric_dtype(df[col]):
                        col_info.update({
                            "min": float(df[col].min()) if not df[col].empty else None,
                            "max": float(df[col].max()) if not df[col].empty else None,
                            "mean": float(df[col].mean()) if not df[col].empty else None,
                            "std": float(df[col].std()) if not df[col].empty else None,
                            "unique_values": int(df[col].nunique())
                        })
                    else:
                        col_info.update({
                            "unique_values": int(df[col].nunique()),
                            "top_values": df[col].value_counts().head(5).to_dict()
                        })
                    profile["columns"][col] = col_info
                
                return 'data_profile', [], {
                    'count': len(df),
                    'profile': profile,
                    'num_rows': profile.get('num_rows', len(df)),
                    'num_columns': profile.get('num_columns', len(df.columns))
                }
        
        elif query_type == "columns":
            print("📝 Traitement des colonnes")
            columns = query_dict.get('columns', list(df.columns))
            return 'columns', [], {
                'columns': columns,
                'count': len(columns)
            }

        
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
    
    
    
    
if __name__ == "__main__":
    
    tests = [
        "HGETALL user:12345",
        "Que fait: HGETALL user:12345",
        "Explique SMEMBERS category:cable",
        "Explique db.products.find({})"
    ]

    for t in tests:
        print(f"{t} → {detect_query_type(t)}")