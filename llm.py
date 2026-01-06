# llm.py
import json
import pandas as pd
from groq import Groq
import os
import re

# Variables globales qui seront initialisées
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
    
    # Vérifier si les données sont chargées
    if not data:
        print("⚠️ Aucune donnée chargée, création d'un DataFrame vide")
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(data)
        print(f"✅ DataFrame créé avec {len(df)} lignes et {len(df.columns)} colonnes")
    
    # Nettoyer les colonnes numériques seulement si le DataFrame n'est pas vide
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

def generate_mongodb_query(question: str) -> str:
    """
    Génère une requête MongoDB à partir d'une question.
    """
    question_lower = question.lower().strip()
    
    # Règles simples
    if "tous les produits" in question_lower or "tous" in question_lower or "tout" in question_lower:
        print("🔍 Règle: Tous les produits")
        return "{}"
    
    # Rating > X
    rating_match = re.search(r'rating\s*[>:]\s*(\d+(?:\.\d+)?)', question_lower)
    if rating_match:
        rating_value = float(rating_match.group(1))
        query = {"rating": {"$gt": rating_value}}
        print(f"🔍 Règle: Rating > {rating_value}")
        return json.dumps(query, indent=2)
    
    # Rating < X
    rating_match_lt = re.search(r'rating\s*[<]\s*(\d+(?:\.\d+)?)', question_lower)
    if rating_match_lt:
        rating_value = float(rating_match_lt.group(1))
        query = {"rating": {"$lt": rating_value}}
        print(f"🔍 Règle: Rating < {rating_value}")
        return json.dumps(query, indent=2)
    
    # Catégorie spécifique
    if "electronics" in question_lower or "électronique" in question_lower:
        query = {"category": {"$regex": "electronics", "$options": "i"}}
        print("🔍 Règle: Catégorie Electronics")
        return json.dumps(query, indent=2)
    
    if "câble" in question_lower or "cable" in question_lower:
        query = {"category": {"$regex": "cable", "$options": "i"}}
        print("🔍 Règle: Catégorie Cable")
        return json.dumps(query, indent=2)
    
    # Par défaut, utiliser le LLM
    if not groq_available:
        return "{}"
    
    try:
        prompt = f"""
        Convertis en requête MongoDB JSON uniquement:
        
        Question: "{question}"
        
        Schéma: product_id, product_name, category, discounted_price, actual_price, 
                discount_percentage, rating, rating_count, about_product
        
        Exemples:
        - "rating > 4" -> {{"rating": {{"$gt": 4}}}}
        - "câbles iPhone" -> {{"product_name": {{"$regex": "iPhone", "$options": "i"}}}}
        - "entre 500 et 1000" -> {{"$and": [{{"discounted_price": {{"$gt": 500}}}}, {{"discounted_price": {{"$lt": 1000}}}}]}}
        
        Réponse JSON uniquement:
        """
        
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "Tu retournes uniquement du JSON MongoDB, pas d'explications."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=200
        )
        
        query_str = response.choices[0].message.content.strip()
        query_str = query_str.replace('```json', '').replace('```', '').replace('`', '').strip()
        
        try:
            json.loads(query_str)
            print(f"✅ Requête LLM générée: {query_str[:100]}...")
            return query_str
        except:
            print(f"⚠️ Requête LLM invalide, retour à {{}}")
            return "{}"
            
    except Exception as e:
        print(f"❌ Erreur LLM: {e}")
        return "{}"

def apply_filter(dataframe, query):
    """Applique un filtre MongoDB-style sur un DataFrame."""
    if not query:
        return dataframe.copy()
    
    mask = pd.Series([True] * len(dataframe), index=dataframe.index)
    
    for key, value in query.items():
        if key == "$and":
            for sub_query in value:
                sub_mask = apply_filter(dataframe, sub_query).index
                mask = mask & dataframe.index.isin(sub_mask)
        elif isinstance(value, dict) and "$gt" in value:
            mask = mask & (dataframe[key] > value["$gt"])
        elif isinstance(value, dict) and "$lt" in value:
            mask = mask & (dataframe[key] < value["$lt"])
        elif isinstance(value, dict) and "$regex" in value:
            pattern = value["$regex"]
            case = False if ("$options" in value and "i" in value["$options"]) else True
            mask = mask & dataframe[key].astype(str).str.contains(pattern, case=case, na=False)
        else:
            mask = mask & (dataframe[key] == value)
    
    return dataframe[mask]

def execute_mongodb_query(query_json: str):
    """
    Exécute une requête MongoDB sur le DataFrame.
    """
    global df
    
    if df.empty:
        print("⚠️ DataFrame vide")
        return []
    
    try:
        # Convertir JSON en dict
        query_dict = json.loads(query_json) if query_json and query_json != "{}" else {}
        
        if not query_dict:
            # Tous les produits
            filtered_df = df.copy()
            print(f"✅ Tous les produits: {len(filtered_df)}")
        else:
            # Appliquer le filtre
            filtered_df = apply_filter(df, query_dict)
            print(f"✅ Produits filtrés: {len(filtered_df)}")
        
        # Préparer les résultats (limiter à 20 pour l'affichage)
        results_df = filtered_df.head(20).copy()
        
        # Formater les colonnes
        results = []
        for _, row in results_df.iterrows():
            product = {
                'product_name': str(row.get('product_name', 'N/A'))[:80] + ("..." if len(str(row.get('product_name', ''))) > 80 else ""),
                'category': str(row.get('category', 'N/A')).split('|')[-1][:40],
                'rating': f"⭐{row['rating']:.1f}" if pd.notna(row.get('rating')) else "N/A",
                'discounted_price': f"₹{row['discounted_price']:.2f}" if pd.notna(row.get('discounted_price')) else "N/A"
            }
            results.append(product)
        
        return results
        
    except Exception as e:
        print(f"❌ Erreur d'exécution: {e}")
        return []