from groq import Groq
from sentence_transformers import SentenceTransformer, util
from typing import Dict, List, Any, Tuple
import os 
import re
import json


# ============================================================================
# NOUVELLE FONCTIONNALITÉ : DÉTECTION ET EXPLICATION DE REQUÊTES
# ============================================================================
def extract_redis_command(text: str) -> str | None:
    """
    Extrait une commande Redis même si elle est incluse dans une phrase
    Exemple:
    - "Que fait HGETALL user:123 ?" -> "HGETALL user:123"
    """
    redis_cmd_pattern = (
        r'\b(GET|SET|DEL|EXISTS|INCR|DECR|'
        r'HGET|HSET|HGETALL|HMGET|HDEL|'
        r'LPUSH|RPUSH|LRANGE|'
        r'SADD|SMEMBERS|SCARD|'
        r'ZADD|ZRANGE|ZREVRANGE|'
        r'EXPIRE|TTL)\b.*'
    )

    match = re.search(redis_cmd_pattern, text, re.IGNORECASE)
    if match:
        return match.group(0).strip()

    return None





def init_groq_client():
    """Initialise le client Groq"""
    global client, groq_available
    
    try:
        api_key = os.getenv("GROQ_API_KEY")
        
        if not api_key or api_key == "votre_clé_api_groq_ici":
            print("⚠️ GROQ_API_KEY non configurée ou invalide")
            print("💡 Conseil: Ajoutez GROQ_API_KEY=votre_clé_réelle dans le fichier .env")
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



# Charger le modèle
model = SentenceTransformer('all-MiniLM-L6-v2')  # petit et rapide

# Liste des préfixes NL
prefixes = [
    "analyse:",
    "explique:",
    "explique",
    "que fait:",
    "que fait",
    "analyze:",
    "explain:",
]

# Encoder les préfixes
prefix_embeddings = model.encode(prefixes, convert_to_tensor=True)

def normalize_nl_prefix(query: str) -> str:
    """
    Supprime les préfixes NL (Analyse:, Explique:, etc.)
    Détection robuste et déterministe
    """
    query = query.strip()
    q_lower = query.lower()

    for prefix in prefixes:
        if q_lower.startswith(prefix):
            return query[len(prefix):].strip()

    return query



def preprocess_query(query: str) -> str:
    # 1️⃣ Nettoyer le langage naturel
    query = normalize_nl_prefix(query)

    # 2️⃣ Extraire une commande Redis si présente
    redis_cmd = extract_redis_command(query)
    if redis_cmd:
        return redis_cmd

    return query






# ============================================================================
# DÉTECTION DU LANGAGE DE BASE DE DONNÉES
# ============================================================================

def detect_database_language(query: str) -> str:
    query = preprocess_query(query).strip()
    
    # 🔴 Redis - Commencer par les patterns les plus spécifiques
    redis_patterns = [
        r'^(GET|SET|DEL|EXISTS|INCR|DECR)\b',
        r'^(HGET|HSET|HGETALL|HMGET|HDEL)\b',
        r'^(LPUSH|RPUSH|LRANGE)\b',
        r'^(SADD|SMEMBERS|SCARD)\b',
        r'^(ZADD|ZRANGE|ZREVRANGE)\b',
        r'^(EXPIRE|TTL)\b',
        r'^\s*(KEYS|SCAN|INFO|CLIENT|AUTH)\b'  # Ajout d'autres commandes Redis
    ]

    if any(re.search(p, query, re.IGNORECASE) for p in redis_patterns):
        return 'redis'

    # 🗄️ HBase - Mettre avant MongoDB et Neo4j pour éviter les conflits
    hbase_patterns = [
        r'^\s*scan\s+\'',        # scan 'table'
        r'^\s*get\s+\'',         # get 'table'
        r'^\s*put\s+\'',         # put 'table'
        r'^\s*delete\s+\'',      # delete 'table'
        r'^\s*count\s+\'',       # count 'table'
        r'^\s*create\s+\'',      # create 'table'
        r'^\s*disable\s+\'',     # disable 'table'
        r'^\s*enable\s+\'',      # enable 'table'
        r'^\s*drop\s+\'',        # drop 'table'
        r'ColumnFamily\:',       # ColumnFamily:
        r'\bRowKey\b',           # RowKey
        r'\bFILTER\s*=>',        # FILTER =>
        r'\bValueFilter\b',      # ValueFilter
        r'\bColumnPrefixFilter\b', # ColumnPrefixFilter
        r'\bSingleColumnValueFilter\b', # SingleColumnValueFilter
        r'\bQualifierFilter\b',  # QualifierFilter
        r'\bRowFilter\b',        # RowFilter
        r'\{\s*FILTER\s*=>',     # { FILTER =>
        r'\}\s*$'                # Se termine par }
    ]

    if any(re.search(p, query, re.IGNORECASE) for p in hbase_patterns):
        return 'hbase'

    # 🍃 MongoDB
    mongodb_patterns = [
        r'\.find\(', r'\.aggregate\(', r'\$match', r'\$group',
        r'\$regex', r'\$gt', r'\$lt', r'db\.', r'\.insert',
        r'\.update', r'\.delete', r'\$project', r'\$sort',
        r'\$limit', r'\$skip', r'\$unwind', r'\.distinct\('
    ]

    if any(re.search(p, query, re.IGNORECASE) for p in mongodb_patterns):
        return 'mongodb'

    # 🔵 Neo4j - Plus spécifique pour éviter les faux positifs
    neo4j_patterns = [
    r'\bMATCH\s*\(',
    r'\bCREATE\s*\(',
    r'\bMERGE\s*\(',
    r'\bRETURN\s+\w',
    r'\bWHERE\s+',
    r'\bSET\s+\w+\s*=',
    r'\bDELETE\s+\w',
    r'\bDETACH\s+DELETE',
    r'\bOPTIONAL\s+MATCH',
    r'\bWITH\s+\w',
    r'\bUNWIND\s+',
    r'\bORDER\s+BY',
    r'\bLIMIT\s+\d+',
    r'\-\s*\[\s*:\s*\w+\s*\]\s*\-\>',
    r'\<\-\s*\[\s*:\s*\w+\s*\]\s*\-',
    r'\bAS\b\s+\w+'
    ]


    # Vérifier si c'est vraiment Neo4j et pas un faux positif
    has_neo4j_pattern = any(re.search(p, query, re.IGNORECASE) for p in neo4j_patterns)
    
    if has_neo4j_pattern:
        # Exclure les faux positifs courants
        false_positives = [
            r'^\s*scan\s+\'',            # scan 'table' (HBase)
            r'^\s*count\s+\'',           # count 'table' (HBase)
            r'^\s*get\s+\'',             # get 'table' (HBase)
            r'^\s*SET\s+\w+\s+\'',       # SET key 'value' (Redis)
            r'^\s*GET\s+\w+$',           # GET key (Redis)
            r'^\s*DEL\s+\w+$',           # DEL key (Redis)
            r'db\.\w+\.',                # db.collection. (MongoDB)
            r'\$match\b',                # $match (MongoDB)
            r'\$group\b',                # $group (MongoDB)
        ]
        
        is_false_positive = any(re.search(p, query, re.IGNORECASE) for p in false_positives)
        
        if not is_false_positive:
            return 'neo4j'

    # 🟦 SQL
    sql_patterns = [
        r'^\s*SELECT\b.*\bFROM\b',
        r'^\s*INSERT\s+INTO\b',
        r'^\s*UPDATE\s+\w+\s+SET\b',
        r'^\s*DELETE\s+FROM\b',
        r'^\s*CREATE\s+TABLE\b',
        r'^\s*ALTER\s+TABLE\b',
        r'^\s*DROP\s+TABLE\b',
        r'\bJOIN\b.*\bON\b',
        r'\bWHERE\b.*\b=\b',
        r'\bGROUP\s+BY\b',
        r'\bORDER\s+BY\b',
        r'\bHAVING\b',
        r'\bUNION\b',
        r'\bVALUES\b',
        r'^\s*TRUNCATE\s+TABLE\b',
        r'\bINNER\s+JOIN\b',
        r'\bLEFT\s+JOIN\b',
        r'\bRIGHT\s+JOIN\b',
        r'\bFULL\s+JOIN\b'
    ]

    if any(re.search(p, query, re.IGNORECASE) for p in sql_patterns):
        return 'sql'

    return 'unknown'



# ============================================================================
# EXPLICATION VIA LLM
# ============================================================================

def explain_query_with_llm(query: str, db_language: str) -> Dict[str, Any]:
    """
    Utilise le LLM pour expliquer une requête de base de données
    """
    if not groq_available:
        return {
            'error': 'LLM non disponible',
            'message': 'Veuillez configurer GROQ_API_KEY'
        }
    
    # Contexte spécifique selon le langage
    context_map = {
        'mongodb': """
MongoDB utilise un modèle de documents JSON/BSON.
Opérateurs courants: $match (filtrage), $group (agrégation), $project (sélection de champs),
$sort (tri), $limit (limitation), $gt/$lt (comparaisons), $regex (expressions régulières).
""",
        'redis': """
Redis est une base de données clé-valeur en mémoire.
Commandes courantes: GET/SET (strings), HGET/HSET (hashes), LPUSH/RPUSH (listes),
SADD (sets), ZADD (sorted sets), EXPIRE (expiration), INCR/DECR (compteurs).
""",
        'hbase': """
HBase est une base de données NoSQL orientée colonnes sur Hadoop.
Structure: RowKey -> ColumnFamily:Qualifier -> Value + Timestamp.
Opérations: get (lecture), scan (parcours), put (écriture), delete (suppression).
""",
        'neo4j': """
Neo4j utilise le langage Cypher pour les graphes.
Concepts: Nodes (nœuds), Relationships (relations), Properties (propriétés).
Clauses: MATCH (recherche), CREATE (création), MERGE (fusion), WHERE (filtrage), RETURN (résultats).
""",
        'sql': """
SQL est le langage standard pour les bases relationnelles.
Clauses: SELECT (sélection), FROM (source), WHERE (filtrage), JOIN (jointures),
GROUP BY (groupement), ORDER BY (tri), HAVING (filtrage post-agrégation).
"""
    }
    
    context = context_map.get(db_language, "Base de données générique")
    
    prompt = f"""
Tu es un expert en bases de données. Analyse et explique cette requête {db_language.upper()}.

CONTEXTE:
{context}

REQUÊTE À ANALYSER:
{query}

INSTRUCTIONS:
1. **Langage détecté**: Confirme le langage (MongoDB, Redis, HBase, Neo4j, SQL)
2. **Objectif**: Explique ce que fait cette requête en langage simple
3. **Décomposition**: Détaille chaque partie de la requête
4. **Résultat attendu**: Décris le type de résultat retourné
5. **Optimisation**: Suggère des améliorations si possible

Réponds en JSON avec cette structure:
{{
  "language": "{db_language}",
  "objective": "Description courte de l'objectif",
  "breakdown": [
    {{"step": "Étape 1", "explanation": "Explication détaillée"}},
    {{"step": "Étape 2", "explanation": "Explication détaillée"}}
  ],
  "expected_result": "Description du résultat",
  "optimization_tips": ["Conseil 1", "Conseil 2"],
  "human_readable": "Traduction en langage naturel de la requête"
}}
"""
    
    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "Tu es un expert en bases de données. Réponds uniquement en JSON valide."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=1500
        )
        
        explanation_str = response.choices[0].message.content.strip()
        explanation_str = explanation_str.replace('```json', '').replace('```', '').strip()
        
        explanation = json.loads(explanation_str)
        return explanation
        
    except json.JSONDecodeError as e:
        print(f"❌ Erreur de parsing JSON: {e}")
        return {
            'error': 'Parsing JSON échoué',
            'raw_response': explanation_str[:500]
        }
    except Exception as e:
        print(f"❌ Erreur LLM: {e}")
        return {'error': str(e)}


def analyze_query(query: str) -> Dict[str, Any]:
    """
    Point d'entrée principal pour analyser une requête de base de données
    
    Args:
        query: La requête à analyser (peut être MongoDB, Redis, HBase, Neo4j, SQL)
    
    Returns:
        Dictionnaire contenant l'analyse complète de la requête
    """
    # 1. Détection du langage
    db_language = detect_database_language(query)
    
    print(f"🔍 Langage détecté: {db_language.upper()}")
    
    if db_language == 'unknown':
        return {
            'status': 'error',
            'message': 'Impossible de détecter le langage de la requête',
            'suggestion': 'Vérifiez la syntaxe ou précisez le type de base de données'
        }
    
    # 2. Explication avec le LLM
    explanation = explain_query_with_llm(query, db_language)
    
    # 3. Retourner le résultat complet
    return {
        'status': 'success',
        'detected_language': db_language,
        'original_query': query,
        'explanation': explanation
    }


def format_explanation_output(analysis: Dict[str, Any]) -> str:
    """
    Formate l'analyse pour un affichage lisible
    """
    if analysis.get('status') == 'error':
        return f"❌ {analysis.get('message', 'Erreur inconnue')}"
    
    explanation = analysis.get('explanation', {})
    
    output = f"""
{'='*80}
🔍 ANALYSE DE REQUÊTE - {analysis['detected_language'].upper()}
{'='*80}

📝 REQUÊTE ORIGINALE:
{analysis['original_query']}

{'='*80}
🎯 OBJECTIF:
{explanation.get('objective', 'N/A')}

{'='*80}
🔨 DÉCOMPOSITION:
"""
    
    for i, step in enumerate(explanation.get('breakdown', []), 1):
        output += f"\n{i}. {step.get('step', 'Étape')}\n"
        output += f"   → {step.get('explanation', 'N/A')}\n"
    
    output += f"""
{'='*80}
📊 RÉSULTAT ATTENDU:
{explanation.get('expected_result', 'N/A')}

{'='*80}
💡 TRADUCTION EN LANGAGE NATUREL:
{explanation.get('human_readable', 'N/A')}

{'='*80}
⚡ CONSEILS D'OPTIMISATION:
"""
    
    for i, tip in enumerate(explanation.get('optimization_tips', []), 1):
        output += f"{i}. {tip}\n"
    
    output += f"{'='*80}\n"
    
    return output


# ============================================================================
# FONCTIONS ORIGINALES (NL → Query)
# ============================================================================

def detect_query_type(question: str) -> str:
    q = question.lower()
    
    # 🔴 D'ABORD vérifier les types de données (priorité)
    # Types de données
    if any(w in q for w in ["type des données", "types des données", "dtype", "schéma", "schema","type", "types"]):
        return "schema"

    # Profil / description complète
    if any(w in q for w in ["information", "informations", "description", "résumé", "profil", "profilage"]):
        return "data_profile"

    # Colonnes uniquement
    if any(w in q for w in ["colonnes", "champs", "attributs", "noms des colonnes"]):
        return "columns"

    # 🆕 ENSUITE vérifier les commandes Redis
    redis_cmd = extract_redis_command(question)
    if redis_cmd:
        return "convert_nosql"
    
    # 🆕 ENSUITE vérifier si c'est une commande de base de données explicite
    db_language = detect_database_language(question)
    if db_language != 'unknown':
        return "convert_nosql"
    
    # Troisième étape : Mots-clés indiquant une demande d'explication
    explain_keywords = [
        "explique", "explain", "que fait", "qu'est-ce que fait", 
        "analyse", "analyze", "décris", "describe",
        "comment fonctionne", "signifie", "veut dire",
        "c'est quoi", "qu'est-ce que c'est", "que fait"
    ]
    
    # Vérifier si la question contient un mot d'explication
    has_explain_keyword = any(keyword in q for keyword in explain_keywords)
    
    if has_explain_keyword:
        # Si c'est une question d'explication, on va analyser avec le LLM
        return "convert_nosql"

    # Groupement
    if any(w in q for w in ["grouper", "group by", "par catégorie", "par type", "par prix"]):
        return "group"

    # Agrégations
    if any(w in q for w in ["moyenne", "moyen", "average", "avg"]):
        return "avg"

    if any(w in q for w in ["combien", "nombre", "count", "total"]):
        return "count"

    if any(w in q for w in ["somme", "sum", "addition"]):
        return "sum"

    if any(w in q for w in ["maximum", "max", "plus élevé", "plus cher"]):
        return "max"

    if any(w in q for w in ["minimum", "min", "moins cher", "plus bas"]):
        return "min"

    # Sélection par défaut
    return "select"