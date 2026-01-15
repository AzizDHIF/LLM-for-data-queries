import google.generativeai as genai
from sentence_transformers import SentenceTransformer, util
from typing import Dict, List, Any, Tuple
import os 
import re
import json
from connectors.api import load_gemini_config

config = load_gemini_config()
API_KEY = config["api_key"]
MODEL = config.get("model", "gemini-2.5-pro")


CURRENT_CRUD_CONTEXT = {
    "operation": None,
    "params": None
}

def handle_crud_continuation(question: str) -> Dict[str, Any] | None:
    """
    Gère la continuité d'une opération CRUD incomplète
    """
    global CURRENT_CRUD_CONTEXT

    if not CURRENT_CRUD_CONTEXT["operation"]:
        return None

    operation = CURRENT_CRUD_CONTEXT["operation"]
    params = CURRENT_CRUD_CONTEXT["params"]
    
    # Extraire les nouveaux champs depuis la réponse utilisateur
    new_params = extract_crud_params(question, operation)

    # Fusion intelligente
    params["data"].update(new_params.get("data", {}))
    params["filter"].update(new_params.get("filter", {}))
    params["fields_to_update"].update(new_params.get("fields_to_update", {}))

    missing = detect_missing_crud_fields(operation, params)

    if missing:
        return {
            "type": "crud_incomplete",
            "operation": operation,
            "params": params,
            "prompt": generate_crud_prompt(operation, missing)
        }

    # Validation finale
    valid, error = validate_crud_data(operation, params)
    if not valid:
        return {"type": "error", "message": error}

    # Génération finale
    queries = generate_crud_queries(operation, params)

    CURRENT_CRUD_CONTEXT = {"operation": None, "params": None}

    return {
        "type": "crud_complete",
        "operation": operation,
        "params": params,
        "queries": queries
    }


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


# Fonction pour charger la configuration Gemini
def load_gemini_config():
    """Charge la configuration Gemini depuis le fichier de configuration"""
    # Ici vous devriez implémenter votre propre logique de chargement de configuration
    # Par exemple, depuis un fichier JSON ou YAML
    # Pour l'exemple, je retourne une configuration par défaut
    return {
        "api_key": os.getenv("GEMINI_API_KEY", ""),
        "model": "gemini-1.5-pro"  # Utilisez "gemini-2.5-pro" quand disponible
    }



# Charger le modèle
try:
    model = SentenceTransformer(
        'all-MiniLM-L6-v2',
        cache_folder="./models",
        local_files_only=True
    )
    print("✅ SentenceTransformer chargé depuis le cache local")
except Exception:
    print("⚠️ SentenceTransformer indisponible (mode fallback)")
    model = None
    
    
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
prefix_embeddings = (
    model.encode(prefixes, convert_to_tensor=True)
    if model else None
)

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
# EXPLICATION VIA LLM (AVEC GEMINI)
# ============================================================================

def explain_query_with_llm(query: str, db_language: str) -> Dict[str, Any]:
    """
    Utilise Gemini pour expliquer une requête de base de données
    """
    global gemini_client, gemini_available
    
    from google.genai import Client
    config = load_gemini_config()
    API_KEY = config["api_key"]
    MODEL = config.get("model", "gemini-2.5-pro")
    client = Client(api_key=API_KEY)
    
    if not gemini_available:
        return {
            'error': 'Gemini non disponible',
            'message': 'Veuillez configurer correctement l\'API Gemini'
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

Réponds UNIQUEMENT en JSON avec cette structure exacte:
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

Ne retourne rien d'autre que le JSON.
"""
    
    try:
        # Utiliser Gemini au lieu de Groq
        response = client.models.generate_content(prompt)
        
        if not response or not response.text:
            raise Exception("Réponse vide de Gemini")
        
        explanation_str = response.text.strip()
        
        # Nettoyer la réponse (retirer les backticks de code si présents)
        explanation_str = explanation_str.replace('```json', '').replace('```', '').strip()
        
        # Parser le JSON
        explanation = json.loads(explanation_str)
        return explanation
        
    except json.JSONDecodeError as e:
        print(f"❌ Erreur de parsing JSON: {e}")
        print(f"Réponse brute: {explanation_str[:500]}")
        return {
            'error': 'Parsing JSON échoué',
            'raw_response': explanation_str[:500] if 'explanation_str' in locals() else 'Pas de réponse'
        }
    except Exception as e:
        print(f"❌ Erreur Gemini: {e}")
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
    
    # 2. Explication avec Gemini
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
    
    # Vérifier si l'explication contient une erreur
    if 'error' in explanation:
        return f"❌ Erreur d'explication: {explanation.get('error')}"
    
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


# 🆕 NOUVELLE FONCTION : Détection des champs manquants
def detect_missing_crud_fields(operation: str, params: Dict[str, Any]) -> List[str]:
    missing = []

    if operation == 'create':
        
        data = params.get('data', {})
        required_fields = ['name', 'price']  # Champs obligatoires minimaux
        
      
        missing_fields = []
        for field in required_fields:
            # ignorer les champs optionnels
            if "(optionnel)" in field:
                continue
            # vérifier les champs obligatoires
            clean_field = field.split("(")[0].strip()
            if clean_field not in params or not params[clean_field]:
                missing_fields.append(clean_field)
        
        # Suggérer d'autres champs optionnels
        optional_fields = ['rating', 'category', 'description']
        for field in optional_fields:
            if field not in data:
                missing.append(f"{field} (optionnel)")
    
    elif operation == 'update':
        filter_q = params.get('filter', {})
        fields_to_update = params.get('fields_to_update', {})
        
        if not filter_q:
            missing.append("filtre (quel document modifier ?)")
        
        if not fields_to_update:
            missing.append("champs à modifier")
    
    elif operation == 'delete':
        filter_q = params.get('filter', {})
        
        if not filter_q:
            missing.append("filtre (quel document supprimer ?)")
    
    return missing


# 🆕 NOUVELLE FONCTION : Génération de réponse conversationnelle
def generate_crud_prompt(operation: str, missing_fields: List[str]) -> str:
    """
    Génère un prompt pour demander les informations manquantes
    """
    prompts = {
        'create': {
            'intro': "🆕 Je vais vous aider à créer un nouveau produit.",
            'fields': {
                'name': "📝 Nom du produit",
                'price': "💰 Prix (en roupies)",
                'rating': "⭐ Note (0-5)",
                'category': "📁 Catégorie",
                'description': "📄 Description"
            },
            'example': """
Exemple :
Créer un produit avec nom="Clavier Mécanique", prix=89.99, rating=4.5, catégorie="Accessoires"
"""
        },
        'update': {
            'intro': "✏️ Je vais vous aider à mettre à jour un produit.",
            'fields': {
                'id': "🔑 ID du produit à modifier",
                'name': "📝 Nouveau nom (optionnel)",
                'price': "💰 Nouveau prix (optionnel)",
                'rating': "⭐ Nouvelle note (optionnel)"
            },
            'example': """
Exemple :
Modifier le produit avec id=123, nouveau prix=199, nouveau rating=5
"""
        },
        'delete': {
            'intro': "🗑️ Je vais vous aider à supprimer un ou plusieurs produits.",
            'fields': {
                'id': "🔑 ID du produit à supprimer",
                'condition': "🔍 Ou une condition (ex: rating < 2)"
            },
            'example': """
Exemples :
- Supprimer le produit avec id=123
- Supprimer les produits avec rating < 2
"""
        }
    }
    
    config = prompts.get(operation, {})
    intro = config.get('intro', f"Opération {operation}")
    fields_info = config.get('fields', {})
    example = config.get('example', '')
    
    # Construire le message
    message_parts = [intro, "\n\n📋 **Informations requises :**\n"]
    
    # Lister les champs manquants
    for field in missing_fields:
        # Extraire le nom du champ (sans "(optionnel)")
        field_name = field.replace(" (optionnel)", "")
        field_label = fields_info.get(field_name, f"• {field}")
        is_optional = "(optionnel)" in field
        
        if is_optional:
            message_parts.append(f"{field_label} _(optionnel)_")
        else:
            message_parts.append(f"{field_label} **[REQUIS]**")
    
    message_parts.append(example)
    
    return "\n".join(message_parts)


# 🆕 NOUVELLE FONCTION : Validation des données CRUD
def validate_crud_data(operation: str, params: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Valide les données pour une opération CRUD
    Retourne: (est_valide, message_erreur)
    """
    if operation == 'create':
        data = params.get('data', {})
        
        # Vérifier les champs obligatoires
        if 'name' not in data or not data['name']:
            return False, "❌ Le nom du produit est obligatoire"
        
        if 'price' in data:
            try:
                price = float(data['price'])
                if price < 0:
                    return False, "❌ Le prix ne peut pas être négatif"
            except (ValueError, TypeError):
                return False, "❌ Le prix doit être un nombre"
        
        if 'rating' in data:
            try:
                rating = float(data['rating'])
                if rating < 0 or rating > 5:
                    return False, "❌ La note doit être entre 0 et 5"
            except (ValueError, TypeError):
                return False, "❌ La note doit être un nombre"
        
        return True, ""
    
    elif operation == 'update':
        filter_q = params.get('filter', {})
        fields_to_update = params.get('fields_to_update', {})
        
        if not filter_q:
            return False, "❌ Vous devez spécifier quel document modifier (id ou condition)"
        
        if not fields_to_update:
            return False, "❌ Vous devez spécifier au moins un champ à modifier"
        
        return True, ""
    
    elif operation == 'delete':
        filter_q = params.get('filter', {})

        if not filter_q:
            return False, "❌ Vous devez spécifier quel(s) document(s) supprimer"

        return True, ""

        
# ============================================================================
# FONCTIONS ORIGINALES (NL → Query)
# ============================================================================

# Ajouter dans classifier.py

def detect_query_type(question: str) -> str:
    """
    Détecte le type de requête en langage naturel
    CORRECTION : Amélioré pour détecter les combinaisons complexes
    """
    q = question.lower()
    
    # 🆕 DÉTECTION DES REQUÊTES COMPLEXES (COUNT + FILTRE)
    # Exemple: "le nombre produits nom contient 'TV'"
    if re.search(r'nombre.*produits.*nom.*contient', q) or \
       re.search(r'combien.*produits.*nom.*contient', q) or \
       re.search(r'count.*products.*name.*contains', q, re.IGNORECASE):
        return "count"  # C'est un comptage avec filtre
    
    # 🆕 DÉTECTION DES REQUÊTES AVEC FILTRE TEXTE
    if re.search(r'produits?.*nom.*contient', q) or \
       re.search(r'products?.*name.*contains', q, re.IGNORECASE):
        return "select"  # Sélection avec filtre texte
    
    # 🆕 DÉTECTION DES REQUÊTES AVEC RATING FILTRE
    if re.search(r'rating.*[><=]+.*\d', q) or \
       re.search(r'note.*[><=]+.*\d', q):
        return "select"  # Sélection avec filtre numérique
    
    # UPDATE / MODIFY en priorité
    update_keywords = [
        "mettre à jour", "mettre a jour", "update",
        "modifier", "modifie", "modify",
        "changer", "change",
        "éditer", "editer", "edit",
        "remplacer", "remplace", "replace"
    ]
    if any(w in q for w in update_keywords):
        return "update"
    
    # CREATE / INSERT
    create_keywords = [
        "créer", "create", "crée",
        "insérer", "inserer", "insert", "insère", "insere",
        "ajouter", "ajoute", "add",
        "nouveau", "nouvelle", "new",
        "enregistrer", "enregistre", "save",
        "je veux créer", "je veux insérer", "je veux ajouter"
    ]
    if any(w in q for w in create_keywords):
        return "create"
    
    # DELETE / REMOVE
    delete_keywords = [
        "supprimer", "supprime", "delete",
        "effacer", "efface", "remove",
        "retirer", "retire", "drop"
    ]
    if any(w in q for w in delete_keywords):
        return "delete"
    
    # Types de données (priorité)
    if any(w in q for w in ["type des données", "types des données", "dtype", "schéma", "schema","type", "types"]):
        return "schema"

    # Profil / description complète
    if any(w in q for w in ["information", "informations", "description", "résumé", "profil", "profilage"]):
        return "data_profile"

    # Colonnes uniquement
    if any(w in q for w in ["colonnes", "champs", "attributs", "noms des colonnes"]):
        return "columns"

    # Vérifier les commandes Redis
    redis_cmd = extract_redis_command(question)
    if redis_cmd:
        return "convert_nosql"
    
    # Vérifier si c'est une commande de base de données explicite
    db_language = detect_database_language(question)
    if db_language != 'unknown':
        return "convert_nosql"
    
    # Mots-clés indiquant une demande d'explication
    explain_keywords = [
        "explique", "explain", "que fait", "qu'est-ce que fait", 
        "analyse", "analyze", "décris", "describe",
        "comment fonctionne", "signifie", "veut dire",
        "c'est quoi", "qu'est-ce que c'est", "que fait"
    ]
    
    has_explain_keyword = any(keyword in q for keyword in explain_keywords)
    
    if has_explain_keyword:
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

    # Sélection par défaut (READ)
    return "select"


def extract_crud_params(question: str, operation: str) -> Dict[str, Any]:
    """
    Extrait les paramètres pour les opérations CRUD depuis la question
    """
    params = {
        'collection': 'products',  # par défaut
        'data': {},
        'filter': {},
        'fields_to_update': {}
    }
    
    # Détecter la collection/table
    collections = ['product', 'user', 'order', 'category']
    for coll in collections:
        if coll in question.lower():
            params['collection'] = coll + 's'
            break
    
    if operation == 'create':
        # Extraire les données à insérer
        # Pattern: "créer un produit avec nom='X', prix=100, rating=4.5"
        
        # Nom/titre
        name_match = re.search(r'nom[=:\s]+["\']?([^"\',.]+)["\']?', question, re.IGNORECASE)
        if name_match:
            params['data']['name'] = name_match.group(1).strip()
        
        # Prix
        price_match = re.search(r'prix[=:\s]+(\d+(?:\.\d+)?)', question, re.IGNORECASE)
        if price_match:
            params['data']['price'] = float(price_match.group(1))
        
        # Rating
        rating_match = re.search(r'rating[=:\s]+(\d+(?:\.\d+)?)', question, re.IGNORECASE)
        if rating_match:
            params['data']['rating'] = float(rating_match.group(1))
        
        # Catégorie
        category_match = re.search(r'catégorie[=:\s]+["\']?([^"\',.]+)["\']?', question, re.IGNORECASE)
        if category_match:
            params['data']['category'] = category_match.group(1).strip()
            
        description_match = re.search(r'description[=:\s]+["\']?([^"\']+)["\']?', question, re.IGNORECASE)
        if description_match:
            params['data']['description'] = description_match.group(1).strip()
    
    elif operation == 'update':
        # Extraire le filtre (quel document modifier)
        id_match = re.search(r'id[=:\s]+["\']?([^"\',.]+)["\']?', question, re.IGNORECASE)
        if id_match:
            params['filter']['_id'] = id_match.group(1).strip()
        
        name_match = re.search(r'nom[=:\s]+["\']?([^"\',.]+)["\']?', question, re.IGNORECASE)
        if name_match and 'avec nom' in question.lower():
            params['filter']['name'] = name_match.group(1).strip()
        
        # Extraire les champs à mettre à jour
        # Pattern: "modifier prix=200, rating=5"
        updates = re.findall(
            r'(prix|rating|nom|catégorie|description)[=:\s]+["\']?([^"\',.]+)["\']?', 
            question, re.IGNORECASE
        )
        field_map = {
            'prix': 'price',
            'rating': 'rating',
            'nom': 'name',
            'catégorie': 'category',
            'description': 'description'   # ← important
        }
        for field, value in updates:
            mapped_field = field_map.get(field.lower(), field)
            try:
                params['fields_to_update'][mapped_field] = float(value)
            except ValueError:
                params['fields_to_update'][mapped_field] = value
        
    
    elif operation == 'delete':
        # Extraire le filtre
        id_match = re.search(r'id[=:\s]+["\']?([^"\',.]+)["\']?', question, re.IGNORECASE)
        if id_match:
            params['filter']['_id'] = id_match.group(1).strip()
        
        name_match = re.search(r'nom[=:\s]+["\']?([^"\',.]+)["\']?', question, re.IGNORECASE)
        if name_match:
            params['filter']['name'] = name_match.group(1).strip()
        
        # Conditions
        rating_match = re.search(r'rating\s*[<>]=?\s*(\d+(?:\.\d+)?)', question)
        if rating_match:
            operator = '<' if '<' in question else '>'
            value = float(rating_match.group(1))
            params['filter']['rating'] = {'$lt' if operator == '<' else '$gt': value}
    
    return params


def generate_crud_queries(operation: str, params: Dict[str, Any]) -> Dict[str, str]:
    """
    Génère les requêtes CRUD pour toutes les bases de données
    """
    collection = params['collection']
    data = params['data']
    filter_q = params['filter']
    fields_to_update = params['fields_to_update']
    
    queries = {}
    
    # ============================================================================
    # MONGODB
    # ============================================================================
    if operation == 'create':
        queries['mongodb'] = f"db.{collection}.insertOne({json.dumps(data, indent=2)})"
    
    elif operation == 'update':
        queries['mongodb'] = f"""db.{collection}.updateOne(
  {json.dumps(filter_q, indent=2)},
  {{ $set: {json.dumps(fields_to_update, indent=2)} }}
)"""
    
    elif operation == 'delete':
        queries['mongodb'] = f"db.{collection}.deleteOne({json.dumps(filter_q, indent=2)})"
    
    # ============================================================================
    # REDIS
    # ============================================================================
    if operation == 'create':
        # Stocker comme hash
        hash_commands = []
        doc_id = data.get('_id', data.get('id', 'new_id'))
        for key, value in data.items():
            hash_commands.append(f"HSET {collection}:{doc_id} {key} \"{value}\"")
        hash_commands.append(f"SADD {collection}:all {doc_id}")
        queries['redis'] = "\n".join(hash_commands)
    
    elif operation == 'update':
        doc_id = filter_q.get('_id', filter_q.get('id', 'unknown'))
        update_commands = []
        for key, value in fields_to_update.items():
            update_commands.append(f"HSET {collection}:{doc_id} {key} \"{value}\"")
        queries['redis'] = "\n".join(update_commands)
    
    elif operation == 'delete':
        doc_id = filter_q.get('_id', filter_q.get('id', 'unknown'))
        queries['redis'] = f"""DEL {collection}:{doc_id}
SREM {collection}:all {doc_id}"""
    
    # ============================================================================
    # HBASE
    # ============================================================================
    if operation == 'create':
        row_key = data.get('_id', data.get('id', 'row_key'))
        put_commands = []
        for key, value in data.items():
            if key not in ['_id', 'id']:
                put_commands.append(f"put '{collection}', '{row_key}', 'data:{key}', '{value}'")
        queries['hbase'] = "\n".join(put_commands)
    
    elif operation == 'update':
        row_key = filter_q.get('_id', filter_q.get('id', 'row_key'))
        put_commands = []
        for key, value in fields_to_update.items():
            put_commands.append(f"put '{collection}', '{row_key}', 'data:{key}', '{value}'")
        queries['hbase'] = "\n".join(put_commands)
    
    elif operation == 'delete':
        row_key = filter_q.get('_id', filter_q.get('id', 'row_key'))
        queries['hbase'] = f"delete '{collection}', '{row_key}'"
    
    # ============================================================================
    # NEO4J
    # ============================================================================
    entity = collection[:-1].capitalize()  # products -> Product
    
    if operation == 'create':
        props = ', '.join([f"{k}: \"{v}\"" if isinstance(v, str) else f"{k}: {v}" 
                          for k, v in data.items()])
        queries['neo4j'] = f"CREATE (n:{entity} {{{props}}}) RETURN n"
    
    elif operation == 'update':
        # Construire WHERE
        where_parts = []
        for key, value in filter_q.items():
            if isinstance(value, str):
                where_parts.append(f"n.{key} = \"{value}\"")
            else:
                where_parts.append(f"n.{key} = {value}")
        where_clause = " AND ".join(where_parts) if where_parts else "true"
        
        # Construire SET
        set_parts = []
        for key, value in fields_to_update.items():
            if isinstance(value, str):
                set_parts.append(f"n.{key} = \"{value}\"")
            else:
                set_parts.append(f"n.{key} = {value}")
        set_clause = ", ".join(set_parts)
        
        queries['neo4j'] = f"""MATCH (n:{entity})
WHERE {where_clause}
SET {set_clause}
RETURN n"""
    
    elif operation == 'delete':
        where_parts = []
        for key, value in filter_q.items():
            if isinstance(value, dict):
                # Opérateurs
                if '$gt' in value:
                    where_parts.append(f"n.{key} > {value['$gt']}")
                elif '$lt' in value:
                    where_parts.append(f"n.{key} < {value['$lt']}")
            elif isinstance(value, str):
                where_parts.append(f"n.{key} = \"{value}\"")
            else:
                where_parts.append(f"n.{key} = {value}")
        where_clause = " AND ".join(where_parts) if where_parts else "true"
        
        queries['neo4j'] = f"""MATCH (n:{entity})
WHERE {where_clause}
DETACH DELETE n"""
    
    # ============================================================================
    # SPARQL (Web Sémantique)
    # ============================================================================
    if operation == 'create':
        # SPARQL INSERT
        triples = []
        subject = f"ex:{collection}/{data.get('_id', 'new')}"
        triples.append(f"{subject} rdf:type ex:{entity} .")
        for key, value in data.items():
            if key not in ['_id', 'id']:
                triples.append(f"{subject} ex:{key} \"{value}\" .")
        
        queries['web_semantique'] = f"""PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

INSERT DATA {{
  {chr(10).join(['  ' + t for t in triples])}
}}"""
    
    elif operation == 'update':
        # SPARQL DELETE/INSERT
        subject = f"ex:{collection}/{filter_q.get('_id', 'unknown')}"
        delete_triples = []
        insert_triples = []
        
        for key, value in fields_to_update.items():
            delete_triples.append(f"{subject} ex:{key} ?old{key} .")
            insert_triples.append(f"{subject} ex:{key} \"{value}\" .")
        
        queries['web_semantique'] = f"""PREFIX ex: <http://example.org/>

DELETE {{
  {chr(10).join(['  ' + t for t in delete_triples])}
}}
INSERT {{
  {chr(10).join(['  ' + t for t in insert_triples])}
}}
WHERE {{
  {chr(10).join(['  ' + t for t in delete_triples])}
}}"""
    
    elif operation == 'delete':
        subject = f"ex:{collection}/{filter_q.get('_id', 'unknown')}"
        queries['web_semantique'] = f"""PREFIX ex: <http://example.org/>

DELETE WHERE {{
  {subject} ?p ?o .
}}"""
    
    return queries


# Variables globales pour Gemini
gemini_client = None
gemini_available = False
