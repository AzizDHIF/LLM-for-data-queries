def clean_cypher_output(cypher_text):
    """
    Nettoie la sortie LLM pour récupérer uniquement la requête Cypher.
    - Supprime les espaces vides en début/fin
    - Supprime les retours à la ligne inutiles
    """
    if not cypher_text:
        return ""
    return cypher_text.strip().replace("\n", " ")


def detect_query_type(nl_query: str) -> str:
    """
    Détecte si la requête NL est une lecture (read)
    ou une écriture (write).
    """
    nl = nl_query.lower()

    write_keywords = [
    # Création
    "create", "add", "insert", "register", "introduce",
    "build", "generate", "make", "define", "store",

    # Mise à jour
    "update", "modify", "change", "edit", "rename",
    "set", "replace", "correct", "fix", "adjust",

    # Suppression
    "delete", "remove", "erase", "drop", "clear",

    # Fusion / liaison
    "merge", "link", "connect", "associate", "relate",
    "attach", "assign", "bind",

    # Désassociation
    "unlink", "disconnect", "detach",

    # États / propriétés
    "mark", "flag", "unflag", "enable", "disable",

    # Relations temporelles
    "move", "transfer", "reassign"
]


    for kw in write_keywords:
        if kw in nl:
            return "write"

    return "read"

def parse_llm_output(text: str) -> dict:
    text = text.strip()

    if text.startswith("QUESTION:"):
        return {
            "type": "clarification",
            "content": text.replace("QUESTION:", "").strip()
        }

    if text.startswith("CYPHER:"):
        cypher = text.replace("CYPHER:", "").strip()
        return {
            "type": "cypher",
            "content": clean_cypher_output(cypher)
        }

 
