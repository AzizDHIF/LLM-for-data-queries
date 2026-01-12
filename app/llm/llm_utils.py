def clean_cypher_output(cypher_text):
    """
    Nettoie la sortie LLM pour récupérer uniquement la requête Cypher.
    - Supprime les espaces vides en début/fin
    - Supprime les retours à la ligne inutiles
    """
    if not cypher_text:
        return ""
    return cypher_text.strip().replace("\n", " ")
