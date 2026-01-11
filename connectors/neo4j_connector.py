from neo4j import GraphDatabase

# Connexion Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"  # ⚠️ change si besoin

driver = GraphDatabase.driver(uri, auth=(user, password))

print("✅ Connexion Neo4j établie")

def show_products(tx):
    query = """
    MATCH (p:Product)
    OPTIONAL MATCH (p)<-[:REVIEWS]-(r:Review)
    RETURN p.product_id AS id,
           p.product_name AS name,
           p.category AS category,
           count(r) AS reviews
    LIMIT 10
    """
    result = tx.run(query)
    for record in result:
        print("\n--- Produit ---")
        print(f"ID: {record['id']}")
        print(f"Nom: {record['name']}")
        print(f"Catégorie: {record['category']}")
        print(f"Nb reviews: {record['reviews']}")

with driver.session() as session:
    session.read_transaction(show_products)

driver.close()
