import yaml
from neo4j import GraphDatabase

# Charger la config YAML
with open("neo4j.yaml", "r") as f:
    config = yaml.safe_load(f)

# Connexion Neo4j
driver = GraphDatabase.driver(
    config["uri"],
    auth=(config["user"], config["password"])
)

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
    for record in tx.run(query):
        print(record)

with driver.session() as session:
    session.execute_read(show_products)

driver.close()
