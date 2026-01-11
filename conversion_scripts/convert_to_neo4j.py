import pandas as pd
from py2neo import Graph, Node, Relationship

# Connexion Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

# Charger le CSV
df = pd.read_csv("data/amazon.csv")

for _, row in df.iterrows():
    # Product node
    product = Node(
        "Product",
        product_id=row["product_id"],
        name=row["product_name"],
        rating=row["rating"],
        price=row["discounted_price"]
    )

    # Category node
    category = Node(
        "Category",
        name=row["category"]
    )

    # User node
    user = Node(
        "User",
        user_id=row["user_id"],
        name=row["user_name"]
    )

    # Review node
    review = Node(
        "Review",
        review_id=row["review_id"],
        title=row["review_title"],
        content=row["review_content"]
    )

    # Merge nodes (évite doublons)
    graph.merge(product, "Product", "product_id")
    graph.merge(category, "Category", "name")
    graph.merge(user, "User", "user_id")
    graph.merge(review, "Review", "review_id")

    # Relations
    graph.merge(Relationship(product, "BELONGS_TO", category))
    graph.merge(Relationship(user, "WROTE", review))
    graph.merge(Relationship(review, "REVIEWS", product))

print("✅ Conversion CSV → Neo4j terminée")
