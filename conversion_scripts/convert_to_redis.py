import pandas as pd
import redis
import json

# Charger le CSV
df = pd.read_csv("data/amazon.csv")

# Séparer en deux "tables"
products_cols = ["product_id", "product_name", "category", "discounted_price",
                 "actual_price", "discount_percentage", "rating", "rating_count",
                 "about_product", "img_link", "product_link"]
reviews_cols = ["review_id", "product_id", "user_id", "user_name",
                "review_title", "review_content"]

products_df = df[products_cols].drop_duplicates("product_id")
reviews_df = df[reviews_cols]

# Connexion à Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Pour chaque produit, stocker les données et les reviews
for _, prod in products_df.iterrows():
    prod_id = prod["product_id"]
    
    # Stocker le produit comme un hash
    product_key = f"product:{prod_id}"
    r.hset(product_key, mapping={
        "product_name": prod["product_name"],
        "category": prod["category"],
        "discounted_price": prod["discounted_price"],
        "actual_price": prod["actual_price"],
        "discount_percentage": prod["discount_percentage"],
        "rating": prod["rating"],
        "rating_count": prod["rating_count"],
        "about_product": prod["about_product"],
        "img_link": prod["img_link"],
        "product_link": prod["product_link"]
    })
    
    # Obtenir toutes les reviews de ce produit et stocker comme liste JSON
    prod_reviews = []
    for _, rev in reviews_df[reviews_df["product_id"] == prod_id].iterrows():
        prod_reviews.append({
            "review_id": rev["review_id"],
            "user_id": rev["user_id"],
            "user_name": rev["user_name"],
            "review_title": rev["review_title"],
            "review_content": rev["review_content"]
        })
    
    if prod_reviews:
        r.set(f"product:{prod_id}:reviews", json.dumps(prod_reviews))

print("✅ Conversion CSV → Redis terminée !")
