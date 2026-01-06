import pandas as pd
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

# Transformer en documents imbriqués
mongo_docs = []

for _, prod in products_df.iterrows():
    prod_id = prod["product_id"]
    
    # Obtenir toutes les reviews de ce produit
    prod_reviews = []
    for _, rev in reviews_df[reviews_df["product_id"] == prod_id].iterrows():
        prod_reviews.append({
            "review_id": rev["review_id"],
            "user_id": rev["user_id"],
            "user_name": rev["user_name"],
            "review_title": rev["review_title"],
            "review_content": rev["review_content"]
        })
    
    # Créer le document final MongoDB
    doc = {
        "product_id": prod_id,
        "product_name": prod["product_name"],
        "category": prod["category"],
        "discounted_price": prod["discounted_price"],
        "actual_price": prod["actual_price"],
        "discount_percentage": prod["discount_percentage"],
        "rating": prod["rating"],
        "rating_count": prod["rating_count"],
        "about_product": prod["about_product"],
        "img_link": prod["img_link"],
        "product_link": prod["product_link"],
        "reviews": prod_reviews
    }
    
    mongo_docs.append(doc)

# Sauvegarder en JSON (ta base MongoDB)
with open("data/mongo_amazon.json", "w") as f:
    json.dump(mongo_docs, f, indent=4)

print("✅ Conversion CSV → MongoDB terminée !")
