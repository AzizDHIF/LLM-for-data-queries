import pandas as pd
import redis
import json

# =========================
# 1. Connexion à Redis
# =========================
r = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True
)

# =========================
# 2. Charger le CSV
# =========================
df = pd.read_csv("data/amazon.csv")

# =========================
# 3. Colonnes
# =========================
products_cols = [
    "product_id", "product_name", "category",
    "discounted_price", "actual_price",
    "discount_percentage", "rating", "rating_count",
    "about_product", "img_link", "product_link"
]

reviews_cols = [
    "review_id", "product_id", "user_id",
    "user_name", "review_title", "review_content"
]

# =========================
# 4. Séparer produits / reviews
# =========================
products_df = df[products_cols].drop_duplicates("product_id")
reviews_df = df[reviews_cols]

# =========================
# 5. Nettoyer Redis (optionnel)
# =========================
# ⚠️ Décommente si tu veux repartir de zéro
# r.flushdb()

# =========================
# 6. Insertion dans Redis
# =========================
for _, prod in products_df.iterrows():
    product_id = prod["product_id"]

    # -------- Produit : HASH --------
    product_key = f"product:{product_id}"

    r.hset(product_key, mapping={
        "product_id": product_id,
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

    # -------- Reviews : LIST --------
    reviews_key = f"product:{product_id}:reviews"

    product_reviews = reviews_df[reviews_df["product_id"] == product_id]

    for _, rev in product_reviews.iterrows():
        review_doc = {
            "review_id": rev["review_id"],
            "user_id": rev["user_id"],
            "user_name": rev["user_name"],
            "review_title": rev["review_title"],
            "review_content": rev["review_content"]
        }

        # Chaque review est stockée comme JSON dans une LIST
        r.rpush(reviews_key, json.dumps(review_doc))

print("✅ Conversion CSV → Redis terminée avec succès !")
