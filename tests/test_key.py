import pandas as pd

# Charger ton CSV
df = pd.read_csv("data/amazon.csv")

# Nettoyer les colonnes numériques si nécessaire
df['rating'] = pd.to_numeric(df['rating'].str.replace(',', '').fillna(0))

# Requête générée par le LLM (ici test)
query = "category.str.contains('Electronics') & rating > 4"

# Évaluer la requête sur le DataFrame
try:
    results = df.query(query)
    print("Résultats filtrés :")
    print(results.head())
except Exception as e:
    print("Erreur lors de l'exécution :", e)
