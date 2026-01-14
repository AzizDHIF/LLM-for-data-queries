import json
from pymongo import MongoClient

def clean_doc(doc):
    """Nettoie un document MongoDB exporté pour insertion dans Docker"""
    # _id
    if "_id" in doc and isinstance(doc["_id"], dict) and "$oid" in doc["_id"]:
        doc["_id"] = doc["_id"]["$oid"]

    # convertir tous les champs dict style MongoDB en types Python
    for k, v in list(doc.items()):
        if isinstance(v, dict):
            if "$numberInt" in v:
                doc[k] = int(v["$numberInt"])
            elif "$numberDouble" in v:
                doc[k] = float(v["$numberDouble"])
            elif "$date" in v:
                # convertir date en string
                doc[k] = str(v["$date"]["$numberLong"])
            else:
                doc[k] = v

    # aplatir imdb
    if "imdb" in doc and isinstance(doc["imdb"], dict):
        imdb = doc["imdb"]
        # rating
        rating = imdb.get("rating")
        if isinstance(rating, dict):
            if "$numberDouble" in rating:
                doc["imdb.rating"] = float(rating["$numberDouble"])
            elif "$numberInt" in rating:
                doc["imdb.rating"] = float(rating["$numberInt"])
        elif isinstance(rating, (int, float)):
            doc["imdb.rating"] = float(rating)
        elif isinstance(rating, str) and rating.strip() != "":
            doc["imdb.rating"] = float(rating)

        # votes
        votes = imdb.get("votes")
        if isinstance(votes, dict):
            if "$numberInt" in votes:
                doc["imdb.votes"] = int(votes["$numberInt"])
            elif "$numberDouble" in votes:
                doc["imdb.votes"] = int(float(votes["$numberDouble"]))
        elif isinstance(votes, (int, float)):
            doc["imdb.votes"] = int(votes)
        elif isinstance(votes, str) and votes.strip() != "":
            doc["imdb.votes"] = int(votes)

        del doc["imdb"]

    return doc


# Connexion à MongoDB Docker
client = MongoClient("mongodb://admin:secret@localhost:27017/")
db = client["sample_mflix"]
collection = db["movies"]

# Charger et nettoyer les données
with open("data/movies.json", "r", encoding="utf-8") as f:
    data = [clean_doc(json.loads(line)) for line in f if line.strip()]

# Insérer dans MongoDB
if data:
    collection.insert_many(data)
    print(f"✅ {len(data)} documents insérés dans la collection 'movies'.")
else:
    print("⚠️ Aucun document trouvé dans le JSON.")
