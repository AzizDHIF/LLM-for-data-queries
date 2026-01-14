# main.py

from llm.mongodb_llm import generate_mongodb_query
from executers.mongodb_executer import MongoExecutor  # version Docker pymongo

def main():
    # ------------------------
    # Connexion MongoDB Docker
    # ------------------------
    executor = MongoExecutor(
        host="localhost",
        port=27017,
        username="admin",
        password="secret",
        database="sample_mflix",  # adapte si ton DB est autre
        collection="movies"
    )

    # ------------------------
    # Questions
    # ------------------------
    questions = [
        "Top 10 films les mieux notés",
        "Combien de films sortis en 1893 ?",
        "Quel est le film le plus long ?",
        "Nombre de films par année",
        "Moyenne des notes IMDb par genre"
    ]

    for q in questions:
        print("\n" + "=" * 80)
        print(f"❓ Question: {q}")

        # Générer la requête MongoDB via le LLM
        mongo_query = generate_mongodb_query(q)
        print("\n🔍 Requête MongoDB générée :")
        print(mongo_query)

        # Exécuter la requête directement sur MongoDB Docker
        print("\n📊 Résultat :")
        try:
            result = executor.run_query(mongo_query)
            if isinstance(result, list):
                for r in result[:10]:  # afficher max 10 lignes pour lisibilité
                    print(r)
            else:
                print(result)
        except Exception as e:
            print(f"⚠️ Erreur lors de l'exécution de la requête: {e}")

        input("\n⏎ Appuyez sur Entrée pour continuer...")


if __name__ == "__main__":
    main()
