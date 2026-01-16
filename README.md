# LLM-for-data-queries

Projet visant à **convertir des questions en langage naturel (NLP) en requêtes NoSQL** et à **expliquer les requêtes NoSQL en langage naturel**, en utilisant un LLM (Gemini AI). Le projet supporte plusieurs bases NoSQL : **MongoDB, Redis, HBase, Neo4J**, et peut générer des données synthétiques à partir de SQL.

---

## Team

- Mohamed Karim Elkadhi
- Aziz Dhif 
- Fatma Chahed

---

## Fonctionnalités

- **Conversion NLP → NoSQL** : Transformer une question en langage naturel en requête MongoDB, Redis, HBase ou Neo4J.
- **Support multi-langage NoSQL** : Possibilité de générer des requêtes dans le langage NoSQL choisi.
- **Explication de requête** : Transformer une requête NoSQL en description en langage naturel.
- **Détection automatique du langage de la requête**.
- **Conversion d’une seule question** : Tester rapidement une question NLP unique.
- **Génération de données synthétiques** : Conversion de SQL en NoSQL pour tester et enrichir les bases.
- **Évaluation automatique** : Comparer les requêtes générées par le LLM aux résultats corrects via CSV.
- **Compatibilité Docker** : Déploiement facile des bases de données.

---

## Timeline du projet (4 semaines)

| Semaine | Objectifs |
|---------|-----------|
| 1       | Compréhension du sujet, fixation des données à exploiter |
| 2       | Mise en place de la structure des données, création des containers Docker, tests des requêtes de divers langages NoSQL |
| 3       | Développement de la partie LLM pour chaque langage, exploitation des données |
| 4       | Tests des différentes exécutions du LLM, évaluation des modèles |

---

## Prérequis

- Python 3.10+
- Docker (recommandé pour MongoDB, Redis, HBase)
- Clé API Gemini
- Bases de données locales ou via Docker Compose

---

## Lancement avec Docker

```bash
docker compose up -d
