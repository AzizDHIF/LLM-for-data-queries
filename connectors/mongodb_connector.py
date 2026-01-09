# connectors/connector.py
import json
import pandas as pd
import os

class DataLoader:
    """
    Classe pour charger et nettoyer les données depuis un fichier JSON
    """

    def __init__(self, path="data/mongo_amazon.json"):
        self.path = path
        self.df = pd.DataFrame()

    def load_data(self) -> pd.DataFrame:
        """
        Charge les données depuis le fichier JSON et crée un DataFrame
        """
        if not os.path.exists(self.path):
            print(f"❌ Erreur : Fichier '{self.path}' non trouvé")
            self.df = pd.DataFrame()
            return self.df

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"✅ Données chargées : {len(data)} produits")
        except json.JSONDecodeError:
            print(f"❌ Erreur : Format JSON invalide dans {self.path}")
            data = []

        # Créer le DataFrame
        if not data:
            print("⚠️ Aucune donnée, création d'un DataFrame vide")
            self.df = pd.DataFrame()
        else:
            self.df = pd.DataFrame(data)
            print(f"✅ DataFrame créé avec {len(self.df)} lignes et {len(self.df.columns)} colonnes")

        return self.df

    def clean_numeric_columns(self) -> pd.DataFrame:
        """
        Nettoie les colonnes numériques du DataFrame self.df
        """
        if self.df.empty:
            print("⚠️ DataFrame vide, rien à nettoyer")
            return self.df

        # Nettoyer rating
        if 'rating' in self.df.columns:
            self.df['rating'] = pd.to_numeric(
                self.df['rating'].astype(str).str.replace(',', '', regex=False).fillna('0'),
                errors='coerce'
            )

        # Nettoyer les prix
        for price_col in ['discounted_price', 'actual_price']:
            if price_col in self.df.columns:
                self.df[price_col] = pd.to_numeric(
                    self.df[price_col].astype(str).str.replace(r'[^\d.]', '', regex=True).fillna('0'),
                    errors='coerce'
                )

        print("✅ Colonnes numériques nettoyées")
        return self.df

    def get_dataframe(self) -> pd.DataFrame:
        """
        Retourne le DataFrame nettoyé
        """
        if self.df.empty:
            self.load_data()
        self.clean_numeric_columns()
        return self.df

    # 🔹 Méthode init_data() dans la classe
    def init_data(self) -> pd.DataFrame:
        """
        Initialise les données depuis le fichier JSON avec la logique de init_data() originale
        """
        if not os.path.exists(self.path):
            print(f"❌ Erreur : Fichier '{self.path}' non trouvé")
            self.df = pd.DataFrame()
            return self.df

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"✅ Données chargées : {len(data)} produits")
        except json.JSONDecodeError:
            print(f"❌ Erreur : Format JSON invalide dans {self.path}")
            data = []

        if not data:
            print("⚠️ Aucune donnée chargée, création d'un DataFrame vide")
            self.df = pd.DataFrame()
            return self.df

        # Création du DataFrame
        self.df = pd.DataFrame(data)
        print(f"✅ DataFrame créé avec {len(self.df)} lignes et {len(self.df.columns)} colonnes")

        # Nettoyage rating et prix
        self.clean_numeric_columns()

        return self.df
