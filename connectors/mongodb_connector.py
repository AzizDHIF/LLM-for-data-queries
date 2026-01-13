import yaml
import json
import pandas as pd
import os


class DataLoader:
    def __init__(self, config_path="config/mongodb.yaml"):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        self.path = config["data"]["path"]
        self.encoding = config["data"].get("encoding", "utf-8")
        self.df = pd.DataFrame()

    def load_data(self) -> pd.DataFrame:
        if not os.path.exists(self.path):
            print(f"❌ Fichier introuvable : {self.path}")
            return pd.DataFrame()

        with open(self.path, "r", encoding=self.encoding) as f:
            data = json.load(f)

        self.df = pd.DataFrame(data)
        return self.df

    def clean_numeric_columns(self) -> pd.DataFrame:
        if self.df.empty:
            return self.df

        if "rating" in self.df.columns:
            self.df["rating"] = pd.to_numeric(
                self.df["rating"].astype(str).str.replace(",", "", regex=False),
                errors="coerce"
            )

        for col in ["discounted_price", "actual_price"]:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(
                    self.df[col].astype(str).str.replace(r"[^\d.]", "", regex=True),
                    errors="coerce"
                )

        return self.df

    def init_data(self) -> pd.DataFrame:
        self.load_data()
        self.clean_numeric_columns()
        return self.df
