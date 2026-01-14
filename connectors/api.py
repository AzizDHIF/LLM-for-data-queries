# =========================
# 0️⃣ Charger la config Gemini depuis YAML
# =========================
import yaml
def load_gemini_config(path="config/gemini.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

