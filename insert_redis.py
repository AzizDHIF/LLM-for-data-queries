import redis
import yaml
import ast

# -----------------------------
# Charger la config depuis redis.yaml
# -----------------------------
with open("config/redis.yaml", "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

r = redis.Redis(
    host=cfg.get("host", "localhost"),
    port=cfg.get("port", 6379),
    db=cfg.get("db", 0),
    decode_responses=cfg.get("decode_responses", True)
)

# -----------------------------
# Chemin du fichier contenant les HSET
# -----------------------------
file_path = r"C:\Users\Fatma CHAHED\LLM-for-data-queries\data\movies.redis"

# -----------------------------
# Lire le fichier et insérer les données
# -----------------------------
with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or not line.startswith("HSET"):
            continue

        # Découper la ligne HSET "movie:1" "title" "Inception" "genre" "Sci-Fi" ...
        try:
            parts = []
            current = ""
            in_quotes = False
            for char in line:
                if char == '"':
                    in_quotes = not in_quotes
                    continue
                if char == " " and not in_quotes:
                    if current:
                        parts.append(current)
                        current = ""
                else:
                    current += char
            if current:
                parts.append(current)
        except Exception as e:
            print(f"⚠️ Ligne ignorée (parse error): {line}")
            continue

        # key et champs/valeurs
        if len(parts) < 3:
            continue

        key = parts[1]
        field_values = parts[2:]

        data = {}
        i = 0
        while i < len(field_values):
            field = field_values[i]
            if i + 1 >= len(field_values):
                break
            value = field_values[i + 1]

            # Essayer de convertir en int ou float
            try:
                if "." in value:
                    value = float(value)
                else:
                    value = int(value)
            except ValueError:
                pass  # laisser en string si ce n'est pas un nombre

            data[field] = value
            i += 2

        # Inserer dans Redis
        try:
            r.hset(key, mapping=data)
        except Exception as e:
            print(f"⚠️ Erreur insertion {key}: {e}")

print("✅ Toutes les données ont été insérées dans Redis !")
