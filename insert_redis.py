import redis
import yaml

# Charger la config depuis redis.yaml
with open("config/redis.yaml", "r") as f:
    cfg = yaml.safe_load(f)

r = redis.Redis(
    host=cfg.get("host", "localhost"),
    port=cfg.get("port", 6379),
    db=cfg.get("db", 0),
    decode_responses=cfg.get("decode_responses", True)
)

file_path = r"C:\Users\Fatma CHAHED\LLM-for-data-queries\data\movies.redis"

with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or not line.startswith("HSET"):
            continue

        parts = line.split()
        key = parts[1].strip('"')
        field_values = parts[2:]

        data = {}
        i = 0
        while i < len(field_values):
            field = field_values[i].strip('"')
            if i+1 >= len(field_values):
                # pas de valeur, on ignore
                break
            value = field_values[i+1].strip('"')

            # convertir en int/float si possible
            if value.replace('.', '', 1).isdigit():
                if '.' in value:
                    value = float(value)
                else:
                    value = int(value)

            data[field] = value
            i += 2

        # Inserer dans Redis
        r.hset(key, mapping=data)

print("✅ Toutes les données ont été insérées dans Redis !")
