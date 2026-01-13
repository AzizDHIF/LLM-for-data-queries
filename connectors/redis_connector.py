import yaml
import redis
import json

# Charger la config YAML
with open("redis.yaml", "r") as f:
    config = yaml.safe_load(f)

# Connexion Redis
r = redis.Redis(
    host=config["host"],
    port=config["port"],
    db=config["db"],
    decode_responses=config["decode_responses"]
)

product_keys = [k for k in r.keys("product:*") if ":reviews" not in k]

for key in product_keys:
    print(f"\n--- {key} ---")
    h = r.hgetall(key)

    for k, v in h.items():
        print(f"{k}: {v}")

    print("Reviews:")
    reviews = r.lrange(f"{h['product_id']}:reviews", 0, -1)
    for rev in reviews:
        print(json.loads(rev))
