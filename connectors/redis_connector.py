import redis, json

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

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
