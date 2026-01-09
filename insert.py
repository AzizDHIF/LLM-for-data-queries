import redis
import shlex

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

file_path = "import_users.redis"  # change if needed

with open(file_path, "r") as f:
    for line_number, line in enumerate(f, start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        
        if line.startswith("HSET"):
            try:
                # Parse en respectant les guillemets
                parts = shlex.split(line)
                key = parts[1]  # le premier argument après HSET
                field_values = parts[2:]
                
                if len(field_values) % 2 != 0:
                    print(f"⚠️ Line {line_number} has an odd number of elements: {line}")
                    continue
                
                fields = {field_values[i]: field_values[i+1] for i in range(0, len(field_values), 2)}
                
                r.hset(key, mapping=fields)
            except Exception as e:
                print(f"❌ Error on line {line_number}: {e}")

print("✅ Loaded all users into Redis")
