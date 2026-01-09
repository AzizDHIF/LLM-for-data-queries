import redis
r=redis.Redis(host='localhost', port=6379, decode_responses=True)

"""try:
    pong=r.ping()
    print("Connected to Redis server successfully.",pong)
except redis.ConnectionError:
    print("Failed to connect to Redis server.")"""
print (r.hgetall("user:1"))
