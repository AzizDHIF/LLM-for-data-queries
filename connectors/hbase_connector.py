import happybase
import json

# Connexion HBase via Thrift
connection = happybase.Connection(
    host='localhost',
    port=9090
)
connection.open()

print("✅ Connexion HBase établie")

# Accéder à la table
table = connection.table('products')

# Scanner tous les produits
print("\n📦 Produits HBase :")

for row_key, data in table.scan(limit=10):
    print("\n--- Produit", row_key.decode(), "---")

    for col, value in data.items():
        col = col.decode()
        value = value.decode()

        # Si la valeur est du JSON (reviews par ex)
        try:
            value = json.loads(value)
        except:
            pass

        print(f"{col}: {value}")

connection.close()
