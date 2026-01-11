import pandas as pd
import happybase

# Charger le CSV
df = pd.read_csv("data/amazon.csv")

# Connexion HBase
connection = happybase.Connection(host='localhost', port=9090)
connection.open()

# Création de la table si inexistante
table_name = 'products'

if table_name.encode() not in connection.tables():
    connection.create_table(
        table_name,
        {
            'info': dict(),
            'price': dict(),
            'review': dict()
        }
    )

table = connection.table(table_name)

# Insertion des données
for _, row in df.iterrows():
    row_key = row['product_id']

    table.put(
        row_key,
        {
            b'info:product_name': str(row['product_name']).encode(),
            b'info:category': str(row['category']).encode(),
            b'info:about_product': str(row['about_product']).encode(),
            b'info:img_link': str(row['img_link']).encode(),
            b'info:product_link': str(row['product_link']).encode(),

            b'price:discounted_price': str(row['discounted_price']).encode(),
            b'price:actual_price': str(row['actual_price']).encode(),
            b'price:discount_percentage': str(row['discount_percentage']).encode(),
            b'price:rating': str(row['rating']).encode(),
            b'price:rating_count': str(row['rating_count']).encode(),

            # Reviews (chaque review est une colonne différente)
            f"review:{row['review_id']}".encode(): str(row['review_content']).encode()
        }
    )

print("✅ Conversion CSV → HBase terminée")
connection.close()
