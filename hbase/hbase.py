import happybase

# Connect to HBase Thrift server running inside Docker
connection = happybase.Connection('localhost', 9090)
connection.open()

# List existing tables
print(connection.tables())
