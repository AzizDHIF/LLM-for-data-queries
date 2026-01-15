



# HBase
def explore_schema_hbase(hbase_table) -> dict:
    """
    hbase_table: instance happybase.Table ou similaire
    """
    result = {
        "database": "hbase",
        "num_rows": 0,
        "schema": {},
        "profile": {}
    }

    # Nombre approximatif de lignes
    try:
        count = sum(1 for _ in hbase_table.scan())
        result["num_rows"] = count
    except:
        result["num_rows"] = None

    # Scan pour récupérer colonnes et familles
    try:
        scan = hbase_table.scan(limit=10)  # sample pour schema
        for _, data in scan:
            for col, val in data.items():
                col_str = col.decode() if isinstance(col, bytes) else col
                result["schema"][col_str] = {"type": "unknown", "non_null": 1, "null": 0}
    except:
        pass

    result["profile"] = result["schema"]
    return result