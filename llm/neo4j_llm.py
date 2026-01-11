





# Neo4j
def explore_schema_neo4j(driver) -> dict:
    """
    driver: neo4j.Driver
    """
    result = {
        "database": "neo4j",
        "num_nodes": 0,
        "schema": {},
        "profile": {}
    }

    with driver.session() as session:
        # Nombre de nœuds total
        try:
            num_nodes = session.run("MATCH (n) RETURN count(n) AS cnt").single().get("cnt")
            result["num_nodes"] = num_nodes
        except:
            pass

        # Types de nœuds
        try:
            labels = session.run("MATCH (n) RETURN distinct labels(n) AS labels").values()
            for l in labels:
                label = l[0][0] if l[0] else "Unknown"
                props = session.run(f"MATCH (n:{label}) RETURN keys(n) AS keys LIMIT 10").values()
                prop_set = set()
                for row in props:
                    for p in row[0]:
                        prop_set.add(p)
                prop_info = {}
                for p in prop_set:
                    prop_info[p] = {"type": "unknown", "non_null": "unknown", "null": "unknown"}
                result["schema"][label] = {"properties": prop_info, "num_nodes": "unknown"}
        except:
            pass

    result["profile"] = result["schema"]
    return result
