from happybase import Connection
from utils.neo4j_llm_utils import detect_query_type
import re


class HBaseExecutor:
    def __init__(self, host='localhost', port=9090, table_name='movies'):
        self.connection = Connection(host, port)
        self.table_name = table_name
        self.table = self.connection.table(table_name)

    # ------------------------------------------------------------------
    # Utils
    # ------------------------------------------------------------------

    def _parse_hbase_response(self, rows):
        """Convert HBase rows to readable JSON format"""
        results = []

        for row_key, data in rows:
            row = {
                "row_key": row_key.decode() if isinstance(row_key, bytes) else row_key
            }
            for col, val in data.items():
                col = col.decode() if isinstance(col, bytes) else col
                val = val.decode() if isinstance(val, bytes) else val
                row[col] = val

            results.append(row)

        return results

    # ------------------------------------------------------------------
    # Shell-like command executor
    # ------------------------------------------------------------------

    def execute_shell_command(self, command: str):
        command = command.strip()

        # --------------------------------------------------------------
        # COUNT
        # --------------------------------------------------------------
        if command == f"count '{self.table_name}'":
            count = sum(1 for _ in self.table.scan())
            return {"result": count}

        # --------------------------------------------------------------
        # SCAN
        # --------------------------------------------------------------
        if command.startswith(f"scan '{self.table_name}'"):
            rows = list(self.table.scan())
            return {"result": self._parse_hbase_response(rows)}

        # --------------------------------------------------------------
        # GET
        # --------------------------------------------------------------
        if command.startswith(f"get '{self.table_name}'"):
            match = re.search(r"get '.*?', '([^']+)'", command)
            if not match:
                return {"error": "Invalid get syntax"}

            row_key = match.group(1)
            data = self.table.row(row_key.encode())

            if not data:
                return {"result": []}

            return {
                "result": self._parse_hbase_response([
                    (row_key.encode(), data)
                ])
            }

        # --------------------------------------------------------------
        # PUT
        # --------------------------------------------------------------
        if command.startswith(f"put '{self.table_name}'"):
            match = re.search(
                r"put '.*?', '([^']+)', '([^']+)', '([^']*)'", command
            )
            if not match:
                return {"error": "Invalid put syntax"}

            row_key, column, value = match.groups()
            cf, col = column.split(':')

            self.table.put(
                row_key.encode(),
                {f"{cf}:{col}".encode(): value.encode()}
            )

            return {"result": f"Row {row_key} inserted/updated"}

        # --------------------------------------------------------------
        # DELETE COLUMN
        # --------------------------------------------------------------
        if command.startswith(f"delete '{self.table_name}'"):
            match = re.search(
                r"delete '.*?', '([^']+)', '([^']+)'", command
            )
            if not match:
                return {"error": "Invalid delete syntax"}

            row_key, column = match.groups()
            self.table.delete(
                row_key.encode(),
                columns=[column.encode()]
            )

            return {"result": f"Column {column} deleted from {row_key}"}

        # --------------------------------------------------------------
        # DELETE ALL
        # --------------------------------------------------------------
        if command.startswith(f"deleteall '{self.table_name}'"):
            match = re.search(
                r"deleteall '.*?', '([^']+)'", command
            )
            if not match:
                return {"error": "Invalid deleteall syntax"}

            row_key = match.group(1)
            self.table.delete(row_key.encode())
            return {"result": f"Row {row_key} deleted"}

        return {"error": f"Unsupported HBase command: {command}"}

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run_query(self, hbase_query: str):
        hbase_query = hbase_query.strip()

        # Direct shell-style commands
        if hbase_query.startswith(("scan", "get", "put", "delete", "deleteall", "count")):
            return self.execute_shell_command(hbase_query)

        # Otherwise fallback to LLM detection
        query_type = detect_query_type(hbase_query)

        if query_type == "read":
            return {"error": "Unsupported read query format"}

        if query_type == "write":
            return {"error": "Unsupported write query format"}

        return {"error": "Could not determine query type"}
