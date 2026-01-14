import json
import re
from pymongo import MongoClient


class MongoExecutor:
    """
    MongoDB Executor
    - Supports READ: find, aggregate, countDocuments
    - Supports WRITE: insertOne, insertMany, updateOne, updateMany, deleteOne, deleteMany
    - Safe JSON parsing (no eval)
    """

    def __init__(
        self,
        host="localhost",
        port=27017,
        username="admin",
        password="secret",
        database="sample_mflix",
        collection="movies"
    ):
        self.client = MongoClient(
            host=host,
            port=port,
            username=username,
            password=password,
            authSource="admin"
        )
        self.db = self.client[database]
        self.collection = self.db[collection]

    # ======================================================
    # UTILS
    # ======================================================
    def _mongo_to_python(self, text: str):
        """
        Convert Mongo shell object / array to Python dict / list
        """
        text = text.strip()

        # Quote keys: { $group: ... } -> { "$group": ... }
        text = re.sub(r'([{\[,]\s*)(\$?[\w\.]+)\s*:', r'\1"\2":', text)

        # Remove trailing commas
        text = re.sub(r',\s*([}\]])', r'\1', text)

        return json.loads(text)

    # ======================================================
    # EXECUTOR
    # ======================================================
    def run_query(self, mongo_query: str):
        mongo_query = mongo_query.strip()

        # Remove garbage before db.movies.*
        if "db.movies" in mongo_query:
            mongo_query = mongo_query[mongo_query.find("db.movies"):]

        # ==================================================
        # READ OPERATIONS
        # ==================================================

        # ---------- AGGREGATE ----------
        if mongo_query.startswith("db.movies.aggregate"):
            pipeline_text = mongo_query.split("aggregate(", 1)[1].rsplit(")", 1)[0]
            pipeline = self._mongo_to_python(pipeline_text)

            if not isinstance(pipeline, list):
                raise TypeError("Aggregation pipeline must be a list")

            result = list(self.collection.aggregate(pipeline))
            return {
                "type": "aggregate",
                "count": len(result),
                "data": result
            }

        # ---------- FIND ----------
        if mongo_query.startswith("db.movies.find"):
            filter_text = mongo_query.split("find(", 1)[1].split(")", 1)[0]
            query = {} if filter_text.strip() == "" else self._mongo_to_python(filter_text)

            cursor = self.collection.find(query)

            if ".sort(" in mongo_query:
                sort_text = mongo_query.split(".sort(", 1)[1].split(")", 1)[0]
                sort_dict = self._mongo_to_python(sort_text)
                cursor = cursor.sort(list(sort_dict.items()))

            if ".limit(" in mongo_query:
                limit = int(mongo_query.split(".limit(", 1)[1].split(")", 1)[0])
                cursor = cursor.limit(limit)

            data = list(cursor)
            return {
                "type": "find",
                "count": len(data),
                "data": data
            }

        # ---------- COUNT ----------
        if mongo_query.startswith("db.movies.countDocuments"):
            filter_text = mongo_query.split("(", 1)[1].split(")", 1)[0]
            query = {} if filter_text.strip() == "" else self._mongo_to_python(filter_text)

            count = self.collection.count_documents(query)
            return {
                "type": "count",
                "count": count
            }

        # ==================================================
        # WRITE OPERATIONS (CRUD)
        # ==================================================

        # ---------- INSERT ONE ----------
        if mongo_query.startswith("db.movies.insertOne"):
            doc_text = mongo_query.split("insertOne(", 1)[1].rsplit(")", 1)[0]
            document = self._mongo_to_python(doc_text)

            result = self.collection.insert_one(document)
            return {
                "type": "insert",
                "inserted_id": str(result.inserted_id)
            }

        # ---------- INSERT MANY ----------
        if mongo_query.startswith("db.movies.insertMany"):
            docs_text = mongo_query.split("insertMany(", 1)[1].rsplit(")", 1)[0]
            documents = self._mongo_to_python(docs_text)

            if not isinstance(documents, list):
                raise TypeError("insertMany expects a list")

            result = self.collection.insert_many(documents)
            return {
                "type": "insert",
                "count": len(result.inserted_ids)
            }

        # ---------- UPDATE ONE ----------
        if mongo_query.startswith("db.movies.updateOne"):
            args = mongo_query.split("updateOne(", 1)[1].rsplit(")", 1)[0]
            filter_text, update_text = args.split(",", 1)

            filter_q = self._mongo_to_python(filter_text)
            update_q = self._mongo_to_python(update_text)

            if "$set" not in update_q:
                raise ValueError("Updates must use $set")

            result = self.collection.update_one(filter_q, update_q)
            return {
                "type": "update",
                "matched": result.matched_count,
                "modified": result.modified_count
            }

        # ---------- UPDATE MANY ----------
        if mongo_query.startswith("db.movies.updateMany"):
            args = mongo_query.split("updateMany(", 1)[1].rsplit(")", 1)[0]
            filter_text, update_text = args.split(",", 1)

            filter_q = self._mongo_to_python(filter_text)
            update_q = self._mongo_to_python(update_text)

            if "$set" not in update_q:
                raise ValueError("Updates must use $set")

            result = self.collection.update_many(filter_q, update_q)
            return {
                "type": "update",
                "matched": result.matched_count,
                "modified": result.modified_count
            }

        # ---------- DELETE ONE ----------
        if mongo_query.startswith("db.movies.deleteOne"):
            filter_text = mongo_query.split("deleteOne(", 1)[1].rsplit(")", 1)[0]
            query = self._mongo_to_python(filter_text)

            result = self.collection.delete_one(query)
            return {
                "type": "delete",
                "deleted": result.deleted_count
            }

        # ---------- DELETE MANY ----------
        if mongo_query.startswith("db.movies.deleteMany"):
            filter_text = mongo_query.split("deleteMany(", 1)[1].rsplit(")", 1)[0]
            query = self._mongo_to_python(filter_text)

            if query == {}:
                raise PermissionError("Refusing deleteMany({})")

            result = self.collection.delete_many(query)
            return {
                "type": "delete",
                "deleted": result.deleted_count
            }

        # ==================================================
        # FALLBACK
        # ==================================================
        raise ValueError("❌ Unsupported MongoDB query")
