from .codec import digest, dumps, loads
from .errors import Conflict, ReplicationError
from .indexes import refresh_document
from .mvcc import checked_revision, latest_row
from .quotas import apply_deltas, delta_tally, merge_tally


class Replication:
    def __init__(self, store):
        self.store = store

    def export(self, since, limit=100):
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ReplicationError("replication limit must be between 1 and 1000 commits")
        with self.store.read() as connection:
            since = checked_revision(self.store, since)
            commits = [loads(row[0]) for row in connection.execute("SELECT payload FROM commits WHERE rev>? ORDER BY rev LIMIT ?", (since, limit))]
            body = {"format": 1, "source": self.store.get("database_id"), "since": since,
                "through": commits[-1]["revision"] if commits else since,
                "catalog_epoch": int(self.store.get("catalog_epoch")), "head": int(self.store.get("head")), "floor": int(self.store.get("floor")),
                "collections": [dict(row) for row in connection.execute("SELECT * FROM collections ORDER BY name")],
                "indexes": [dict(row) for row in connection.execute("SELECT * FROM index_defs ORDER BY collection,name")],
                "quota_version": int(self.store.get("quota_version")),
                "quota_groups": [dict(row) for row in connection.execute("SELECT * FROM quota_groups ORDER BY name")],
                "collection_quotas": [dict(row) for row in connection.execute("SELECT * FROM collection_quotas ORDER BY collection")],
                "commits": commits}
            return {"body": body, "sha256": digest(body)}

    def apply(self, envelope):
        try:
            body = envelope["body"]
            if envelope["sha256"] != digest(body) or body["format"] != 1:
                raise ReplicationError("replication envelope checksum/format mismatch")
            commits = body["commits"]
            expected = list(range(body["since"] + 1, body["through"] + 1))
            if [item["revision"] for item in commits] != expected or body["through"] > body["head"]:
                raise ReplicationError("replication envelope has a revision gap")
            with self.store.atomic() as connection:
                if self.store.get("mode") != "replica" or self.store.get("upstream") != body["source"]:
                    raise ReplicationError("envelope does not match configured upstream")
                head = int(self.store.get("head"))
                if body["since"] > head:
                    raise ReplicationError("missing preceding replication range")
                if body["through"] < head or (body["through"] == head and body["catalog_epoch"] <= int(self.store.get("catalog_epoch"))):
                    return {"revision": head, "applied": 0}
                for definition in body["collections"]:
                    connection.execute("INSERT INTO collections VALUES (?,?,?) ON CONFLICT(name) DO UPDATE SET schema=excluded.schema,generation=excluded.generation",
                                       (definition["name"], definition["schema"], definition["generation"]))
                connection.execute("DELETE FROM collection_quotas")
                connection.execute("DELETE FROM quota_groups")
                for definition in body["quota_groups"]:
                    connection.execute("INSERT INTO quota_groups VALUES (?,?,?)",
                                       (definition["name"], definition["max_documents"], definition["max_bytes"]))
                for definition in body["collection_quotas"]:
                    connection.execute("INSERT INTO collection_quotas VALUES (?,?,?,?)",
                                       (definition["collection"], definition["max_documents"], definition["max_bytes"], definition["quota_group"]))
                connection.execute("DELETE FROM index_values")
                connection.execute("DELETE FROM index_defs")
                for definition in body["indexes"] if body["through"] == body["head"] else []:
                    connection.execute("INSERT INTO index_defs VALUES (?,?,?,?)", (definition["collection"], definition["name"], definition["field"], definition["unique_flag"]))
                applied = 0
                tally = {}
                for commit in commits:
                    revision = commit["revision"]
                    if revision <= head:
                        continue
                    entries = []
                    for operation in commit["operations"]:
                        document = operation["document"]
                        old = latest_row(connection, operation["collection"], operation["key"], revision - 1)
                        body_text = dumps(document) if document is not None else None
                        entries.append((operation["collection"], old["body"] if old is not None else None, body_text))
                        connection.execute("INSERT INTO versions VALUES (?,?,?,?,?)", (operation["collection"], operation["key"], revision,
                            body_text, operation["expires_at"]))
                    merge_tally(tally, delta_tally(entries))
                    connection.execute("INSERT INTO commits(rev,payload) VALUES (?,?)", (revision, dumps(commit)))
                    applied += 1
                apply_deltas(connection, tally)
                self.store.set("head", body["through"])
                self.store.set("catalog_epoch", body["catalog_epoch"] if body["through"] == body["head"] else -1)
                self.store.set("quota_version", body["quota_version"] if body["through"] == body["head"] else -1)
                from .mvcc import visible_rows
                for collection in connection.execute("SELECT name FROM collections").fetchall():
                    for row in visible_rows(connection, collection[0], body["through"]):
                        refresh_document(connection, collection[0], row["key"], loads(row["body"]))
                return {"revision": body["through"], "applied": applied}
        except (KeyError, TypeError, ValueError) as exc:
            raise ReplicationError("malformed replication envelope") from exc
