from .codec import name
from .errors import Conflict, InvalidDocument, NotFound, QuotaExceeded

LIVE_USAGE_SQL = """SELECT v.collection AS collection, COUNT(*) AS documents,
  COALESCE(SUM(LENGTH(CAST(v.body AS BLOB))),0) AS bytes
  FROM versions v JOIN
  (SELECT collection,key,MAX(rev) AS r FROM versions GROUP BY collection,key) latest
  ON v.collection=latest.collection AND v.key=latest.key AND v.rev=latest.r
  WHERE v.body IS NOT NULL GROUP BY v.collection"""


def body_bytes(body):
    return len(body.encode("utf-8")) if body is not None else 0


def delta_tally(entries):
    """Net live-document/byte deltas per collection for one commit.

    entries yield (collection, old_body, new_body) with bodies as stored JSON
    text or None for tombstones/missing documents. History and tombstones do
    not count toward usage.
    """
    tally = {}
    for collection, old_body, new_body in entries:
        documents, logical_bytes = tally.get(collection, (0, 0))
        if old_body is None and new_body is not None:
            documents += 1
        elif old_body is not None and new_body is None:
            documents -= 1
        logical_bytes += body_bytes(new_body) - body_bytes(old_body)
        tally[collection] = (documents, logical_bytes)
    return tally


def merge_tally(total, part):
    for collection, (documents, logical_bytes) in part.items():
        prior_documents, prior_bytes = total.get(collection, (0, 0))
        total[collection] = (prior_documents + documents, prior_bytes + logical_bytes)
    return total


def apply_deltas(connection, tally):
    for collection, (documents, logical_bytes) in tally.items():
        if not documents and not logical_bytes:
            continue
        connection.execute("""INSERT INTO quota_usage VALUES (?,?,?)
          ON CONFLICT(collection) DO UPDATE SET
          documents=quota_usage.documents+excluded.documents,
          bytes=quota_usage.bytes+excluded.bytes""", (collection, documents, logical_bytes))


def _check_metric(scope, metric, current, delta, limit):
    if limit is None:
        return
    projected = current + delta
    # An already exceeded metric may not worsen; a metric within its limit may
    # not newly breach. Both rules reduce to projected <= max(limit, current).
    if projected > max(limit, current):
        raise QuotaExceeded(
            f"{scope} {metric} quota: current {current}, limit {limit}, commit projects {projected}")


def check_admission(connection, tally):
    """Judge a whole commit by net per-collection and per-group usage."""
    touched = [collection for collection, delta in tally.items() if delta != (0, 0)]
    if not touched:
        return
    marks = ",".join("?" for _ in touched)
    configs = {row["collection"]: row for row in connection.execute(
        f"SELECT * FROM collection_quotas WHERE collection IN ({marks})", touched)}
    usage = {row["collection"]: (row["documents"], row["bytes"]) for row in connection.execute(
        f"SELECT * FROM quota_usage WHERE collection IN ({marks})", touched)}
    groups = {}
    for collection in touched:
        config = configs.get(collection)
        documents_delta, bytes_delta = tally[collection]
        current_documents, current_bytes = usage.get(collection, (0, 0))
        if config is not None:
            _check_metric("collection " + collection, "documents", current_documents, documents_delta, config["max_documents"])
            _check_metric("collection " + collection, "bytes", current_bytes, bytes_delta, config["max_bytes"])
            if config["quota_group"] is not None:
                member_documents, member_bytes = groups.get(config["quota_group"], (0, 0))
                groups[config["quota_group"]] = (member_documents + documents_delta, member_bytes + bytes_delta)
    for group, (documents_delta, bytes_delta) in groups.items():
        limits = connection.execute("SELECT * FROM quota_groups WHERE name=?", (group,)).fetchone()
        if limits is None:
            continue
        current = connection.execute(
            """SELECT COALESCE(SUM(u.documents),0) AS documents, COALESCE(SUM(u.bytes),0) AS bytes
            FROM collection_quotas q LEFT JOIN quota_usage u ON u.collection=q.collection
            WHERE q.quota_group=?""", (group,)).fetchone()
        _check_metric("quota group " + group, "documents", current["documents"], documents_delta, limits["max_documents"])
        _check_metric("quota group " + group, "bytes", current["bytes"], bytes_delta, limits["max_bytes"])


def live_usage(connection):
    """Ground-truth live usage recomputed from stored document bodies."""
    return {row["collection"]: (row["documents"], row["bytes"])
            for row in connection.execute(LIVE_USAGE_SQL)}


def stored_usage(connection):
    return {row["collection"]: (row["documents"], row["bytes"])
            for row in connection.execute("SELECT * FROM quota_usage")}


def _limit(value, label):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidDocument(label + " must be a nonnegative integer or None")
    return value


class Quotas:
    """Per-collection and shared-group quotas over live documents.

    Live usage counts each key's current non-tombstone version only; logical
    bytes are the UTF-8 length of the stored canonical JSON body. Historical
    versions and tombstones never count. Counters are maintained inside each
    commit and reconciled against stored documents when the database opens.
    """
    def __init__(self, store):
        self.store = store
        self.last_reconcile = None

    def _bump(self):
        self.store.set("quota_version", int(self.store.get("quota_version")) + 1)

    def set_group(self, group, max_documents=None, max_bytes=None):
        name(group)
        limits = (_limit(max_documents, "max_documents"), _limit(max_bytes, "max_bytes"))
        with self.store.atomic(writable=True) as connection:
            connection.execute("""INSERT INTO quota_groups VALUES (?,?,?)
              ON CONFLICT(name) DO UPDATE SET max_documents=excluded.max_documents,max_bytes=excluded.max_bytes""",
              (group, limits[0], limits[1]))
            self._bump()
        return {"name": group, "max_documents": limits[0], "max_bytes": limits[1]}

    def drop_group(self, group):
        with self.store.atomic(writable=True) as connection:
            members = [row[0] for row in connection.execute(
                "SELECT collection FROM collection_quotas WHERE quota_group=? ORDER BY collection", (group,))]
            if members:
                raise Conflict("quota group still has member collections: " + ",".join(members))
            if not connection.execute("DELETE FROM quota_groups WHERE name=?", (group,)).rowcount:
                raise NotFound("unknown quota group: " + group)
            self._bump()

    def set_collection(self, collection, max_documents=None, max_bytes=None, group=None):
        name(collection)
        limits = (_limit(max_documents, "max_documents"), _limit(max_bytes, "max_bytes"))
        if group is not None:
            name(group)
        with self.store.atomic(writable=True) as connection:
            if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                raise NotFound("unknown collection: " + collection)
            if group is not None and not connection.execute("SELECT 1 FROM quota_groups WHERE name=?", (group,)).fetchone():
                raise NotFound("unknown quota group: " + group)
            connection.execute("""INSERT INTO collection_quotas VALUES (?,?,?,?)
              ON CONFLICT(collection) DO UPDATE SET max_documents=excluded.max_documents,
              max_bytes=excluded.max_bytes,quota_group=excluded.quota_group""",
              (collection, limits[0], limits[1], group))
            self._bump()
        return {"collection": collection, "max_documents": limits[0], "max_bytes": limits[1], "group": group}

    def clear_collection(self, collection):
        with self.store.atomic(writable=True) as connection:
            if not connection.execute("DELETE FROM collection_quotas WHERE collection=?", (collection,)).rowcount:
                raise NotFound("collection has no quota configuration: " + collection)
            self._bump()

    def usage(self):
        """Current counters with the commit revision and quota version they describe."""
        with self.store.read() as connection:
            counters = stored_usage(connection)
            configs = {row["collection"]: row for row in connection.execute("SELECT * FROM collection_quotas")}
            collections = {}
            for collection in sorted(set(counters) | set(configs)):
                documents, logical_bytes = counters.get(collection, (0, 0))
                config = configs.get(collection)
                collections[collection] = {
                    "documents": documents, "bytes": logical_bytes,
                    "max_documents": config["max_documents"] if config else None,
                    "max_bytes": config["max_bytes"] if config else None,
                    "group": config["quota_group"] if config else None}
            groups = {}
            for row in connection.execute("SELECT * FROM quota_groups ORDER BY name"):
                members = sorted(collection for collection, config in configs.items()
                                 if config["quota_group"] == row["name"])
                groups[row["name"]] = {
                    "documents": sum(counters.get(member, (0, 0))[0] for member in members),
                    "bytes": sum(counters.get(member, (0, 0))[1] for member in members),
                    "max_documents": row["max_documents"], "max_bytes": row["max_bytes"],
                    "members": members}
            return {"revision": int(self.store.get("head")),
                    "quota_version": int(self.store.get("quota_version")),
                    "collections": collections, "groups": groups}

    def reconcile(self):
        """Re-derive usage counters from stored documents and correct drift.

        Runs on every Database open (primary and replica) so files written
        before quotas existed, or restored/copied files, report exact usage.
        """
        with self.store.atomic() as connection:
            actual = live_usage(connection)
            stored = stored_usage(connection)
            corrected = {}
            for collection in sorted(set(actual) | set(stored)):
                want = actual.get(collection, (0, 0))
                have = stored.get(collection, (0, 0))
                if have != want:
                    corrected[collection] = {"stored": {"documents": have[0], "bytes": have[1]},
                                             "actual": {"documents": want[0], "bytes": want[1]}}
            if corrected:
                connection.execute("DELETE FROM quota_usage")
                for collection, (documents, logical_bytes) in actual.items():
                    if documents or logical_bytes:
                        connection.execute("INSERT INTO quota_usage VALUES (?,?,?)",
                                           (collection, documents, logical_bytes))
            self.last_reconcile = {"revision": int(self.store.get("head")),
                                   "quota_version": int(self.store.get("quota_version")),
                                   "consistent": not corrected, "corrected": corrected}
            return self.last_reconcile
