"""Reproduce operational boundaries through real temporary database operations."""
import json
import sqlite3
import sys
import tempfile
import threading
import time
from pathlib import Path
from strataleaf import Database, QuotaExceeded
from .workflows import seed


def contention(folder):
    with Database(folder / 'observe.db', timeout=0.03) as db:
        seed(db)
        holder = sqlite3.connect(folder / 'observe.db', isolation_level=None)
        try:
            holder.execute('BEGIN IMMEDIATE')
            started = time.monotonic()
            try:
                db.batch().delete('segments', 'seg_00').commit()
            except sqlite3.OperationalError as exc:
                blocked = {'exception': type(exc).__name__, 'message': str(exc),
                           'elapsed_ms': round((time.monotonic() - started) * 1000, 2)}
            else:
                raise RuntimeError('expected SQLite writer contention was not observed')
        finally:
            holder.rollback()
            holder.close()
        result = db.batch().delete('segments', 'seg_00').commit()
        return {'blocked_write': blocked, 'after_release': result.__dict__, 'inspection': db.diagnostics.inspect(True)}


def retention_pressure(folder):
    with Database(folder / 'observe.db') as db:
        first = seed(db)
        db.retention.pin('long_reader', first)
        db.feed.register('offline_replica', first)
        for value in range(3):
            db.batch().put('segments', 'seg_00', {'region': 'east', 'sequence': value + 10}).commit()
        both = db.retention.compact(db.revision)
        db.retention.unpin('long_reader')
        one = db.retention.compact(db.revision)
        db.feed.acknowledge('offline_replica', db.revision)
        none = db.retention.compact(db.revision)
        return {'both_holds': both, 'one_hold': one, 'released': none, 'inspection': db.diagnostics.inspect(True)}


def quota_contention(folder):
    path = folder / 'observe.db'
    with Database(path) as db:
        seed(db)
        db.quotas.set_collection('segments', max_documents=7)
    barrier = threading.Barrier(2)
    results = {}

    def worker(tag, key):
        with Database(path, timeout=5.0) as handle:
            barrier.wait()
            try:
                committed = handle.batch().put('segments', key, {'region': 'east', 'sequence': 99}).commit()
                results[tag] = {'outcome': 'committed', 'revision': committed.revision}
            except QuotaExceeded as exc:
                results[tag] = {'outcome': 'rejected', 'message': str(exc)}

    threads = [threading.Thread(target=worker, args=('connection_a', 'race_a')),
               threading.Thread(target=worker, args=('connection_b', 'race_b'))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    with Database(path) as db:
        final = db.quotas.usage()['collections']['segments']
    winners = sorted(tag for tag, result in results.items() if result['outcome'] == 'committed')
    if len(winners) != 1 or final['documents'] != final['max_documents']:
        raise RuntimeError('last quota slot was not awarded to exactly one connection')
    return {'contenders': results, 'winner': winners[0], 'final_usage': final}


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='strataleaf-observation-') as directory:
        operation = {'contention': contention, 'retention_pressure': retention_pressure,
                     'quota_contention': quota_contention}[sys.argv[1]]
        print(json.dumps(operation(Path(directory)), ensure_ascii=False, sort_keys=True))
