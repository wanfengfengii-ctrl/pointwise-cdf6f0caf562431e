import sqlite3

from audit.snapshots import PublishedBook, load_snapshot
from lake.readings import require_name


class UnknownResult(LookupError):
    pass


def lookup_result(connection: sqlite3.Connection, result_id: str) -> PublishedBook:
    require_name(result_id, 'result id')
    result = load_snapshot(connection, result_id)
    if result is None:
        raise UnknownResult(f'no published result {result_id}')
    return result
