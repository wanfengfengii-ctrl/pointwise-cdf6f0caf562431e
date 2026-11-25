import sqlite3

from lake.batches import find_frozen_reading
from lake.readings import ReadingId


def read_current_owner(connection: sqlite3.Connection, identity: ReadingId) -> str:
    row = connection.execute('SELECT station FROM current_owners WHERE reading_key = ?',
                             (identity.storage_key(),)).fetchone()
    if row is not None:
        return row[0]
    reading = find_frozen_reading(connection, identity)
    if reading is None:
        raise KeyError(f'unknown frozen reading {identity}')
    return reading.identity.origin_station


def write_current_owner(connection: sqlite3.Connection, identity: ReadingId, station: str) -> None:
    connection.execute('''
        INSERT INTO current_owners (reading_key, station) VALUES (?, ?)
        ON CONFLICT (reading_key) DO UPDATE SET station = excluded.station
    ''', (identity.storage_key(), station))
