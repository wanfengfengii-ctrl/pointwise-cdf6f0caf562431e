from dataclasses import dataclass
import json
import sqlite3

from lake.readings import Reading, ReadingId, canonical_json


@dataclass(frozen=True)
class FrozenBatch:
    batch_id: str
    day: str
    station: str
    readings: tuple[Reading, ...]
    result_id: str


def create_batches(connection: sqlite3.Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS batches (
            batch_id TEXT PRIMARY KEY,
            day TEXT NOT NULL REFERENCES accounting_days(day),
            station TEXT NOT NULL,
            body BLOB NOT NULL,
            result_id TEXT NOT NULL REFERENCES snapshots(result_id)
        )
    ''')
    connection.execute('CREATE INDEX IF NOT EXISTS batches_by_day ON batches(day, batch_id)')


def _decode_rows(body: bytes) -> tuple[Reading, ...]:
    return tuple(Reading.from_wire(value) for value in json.loads(body))


def find_batch(connection: sqlite3.Connection, batch_id: str) -> FrozenBatch | None:
    row = connection.execute('''
        SELECT day, station, body, result_id FROM batches WHERE batch_id = ?
    ''', (batch_id,)).fetchone()
    if row is None:
        return None
    day, station, body, result_id = row
    return FrozenBatch(batch_id, day, station, _decode_rows(body), result_id)


def write_batch(connection: sqlite3.Connection, batch: FrozenBatch) -> None:
    body = canonical_json([reading.to_wire() for reading in batch.readings])
    connection.execute('''
        INSERT INTO batches (batch_id, day, station, body, result_id) VALUES (?, ?, ?, ?, ?)
    ''', (batch.batch_id, batch.day, batch.station, body, batch.result_id))


def find_frozen_reading(connection: sqlite3.Connection, identity: ReadingId) -> Reading | None:
    batch = find_batch(connection, identity.batch_id)
    if batch is None or batch.day != identity.day or batch.station != identity.origin_station:
        return None
    if identity.position > len(batch.readings):
        return None
    reading = batch.readings[identity.position - 1]
    if reading.identity != identity:
        return None
    return reading


def list_day_readings(connection: sqlite3.Connection, day: str) -> tuple[Reading, ...]:
    batches = connection.execute('SELECT body FROM batches WHERE day = ? ORDER BY batch_id',
                                 (day,))
    return tuple(reading for (body,) in batches for reading in _decode_rows(body))
