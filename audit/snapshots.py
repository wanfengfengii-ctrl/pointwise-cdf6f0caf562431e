from contextlib import contextmanager
from dataclasses import dataclass
import json
import sqlite3

from lake.readings import Reading, canonical_json


def connect_database(path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute('PRAGMA foreign_keys = ON')
    return connection


@contextmanager
def atomic(connection: sqlite3.Connection, *, write: bool = True):
    connection.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
    try:
        yield
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


@dataclass(frozen=True)
class BookRow:
    station: str
    reading: Reading


@dataclass(frozen=True)
class BookChange:
    station: str
    direction: int
    reading: Reading


def original_order(row: BookRow) -> tuple:
    identity = row.reading.identity
    return identity.batch_id, identity.position, identity.origin_station


@dataclass(frozen=True)
class PublishedBook:
    result_id: str
    day: str
    stations: tuple[str, ...]
    kind: str
    rows: tuple[BookRow, ...]
    changes: tuple[BookChange, ...] = ()
    subscription_id: str | None = None
    opening_id: str | None = None
    transfer_id: str | None = None
    receipt_id: str | None = None

    def body(self) -> dict:
        return {
            'day': self.day,
            'stations': list(self.stations),
            'kind': self.kind,
            'rows': [[row.station, row.reading.to_wire()] for row in self.rows],
            'changes': [[item.station, item.direction, item.reading.to_wire()]
                        for item in self.changes],
            'subscription_id': self.subscription_id,
            'opening_id': self.opening_id,
            'transfer_id': self.transfer_id,
            'receipt_id': self.receipt_id,
        }


def create_snapshots(connection: sqlite3.Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS snapshots (
            result_id TEXT PRIMARY KEY,
            body BLOB NOT NULL
        )
    ''')


def write_snapshot(connection: sqlite3.Connection, book: PublishedBook) -> None:
    connection.execute('INSERT INTO snapshots (result_id, body) VALUES (?, ?)',
                       (book.result_id, canonical_json(book.body())))


def load_snapshot(connection: sqlite3.Connection, result_id: str) -> PublishedBook | None:
    row = connection.execute('SELECT body FROM snapshots WHERE result_id = ?',
                             (result_id,)).fetchone()
    if row is None:
        return None
    body = json.loads(row[0])
    return PublishedBook(
        result_id, body['day'], tuple(body['stations']), body['kind'],
        tuple(BookRow(station, Reading.from_wire(value)) for station, value in body['rows']),
        tuple(BookChange(station, direction, Reading.from_wire(value))
              for station, direction, value in body['changes']),
        subscription_id=body['subscription_id'], opening_id=body['opening_id'],
        transfer_id=body['transfer_id'], receipt_id=body['receipt_id'],
    )
