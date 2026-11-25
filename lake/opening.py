import json
import sqlite3

from audit.snapshots import (BookRow, PublishedBook, atomic, create_snapshots, load_snapshot,
                             original_order, write_snapshot)
from exchange.subscriptions import create_subscriptions
from lake.batches import create_batches, list_day_readings
from lake.readings import (Reading, canonical_json, content_id, normalize_stations,
                           require_day)


_SOURCE_SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS accounting_days (
        day TEXT PRIMARY KEY,
        stations BLOB NOT NULL,
        empty_result_id TEXT NOT NULL REFERENCES snapshots(result_id)
    )''',
    '''CREATE TABLE IF NOT EXISTS transfers (
        source_commit INTEGER PRIMARY KEY,
        transfer_id TEXT NOT NULL UNIQUE,
        day TEXT NOT NULL REFERENCES accounting_days(day),
        reading_key BLOB NOT NULL,
        reading BLOB NOT NULL,
        before_station TEXT NOT NULL,
        after_station TEXT NOT NULL
    )''',
    '''CREATE INDEX IF NOT EXISTS transfer_reading_order
        ON transfers(reading_key, source_commit)''',
    '''CREATE TABLE IF NOT EXISTS current_owners (
        reading_key BLOB PRIMARY KEY,
        station TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS source_postings (
        transfer_id TEXT NOT NULL REFERENCES transfers(transfer_id),
        direction INTEGER NOT NULL CHECK (direction IN (-1, 1)),
        station TEXT NOT NULL,
        net_kg TEXT NOT NULL,
        net_missing INTEGER NOT NULL,
        PRIMARY KEY (transfer_id, direction)
    )''',
    '''CREATE TABLE IF NOT EXISTS issues (
        subscription_id TEXT NOT NULL REFERENCES subscriptions(subscription_id),
        transfer_id TEXT NOT NULL REFERENCES transfers(transfer_id),
        manifest BLOB NOT NULL,
        PRIMARY KEY (subscription_id, transfer_id)
    )''',
    '''CREATE TABLE IF NOT EXISTS issue_packets (
        subscription_id TEXT NOT NULL,
        transfer_id TEXT NOT NULL,
        station TEXT NOT NULL,
        encoded BLOB NOT NULL,
        PRIMARY KEY (subscription_id, transfer_id, station),
        FOREIGN KEY (subscription_id, transfer_id) REFERENCES issues(subscription_id, transfer_id)
    )''',
)


def initialize_source(connection: sqlite3.Connection) -> None:
    with atomic(connection):
        create_snapshots(connection)
        create_subscriptions(connection)
        for statement in _SOURCE_SCHEMA:
            connection.execute(statement)
        create_batches(connection)


def _day_record(connection: sqlite3.Connection, day: str):
    return connection.execute('SELECT stations, empty_result_id FROM accounting_days WHERE day = ?',
                              (day,)).fetchone()


def day_stations(connection: sqlite3.Connection, day: str) -> tuple[str, ...]:
    require_day(day)
    row = _day_record(connection, day)
    if row is None:
        raise KeyError(f'accounting day {day} is not open')
    return tuple(json.loads(row[0]))


def _opening_snapshot(day: str, stations: tuple[str, ...], rows: tuple[BookRow, ...]):
    result_id = content_id('opening', [day, stations, [row.reading.to_wire() for row in rows]])
    return PublishedBook(result_id, day, stations, 'opening', rows)


def open_day(connection: sqlite3.Connection, day: str, stations=('east', 'west')) -> PublishedBook:
    require_day(day)
    requested = normalize_stations(stations)
    with atomic(connection):
        row = _day_record(connection, day)
        if row is not None:
            stations_body, empty_result_id = row
            if tuple(json.loads(stations_body)) != requested:
                raise ValueError('accounting day already has a different station set')
            return load_snapshot(connection, empty_result_id)
        empty = _opening_snapshot(day, requested, ())
        write_snapshot(connection, empty)
        connection.execute('''
            INSERT INTO accounting_days (day, stations, empty_result_id) VALUES (?, ?, ?)
        ''', (day, canonical_json(requested), empty.result_id))
        return empty


def read_opening_rows(connection: sqlite3.Connection, day: str) -> tuple[BookRow, ...]:
    day_stations(connection, day)
    return tuple(BookRow(reading.identity.origin_station, reading)
                 for reading in list_day_readings(connection, day))


def build_opening_book(connection: sqlite3.Connection, day: str,
                       additional: tuple[Reading, ...] = ()) -> PublishedBook:
    stations = day_stations(connection, day)
    rows = read_opening_rows(connection, day)
    incoming = tuple(BookRow(reading.identity.origin_station, reading) for reading in additional)
    return _opening_snapshot(day, stations, tuple(sorted(rows + incoming, key=original_order)))
