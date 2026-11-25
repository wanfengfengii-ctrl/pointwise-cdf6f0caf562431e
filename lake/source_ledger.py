from contextlib import nullcontext
from dataclasses import dataclass
import sqlite3

from audit.snapshots import atomic
from lake.measurement import StationMeasure, measure_change, measure_readings
from lake.opening import build_opening_book
from lake.readings import Reading


@dataclass(frozen=True)
class SourcePosting:
    station: str
    direction: int
    net: StationMeasure


def next_source_commit(connection: sqlite3.Connection) -> int:
    return connection.execute('SELECT COALESCE(MAX(source_commit), 0) + 1 FROM transfers').fetchone()[0]


def write_source_postings(connection: sqlite3.Connection, transfer_id: str, reading: Reading,
                          before: str, after: str) -> None:
    postings = []
    for station, direction in ((before, -1), (after, 1)):
        net = measure_change(reading, direction)
        postings.append((transfer_id, direction, station, str(net.kg), net.missing))
    connection.executemany('''
        INSERT INTO source_postings (transfer_id, direction, station, net_kg, net_missing)
        VALUES (?, ?, ?, ?, ?)
    ''', postings)


def read_source_station_book(connection: sqlite3.Connection, day: str) -> dict[str, StationMeasure]:
    transaction = nullcontext() if connection.in_transaction else atomic(connection, write=False)
    with transaction:
        opening = build_opening_book(connection, day)
        by_station = {station: [] for station in opening.stations}
        for row in opening.rows:
            by_station[row.station].append(row.reading)
        totals = {station: measure_readings(readings) for station, readings in by_station.items()}
        postings = connection.execute('''
            SELECT p.station, p.net_kg, p.net_missing
            FROM source_postings AS p JOIN transfers AS t USING (transfer_id)
            WHERE t.day = ? ORDER BY t.source_commit, p.direction
        ''', (day,))
        for station, kg, missing in postings:
            totals[station] += StationMeasure(int(kg), missing)
        return totals
