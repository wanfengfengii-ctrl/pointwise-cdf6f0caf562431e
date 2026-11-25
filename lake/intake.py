import sqlite3

from audit.snapshots import atomic, write_snapshot
from lake.batches import FrozenBatch, find_batch, write_batch
from lake.opening import build_opening_book, day_stations
from lake.readings import parse_readings


def accept_batch(connection: sqlite3.Connection, day: str, station: str,
                 batch_id: str, rows) -> FrozenBatch:
    readings = parse_readings(day, station, batch_id, rows)
    with atomic(connection):
        previous = find_batch(connection, batch_id)
        if previous is not None:
            if (previous.day, previous.station, previous.readings) != (day, station, readings):
                raise ValueError('batch id already holds a different report')
            return previous
        if station not in day_stations(connection, day):
            raise ValueError('reporting station is outside the accounting day')
        book = build_opening_book(connection, day, readings)
        batch = FrozenBatch(batch_id, day, station, readings, book.result_id)
        write_snapshot(connection, book)
        write_batch(connection, batch)
        return batch
