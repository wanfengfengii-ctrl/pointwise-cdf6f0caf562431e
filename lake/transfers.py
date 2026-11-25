from dataclasses import dataclass
import json
import sqlite3

from audit.snapshots import atomic
from lake.batches import find_frozen_reading
from lake.opening import day_stations
from lake.ownership import read_current_owner, write_current_owner
from lake.readings import Reading, ReadingId, canonical_json, require_day, require_name
from lake.source_ledger import next_source_commit, write_source_postings


@dataclass(frozen=True)
class FormalTransfer:
    transfer_id: str
    day: str
    source_commit: int
    reading: Reading
    before: str
    after: str


def find_formal_transfer(connection: sqlite3.Connection, transfer_id: str) -> FormalTransfer | None:
    row = connection.execute('''
        SELECT day, source_commit, reading, before_station, after_station
        FROM transfers WHERE transfer_id = ?
    ''', (transfer_id,)).fetchone()
    if row is None:
        return None
    day, source_commit, reading, before, after = row
    return FormalTransfer(transfer_id, day, source_commit, Reading.from_wire(json.loads(reading)),
                          before, after)


def commit_transfer(connection: sqlite3.Connection, transfer_id: str, day: str,
                    reading_id: ReadingId, before: str, after: str) -> FormalTransfer:
    require_name(transfer_id, 'transfer id')
    require_day(day)
    require_name(before, 'before station')
    require_name(after, 'after station')
    if not isinstance(reading_id, ReadingId) or reading_id.day != day:
        raise ValueError('a transfer must keep the original accounting day')
    if before == after:
        raise ValueError('a transfer requires two distinct stations')
    with atomic(connection):
        previous = find_formal_transfer(connection, transfer_id)
        if previous is not None:
            if (previous.day, previous.reading.identity, previous.before, previous.after) != (
                    day, reading_id, before, after):
                raise ValueError('transfer id already holds different terms')
            return previous
        stations = day_stations(connection, day)
        if before not in stations or after not in stations:
            raise ValueError('transfer station is outside the accounting day')
        reading = find_frozen_reading(connection, reading_id)
        if reading is None:
            raise ValueError('transfer refers to an unknown original reading')
        if read_current_owner(connection, reading_id) != before:
            raise ValueError('submitted before station is not the current formal owner')
        source_commit = next_source_commit(connection)
        connection.execute('''
            INSERT INTO transfers (source_commit, transfer_id, day, reading_key, reading,
                                   before_station, after_station)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (source_commit, transfer_id, day, reading_id.storage_key(),
              canonical_json(reading.to_wire()), before, after))
        write_current_owner(connection, reading_id, after)
        write_source_postings(connection, transfer_id, reading, before, after)
        return FormalTransfer(transfer_id, day, source_commit, reading, before, after)
