import sqlite3

from lake.measurement import StationMeasure
from lake.readings import require_name
from lake.source_ledger import SourcePosting
from lake.transfers import FormalTransfer, find_formal_transfer


def lookup_formal_transfer(connection: sqlite3.Connection, transfer_id: str) -> FormalTransfer:
    require_name(transfer_id, 'transfer id')
    transfer = find_formal_transfer(connection, transfer_id)
    if transfer is None:
        raise KeyError(f'unknown formal transfer {transfer_id}')
    return transfer


def read_transfer_postings(connection: sqlite3.Connection, transfer_id: str) -> tuple[SourcePosting, ...]:
    lookup_formal_transfer(connection, transfer_id)
    rows = connection.execute('''
        SELECT station, direction, net_kg, net_missing FROM source_postings
        WHERE transfer_id = ? ORDER BY direction
    ''', (transfer_id,))
    return tuple(SourcePosting(station, direction, StationMeasure(int(kg), missing))
                 for station, direction, kg, missing in rows)


def previous_visible_transfer(connection: sqlite3.Connection, transfer: FormalTransfer,
                              stations: tuple[str, ...]) -> str | None:
    history = connection.execute('''
        SELECT transfer_id, before_station, after_station FROM transfers
        WHERE reading_key = ? AND source_commit < ? ORDER BY source_commit DESC
    ''', (transfer.reading.identity.storage_key(), transfer.source_commit))
    for transfer_id, before, after in history:
        if before in stations or after in stations:
            return transfer_id
    return None
