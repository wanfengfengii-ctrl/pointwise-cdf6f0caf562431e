from dataclasses import dataclass
import sqlite3

from audit.lookup import lookup_result
from audit.snapshots import atomic, load_snapshot, write_snapshot
from exchange.checkpoint import (CheckpointEntry, CheckpointPiece, _validate_fixed_opening,
                                 build_cutoff_book, encode_checkpoint)
from exchange.eligibility import project_obligations
from exchange.manifest import IssueManifest, build_manifest, decode_manifest, encode_manifest
from exchange.packets import StationPacket, encode_packet, encode_station_body
from exchange.subscriptions import load_subscription
from lake.readings import digest, require_name
from lake.source_audit import lookup_formal_transfer, previous_visible_transfer


@dataclass(frozen=True)
class IssuedDelivery:
    manifest: IssueManifest
    packets: tuple[tuple[str, bytes], ...]


def _find_issue(connection: sqlite3.Connection, subscription_id: str,
                transfer_id: str) -> IssuedDelivery | None:
    key = (subscription_id, transfer_id)
    row = connection.execute('''
        SELECT manifest FROM issues WHERE subscription_id = ? AND transfer_id = ?
    ''', key).fetchone()
    if row is None:
        return None
    packets = connection.execute('''
        SELECT station, encoded FROM issue_packets
        WHERE subscription_id = ? AND transfer_id = ? ORDER BY station
    ''', key).fetchall()
    return IssuedDelivery(decode_manifest(row[0]), tuple(packets))


def load_issue(connection: sqlite3.Connection, subscription_id: str, transfer_id: str) -> IssuedDelivery:
    require_name(subscription_id, 'subscription id')
    require_name(transfer_id, 'transfer id')
    issue = _find_issue(connection, subscription_id, transfer_id)
    if issue is None:
        raise KeyError(f'no issue for {subscription_id}/{transfer_id}')
    return issue


def issue_transfer(connection: sqlite3.Connection, subscription_id: str,
                   transfer_id: str) -> IssuedDelivery:
    require_name(subscription_id, 'subscription id')
    require_name(transfer_id, 'transfer id')
    with atomic(connection):
        previous = _find_issue(connection, subscription_id, transfer_id)
        if previous is not None:
            return previous
        subscription = load_subscription(connection, subscription_id)
        transfer = lookup_formal_transfer(connection, transfer_id)
        obligations = project_obligations(transfer, subscription)
        bodies = {item.station: encode_station_body(item.station, item.direction, transfer.reading)
                  for item in obligations}
        predecessor = previous_visible_transfer(connection, transfer, subscription.stations)
        manifest = build_manifest(subscription, transfer, bodies, predecessor)
        packets = tuple((item.station, encode_packet(StationPacket(
            manifest, item.station, item.direction, transfer.reading))) for item in obligations)
        connection.execute('''
            INSERT INTO issues (subscription_id, transfer_id, manifest) VALUES (?, ?, ?)
        ''', (subscription_id, transfer_id, encode_manifest(manifest)))
        connection.executemany('''
            INSERT INTO issue_packets (subscription_id, transfer_id, station, encoded)
            VALUES (?, ?, ?, ?)
        ''', ((subscription_id, transfer_id, station, encoded) for station, encoded in packets))
        return IssuedDelivery(manifest, packets)


def issue_checkpoint(connection: sqlite3.Connection, subscription_id: str,
                     opening_id: str, cutoff: int) -> bytes:
    require_name(subscription_id, 'subscription id')
    if type(cutoff) is not int or cutoff < 0:
        raise ValueError('checkpoint cutoff must be zero or a formal source commit')
    subscription = load_subscription(connection, subscription_id)
    opening = lookup_result(connection, opening_id)

    # Validate the fixed scope before creating any V1 issue records.
    _validate_fixed_opening(subscription, opening)

    if cutoff != 0:
        cutoff_row = connection.execute('''
            SELECT day FROM transfers WHERE source_commit = ?
        ''', (cutoff,)).fetchone()
        if cutoff_row is None or cutoff_row[0] != subscription.day:
            raise ValueError('checkpoint cutoff is not a formal commit on this day')

    transfer_ids = [row[0] for row in connection.execute('''
        SELECT transfer_id FROM transfers
        WHERE day = ? AND source_commit <= ?
        ORDER BY source_commit
    ''', (subscription.day, cutoff))]
    roster = {row.reading.identity for row in opening.rows}

    entries = []
    for transfer_id in transfer_ids:
        transfer = lookup_formal_transfer(connection, transfer_id)
        if transfer.reading.identity not in roster:
            continue
        issued = issue_transfer(connection, subscription_id, transfer_id)
        packet_ids = tuple((station, digest(encoded)) for station, encoded in issued.packets)
        entries.append(CheckpointEntry(issued.manifest, packet_ids))

    entries = tuple(entries)
    cutoff_book = build_cutoff_book(subscription, opening, cutoff, entries)
    if load_snapshot(connection, cutoff_book.result_id) is None:
        with atomic(connection):
            write_snapshot(connection, cutoff_book)
    piece = CheckpointPiece(subscription, opening, cutoff, cutoff_book, entries)
    return encode_checkpoint(piece)
