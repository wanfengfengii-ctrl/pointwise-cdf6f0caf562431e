import sqlite3

from audit.snapshots import (BookChange, BookRow, PublishedBook, atomic, create_snapshots,
                             load_snapshot, original_order, write_snapshot)
from exchange.subscriptions import FrozenSubscription, create_subscriptions, store_subscription
from lake.readings import content_id
from receiver.evidence import VerifiedArrival
from receiver.pending import create_pending
from receiver.receipts import FirstReceipt, create_receipts, write_first_receipt


def initialize_receiver(connection: sqlite3.Connection) -> None:
    with atomic(connection):
        create_snapshots(connection)
        create_subscriptions(connection)
        create_pending(connection)
        create_receipts(connection)
        connection.execute('''
            CREATE TABLE IF NOT EXISTS links (
                subscription_id TEXT PRIMARY KEY REFERENCES subscriptions(subscription_id),
                opening_result_id TEXT NOT NULL REFERENCES snapshots(result_id),
                current_result_id TEXT NOT NULL REFERENCES snapshots(result_id)
            )
        ''')
        connection.execute('''
            CREATE TABLE IF NOT EXISTS link_row_progress (
                subscription_id TEXT NOT NULL REFERENCES links(subscription_id),
                reading_key BLOB NOT NULL,
                transfer_id TEXT NOT NULL,
                PRIMARY KEY (subscription_id, reading_key),
                FOREIGN KEY (subscription_id, transfer_id)
                    REFERENCES first_receipts(subscription_id, transfer_id)
            )
        ''')


def install_opening_link(connection: sqlite3.Connection, subscription: FrozenSubscription,
                         opening: PublishedBook) -> str:
    if opening.kind != 'opening' or opening.day != subscription.day:
        raise ValueError('a linked opening requires an original book of the same day')
    if not set(subscription.stations).issubset(opening.stations):
        raise ValueError('original book does not cover the subscription stations')
    rows = tuple(row for row in opening.rows if row.station in subscription.stations)
    baseline_id = content_id('linked-opening', [subscription.to_wire(), opening.result_id])
    baseline = PublishedBook(baseline_id, subscription.day, subscription.stations, 'linked', rows,
                             subscription_id=subscription.subscription_id, opening_id=opening.result_id)
    with atomic(connection):
        store_subscription(connection, subscription)
        previous = connection.execute('''
            SELECT opening_result_id FROM links WHERE subscription_id = ?
        ''', (subscription.subscription_id,)).fetchone()
        if previous is not None:
            if load_snapshot(connection, previous[0]) != baseline:
                raise ValueError('subscription already has a different local opening')
            return previous[0]
        write_snapshot(connection, baseline)
        connection.execute('''
            INSERT INTO links (subscription_id, opening_result_id, current_result_id) VALUES (?, ?, ?)
        ''', (subscription.subscription_id, baseline_id, baseline_id))
        return baseline_id


def current_linked_result(connection: sqlite3.Connection, subscription_id: str) -> str:
    row = connection.execute('SELECT current_result_id FROM links WHERE subscription_id = ?',
                             (subscription_id,)).fetchone()
    if row is None:
        raise KeyError(f'no opening link for subscription {subscription_id}')
    return row[0]


def _apply_changes(current: PublishedBook, changes: tuple[BookChange, ...]) -> tuple[BookRow, ...]:
    rows = {row.reading.identity: row for row in current.rows}
    for change in sorted(changes, key=lambda item: item.direction):
        identity = change.reading.identity
        row = BookRow(change.station, change.reading)
        if change.direction == -1:
            if rows.get(identity) != row:
                raise ValueError('withdrawal differs from the current linked original row')
            del rows[identity]
        else:
            if identity in rows:
                raise ValueError('arrival duplicates a current linked original row')
            rows[identity] = row
    return tuple(sorted(rows.values(), key=original_order))


def publish_linked_book(connection: sqlite3.Connection, subscription: FrozenSubscription,
                        arrivals: tuple[VerifiedArrival, ...]) -> FirstReceipt:
    if not arrivals:
        raise ValueError('posting requires at least one verified station piece')
    ordered = tuple(sorted(arrivals, key=lambda item: item.packet.station))
    first = ordered[0].packet
    manifest = first.manifest
    if manifest.subscription != subscription:
        raise ValueError('posting scope differs from the local subscription')
    if any(item.packet.manifest != manifest or item.packet.reading != first.reading for item in ordered):
        raise ValueError('station pieces disagree about the original reading or issue')
    if len({item.packet.station for item in ordered}) != len(ordered):
        raise ValueError('posting contains duplicate stations')

    subscription_id = subscription.subscription_id
    reading_key = first.reading.identity.storage_key()
    progress = connection.execute('''
        SELECT transfer_id FROM link_row_progress WHERE subscription_id = ? AND reading_key = ?
    ''', (subscription_id, reading_key)).fetchone()
    previous_transfer = progress[0] if progress is not None else None
    if previous_transfer != manifest.predecessor:
        raise ValueError('the original row has a different posted predecessor')

    current = load_snapshot(connection, current_linked_result(connection, subscription_id))
    changes = tuple(BookChange(item.packet.station, item.packet.direction, item.packet.reading)
                    for item in ordered)
    rows = _apply_changes(current, changes)
    piece_ids = tuple((item.packet.station, item.packet_id) for item in ordered)
    issue_id = manifest.issue_id
    result_id = content_id('linked', [current.result_id, issue_id, piece_ids])
    receipt_id = content_id('receipt', [subscription_id, manifest.transfer_id, result_id])
    receipt = FirstReceipt(subscription_id, manifest.transfer_id, receipt_id, result_id,
                           issue_id, piece_ids)
    book = PublishedBook(result_id, subscription.day, subscription.stations, 'linked', rows, changes,
                         subscription_id=subscription_id, opening_id=current.opening_id,
                         transfer_id=manifest.transfer_id, receipt_id=receipt_id)
    write_snapshot(connection, book)
    write_first_receipt(connection, receipt)
    connection.execute('''
        INSERT INTO link_row_progress (subscription_id, reading_key, transfer_id) VALUES (?, ?, ?)
        ON CONFLICT (subscription_id, reading_key) DO UPDATE SET transfer_id = excluded.transfer_id
    ''', (subscription_id, reading_key, manifest.transfer_id))
    connection.execute('UPDATE links SET current_result_id = ? WHERE subscription_id = ?',
                       (result_id, subscription_id))
    return receipt
