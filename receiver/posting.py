import sqlite3

from audit.snapshots import (BookChange, BookRow, PublishedBook, apply_changes, atomic,
                             create_snapshots, load_snapshot, write_snapshot)
from exchange.checkpoint import decode_checkpoint
from exchange.subscriptions import (FrozenSubscription, create_subscriptions, load_subscription,
                                    store_subscription)
from lake.readings import content_id, digest
from receiver.evidence import VerifiedArrival
from receiver.pending import clear_pending, create_pending, load_pending
from receiver.receipts import FirstReceipt, create_receipts, load_first_receipt, write_first_receipt


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
                PRIMARY KEY (subscription_id, reading_key)
            )
        ''')
        connection.execute('''
            CREATE TABLE IF NOT EXISTS checkpoint_adoptions (
                subscription_id TEXT PRIMARY KEY REFERENCES subscriptions(subscription_id),
                piece_digest TEXT NOT NULL UNIQUE,
                piece BLOB NOT NULL,
                opening_id TEXT NOT NULL,
                cutoff INTEGER NOT NULL,
                result_id TEXT NOT NULL UNIQUE REFERENCES snapshots(result_id)
            )
        ''')
        connection.execute('''
            CREATE TABLE IF NOT EXISTS checkpoint_coverage (
                subscription_id TEXT NOT NULL REFERENCES checkpoint_adoptions(subscription_id)
                    ON DELETE CASCADE,
                transfer_id TEXT NOT NULL,
                station TEXT NOT NULL,
                packet_id TEXT NOT NULL,
                result_id TEXT NOT NULL REFERENCES snapshots(result_id),
                PRIMARY KEY (subscription_id, transfer_id, station)
            )
        ''')
        _migrate_progress_without_receipt_foreign_key(connection)


def _migrate_progress_without_receipt_foreign_key(connection: sqlite3.Connection) -> None:
    foreign_keys = connection.execute('PRAGMA foreign_key_list(link_row_progress)').fetchall()
    if not any(row[2] == 'first_receipts' for row in foreign_keys):
        return
    connection.execute('ALTER TABLE link_row_progress RENAME TO legacy_link_row_progress')
    connection.execute('''
        CREATE TABLE link_row_progress (
            subscription_id TEXT NOT NULL REFERENCES links(subscription_id),
            reading_key BLOB NOT NULL,
            transfer_id TEXT NOT NULL,
            PRIMARY KEY (subscription_id, reading_key)
        )
    ''')
    connection.execute('''
        INSERT INTO link_row_progress (subscription_id, reading_key, transfer_id)
        SELECT subscription_id, reading_key, transfer_id
        FROM legacy_link_row_progress
    ''')
    connection.execute('DROP TABLE legacy_link_row_progress')


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
    return apply_changes(current.rows, changes)


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


def load_checkpoint_adoption(connection: sqlite3.Connection, subscription_id: str):
    return connection.execute('''
        SELECT piece, piece_digest, opening_id, cutoff, result_id
        FROM checkpoint_adoptions WHERE subscription_id = ?
    ''', (subscription_id,)).fetchone()


def checkpoint_packet_identity(connection: sqlite3.Connection, subscription_id: str,
                               transfer_id: str, station: str):
    return connection.execute('''
        SELECT packet_id FROM checkpoint_coverage
        WHERE subscription_id = ? AND transfer_id = ? AND station = ?
    ''', (subscription_id, transfer_id, station)).fetchone()


def adopt_checkpoint(connection: sqlite3.Connection, subscription_id: str,
                     encoded_piece: bytes) -> str:
    piece = decode_checkpoint(encoded_piece)
    if piece.subscription.subscription_id != subscription_id:
        raise ValueError('checkpoint belongs to a different subscription id')
    with atomic(connection):
        subscription = load_subscription(connection, subscription_id)
        if piece.subscription != subscription:
            raise ValueError('checkpoint frozen subscription terms do not match the local one')

        existing = connection.execute('''
            SELECT piece, result_id FROM checkpoint_adoptions WHERE subscription_id = ?
        ''', (subscription_id,)).fetchone()
        if existing is not None:
            if existing[0] == encoded_piece:
                return existing[1]
            raise ValueError('subscription has already adopted a different checkpoint piece')

        link = connection.execute('''
            SELECT opening_result_id, current_result_id FROM links WHERE subscription_id = ?
        ''', (subscription_id,)).fetchone()
        if link is None:
            raise KeyError(f'no opening link for subscription {subscription_id}')
        baseline_id, current_id = link
        baseline = load_snapshot(connection, baseline_id)
        current = load_snapshot(connection, current_id)
        if baseline is None or current is None:
            raise KeyError(f'published link account is missing for {subscription_id}')
        if baseline.opening_id != piece.opening_id or current.opening_id != piece.opening_id:
            raise ValueError('checkpoint does not bind the installed original account')

        readings = {row.reading.identity: row.reading for row in piece.opening.rows}
        rows = current.rows
        adopted_changes: list[BookChange] = []
        checkpoint_progress: dict[bytes, str] = {}
        entry_transfers_by_reading: dict[bytes, set[str]] = {}

        for entry in piece.entries:
            manifest = entry.manifest
            transfer_id = manifest.transfer_id
            reading_key = manifest.reading_id.storage_key()
            entry_transfers_by_reading.setdefault(reading_key, set()).add(transfer_id)
            expected_packet_ids = dict(entry.packet_ids)

            receipt = load_first_receipt(connection, subscription_id, transfer_id)
            if receipt is not None:
                if (receipt.issue_id != manifest.issue_id or
                        dict(receipt.piece_ids) != expected_packet_ids):
                    raise ValueError('checkpoint conflicts with an actual settled receipt')
            else:
                pending = load_pending(connection, subscription_id, transfer_id)
                if pending is not None:
                    if pending.manifest != manifest:
                        raise ValueError('checkpoint conflicts with a pending delivery manifest')
                    for station, payload in pending.packets:
                        if digest(payload) != expected_packet_ids.get(station):
                            raise ValueError('checkpoint conflicts with a pending station piece')

                reading = readings[manifest.reading_id]
                changes = tuple(BookChange(piece_commitment.station,
                                          piece_commitment.direction, reading)
                                for piece_commitment in manifest.commitments)
                rows = apply_changes(rows, changes)
                adopted_changes.extend(changes)

            if manifest.commitments:
                checkpoint_progress[reading_key] = transfer_id

        result_id = content_id('checkpoint', [
            current.result_id,
            piece.checkpoint_id,
            [(row.station, row.reading.to_wire()) for row in rows],
            [(change.station, change.direction, change.reading.to_wire())
             for change in adopted_changes],
        ])
        book = PublishedBook(
            result_id, subscription.day, subscription.stations, 'checkpoint',
            rows, tuple(adopted_changes), subscription_id=subscription_id,
            opening_id=piece.opening_id,
        )
        write_snapshot(connection, book)

        connection.execute('''
            INSERT INTO checkpoint_adoptions
                (subscription_id, piece_digest, piece, opening_id, cutoff, result_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (subscription_id, digest(encoded_piece), encoded_piece,
              piece.opening_id, piece.cutoff, result_id))
        connection.executemany('''
            INSERT INTO checkpoint_coverage
                (subscription_id, transfer_id, station, packet_id, result_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ((subscription_id, entry.manifest.transfer_id, station, packet_id, result_id)
              for entry in piece.entries
              for station, packet_id in entry.packet_ids))

        for reading_key, checkpoint_transfer_id in checkpoint_progress.items():
            previous = connection.execute('''
                SELECT transfer_id FROM link_row_progress
                WHERE subscription_id = ? AND reading_key = ?
            ''', (subscription_id, reading_key)).fetchone()
            if previous is None:
                connection.execute('''
                    INSERT INTO link_row_progress (subscription_id, reading_key, transfer_id)
                    VALUES (?, ?, ?)
                ''', (subscription_id, reading_key, checkpoint_transfer_id))
            elif previous[0] != checkpoint_transfer_id:
                # An actual post-cutoff receipt is newer than this checkpoint and must win.
                if previous[0] in entry_transfers_by_reading.get(reading_key, set()):
                    connection.execute('''
                        UPDATE link_row_progress SET transfer_id = ?
                        WHERE subscription_id = ? AND reading_key = ?
                    ''', (checkpoint_transfer_id, subscription_id, reading_key))

        for entry in piece.entries:
            clear_pending(connection, subscription_id, entry.manifest.transfer_id)

        connection.execute('UPDATE links SET current_result_id = ? WHERE subscription_id = ?',
                           (result_id, subscription_id))
        return result_id
