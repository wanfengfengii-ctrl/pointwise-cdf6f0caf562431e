from dataclasses import dataclass
import sqlite3

from exchange.manifest import IssueManifest, decode_manifest, encode_manifest
from receiver.evidence import VerifiedArrival


@dataclass(frozen=True)
class PendingDelivery:
    manifest: IssueManifest
    packets: tuple[tuple[str, bytes], ...]

    @property
    def transfer_id(self) -> str:
        return self.manifest.transfer_id

    def missing_stations(self) -> tuple[str, ...]:
        arrived = {station for station, _ in self.packets}
        return tuple(piece.station for piece in self.manifest.commitments if piece.station not in arrived)


def create_pending(connection: sqlite3.Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS pending_deliveries (
            subscription_id TEXT NOT NULL REFERENCES subscriptions(subscription_id),
            transfer_id TEXT NOT NULL,
            manifest BLOB NOT NULL,
            PRIMARY KEY (subscription_id, transfer_id)
        )
    ''')
    connection.execute('''
        CREATE TABLE IF NOT EXISTS pending_packets (
            subscription_id TEXT NOT NULL,
            transfer_id TEXT NOT NULL,
            station TEXT NOT NULL,
            encoded BLOB NOT NULL,
            PRIMARY KEY (subscription_id, transfer_id, station),
            FOREIGN KEY (subscription_id, transfer_id)
                REFERENCES pending_deliveries(subscription_id, transfer_id) ON DELETE CASCADE
        )
    ''')


def load_pending(connection: sqlite3.Connection, subscription_id: str,
                 transfer_id: str) -> PendingDelivery | None:
    rows = connection.execute('''
        SELECT d.manifest, p.station, p.encoded
        FROM pending_deliveries AS d LEFT JOIN pending_packets AS p
          USING (subscription_id, transfer_id)
        WHERE d.subscription_id = ? AND d.transfer_id = ? ORDER BY p.station
    ''', (subscription_id, transfer_id)).fetchall()
    if not rows:
        return None
    manifest = decode_manifest(rows[0][0])
    return PendingDelivery(manifest, tuple((station, encoded) for _, station, encoded in rows
                                           if station is not None))


def record_pending_piece(connection: sqlite3.Connection, arrival: VerifiedArrival) -> PendingDelivery:
    packet = arrival.packet
    manifest = packet.manifest
    key = (manifest.subscription.subscription_id, manifest.transfer_id)
    pending = load_pending(connection, *key)
    if pending is None:
        connection.execute('''
            INSERT INTO pending_deliveries (subscription_id, transfer_id, manifest) VALUES (?, ?, ?)
        ''', (*key, encode_manifest(manifest)))
    else:
        if pending.manifest != manifest:
            raise ValueError('station packet has a different manifest for the pending transfer')
        existing = dict(pending.packets).get(packet.station)
        if existing is not None:
            if existing != arrival.encoded:
                raise ValueError('station packet differs from the pending original piece')
            return pending
    connection.execute('''
        INSERT INTO pending_packets (subscription_id, transfer_id, station, encoded)
        VALUES (?, ?, ?, ?)
    ''', (*key, packet.station, arrival.encoded))
    return load_pending(connection, *key)


def clear_pending(connection: sqlite3.Connection, subscription_id: str, transfer_id: str) -> None:
    connection.execute('''
        DELETE FROM pending_deliveries WHERE subscription_id = ? AND transfer_id = ?
    ''', (subscription_id, transfer_id))
