from dataclasses import dataclass
import json
import sqlite3

from lake.readings import canonical_json


@dataclass(frozen=True)
class FirstReceipt:
    subscription_id: str
    transfer_id: str
    receipt_id: str
    result_id: str
    issue_id: str
    piece_ids: tuple[tuple[str, str], ...]


def create_receipts(connection: sqlite3.Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS first_receipts (
            subscription_id TEXT NOT NULL REFERENCES subscriptions(subscription_id),
            transfer_id TEXT NOT NULL,
            receipt_id TEXT NOT NULL UNIQUE,
            result_id TEXT NOT NULL UNIQUE REFERENCES snapshots(result_id),
            issue_id TEXT NOT NULL,
            piece_ids BLOB NOT NULL,
            PRIMARY KEY (subscription_id, transfer_id)
        )
    ''')


def load_first_receipt(connection: sqlite3.Connection, subscription_id: str,
                       transfer_id: str) -> FirstReceipt | None:
    row = connection.execute('''
        SELECT receipt_id, result_id, issue_id, piece_ids FROM first_receipts
        WHERE subscription_id = ? AND transfer_id = ?
    ''', (subscription_id, transfer_id)).fetchone()
    if row is None:
        return None
    receipt_id, result_id, issue_id, pieces = row
    return FirstReceipt(subscription_id, transfer_id, receipt_id, result_id, issue_id,
                        tuple((station, identity) for station, identity in json.loads(pieces)))


def write_first_receipt(connection: sqlite3.Connection, receipt: FirstReceipt) -> None:
    connection.execute('''
        INSERT INTO first_receipts (subscription_id, transfer_id, receipt_id,
                                    result_id, issue_id, piece_ids)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (receipt.subscription_id, receipt.transfer_id, receipt.receipt_id, receipt.result_id,
          receipt.issue_id, canonical_json(receipt.piece_ids)))
