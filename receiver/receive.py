import sqlite3

from audit.snapshots import atomic
from exchange.subscriptions import load_subscription
from receiver.evidence import verify_arrival
from receiver.pending import PendingDelivery, clear_pending, record_pending_piece
from receiver.posting import current_linked_result, publish_linked_book
from receiver.receipts import FirstReceipt, load_first_receipt


def receive_packet(connection: sqlite3.Connection, subscription_id: str,
                   encoded: bytes) -> PendingDelivery | FirstReceipt:
    with atomic(connection):
        subscription = load_subscription(connection, subscription_id)
        arrival = verify_arrival(subscription, encoded)
        packet = arrival.packet
        transfer_id = packet.manifest.transfer_id
        receipt = load_first_receipt(connection, subscription_id, transfer_id)
        if receipt is not None:
            if (receipt.issue_id != packet.manifest.issue_id or
                    dict(receipt.piece_ids).get(packet.station) != arrival.packet_id):
                raise ValueError('station packet differs from the settled issue')
            return receipt
        current_linked_result(connection, subscription_id)
        pending = record_pending_piece(connection, arrival)
        if pending.missing_stations():
            return pending
        arrivals = tuple(verify_arrival(subscription, payload) for _, payload in pending.packets)
        receipt = publish_linked_book(connection, subscription, arrivals)
        clear_pending(connection, subscription_id, transfer_id)
        return receipt
