import json
import sqlite3

from audit.handover import collect_handover
from audit.lookup import UnknownResult, lookup_result
from exchange.issue import issue_transfer
from exchange.subscriptions import load_subscription, register_subscription
from lake.intake import accept_batch
from lake.measurement import StationMeasure
from lake.opening import initialize_source, open_day
from lake.readings import canonical_json
from lake.transfers import commit_transfer
from receiver.pending import PendingDelivery, load_pending
from receiver.posting import current_linked_result, install_opening_link
from receiver.receive import receive_packet
from receiver.receipts import FirstReceipt, load_first_receipt
from tests.support import (DAY, DISTRICT, EAST_ONLY, PEER, STATIONS, TRANSFER_ID, ProjectCase,
                           station_kg)


class HandoverTests(ProjectCase):
    def test_v6_east_first_preserves_old_book_until_actual_west_arrival(self):
        receiver = self.open_receiver()
        day = self.dispatch_round(receiver)
        old_id = day.openings[DISTRICT]
        pending = receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'east'))
        self.assertIsInstance(pending, PendingDelivery)
        self.assertEqual((pending.transfer_id, pending.missing_stations()), (TRANSFER_ID, ('west',)))
        self.assertEqual(current_linked_result(receiver, DISTRICT), old_id)
        self.assertEqual(station_kg(collect_handover(receiver, old_id)), {'east': 7, 'west': 0})
        self.assertIsNone(load_first_receipt(receiver, DISTRICT, TRANSFER_ID))
        with self.assertRaises(UnknownResult):
            collect_handover(receiver, TRANSFER_ID)
        self.assertEqual(receiver.execute('SELECT count(*) FROM snapshots').fetchone()[0], 3)
        receipt = receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'west'))
        self.assertIsInstance(receipt, FirstReceipt)
        self.assertEqual(current_linked_result(receiver, DISTRICT), receipt.result_id)
        posted = collect_handover(receiver, receipt.result_id)
        self.assertEqual(station_kg(posted), {'east': 0, 'west': 7})
        self.assertEqual(posted.accounts.net, {'east': StationMeasure(-7), 'west': StationMeasure(7)})
        self.assertEqual(posted.book.receipt_id, receipt.receipt_id)
        self.assertEqual(posted.book.transfer_id, TRANSFER_ID)
        self.assertIsNone(load_pending(receiver, DISTRICT, TRANSFER_ID))

    def test_v6_duplicate_waiting_piece_and_conflicting_body_do_not_change_progress(self):
        receiver = self.open_receiver()
        day = self.dispatch_round(receiver)
        east = day.piece(DISTRICT, 'east')
        west = day.piece(DISTRICT, 'west')
        pending = receive_packet(receiver, DISTRICT, east)
        self.assertEqual(receive_packet(receiver, DISTRICT, east), pending)
        self.assertEqual(len(load_pending(receiver, DISTRICT, TRANSFER_ID).packets), 1)
        changed = json.loads(west)
        changed['body']['reading']['origin'] = 'equal-weight-wrong-origin'
        with self.assertRaisesRegex(ValueError, 'commitment'):
            receive_packet(receiver, DISTRICT, canonical_json(changed))
        self.assertEqual(load_pending(receiver, DISTRICT, TRANSFER_ID), pending)
        self.assertIsNone(load_first_receipt(receiver, DISTRICT, TRANSFER_ID))
        self.assertIsInstance(receive_packet(receiver, DISTRICT, west), FirstReceipt)

    def test_v7_east_only_settles_offline_and_replays_after_reopen(self):
        receiver = self.open_receiver()
        day = self.dispatch_round(receiver)
        self.assertIsInstance(receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'east')), PendingDelivery)
        east = day.piece(EAST_ONLY, 'east')
        receipt = receive_packet(receiver, EAST_ONLY, east)
        self.assertIsInstance(receipt, FirstReceipt)
        self.assertEqual(station_kg(collect_handover(receiver, receipt.result_id)), {'east': 0})
        self.assertEqual(station_kg(self.current_report(receiver, DISTRICT)), {'east': 7, 'west': 0})
        receiver.close()
        reopened = self.open_receiver()
        self.assertEqual(receive_packet(reopened, EAST_ONLY, east), receipt)
        self.assertEqual(load_first_receipt(reopened, EAST_ONLY, TRANSFER_ID), receipt)
        self.assertEqual(station_kg(collect_handover(reopened, day.openings[EAST_ONLY])), {'east': 7})
        self.assertEqual(reopened.execute('SELECT count(*) FROM first_receipts').fetchone()[0], 1)
        self.assertEqual(reopened.execute('SELECT count(*) FROM snapshots').fetchone()[0], 4)

    def test_v8_pending_restart_wrong_subscription_and_history(self):
        receiver = self.open_receiver()
        day = self.dispatch_round(receiver)
        receiver.close()
        first_arrival = self.receive_in_new_process(DISTRICT, day.piece(DISTRICT, 'west'))
        self.assertEqual(first_arrival, {'transfer_id': TRANSFER_ID, 'missing': ['east']})
        reopened = self.open_receiver()
        pending = load_pending(reopened, DISTRICT, TRANSFER_ID)
        self.assertEqual(pending.missing_stations(), ('east',))
        with self.assertRaisesRegex(ValueError, 'different frozen subscription'):
            receive_packet(reopened, DISTRICT, day.piece(PEER, 'east'))
        self.assertEqual(load_pending(reopened, DISTRICT, TRANSFER_ID), pending)
        self.assertIsNone(load_first_receipt(reopened, DISTRICT, TRANSFER_ID))
        receipt = receive_packet(reopened, DISTRICT, day.piece(DISTRICT, 'east'))
        reopened.close()
        reopened = self.open_receiver()
        self.assertEqual(receive_packet(reopened, DISTRICT, day.piece(DISTRICT, 'west')), receipt)
        self.assertEqual(load_first_receipt(reopened, DISTRICT, TRANSFER_ID), receipt)
        old = collect_handover(reopened, day.openings[DISTRICT])
        new = collect_handover(reopened, receipt.result_id)
        self.assertEqual((station_kg(old), station_kg(new)), ({'east': 7, 'west': 0}, {'east': 0, 'west': 7}))
        self.assertEqual((old.accounts.total_kg, new.accounts.total_kg), (7, 7))
        self.assertEqual(new.book.stations, ('east', 'west'))
        self.assertEqual(new.book.subscription_id, DISTRICT)
        self.assertEqual((old.book.receipt_id, new.book.receipt_id), (None, receipt.receipt_id))
        self.assertEqual((old.book.transfer_id, new.book.transfer_id), (None, TRANSFER_ID))
        self.assertEqual([(item.identity.position, item.station, item.kg)
                          for item in old.provenance.current], [(1, 'east', 7), (2, 'east', 0), (3, 'east', None)])
        self.assertEqual([(item.identity.position, item.station, item.kg)
                          for item in new.provenance.current], [(1, 'west', 7), (2, 'east', 0), (3, 'east', None)])
        self.assertEqual([item.origin for item in new.provenance.current],
                         ['east-meter-A/0001', 'east-meter-A/0002', 'east-meter-A/0003'])
        self.assertEqual(old.provenance.changes, ())
        self.assertEqual([(item.station, item.direction, item.origin) for item in new.provenance.changes],
                         [('east', -1, 'east-meter-A/0001'), ('west', 1, 'east-meter-A/0001')])

    def test_v8_equivalent_reading_from_another_batch_cannot_fill_pending_issue(self):
        unrelated_source = self.connect(self.workspace / 'unrelated.sqlite3')
        initialize_source(unrelated_source)
        open_day(unrelated_source, DAY, STATIONS)
        wrong_batch = accept_batch(unrelated_source, DAY, 'east', 'other-batch',
                                   [{'kg': 7, 'origin': 'east-meter-A/0001'}])
        commit_transfer(unrelated_source, TRANSFER_ID, DAY, wrong_batch.readings[0].identity, 'east', 'west')
        register_subscription(unrelated_source, DISTRICT, DAY, STATIONS)
        wrong_issue = issue_transfer(unrelated_source, DISTRICT, TRANSFER_ID)
        unrelated_source.close()
        receiver = self.open_receiver()
        day = self.dispatch_round(receiver)
        pending = receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'west'))
        receiver.close()
        receiver = self.open_receiver()
        with self.assertRaisesRegex(ValueError, 'different manifest'):
            receive_packet(receiver, DISTRICT, dict(wrong_issue.packets)['east'])
        self.assertEqual(load_pending(receiver, DISTRICT, TRANSFER_ID), pending)
        self.assertIsNone(load_first_receipt(receiver, DISTRICT, TRANSFER_ID))
        correct = receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'east'))
        self.assertEqual(station_kg(collect_handover(receiver, correct.result_id)), {'east': 0, 'west': 7})
        with self.assertRaisesRegex(ValueError, 'settled issue'):
            receive_packet(receiver, DISTRICT, dict(wrong_issue.packets)['east'])
        self.assertEqual(load_first_receipt(receiver, DISTRICT, TRANSFER_ID), correct)

    def test_opening_reinstall_preserves_first_receipt_and_history(self):
        receiver = self.open_receiver()
        batch = self.accept_sample()
        original = lookup_result(self.source, batch.result_id)
        self.transfer_sample(batch)
        subscription = register_subscription(self.source, EAST_ONLY, DAY, ('east',))
        baseline = install_opening_link(receiver, subscription, original)
        piece = dict(issue_transfer(self.source, EAST_ONLY, TRANSFER_ID).packets)['east']
        changed = accept_batch(self.source, DAY, 'east', 'later-batch', [{'kg': 2, 'origin': 'east/later'}])
        different_opening = lookup_result(self.source, changed.result_id)
        self.source.close()
        receipt = receive_packet(receiver, EAST_ONLY, piece)
        receiver.close()
        receiver = self.open_receiver()
        self.assertEqual(install_opening_link(receiver, subscription, original), baseline)
        with self.assertRaises(ValueError):
            install_opening_link(receiver, subscription, different_opening)
        self.assertEqual(load_subscription(receiver, EAST_ONLY), subscription)
        self.assertEqual(current_linked_result(receiver, EAST_ONLY), receipt.result_id)
        self.assertEqual(receive_packet(receiver, EAST_ONLY, piece), receipt)

    def test_failed_receipt_write_rolls_back_new_book_and_last_piece(self):
        receiver = self.open_receiver()
        day = self.dispatch_round(receiver)
        pending = receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'east'))
        receiver.execute('''
            CREATE TEMP TRIGGER deny_receipt BEFORE INSERT ON first_receipts
            BEGIN SELECT RAISE(ABORT, 'receipt write unavailable'); END
        ''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt write unavailable'):
            receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'west'))
        self.assertEqual(load_pending(receiver, DISTRICT, TRANSFER_ID), pending)
        self.assertEqual(current_linked_result(receiver, DISTRICT), day.openings[DISTRICT])
        self.assertEqual(receiver.execute('SELECT count(*) FROM snapshots').fetchone()[0], 3)
        self.assertIsNone(load_first_receipt(receiver, DISTRICT, TRANSFER_ID))
        receiver.execute('DROP TRIGGER deny_receipt')
        receipt = receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'west'))
        self.assertEqual(station_kg(collect_handover(receiver, receipt.result_id)), {'east': 0, 'west': 7})

    def test_consecutive_moves_check_predecessor_and_replay_before_current_state(self):
        batch = self.accept_sample()
        opening = lookup_result(self.source, batch.result_id)
        first = self.transfer_sample(batch)
        commit_transfer(self.source, 'return', DAY, first.reading.identity, 'west', 'east')
        subscription = register_subscription(self.source, EAST_ONLY, DAY, ('east',))
        receiver = self.open_receiver()
        install_opening_link(receiver, subscription, opening)
        leave = dict(issue_transfer(self.source, EAST_ONLY, TRANSFER_ID).packets)['east']
        enter = dict(issue_transfer(self.source, EAST_ONLY, 'return').packets)['east']
        self.source.close()
        with self.assertRaisesRegex(ValueError, 'posted predecessor'):
            receive_packet(receiver, EAST_ONLY, enter)
        self.assertIsNone(load_first_receipt(receiver, EAST_ONLY, 'return'))
        one = receive_packet(receiver, EAST_ONLY, leave)
        two = receive_packet(receiver, EAST_ONLY, enter)
        self.assertEqual(station_kg(collect_handover(receiver, one.result_id)), {'east': 0})
        self.assertEqual(station_kg(collect_handover(receiver, two.result_id)), {'east': 7})
        self.assertEqual(receive_packet(receiver, EAST_ONLY, leave), one)
        self.assertEqual(current_linked_result(receiver, EAST_ONLY), two.result_id)
