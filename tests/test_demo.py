import json
import sqlite3
import unittest

from audit.handover import collect_handover
from receiver.pending import PendingDelivery
from receiver.receive import receive_packet
from receiver.receipts import FirstReceipt, load_first_receipt
from tests.support import BATCH, DAY, DISTRICT, EAST_ONLY, TRANSFER_ID, ProjectCase, station_kg


class WorkingDayDemo(ProjectCase):
    def test_offline_shift_handover(self):
        receiver = self.open_receiver()
        day = self.dispatch_round(receiver)
        with self.assertRaises(sqlite3.ProgrammingError):
            self.source.execute('SELECT 1')

        waiting = receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'east'))
        self.assertIsInstance(waiting, PendingDelivery)
        self.assertEqual(waiting.transfer_id, TRANSFER_ID)
        old_while_waiting = self.current_report(receiver, DISTRICT)
        actual = {
            'missing': list(waiting.missing_stations()),
            'stations': station_kg(old_while_waiting),
            'has_receipt': load_first_receipt(receiver, DISTRICT, TRANSFER_ID) is not None,
        }
        expected = {'missing': ['west'], 'stations': {'east': 7, 'west': 0}, 'has_receipt': False}
        self.assertEqual(actual, expected)

        single = receive_packet(receiver, EAST_ONLY, day.piece(EAST_ONLY, 'east'))
        district = receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'west'))
        self.assertIsInstance(single, FirstReceipt)
        self.assertIsInstance(district, FirstReceipt)
        old = collect_handover(receiver, day.openings[DISTRICT])
        new = collect_handover(receiver, district.result_id)
        downstream_actual = {
            'east_only': station_kg(collect_handover(receiver, single.result_id)),
            'district_new': station_kg(new),
            'district_old': station_kg(old),
            'district_net_kg': {station: net.kg for station, net in new.accounts.net.items()},
            'transferred_origin': next(item.origin for item in new.provenance.current
                                       if item.station == 'west'),
            'same_receipt_on_replay': receive_packet(receiver, DISTRICT, day.piece(DISTRICT, 'east')) == district,
        }
        downstream_expected = {
            'east_only': {'east': 0}, 'district_new': {'east': 0, 'west': 7},
            'district_old': {'east': 7, 'west': 0}, 'district_net_kg': {'east': -7, 'west': 7},
            'transferred_origin': 'east-meter-A/0001', 'same_receipt_on_replay': True,
        }
        self.assertEqual(downstream_actual, downstream_expected)
        self.assertEqual(new.book.receipt_id, district.receipt_id)
        self.assertEqual(old.book, old_while_waiting.book)
        scenario = {
            'case_id': 'V6',
            'input': {'day': DAY, 'rows': BATCH['rows'], 'transfer': TRANSFER_ID,
                      'route': ['east', 'west'], 'delivery_order': ['east', 'west']},
            'expected': expected,
            'actual': actual,
            'downstream_expected': downstream_expected,
            'downstream_actual': downstream_actual,
            'code_paths': ['lake/intake.py', 'lake/transfers.py', 'exchange/issue.py',
                           'receiver/receive.py', 'receiver/posting.py', 'audit/handover.py'],
            'oracle': '原东站 7 + 0 = 7 kg，缺测不计。双站缺西时保持原账，接齐后东减 7、西增 7。仅东片已覆盖仅东订阅，本站减 7。',
        }
        print('POINTWISE_SCENARIO ' + json.dumps(scenario, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    unittest.main()
