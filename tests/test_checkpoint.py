import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from audit.handover import collect_handover
from audit.reconcile import reconcile_result
from audit.snapshots import connect_database
from checkpoint_scenario import DISTRICT, check_v1, current_view, deny_commit, prepare_source
from checkpoint_demo import run_demo
from exchange.checkpoint import decode_checkpoint
from exchange.issue import issue_checkpoint, issue_transfer
from exchange.subscriptions import register_subscription
from lake.readings import canonical_json, digest
from receiver.pending import load_pending
from receiver.posting import (adopt_checkpoint, current_linked_result,
                              initialize_receiver, install_opening_link)
from receiver.receive import receive_packet
from receiver.receipts import load_first_receipt


class ProvidedScenarioTests(unittest.TestCase):
    def test_v1_fixture_contract_still_passes(self):
        check_v1(None)


class CheckpointDemoTests(unittest.TestCase):
    def test_demo_reads_repaired_west_account_and_coverage(self):
        with TemporaryDirectory(prefix='checkpoint-demo-test-') as directory:
            result = run_demo(Path(directory))
        self.assertEqual(result['repair_kg'], {'east': 0, 'west': 11})
        self.assertTrue(result['r_still_outside'])
        self.assertEqual(result['T2_west_coverage'][0], 'covered')
        self.assertEqual(result['after_T5_return_kg'], {'east': 7, 'west': 11})


class CheckpointIssueTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(prefix='checkpoint-issue-')
        self.path = Path(self.temp.name) / 'source.sqlite3'
        self.source = connect_database(self.path)
        self.day = prepare_source(self.source)
        self.issues = {key: issue_transfer(self.source, DISTRICT, key)
                       for key in self.day.transfers}

    def tearDown(self):
        self.source.close()
        self.temp.cleanup()

    def test_checkpoint_bytes_are_stable_and_bind_exact_pieces(self):
        cutoff = self.day.transfers['T2'].source_commit
        piece = issue_checkpoint(self.source, DISTRICT, self.day.opening.result_id, cutoff)
        decoded = decode_checkpoint(piece)
        self.assertEqual([entry.manifest.transfer_id for entry in decoded.entries],
                         ['T1', 'T2'])
        cutoff_kg = {station: measure.kg
                     for station, measure in reconcile_result(decoded.cutoff_book).stations.items()}
        self.assertEqual(cutoff_kg, {'east': 0, 'west': 18})

        for entry in decoded.entries:
            expected = {station: digest(encoded)
                        for station, encoded in self.issues[entry.manifest.transfer_id].packets}
            self.assertEqual(dict(entry.packet_ids), expected)

        self.source.close()
        reopened = connect_database(self.path)
        try:
            self.assertEqual(
                issue_checkpoint(reopened, DISTRICT, self.day.opening.result_id, cutoff), piece)
        finally:
            reopened.close()

    def test_invalid_cutoff_is_value_error(self):
        with self.assertRaises(ValueError):
            issue_checkpoint(self.source, DISTRICT, self.day.opening.result_id, -1)
        with self.assertRaises(ValueError):
            issue_checkpoint(self.source, DISTRICT, self.day.opening.result_id, 999)
        with self.assertRaises(ValueError):
            issue_checkpoint(self.source, DISTRICT, self.day.opening.result_id, True)


class CheckpointAdoptionTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(prefix='checkpoint-adopt-')
        self.root = Path(self.temp.name)
        self.source_path = self.root / 'source.sqlite3'
        self.local_path = self.root / 'local.sqlite3'
        self.source = connect_database(self.source_path)
        self.day = prepare_source(self.source)
        self.issues = {key: issue_transfer(self.source, DISTRICT, key)
                       for key in self.day.transfers}
        self.piece_t2 = issue_checkpoint(
            self.source, DISTRICT, self.day.opening.result_id, 2)
        self.piece_t3 = issue_checkpoint(
            self.source, DISTRICT, self.day.opening.result_id, 3)
        self.source.close()
        self.local = connect_database(self.local_path)
        initialize_receiver(self.local)
        install_opening_link(self.local, self.day.subscription, self.day.opening)

    def tearDown(self):
        self.local.close()
        self.temp.cleanup()

    def packet(self, key, station):
        return dict(self.issues[key].packets)[station]

    def send(self, key, station):
        return receive_packet(self.local, DISTRICT, self.packet(key, station))

    def finish(self, key):
        result = None
        for _, encoded in self.issues[key].packets:
            result = receive_packet(self.local, DISTRICT, encoded)
        return result

    def test_earlier_checkpoint_repairs_missing_s_but_keeps_actual_exit(self):
        first = self.finish('T1')
        waiting_t2 = self.send('T2', 'east')
        t3_receipt = self.finish('T3')
        future_t6 = self.send('T6', 'east')
        self.assertEqual(current_view(self.local),
                         ({'east': 11, 'west': 0}, [('raw/S', 'east', 11)]))

        account = adopt_checkpoint(self.local, DISTRICT, self.piece_t2)
        self.assertEqual(current_linked_result(self.local, DISTRICT), account)
        self.assertEqual(current_view(self.local),
                         ({'east': 0, 'west': 11}, [('raw/S', 'west', 11)]))
        report = collect_handover(self.local, account)
        self.assertEqual(report.book.kind, 'checkpoint')
        self.assertEqual([(row.reading.origin, row.station, row.reading.kg)
                          for row in report.book.rows], [('raw/S', 'west', 11)])

        self.assertEqual(self.send('T1', 'east'), first)
        self.assertEqual(self.send('T3', 'west'), t3_receipt)
        self.assertEqual(self.send('T2', 'west'), ('covered', account))
        self.assertEqual(self.send('T2', 'east'), ('covered', account))
        self.assertIsNone(load_first_receipt(self.local, DISTRICT, 'T2'))
        self.assertIsNone(load_pending(self.local, DISTRICT, 'T2'))
        self.assertEqual(load_pending(self.local, DISTRICT, 'T6'), future_t6)
        self.assertEqual(future_t6.missing_stations(), ('west',))

        t6_receipt = self.send('T6', 'west')
        self.assertEqual(current_view(self.local),
                         ({'east': 11, 'west': 0}, [('raw/S', 'east', 11)]))
        self.assertEqual(adopt_checkpoint(self.local, DISTRICT, self.piece_t2), account)
        self.assertEqual(current_linked_result(self.local, DISTRICT), t6_receipt.result_id)
        self.assertEqual(self.send('T2', 'west'), ('covered', account))
        self.assertEqual(waiting_t2.transfer_id, 'T2')

    def test_failed_adoption_commit_preserves_state_and_reopen_succeeds(self):
        self.finish('T1')
        waiting_t2 = self.send('T2', 'east')
        self.finish('T3')
        future_t6 = self.send('T6', 'east')
        before = current_view(self.local)
        current = current_linked_result(self.local, DISTRICT)

        with deny_commit(self.local):
            with self.assertRaises(sqlite3.DatabaseError):
                adopt_checkpoint(self.local, DISTRICT, self.piece_t2)
        self.assertFalse(self.local.in_transaction)
        self.assertEqual(current_view(self.local), before)
        self.assertEqual(current_linked_result(self.local, DISTRICT), current)
        self.assertEqual(load_pending(self.local, DISTRICT, 'T2'), waiting_t2)
        self.assertEqual(load_pending(self.local, DISTRICT, 'T6'), future_t6)

        self.local.close()
        reopened = connect_database(self.local_path)
        initialize_receiver(reopened)
        self.assertEqual(current_view(reopened), before)
        account = adopt_checkpoint(reopened, DISTRICT, self.piece_t2)
        self.assertEqual(current_view(reopened),
                         ({'east': 0, 'west': 11}, [('raw/S', 'west', 11)]))
        t6_receipt = receive_packet(
            reopened, DISTRICT, self.packet('T6', 'west'))
        self.assertEqual(t6_receipt.transfer_id, 'T6')
        self.assertEqual(adopt_checkpoint(reopened, DISTRICT, self.piece_t2), account)
        reopened.close()

        audit_connection = connect_database(self.local_path)
        try:
            report = collect_handover(audit_connection, account)
            self.assertEqual({s: m.kg for s, m in report.accounts.stations.items()},
                             {'east': 0, 'west': 11})
        finally:
            audit_connection.close()

    def test_fresh_t3_covered_exit_keeps_t5_visible_predecessor(self):
        account = adopt_checkpoint(self.local, DISTRICT, self.piece_t3)
        self.assertEqual(current_view(self.local),
                         ({'east': 0, 'west': 11}, [('raw/S', 'west', 11)]))
        self.assertEqual(self.send('T3', 'west'), ('covered', account))
        self.assertIsNone(load_first_receipt(self.local, DISTRICT, 'T3'))
        t5 = self.send('T5', 'east')
        self.assertEqual(t5.transfer_id, 'T5')
        self.assertEqual(current_view(self.local),
                         ({'east': 7, 'west': 11},
                          [('raw/R', 'east', 7), ('raw/S', 'west', 11)]))

    def test_zero_and_missing_r_preserve_measurement_kind(self):
        for r_kg, expected_kg, expected_missing in ((0, 0, 0), (None, 0, 1)):
            source_path = self.root / f'source-{r_kg}.sqlite3'
            local_path = self.root / f'local-{r_kg}.sqlite3'
            source = connect_database(source_path)
            day = prepare_source(source, r_kg=r_kg)
            issue_transfer(source, DISTRICT, 'T3')
            issue_transfer(source, DISTRICT, 'T5')
            piece = issue_checkpoint(source, DISTRICT, day.opening.result_id, 3)
            source.close()

            local = connect_database(local_path)
            initialize_receiver(local)
            install_opening_link(local, day.subscription, day.opening)
            account = adopt_checkpoint(local, DISTRICT, piece)
            # Reopen source only to obtain the deterministic issued T3/T5 bytes.
            source = connect_database(source_path)
            t3_packet = dict(issue_transfer(source, DISTRICT, 'T3').packets)['west']
            t5_packet = dict(issue_transfer(source, DISTRICT, 'T5').packets)['east']
            source.close()

            self.assertEqual(receive_packet(local, DISTRICT, t3_packet),
                             ('covered', account))
            t5 = receive_packet(local, DISTRICT, t5_packet)
            report = collect_handover(local, t5.result_id)
            self.assertEqual({s: m.kg for s, m in report.accounts.stations.items()},
                             {'east': expected_kg, 'west': 11})
            self.assertEqual(report.accounts.stations['east'].missing, expected_missing)
            r_row = next(row for row in report.book.rows if row.station == 'east')
            self.assertIs(r_row.reading.kg, r_kg)
            self.assertEqual(adopt_checkpoint(local, DISTRICT, piece), account)
            local.close()

    def test_other_checkpoint_and_corrupt_or_wrong_materials_are_rejected(self):
        account = adopt_checkpoint(self.local, DISTRICT, self.piece_t2)
        with self.assertRaises(ValueError):
            adopt_checkpoint(self.local, DISTRICT, self.piece_t3)
        with self.assertRaises(ValueError):
            adopt_checkpoint(self.local, DISTRICT, b'{"not":"checkpoint"}')

        changed = json.loads(self.piece_t2)
        changed['cutoff'] = 3
        with self.assertRaises(ValueError):
            adopt_checkpoint(self.local, DISTRICT, canonical_json(changed))
        self.assertEqual(adopt_checkpoint(self.local, DISTRICT, self.piece_t2), account)

        wrong_packet = json.loads(self.packet('T2', 'west'))
        wrong_packet['body']['reading']['origin'] = 'raw/not-S'
        with self.assertRaises(ValueError):
            receive_packet(self.local, DISTRICT, canonical_json(wrong_packet))
        self.assertEqual(self.send('T2', 'west'), ('covered', account))

    def test_wrong_subscription_and_opening_are_rejected(self):
        other_sub = 'other-subscription'
        source = connect_database(self.source_path)
        register_subscription(source, other_sub, self.day.subscription.day, ('east', 'west'))
        wrong_sub_piece = issue_checkpoint(
            source, other_sub, self.day.opening.result_id, 2)
        source.close()
        with self.assertRaises(ValueError):
            adopt_checkpoint(self.local, DISTRICT, wrong_sub_piece)

        other_path = self.root / 'other-source.sqlite3'
        other_source = connect_database(other_path)
        other_day = prepare_source(other_source, r_kg=0)
        wrong_opening_piece = issue_checkpoint(
            other_source, DISTRICT, other_day.opening.result_id, 2)
        other_source.close()
        with self.assertRaises(ValueError):
            adopt_checkpoint(self.local, DISTRICT, wrong_opening_piece)


if __name__ == '__main__':
    unittest.main()
