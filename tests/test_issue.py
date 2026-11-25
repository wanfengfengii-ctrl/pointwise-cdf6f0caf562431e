from audit.handover import collect_handover
from audit.lookup import lookup_result
from exchange.issue import issue_transfer, load_issue
from exchange.manifest import decode_manifest, encode_manifest
from exchange.packets import decode_packet, encode_packet
from exchange.subscriptions import load_subscription, register_subscription
from lake.intake import accept_batch
from lake.opening import initialize_source, open_day
from lake.transfers import commit_transfer
from receiver.pending import load_pending
from receiver.posting import current_linked_result, install_opening_link
from receiver.receipts import load_first_receipt
from tests.support import DAY, DISTRICT, EAST_ONLY, TRANSFER_ID, ProjectCase, station_kg


class IssueTests(ProjectCase):
    def test_v5_offline_scope_and_canonical_piece_encodings(self):
        batch = self.accept_sample()
        formal = self.transfer_sample(batch)
        both = register_subscription(self.source, DISTRICT, DAY, ('west', 'east'))
        only_east = register_subscription(self.source, EAST_ONLY, DAY, ('east',))
        double_issue = issue_transfer(self.source, both.subscription_id, TRANSFER_ID)
        single_issue = issue_transfer(self.source, only_east.subscription_id, TRANSFER_ID)
        self.source.close()
        self.assertEqual([station for station, _ in double_issue.packets], ['east', 'west'])
        self.assertEqual([station for station, _ in single_issue.packets], ['east'])
        first_double = decode_packet(dict(double_issue.packets)['east'])
        first_single = decode_packet(dict(single_issue.packets)['east'])
        self.assertEqual(first_double.reading, first_single.reading)
        self.assertEqual(first_double.reading.identity, formal.reading.identity)
        self.assertEqual([(item.station, item.direction) for item in first_double.manifest.commitments],
                         [('east', -1), ('west', 1)])
        self.assertEqual([(item.station, item.direction) for item in first_single.manifest.commitments],
                         [('east', -1)])
        self.assertNotEqual(first_double.manifest.issue_id, first_single.manifest.issue_id)
        for issue in (double_issue, single_issue):
            self.assertEqual(decode_manifest(encode_manifest(issue.manifest)), issue.manifest)
            for _, payload in issue.packets:
                self.assertEqual(decode_packet(payload).manifest, issue.manifest)
                self.assertEqual(encode_packet(decode_packet(payload)), payload)

    def test_v5_registration_is_frozen_even_for_equal_station_sets(self):
        with self.assertRaises(ValueError):
            register_subscription(self.source, DISTRICT, DAY, ())
        with self.assertRaises(KeyError):
            load_subscription(self.source, DISTRICT)
        original = register_subscription(self.source, DISTRICT, DAY, ('west', 'east', 'east'))
        self.assertEqual(original.stations, ('east', 'west'))
        self.assertEqual(register_subscription(self.source, DISTRICT, DAY, {'east', 'west'}), original)
        with self.assertRaises(ValueError):
            register_subscription(self.source, DISTRICT, DAY, ('east',))
        with self.assertRaises(ValueError):
            register_subscription(self.source, DISTRICT, '2026-08-13', original.stations)
        self.assertEqual(load_subscription(self.source, DISTRICT), original)

    def test_v5_reissue_keeps_original_material_after_owner_changes_and_reopen(self):
        formal = self.transfer_sample(self.accept_sample())
        register_subscription(self.source, DISTRICT, DAY, ('east', 'west'))
        first = issue_transfer(self.source, DISTRICT, TRANSFER_ID)
        commit_transfer(self.source, 'move-back', DAY, formal.reading.identity, 'west', 'east')
        self.source.close()
        reopened = self.connect(self.source_path)
        initialize_source(reopened)
        self.assertEqual(load_issue(reopened, DISTRICT, TRANSFER_ID), first)
        self.assertEqual(issue_transfer(reopened, DISTRICT, TRANSFER_ID), first)
        later = issue_transfer(reopened, DISTRICT, 'move-back')
        self.assertEqual(later.manifest.predecessor, TRANSFER_ID)
        self.assertNotEqual(first.manifest.issue_id, later.manifest.issue_id)
        self.assertEqual(decode_packet(dict(later.packets)['west']).direction, -1)

    def test_v5_invalid_issue_does_not_create_material(self):
        register_subscription(self.source, DISTRICT, DAY, ('east', 'west'))
        register_subscription(self.source, 'another-day', '2026-08-13', ('east', 'west'))
        with self.assertRaises(KeyError):
            issue_transfer(self.source, DISTRICT, 'not-committed')
        self.transfer_sample(self.accept_sample())
        with self.assertRaises(ValueError):
            issue_transfer(self.source, 'another-day', TRANSFER_ID)
        with self.assertRaises(KeyError):
            issue_transfer(self.source, 'unknown-subscription', TRANSFER_ID)
        keys = [(DISTRICT, 'not-committed'), ('another-day', TRANSFER_ID),
                ('unknown-subscription', TRANSFER_ID)]
        for subscription_id, transfer_id in keys:
            with self.assertRaises(KeyError):
                load_issue(self.source, subscription_id, transfer_id)
        self.assertEqual(self.source.execute('SELECT count(*) FROM issue_packets').fetchone()[0], 0)

    def test_v5_empty_projection_reissues_without_local_changes(self):
        day = '2026-08-13'
        open_day(self.source, day, ('north', 'east', 'west'))
        batch = accept_batch(self.source, day, 'east', 'another-east', [{'kg': 7, 'origin': 'east/1'}])
        commit_transfer(self.source, 'other-scope', day, batch.readings[0].identity, 'east', 'west')
        subscription = register_subscription(self.source, 'north-only', day, ('north',))
        receiver = self.open_receiver()
        baseline = install_opening_link(receiver, subscription, lookup_result(self.source, batch.result_id))
        issue = issue_transfer(self.source, 'north-only', 'other-scope')
        self.assertEqual(issue.manifest.commitments, ())
        self.assertEqual(issue.packets, ())
        self.assertEqual(load_issue(self.source, 'north-only', 'other-scope'), issue)
        self.assertEqual(issue_transfer(self.source, 'north-only', 'other-scope'), issue)
        self.source.close()
        self.assertEqual(current_linked_result(receiver, 'north-only'), baseline)
        self.assertEqual(station_kg(collect_handover(receiver, baseline)), {'north': 0})
        self.assertIsNone(load_pending(receiver, 'north-only', 'other-scope'))
        self.assertIsNone(load_first_receipt(receiver, 'north-only', 'other-scope'))
