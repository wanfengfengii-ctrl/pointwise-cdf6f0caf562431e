from audit.handover import collect_handover
from audit.lookup import UnknownResult
from lake.batches import find_batch, list_day_readings
from lake.intake import accept_batch
from lake.measurement import StationMeasure
from lake.opening import initialize_source, open_day
from tests.support import BATCH, DAY, STATIONS, ProjectCase, station_kg


class IntakeTests(ProjectCase):
    def test_v1_original_order_zero_missing_and_empty_station(self):
        empty = collect_handover(self.source, self.empty.result_id)
        self.assertEqual(station_kg(empty), {'east': 0, 'west': 0})
        self.assertEqual(empty.provenance.current, ())
        batch = self.accept_sample()
        report = collect_handover(self.source, batch.result_id)
        self.assertEqual(report.accounts.stations,
                         {'east': StationMeasure(7, 1), 'west': StationMeasure(0, 0)})
        self.assertEqual(report.accounts.total_kg, 7)
        self.assertEqual([(row.identity.position, row.kg, row.origin)
                          for row in report.provenance.current],
                         [(1, 7, 'east-meter-A/0001'), (2, 0, 'east-meter-A/0002'),
                          (3, None, 'east-meter-A/0003')])
        self.assertEqual(report.provenance.changes, ())
        self.assertIsNone(report.book.receipt_id)

    def test_v1_initialization_preserves_added_batch_and_original_numbers(self):
        first = self.accept_sample()
        extra = accept_batch(self.source, DAY, 'west', 'added-west',
                             [{'kg': 5, 'origin': 'operator/west/1'}])
        self.source.close()
        reopened = self.connect(self.source_path)
        initialize_source(reopened)
        initialize_source(reopened)
        self.assertEqual(open_day(reopened, DAY, reversed(STATIONS)), self.empty)
        self.assertEqual(find_batch(reopened, first.batch_id), first)
        self.assertEqual(find_batch(reopened, extra.batch_id), extra)
        self.assertEqual(station_kg(collect_handover(reopened, first.result_id)), {'east': 7, 'west': 0})
        report = collect_handover(reopened, extra.result_id)
        self.assertEqual(station_kg(report), {'east': 7, 'west': 5})
        self.assertEqual([(row.identity.batch_id, row.identity.position) for row in report.provenance.current],
                         [('added-west', 1), ('east-day-01', 1), ('east-day-01', 2), ('east-day-01', 3)])
        self.assertEqual(len(list_day_readings(reopened, DAY)), 4)

    def test_v1_unknown_result_and_invalid_dates(self):
        with self.assertRaises(UnknownResult):
            collect_handover(self.source, 'unpublished')
        for value in ('2026-02-30', '20260812', None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                open_day(self.source, value, STATIONS)
        self.assertEqual(self.source.execute('SELECT count(*) FROM accounting_days').fetchone()[0], 1)

    def test_v2_exact_replay_and_changed_weight(self):
        original = self.accept_sample()
        self.assertEqual(self.accept_sample(), original)
        changed = [dict(row) for row in BATCH['rows']]
        changed[0]['kg'] = 11
        with self.assertRaises(ValueError):
            accept_batch(self.source, DAY, 'east', BATCH['id'], changed)
        self.assertEqual(find_batch(self.source, BATCH['id']), original)
        self.assertEqual(station_kg(collect_handover(self.source, original.result_id)), {'east': 7, 'west': 0})
        self.assertEqual(self.source.execute('SELECT count(*) FROM snapshots').fetchone()[0], 2)

    def test_v2_same_total_conflict_preserves_frozen_detail(self):
        batch = self.accept_sample()
        baseline = collect_handover(self.source, batch.result_id)
        rows = BATCH['rows']
        conflicts = [
            [dict(rows[0], origin='different-origin'), rows[1], rows[2]],
            [rows[0], rows[1], dict(rows[2], kg=0)],
            [rows[1], rows[0], rows[2]],
        ]
        for changed in conflicts:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                accept_batch(self.source, DAY, 'east', batch.batch_id, changed)
        self.assertEqual(collect_handover(self.source, batch.result_id), baseline)

    def test_invalid_types_and_exact_integer_precision(self):
        for value in (True, 7.0, '7'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                accept_batch(self.source, DAY, 'east', 'invalid', [{'kg': value, 'origin': 'meter'}])
        with self.assertRaises(ValueError):
            accept_batch(self.source, DAY, 'east', 'invalid', [{'kg': 1, 'origin': ' '}])
        self.assertIsNone(find_batch(self.source, 'invalid'))
        big = 10 ** 30
        batch = accept_batch(self.source, DAY, 'west', 'big', [
            {'kg': big, 'origin': 'west/1'}, {'kg': -3, 'origin': 'west/2'},
        ])
        self.assertEqual(station_kg(collect_handover(self.source, batch.result_id)),
                         {'east': 0, 'west': 999999999999999999999999999997})
