from audit.handover import collect_handover
from lake.measurement import StationMeasure
from lake.opening import open_day
from lake.ownership import read_current_owner
from lake.readings import ReadingId
from lake.source_audit import lookup_formal_transfer, read_transfer_postings
from lake.source_ledger import read_source_station_book
from lake.transfers import commit_transfer
from tests.support import DAY, TRANSFER_ID, ProjectCase, station_kg


class TransferTests(ProjectCase):
    def test_v3_transfer_commits_both_source_postings_and_conserves_weight(self):
        batch = self.accept_sample()
        formal = self.transfer_sample(batch)
        self.assertEqual((formal.transfer_id, formal.source_commit, formal.before, formal.after),
                         (TRANSFER_ID, 1, 'east', 'west'))
        self.assertEqual(formal.reading, batch.readings[0])
        self.assertEqual(read_current_owner(self.source, formal.reading.identity), 'west')
        self.assertEqual(lookup_formal_transfer(self.source, TRANSFER_ID), formal)
        postings = read_transfer_postings(self.source, TRANSFER_ID)
        self.assertEqual([(row.station, row.direction, row.net.kg) for row in postings],
                         [('east', -1, -7), ('west', 1, 7)])
        current = read_source_station_book(self.source, DAY)
        self.assertEqual(current, {'east': StationMeasure(0, 1), 'west': StationMeasure(7, 0)})
        self.assertEqual(sum(measure.kg for measure in current.values()), 7)
        self.assertEqual(station_kg(collect_handover(self.source, batch.result_id)), {'east': 7, 'west': 0})

    def test_v3_zero_and_missing_have_distinct_postings_and_origins(self):
        batch = self.accept_sample()
        zero = commit_transfer(self.source, 'move-zero', DAY, batch.readings[1].identity, 'east', 'west')
        self.assertEqual(read_source_station_book(self.source, DAY),
                         {'east': StationMeasure(7, 1), 'west': StationMeasure(0, 0)})
        missing = commit_transfer(self.source, 'move-missing', DAY, batch.readings[2].identity, 'east', 'west')
        self.assertEqual((zero.reading.kg, zero.reading.origin), (0, 'east-meter-A/0002'))
        self.assertEqual((missing.reading.kg, missing.reading.origin), (None, 'east-meter-A/0003'))
        self.assertEqual([item.net for item in read_transfer_postings(self.source, zero.transfer_id)],
                         [StationMeasure(0, 0), StationMeasure(0, 0)])
        self.assertEqual([item.net for item in read_transfer_postings(self.source, missing.transfer_id)],
                         [StationMeasure(0, -1), StationMeasure(0, 1)])
        self.assertEqual(read_source_station_book(self.source, DAY),
                         {'east': StationMeasure(7, 0), 'west': StationMeasure(0, 1)})

    def test_v4_stale_origin_leaves_commit_sequence_and_source_book_unchanged(self):
        original = self.accept_sample()
        formal = self.transfer_sample(original)
        old_book = read_source_station_book(self.source, DAY)
        identity = formal.reading.identity
        with self.assertRaises(ValueError):
            commit_transfer(self.source, 'stale', DAY, identity, 'east', 'west')
        with self.assertRaises(KeyError):
            lookup_formal_transfer(self.source, 'stale')
        self.assertEqual(read_current_owner(self.source, identity), 'west')
        self.assertEqual(read_source_station_book(self.source, DAY), old_book)
        self.assertEqual(lookup_formal_transfer(self.source, TRANSFER_ID), formal)
        self.assertEqual(self.transfer_sample(original), formal)
        next_formal = commit_transfer(self.source, 'return', DAY, identity, 'west', 'east')
        self.assertEqual(next_formal.source_commit, 2)

    def test_v4_cross_day_and_unknown_original_do_not_commit(self):
        batch = self.accept_sample()
        other_day = '2026-08-13'
        open_day(self.source, other_day)
        old_book = read_source_station_book(self.source, DAY)
        with self.assertRaises(ValueError):
            commit_transfer(self.source, 'cross-day', other_day, batch.readings[0].identity, 'east', 'west')
        with self.assertRaises(ValueError):
            commit_transfer(self.source, 'unknown-row', DAY,
                            ReadingId(DAY, 'east', batch.batch_id, 8), 'east', 'west')
        for transfer_id in ('cross-day', 'unknown-row'):
            with self.assertRaises(KeyError):
                lookup_formal_transfer(self.source, transfer_id)
        self.assertEqual(read_current_owner(self.source, batch.readings[0].identity), 'east')
        self.assertEqual(read_source_station_book(self.source, DAY), old_book)
        self.assertEqual(self.transfer_sample(batch).source_commit, 1)
