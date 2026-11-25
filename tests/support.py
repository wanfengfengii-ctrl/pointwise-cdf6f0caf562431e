from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

from audit.handover import collect_handover
from audit.lookup import lookup_result
from audit.snapshots import connect_database
from exchange.issue import IssuedDelivery, issue_transfer
from exchange.subscriptions import register_subscription
from lake.intake import accept_batch
from lake.opening import initialize_source, open_day
from lake.transfers import commit_transfer
from receiver.pending import PendingDelivery
from receiver.posting import current_linked_result, initialize_receiver, install_opening_link
from receiver.receive import receive_packet


ROOT = Path(__file__).resolve().parent.parent


def read_sample(name):
    return json.loads((ROOT / 'samples' / name).read_text(encoding='utf-8'))


DAY_SAMPLE = read_sample('station-day.json')
DISPATCH_SAMPLE = read_sample('dispatch.json')
DAY = DAY_SAMPLE['day']
STATIONS = tuple(DAY_SAMPLE['stations'])
BATCH = DAY_SAMPLE['batch']
TRANSFER = DISPATCH_SAMPLE['transfer']
TRANSFER_ID = TRANSFER['id']
DISTRICT = 'district'
EAST_ONLY = 'east-only'
PEER = 'district-peer'


def station_kg(report):
    return {station: measure.kg for station, measure in report.accounts.stations.items()}


@dataclass(frozen=True)
class DayDispatch:
    openings: dict[str, str]
    issues: dict[str, IssuedDelivery]

    def piece(self, subscription_id: str, station: str) -> bytes:
        return dict(self.issues[subscription_id].packets)[station]


class ProjectCase(unittest.TestCase):
    def setUp(self):
        workspace = TemporaryDirectory(prefix='qingwan-')
        self.addCleanup(workspace.cleanup)
        self.workspace = Path(workspace.name)
        self.source_path = self.workspace / 'source.sqlite3'
        self.receiver_path = self.workspace / 'district.sqlite3'
        self.source = self.connect(self.source_path)
        initialize_source(self.source)
        self.empty = open_day(self.source, DAY, STATIONS)

    def connect(self, path):
        connection = connect_database(path)
        self.addCleanup(connection.close)
        return connection

    def open_receiver(self):
        connection = self.connect(self.receiver_path)
        initialize_receiver(connection)
        return connection

    def accept_sample(self):
        return accept_batch(self.source, DAY, BATCH['station'], BATCH['id'], BATCH['rows'])

    def transfer_sample(self, batch):
        identity = batch.readings[TRANSFER['position'] - 1].identity
        return commit_transfer(self.source, TRANSFER_ID, DAY, identity,
                               TRANSFER['before'], TRANSFER['after'])

    def dispatch_round(self, receiver):
        original = self.accept_sample()
        opening = lookup_result(self.source, original.result_id)
        self.transfer_sample(original)
        openings = {}
        issues = {}
        for subscription_id, stations in DISPATCH_SAMPLE['subscriptions'].items():
            subscription = register_subscription(self.source, subscription_id, DAY, stations)
            openings[subscription_id] = install_opening_link(receiver, subscription, opening)
            issues[subscription_id] = issue_transfer(self.source, subscription_id, TRANSFER_ID)
        self.source.close()
        return DayDispatch(openings, issues)

    def current_report(self, receiver, subscription_id):
        return collect_handover(receiver, current_linked_result(receiver, subscription_id))

    def receive_in_new_process(self, subscription_id, payload):
        packet_path = self.workspace / 'route.packet'
        packet_path.write_bytes(payload)
        result = subprocess.run(
            [sys.executable, '-B', '-m', 'tests.support', str(self.receiver_path),
             subscription_id, str(packet_path)],
            cwd=ROOT, capture_output=True, text=True, timeout=30, check=True,
        )
        return json.loads(result.stdout)


def _receive_from_arguments(database, subscription_id, packet_path):
    connection = connect_database(database)
    try:
        initialize_receiver(connection)
        outcome = receive_packet(connection, subscription_id, Path(packet_path).read_bytes())
        if isinstance(outcome, PendingDelivery):
            output = {'transfer_id': outcome.transfer_id, 'missing': list(outcome.missing_stations())}
        else:
            output = {'transfer_id': outcome.transfer_id, 'receipt_id': outcome.receipt_id,
                      'result_id': outcome.result_id}
        print(json.dumps(output, sort_keys=True))
    finally:
        connection.close()


if __name__ == '__main__':
    _receive_from_arguments(*sys.argv[1:])
