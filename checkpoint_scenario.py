"""Reusable V1 data for the checkpoint exercise. Run directly to check the fixture."""
from contextlib import closing, contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

from audit.handover import collect_handover
from audit.lookup import lookup_result
from audit.snapshots import PublishedBook, connect_database
from exchange.issue import issue_transfer
from exchange.subscriptions import FrozenSubscription, register_subscription
from lake.intake import accept_batch
from lake.opening import initialize_source, open_day
from lake.transfers import FormalTransfer, commit_transfer
from receiver.pending import load_pending
from receiver.posting import current_linked_result, initialize_receiver, install_opening_link
from receiver.receive import receive_packet
from receiver.receipts import load_first_receipt


DAY = "2026-08-12"
DISTRICT = "checkpoint-district"


@dataclass(frozen=True)
class CheckpointDay:
    opening: PublishedBook
    subscription: FrozenSubscription
    transfers: dict[str, FormalTransfer]


def prepare_source(source, r_kg=7):
    initialize_source(source)
    open_day(source, DAY, ("east", "west", "out-a", "out-b"))
    batch = accept_batch(source, DAY, "east", "checkpoint-seed", [
        {"kg": r_kg, "origin": "raw/R"}, {"kg": 11, "origin": "raw/S"},
    ])
    opening = lookup_result(source, batch.result_id)
    subscription = register_subscription(source, DISTRICT, DAY, ("east", "west"))
    r, s = (reading.identity for reading in batch.readings)
    moves = (
        ("T1", r, "east", "west"),
        ("T2", s, "east", "west"),
        ("T3", r, "west", "out-a"),
        ("T4", r, "out-a", "out-b"),
        ("T5", r, "out-b", "east"),
        ("T6", s, "west", "east"),
    )
    transfers = {key: commit_transfer(source, key, DAY, row, before, after)
                 for key, row, before, after in moves}
    return CheckpointDay(opening, subscription, transfers)


@contextmanager
def deny_commit(connection):
    def authorize(action, operation, *_):
        if action == sqlite3.SQLITE_TRANSACTION and operation == "COMMIT":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    connection.set_authorizer(authorize)
    try:
        yield
    finally:
        connection.set_authorizer(None)


def current_view(local):
    report = collect_handover(local, current_linked_result(local, DISTRICT))
    kg = {station: measure.kg for station, measure in report.accounts.stations.items()}
    rows = [(entry.origin, entry.station, entry.kg) for entry in report.provenance.current]
    return kg, rows


def check_v1(directory):
    def open_db(name):
        path = ":memory:" if directory is None else directory / f"{name}.sqlite3"
        return closing(connect_database(path))

    with open_db("source") as source:
        day = prepare_source(source)
        issues = {key: issue_transfer(source, DISTRICT, key) for key in day.transfers}

    def send(local, key, station):
        return receive_packet(local, DISTRICT, dict(issues[key].packets)[station])

    def finish(local, key):
        for _, data in issues[key].packets:
            result = receive_packet(local, DISTRICT, data)
        return result

    with open_db("local") as local, open_db("control") as control:
        for database in (local, control):
            initialize_receiver(database)
            install_opening_link(database, day.subscription, day.opening)

        first = finish(local, "T1")
        waiting = send(local, "T2", "east")
        finish(local, "T3")
        future = send(local, "T6", "east")
        local_view = current_view(local)
        assert local_view == ({"east": 11, "west": 0}, [("raw/S", "east", 11)])
        assert send(local, "T2", "east") == waiting
        assert waiting.missing_stations() == future.missing_stations() == ("west",)
        assert send(local, "T1", "east") == first
        assert load_first_receipt(local, DISTRICT, "T2") is None

        finish(control, "T1")
        finish(control, "T2")
        cutoff_view = current_view(control)
        assert cutoff_view == ({"east": 0, "west": 18}, [("raw/R", "west", 7), ("raw/S", "west", 11)])
        assert issues["T4"].packets == () and issues["T5"].manifest.predecessor == "T3"
        try:
            finish(control, "T5")
        except ValueError as error:
            assert "posted predecessor" in str(error)
        else:
            raise AssertionError("V1 posted the return before T3")
        assert load_pending(control, DISTRICT, "T5") is None
        finish(control, "T3")
        assert current_view(control) == ({"east": 0, "west": 11}, [("raw/S", "west", 11)])
        finish(control, "T5")
        return_view = current_view(control)
        assert return_view == ({"east": 7, "west": 11}, [("raw/R", "east", 7), ("raw/S", "west", 11)])

        with deny_commit(local):
            try:
                send(local, "T2", "west")
            except sqlite3.DatabaseError:
                pass
            else:
                raise AssertionError("SQLite accepted an explicitly denied commit")
        assert not local.in_transaction and current_view(local) == local_view
        assert load_pending(local, DISTRICT, "T2") == waiting
        assert load_pending(local, DISTRICT, "T6") == future
        assert load_first_receipt(local, DISTRICT, "T2") is None

    print(json.dumps({
        "local_after_T3": local_view,
        "complete_through_T2": cutoff_view,
        "T2_missing": waiting.missing_stations(),
        "T6_missing": future.missing_stations(),
        "T5_predecessor": issues["T5"].manifest.predecessor,
        "return_after_actual_T3": return_view,
    }, sort_keys=True))


if __name__ == "__main__":
    with TemporaryDirectory(prefix="qingwan-checkpoint-") as directory:
        check_v1(Path(directory))
