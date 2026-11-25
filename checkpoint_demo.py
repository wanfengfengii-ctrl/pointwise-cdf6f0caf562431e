"""Materialize the checkpoint repair scenario and print auditable results."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from audit.handover import collect_handover
from audit.snapshots import connect_database
from exchange.checkpoint import decode_checkpoint
from exchange.issue import issue_checkpoint, issue_transfer
from receiver.pending import load_pending
from receiver.posting import (adopt_checkpoint, current_linked_result,
                              initialize_receiver, install_opening_link)
from receiver.receive import receive_packet
from receiver.receipts import load_first_receipt
from checkpoint_scenario import DISTRICT, prepare_source


def _packet(issues, transfer_id, station):
    return dict(issues[transfer_id].packets)[station]


def _finish(local, issues, transfer_id):
    result = None
    for _, encoded in issues[transfer_id].packets:
        result = receive_packet(local, DISTRICT, encoded)
    return result


def _station_kg(report):
    return {station: measure.kg for station, measure in report.accounts.stations.items()}


def _district_rows(report):
    return [(trace.origin, trace.station, trace.kg) for trace in report.provenance.current]


def run_demo(directory: Path):
    source_path = directory / 'qingwan-source.sqlite3'
    local_path = directory / 'qingwan-district.sqlite3'

    source = connect_database(source_path)
    day = prepare_source(source)
    issues = {key: issue_transfer(source, DISTRICT, key) for key in day.transfers}
    checkpoint_t2 = issue_checkpoint(
        source, DISTRICT, day.opening.result_id, day.transfers['T2'].source_commit)
    checkpoint_t3 = issue_checkpoint(
        source, DISTRICT, day.opening.result_id, day.transfers['T3'].source_commit)
    checkpoint_t2_piece = decode_checkpoint(checkpoint_t2)
    cutoff_view = checkpoint_t2_piece.cutoff_book
    cutoff_report = collect_handover(source, cutoff_view.result_id)
    cutoff_kg = {station: measure.kg
                 for station, measure in cutoff_report.accounts.stations.items()}
    checkpoint_t3_piece = decode_checkpoint(checkpoint_t3)
    cutoff_t3_view = checkpoint_t3_piece.cutoff_book
    cutoff_t3_kg = {station: measure.kg for station, measure in
                    collect_handover(source, cutoff_t3_view.result_id).accounts.stations.items()}
    source.close()

    local = connect_database(local_path)
    initialize_receiver(local)
    install_opening_link(local, day.subscription, day.opening)

    first_t1 = _finish(local, issues, 'T1')
    waiting_t2 = receive_packet(local, DISTRICT, _packet(issues, 'T2', 'east'))
    actual_t3 = _finish(local, issues, 'T3')
    waiting_t6 = receive_packet(local, DISTRICT, _packet(issues, 'T6', 'east'))
    before_report = collect_handover(local, current_linked_result(local, DISTRICT))

    checkpoint_account = adopt_checkpoint(local, DISTRICT, checkpoint_t2)
    local.close()

    # Reopen the district database and read the published repair account by number.
    local = connect_database(local_path)
    initialize_receiver(local)
    repair_report = collect_handover(local, checkpoint_account)
    repair_kg = _station_kg(repair_report)
    repair_rows = _district_rows(repair_report)

    t1_replay = receive_packet(local, DISTRICT, _packet(issues, 'T1', 'east'))
    t3_replay = receive_packet(local, DISTRICT, _packet(issues, 'T3', 'west'))
    t2_west_cover = receive_packet(local, DISTRICT, _packet(issues, 'T2', 'west'))
    t2_east_cover = receive_packet(local, DISTRICT, _packet(issues, 'T2', 'east'))
    t2_first_receipt = load_first_receipt(local, DISTRICT, 'T2')
    t2_pending = load_pending(local, DISTRICT, 'T2')
    t6_still_waiting = load_pending(local, DISTRICT, 'T6') is not None
    t6_missing = list(load_pending(local, DISTRICT, 'T6').missing_stations())

    # The post-cutoff pending piece remains actionable after adoption.
    t6_west_receipt = receive_packet(local, DISTRICT, _packet(issues, 'T6', 'west'))
    current_after_t6 = current_linked_result(local, DISTRICT)
    readopted_account = adopt_checkpoint(local, DISTRICT, checkpoint_t2)
    current_remains_t6 = current_linked_result(local, DISTRICT) == t6_west_receipt.result_id
    local.close()

    # A second isolated district adopts the T3 cutoff itself.  The covered exit leaves
    # visible predecessor T3, so the real T5 return packet can post normally.
    return_path = directory / 'qingwan-return-district.sqlite3'
    return_local = connect_database(return_path)
    initialize_receiver(return_local)
    install_opening_link(return_local, day.subscription, day.opening)
    exit_account = adopt_checkpoint(return_local, DISTRICT, checkpoint_t3)
    covered_t3 = receive_packet(return_local, DISTRICT, _packet(issues, 'T3', 'west'))
    t5_return = receive_packet(return_local, DISTRICT, _packet(issues, 'T5', 'east'))
    return_report = collect_handover(return_local, t5_return.result_id)
    return_local.close()

    r_in_repair_district = any(origin == 'raw/R' for origin, _, _ in repair_rows)
    result = {
        'case': 'qingwan-checkpoint-repair',
        'fixed_opening_id': day.opening.result_id,
        'source_cutoff_T2_account': cutoff_view.result_id,
        'source_cutoff_T2_kg': cutoff_kg,
        'checkpoint_T2_coverage': [
            [entry.manifest.transfer_id, station, packet_id]
            for entry in checkpoint_t2_piece.entries
            for station, packet_id in entry.packet_ids
        ],
        'checkpoint_T3_visible_progress': {
            entry.manifest.transfer_id: entry.manifest.predecessor
            for entry in checkpoint_t3_piece.entries
        },
        'source_cutoff_T3_kg': cutoff_t3_kg,
        'before_adoption_kg': _station_kg(before_report),
        'checkpoint_account': checkpoint_account,
        'repair_kg': repair_kg,
        'repair_rows': repair_rows,
        'west_repaired_kg': repair_kg['west'],
        'r_in_district': r_in_repair_district,
        'r_still_outside': not r_in_repair_district,
        'reopened_by_account': repair_report.book.result_id == checkpoint_account,
        'T1_replay_is_original_receipt': t1_replay == first_t1,
        'T3_replay_is_original_receipt': t3_replay == actual_t3,
        'T2_west_coverage': list(t2_west_cover),
        'T2_east_coverage': list(t2_east_cover),
        'T2_has_first_receipt': t2_first_receipt is not None,
        'T2_pending_cleared': t2_pending is None,
        'T6_pending_preserved': t6_still_waiting,
        'T6_missing_stations': t6_missing,
        'T6_west_posts_actual_receipt': type(t6_west_receipt).__name__ == 'FirstReceipt',
        'readopt_returns_original_checkpoint_account': readopted_account == checkpoint_account,
        'current_account_remains_later_T6': current_remains_t6,
        'T3_covered_exit_account': exit_account,
        'T3_late_piece_coverage': list(covered_t3),
        'T5_return_result_id': t5_return.result_id,
        'after_T5_return_kg': _station_kg(return_report),
        'after_T5_return_rows': _district_rows(return_report),
        'waiting_T2_missing_before_adopt': list(waiting_t2.missing_stations()),
    }
    print('POINTWISE_CHECKPOINT_DEMO ' + json.dumps(result, ensure_ascii=False, sort_keys=True))
    outside_text = '是' if not r_in_repair_district else '否'
    assert repair_kg == {'east': 0, 'west': 11}
    assert not r_in_repair_district
    assert t2_west_cover == ('covered', checkpoint_account)
    assert t2_east_cover == ('covered', checkpoint_account)
    assert t2_first_receipt is None and t2_pending is None
    assert _station_kg(return_report) == {'east': 7, 'west': 11}
    print(f"演示结果：西站补到 {repair_kg['west']} kg；R 仍在区外：{outside_text}；"
          f"T2 西片返回 {t2_west_cover[0]}；T5 返区后东 {_station_kg(return_report)['east']}、"
          f"西 {_station_kg(return_report)['west']}。")
    return result


def main():
    with TemporaryDirectory(prefix='qingwan-checkpoint-demo-') as directory:
        run_demo(Path(directory))


if __name__ == '__main__':
    main()
