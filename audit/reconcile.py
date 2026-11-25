from dataclasses import dataclass

from audit.snapshots import PublishedBook
from lake.measurement import StationMeasure, measure_change, measure_readings


@dataclass(frozen=True)
class ReconciledBook:
    stations: dict[str, StationMeasure]
    net: dict[str, StationMeasure]

    @property
    def total_kg(self) -> int:
        return sum(measure.kg for measure in self.stations.values())


def reconcile_result(book: PublishedBook) -> ReconciledBook:
    by_station = {station: [] for station in book.stations}
    for row in book.rows:
        by_station[row.station].append(row.reading)
    stations = {station: measure_readings(readings) for station, readings in by_station.items()}
    net = dict.fromkeys(book.stations, StationMeasure())
    for change in book.changes:
        net[change.station] += measure_change(change.reading, change.direction)
    return ReconciledBook(stations, net)
