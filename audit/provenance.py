from dataclasses import dataclass

from audit.snapshots import PublishedBook
from lake.readings import ReadingId


@dataclass(frozen=True)
class OriginTrace:
    identity: ReadingId
    station: str
    origin: str
    kg: int | None
    direction: int | None = None


@dataclass(frozen=True)
class Provenance:
    current: tuple[OriginTrace, ...]
    changes: tuple[OriginTrace, ...]


def trace_origins(book: PublishedBook) -> Provenance:
    current = tuple(OriginTrace(row.reading.identity, row.station, row.reading.origin, row.reading.kg)
                    for row in book.rows)
    changes = tuple(OriginTrace(item.reading.identity, item.station, item.reading.origin,
                                item.reading.kg, item.direction) for item in book.changes)
    return Provenance(current, changes)
