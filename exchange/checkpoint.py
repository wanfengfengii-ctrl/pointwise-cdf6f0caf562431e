from dataclasses import dataclass, replace
import json

from audit.snapshots import (BookChange, PublishedBook, apply_changes,
                             published_book_from_body)
from exchange.manifest import IssueManifest, decode_manifest
from exchange.packets import StationPacket, encode_packet
from exchange.subscriptions import FrozenSubscription
from lake.readings import canonical_json, content_id, digest, require_name


CHECKPOINT_WIRE_KEYS = {
    'version', 'kind', 'checkpoint_id', 'subscription', 'opening_id',
    'opening', 'cutoff', 'cutoff_book', 'entries',
}
ENTRY_WIRE_KEYS = {'manifest', 'packet_ids'}
BOOK_WIRE_KEYS = {'result_id', 'body'}


@dataclass(frozen=True)
class CheckpointEntry:
    manifest: IssueManifest
    packet_ids: tuple[tuple[str, str], ...]

    def __post_init__(self):
        if not isinstance(self.manifest, IssueManifest):
            raise ValueError('checkpoint entry requires a V1 manifest')
        if type(self.packet_ids) is not tuple:
            raise ValueError('checkpoint packet identities must be a tuple')
        stations = []
        for item in self.packet_ids:
            if type(item) is not tuple or len(item) != 2:
                raise ValueError('invalid checkpoint packet identity')
            station, packet_id = item
            require_name(station, 'checkpoint packet station')
            if (type(packet_id) is not str or len(packet_id) != 64 or
                    any(char not in '0123456789abcdef' for char in packet_id)):
                raise ValueError('invalid checkpoint packet digest')
            stations.append(station)
        expected_stations = tuple(piece.station for piece in self.manifest.commitments)
        if tuple(stations) != expected_stations:
            raise ValueError('checkpoint packet identities do not match the V1 pieces')


@dataclass(frozen=True)
class CheckpointPiece:
    subscription: FrozenSubscription
    opening: PublishedBook
    cutoff: int
    cutoff_book: PublishedBook
    entries: tuple[CheckpointEntry, ...]

    def __post_init__(self):
        if not isinstance(self.subscription, FrozenSubscription):
            raise ValueError('checkpoint requires a frozen subscription')
        if not isinstance(self.opening, PublishedBook):
            raise ValueError('checkpoint requires the fixed original book')
        if not isinstance(self.cutoff_book, PublishedBook):
            raise ValueError('checkpoint requires a cutoff book')
        if type(self.cutoff) is not int or self.cutoff < 0:
            raise ValueError('checkpoint cutoff must be a non-negative integer')
        if type(self.entries) is not tuple:
            raise ValueError('checkpoint entries must be a tuple')
        for entry in self.entries:
            if not isinstance(entry, CheckpointEntry):
                raise ValueError('invalid checkpoint entry')

    @property
    def opening_id(self) -> str:
        return self.opening.result_id

    def core_wire(self) -> dict:
        return {
            'subscription': self.subscription.to_wire(),
            'opening_id': self.opening.result_id,
            'opening': book_to_wire(self.opening),
            'cutoff': self.cutoff,
            'cutoff_book': book_to_wire(self.cutoff_book),
            'entries': [{
                'manifest': entry.manifest.to_wire(),
                'packet_ids': [[station, packet_id]
                               for station, packet_id in entry.packet_ids],
            } for entry in self.entries],
        }

    @property
    def checkpoint_id(self) -> str:
        return content_id('checkpoint', self.core_wire())

    def to_wire(self) -> dict:
        return {
            'version': 1,
            'kind': 'checkpoint',
            'checkpoint_id': self.checkpoint_id,
            **self.core_wire(),
        }


def book_to_wire(book: PublishedBook) -> dict:
    return {'result_id': book.result_id, 'body': book.body()}


def book_from_wire(value) -> PublishedBook:
    if type(value) is not dict or value.keys() != BOOK_WIRE_KEYS:
        raise ValueError('invalid checkpoint book envelope')
    require_name(value['result_id'], 'checkpoint book result id')
    if type(value['body']) is not dict:
        raise ValueError('invalid checkpoint book body')
    return published_book_from_body(value['result_id'], value['body'])


def encode_checkpoint(piece: CheckpointPiece) -> bytes:
    return canonical_json(piece.to_wire())


def decode_checkpoint(encoded: bytes) -> CheckpointPiece:
    if type(encoded) is not bytes:
        raise ValueError('checkpoint piece must be bytes')
    try:
        outer = json.loads(encoded)
        if type(outer) is not dict or outer.keys() != CHECKPOINT_WIRE_KEYS:
            raise ValueError('invalid checkpoint envelope')
        if type(outer['version']) is not int or outer['version'] != 1:
            raise ValueError('unsupported checkpoint version')
        if outer['kind'] != 'checkpoint':
            raise ValueError('unsupported checkpoint kind')
        require_name(outer['checkpoint_id'], 'checkpoint id')
        if type(outer['opening_id']) is not str:
            raise ValueError('invalid checkpoint opening id')

        subscription = FrozenSubscription.from_wire(outer['subscription'])
        opening = book_from_wire(outer['opening'])
        cutoff = outer['cutoff']
        if type(cutoff) is not int or cutoff < 0:
            raise ValueError('invalid checkpoint cutoff')
        cutoff_book = book_from_wire(outer['cutoff_book'])

        entries = []
        if type(outer['entries']) is not list:
            raise ValueError('invalid checkpoint entries')
        for item in outer['entries']:
            if type(item) is not dict or item.keys() != ENTRY_WIRE_KEYS:
                raise ValueError('invalid checkpoint entry envelope')
            manifest = decode_manifest(canonical_json(item['manifest']))
            if type(item['packet_ids']) is not list:
                raise ValueError('invalid checkpoint packet list')
            packet_ids = tuple((station, packet_id)
                               for station, packet_id in item['packet_ids'])
            entries.append(CheckpointEntry(manifest, packet_ids))

        piece = CheckpointPiece(subscription, opening, cutoff, cutoff_book, tuple(entries))
        if outer['opening_id'] != piece.opening_id:
            raise ValueError('checkpoint opening id contradicts its original book')
        if outer['checkpoint_id'] != piece.checkpoint_id:
            raise ValueError('checkpoint identity does not match its body')
        if encode_checkpoint(piece) != encoded:
            raise ValueError('checkpoint encoding must be canonical')
        verify_checkpoint(piece)
        return piece
    except json.JSONDecodeError as error:
        raise ValueError('invalid checkpoint piece encoding') from error
    except (KeyError, IndexError, TypeError, UnicodeError, RecursionError) as error:
        raise ValueError('invalid checkpoint piece') from error


def _validate_fixed_opening(subscription: FrozenSubscription,
                            opening: PublishedBook) -> dict:
    if opening.kind != 'opening':
        raise ValueError('checkpoint must bind a published original opening book')
    if opening.day != subscription.day:
        raise ValueError('checkpoint original book and subscription differ by day')
    if not set(subscription.stations).issubset(opening.stations):
        raise ValueError('checkpoint original book does not cover the subscription')
    expected_id = content_id('opening', [
        opening.day, opening.stations,
        [row.reading.to_wire() for row in opening.rows],
    ])
    if opening.result_id != expected_id:
        raise ValueError('checkpoint original book identity is inconsistent')
    expected_order = tuple(sorted(opening.rows, key=lambda row: (
        row.reading.identity.batch_id,
        row.reading.identity.position,
        row.reading.identity.origin_station,
    )))
    if opening.rows != expected_order:
        raise ValueError('checkpoint original book has the wrong original order')
    return {row.reading.identity: row.reading for row in opening.rows}


def build_cutoff_book(subscription: FrozenSubscription, opening: PublishedBook,
                      cutoff: int, entries: tuple[CheckpointEntry, ...]) -> PublishedBook:
    readings = _validate_fixed_opening(subscription, opening)
    rows = tuple(row for row in opening.rows if row.station in subscription.stations)
    changes = []
    progress: dict[bytes, str] = {}
    seen: set[str] = set()
    last_commit = 0

    for entry in entries:
        manifest = entry.manifest
        if manifest.subscription != subscription:
            raise ValueError('checkpoint entry belongs to a different subscription')
        if manifest.transfer_id in seen:
            raise ValueError('checkpoint contains a duplicate transfer')
        if manifest.source_commit <= last_commit:
            raise ValueError('checkpoint entries are not in source commit order')
        if manifest.source_commit > cutoff:
            raise ValueError('checkpoint entry is after the selected cutoff')
        if manifest.reading_id not in readings:
            raise ValueError('checkpoint entry is outside the fixed original roster')
        reading = readings[manifest.reading_id]
        expected_packet_ids = {
            piece.station: digest(encode_packet(StationPacket(
                manifest, piece.station, piece.direction, reading)))
            for piece in manifest.commitments
        }
        if dict(entry.packet_ids) != expected_packet_ids:
            raise ValueError('checkpoint packet identity is not the exact V1 station piece')
        key = manifest.reading_id.storage_key()
        if manifest.predecessor != progress.get(key):
            raise ValueError('checkpoint entry has the wrong visible predecessor')

        piece_changes = tuple(BookChange(piece.station, piece.direction, reading)
                              for piece in manifest.commitments)
        rows = apply_changes(rows, piece_changes)
        changes.extend(piece_changes)
        if manifest.commitments:
            progress[key] = manifest.transfer_id
        seen.add(manifest.transfer_id)
        last_commit = manifest.source_commit

    book = PublishedBook(
        '', subscription.day, subscription.stations, 'checkpoint-cutoff',
        rows, tuple(changes), subscription_id=subscription.subscription_id,
        opening_id=opening.result_id,
    )
    result_id = content_id('checkpoint-cutoff', book.body())
    return replace(book, result_id=result_id)


def verify_checkpoint(piece: CheckpointPiece) -> None:
    expected = build_cutoff_book(piece.subscription, piece.opening,
                                 piece.cutoff, piece.entries)
    if piece.cutoff_book != expected:
        raise ValueError('checkpoint cutoff account contradicts its transfer coverage')


def coverage_mapping(piece: CheckpointPiece) -> dict[tuple[str, str], str]:
    return {
        (entry.manifest.transfer_id, station): packet_id
        for entry in piece.entries
        for station, packet_id in entry.packet_ids
    }
