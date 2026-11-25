from dataclasses import dataclass
from datetime import date
from hashlib import sha256
import json


def require_name(value, field: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f'invalid {field}')
    return value


def require_day(value) -> str:
    if type(value) is not str:
        raise ValueError('invalid accounting day')
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise ValueError('invalid accounting day') from error
    if parsed.isoformat() != value:
        raise ValueError('accounting day must use YYYY-MM-DD')
    return value


def normalize_stations(stations) -> tuple[str, ...]:
    if isinstance(stations, (str, bytes)):
        raise ValueError('stations must be a collection of station names')
    try:
        result = tuple(sorted({require_name(station, 'station') for station in stations}))
    except TypeError as error:
        raise ValueError('stations must be a collection of station names') from error
    if not result:
        raise ValueError('station set must be nonempty')
    return result


def canonical_json(value) -> bytes:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True,
                      separators=(',', ':')).encode('ascii')


def digest(data: bytes) -> str:
    return sha256(data).hexdigest()


def content_id(kind: str, value) -> str:
    return f'{kind}-{digest(canonical_json(value))}'


@dataclass(frozen=True, order=True)
class ReadingId:
    day: str
    origin_station: str
    batch_id: str
    position: int

    def __post_init__(self):
        require_day(self.day)
        require_name(self.origin_station, 'origin station')
        require_name(self.batch_id, 'batch id')
        if type(self.position) is not int or self.position < 1:
            raise ValueError('reading position must be a positive integer')

    def to_wire(self) -> list:
        return [self.day, self.origin_station, self.batch_id, self.position]

    @classmethod
    def from_wire(cls, value):
        if type(value) is not list or len(value) != 4:
            raise ValueError('invalid reading identity')
        return cls(*value)

    def storage_key(self) -> bytes:
        return canonical_json(self.to_wire())


@dataclass(frozen=True)
class Reading:
    identity: ReadingId
    kg: int | None
    origin: str

    def __post_init__(self):
        if not isinstance(self.identity, ReadingId):
            raise ValueError('invalid reading identity')
        if self.kg is not None and type(self.kg) is not int:
            raise ValueError('kg must be an integer or null')
        if type(self.origin) is not str or not self.origin.strip():
            raise ValueError('reading origin is required')

    def to_wire(self) -> dict:
        return {'id': self.identity.to_wire(), 'kg': self.kg, 'origin': self.origin}

    @classmethod
    def from_wire(cls, value):
        if type(value) is not dict or value.keys() != {'id', 'kg', 'origin'}:
            raise ValueError('invalid reading body')
        return cls(ReadingId.from_wire(value['id']), value['kg'], value['origin'])


def parse_readings(day: str, station: str, batch_id: str, rows) -> tuple[Reading, ...]:
    require_day(day)
    require_name(station, 'station')
    require_name(batch_id, 'batch id')
    if not isinstance(rows, (list, tuple)) or not rows:
        raise ValueError('a batch needs a nonempty ordered row sequence')
    readings = []
    for position, row in enumerate(rows, start=1):
        if type(row) is not dict or row.keys() != {'kg', 'origin'}:
            raise ValueError(f'invalid batch row at position {position}')
        identity = ReadingId(day, station, batch_id, position)
        readings.append(Reading(identity, row['kg'], row['origin']))
    return tuple(readings)
