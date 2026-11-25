from dataclasses import dataclass
import json

from exchange.manifest import IssueManifest, decode_manifest
from lake.readings import Reading, canonical_json, require_name


@dataclass(frozen=True)
class StationPacket:
    manifest: IssueManifest
    station: str
    direction: int
    reading: Reading

    def __post_init__(self):
        require_name(self.station, 'packet station')
        if type(self.direction) is not int or self.direction not in (-1, 1):
            raise ValueError('invalid packet direction')
        if not isinstance(self.reading, Reading):
            raise ValueError('invalid packet reading')


def _station_body(station: str, direction: int, reading: Reading) -> dict:
    return {'station': station, 'direction': direction, 'reading': reading.to_wire()}


def encode_station_body(station: str, direction: int, reading: Reading) -> bytes:
    return canonical_json(_station_body(station, direction, reading))


def encode_packet(packet: StationPacket) -> bytes:
    return canonical_json({
        'version': 1,
        'manifest': packet.manifest.to_wire(),
        'body': _station_body(packet.station, packet.direction, packet.reading),
    })


def decode_packet(encoded: bytes) -> StationPacket:
    if type(encoded) is not bytes:
        raise ValueError('station packet must be bytes')
    try:
        outer = json.loads(encoded)
        if type(outer) is not dict or outer.keys() != {'version', 'manifest', 'body'}:
            raise ValueError('invalid packet envelope')
        if type(outer['version']) is not int or outer['version'] != 1:
            raise ValueError('unsupported packet version')
        body = outer['body']
        if type(body) is not dict or body.keys() != {'station', 'direction', 'reading'}:
            raise ValueError('invalid station body')
        manifest = decode_manifest(canonical_json(outer['manifest']))
        packet = StationPacket(manifest, body['station'], body['direction'],
                               Reading.from_wire(body['reading']))
        if encode_packet(packet) != encoded:
            raise ValueError('station packet encoding must be canonical')
        return packet
    except (UnicodeError, RecursionError) as error:
        raise ValueError('invalid station packet encoding') from error
