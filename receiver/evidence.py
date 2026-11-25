from dataclasses import dataclass

from exchange.packets import StationPacket, decode_packet, encode_station_body
from exchange.subscriptions import FrozenSubscription
from lake.readings import digest


@dataclass(frozen=True)
class VerifiedArrival:
    packet: StationPacket
    encoded: bytes
    packet_id: str


def verify_arrival(subscription: FrozenSubscription, encoded: bytes) -> VerifiedArrival:
    packet = decode_packet(encoded)
    manifest = packet.manifest
    if manifest.subscription != subscription:
        raise ValueError('station packet belongs to a different frozen subscription')
    if packet.reading.identity != manifest.reading_id:
        raise ValueError('station packet original row differs from the manifest')
    for piece in manifest.commitments:
        if piece.station == packet.station:
            body = encode_station_body(packet.station, packet.direction, packet.reading)
            if piece.direction != packet.direction or piece.body_digest != digest(body):
                raise ValueError('station packet body contradicts its commitment')
            return VerifiedArrival(packet, encoded, digest(encoded))
    raise ValueError('station has no obligation in this issue')
