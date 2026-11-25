from collections.abc import Mapping
from dataclasses import dataclass
import json

from exchange.eligibility import StationObligation, project_obligations, scoped_directions
from exchange.subscriptions import FrozenSubscription
from lake.readings import ReadingId, canonical_json, digest, require_name
from lake.transfers import FormalTransfer


@dataclass(frozen=True)
class PieceCommitment:
    station: str
    direction: int
    body_digest: str

    def __post_init__(self):
        require_name(self.station, 'commitment station')
        if type(self.direction) is not int or self.direction not in (-1, 1):
            raise ValueError('invalid commitment direction')
        if (type(self.body_digest) is not str or len(self.body_digest) != 64 or
                any(char not in '0123456789abcdef' for char in self.body_digest)):
            raise ValueError('invalid commitment digest')


@dataclass(frozen=True)
class IssueManifest:
    subscription: FrozenSubscription
    transfer_id: str
    source_commit: int
    reading_id: ReadingId
    before: str
    after: str
    predecessor: str | None
    commitments: tuple[PieceCommitment, ...]

    def __post_init__(self):
        require_name(self.transfer_id, 'transfer id')
        require_name(self.before, 'before station')
        require_name(self.after, 'after station')
        if self.predecessor is not None:
            require_name(self.predecessor, 'predecessor')
            if self.predecessor == self.transfer_id:
                raise ValueError('a transfer cannot precede itself')
        if self.before == self.after or self.reading_id.day != self.subscription.day:
            raise ValueError('manifest contains an invalid station transfer')
        if type(self.source_commit) is not int or self.source_commit < 1:
            raise ValueError('manifest source commit must be a positive integer')
        declared = tuple(StationObligation(p.station, p.direction) for p in self.commitments)
        if declared != scoped_directions(self.before, self.after, self.subscription.stations):
            raise ValueError('manifest commitments contradict the frozen scope')

    @property
    def issue_id(self) -> str:
        return 'issue-' + digest(encode_manifest(self))

    def to_wire(self) -> dict:
        return {
            'subscription': self.subscription.to_wire(),
            'transfer_id': self.transfer_id,
            'source_commit': self.source_commit,
            'reading_id': self.reading_id.to_wire(),
            'before': self.before,
            'after': self.after,
            'predecessor': self.predecessor,
            'pieces': [[p.station, p.direction, p.body_digest] for p in self.commitments],
        }


def build_manifest(subscription: FrozenSubscription, transfer: FormalTransfer,
                   bodies: Mapping[str, bytes], predecessor: str | None = None) -> IssueManifest:
    obligations = project_obligations(transfer, subscription)
    if set(bodies) != {obligation.station for obligation in obligations}:
        raise ValueError('station bodies do not cover the projected obligations')
    commitments = tuple(PieceCommitment(item.station, item.direction, digest(bodies[item.station]))
                        for item in obligations)
    return IssueManifest(subscription, transfer.transfer_id, transfer.source_commit,
                         transfer.reading.identity, transfer.before, transfer.after,
                         predecessor, commitments)


def encode_manifest(manifest: IssueManifest) -> bytes:
    return canonical_json(manifest.to_wire())


def decode_manifest(encoded: bytes) -> IssueManifest:
    body = json.loads(encoded)
    fields = {'subscription', 'transfer_id', 'source_commit', 'reading_id', 'before',
              'after', 'predecessor', 'pieces'}
    if type(body) is not dict or body.keys() != fields or type(body['pieces']) is not list:
        raise ValueError('invalid manifest body')
    commitments = []
    for item in body['pieces']:
        if type(item) is not list or len(item) != 3:
            raise ValueError('invalid manifest commitment')
        commitments.append(PieceCommitment(*item))
    manifest = IssueManifest(
        FrozenSubscription.from_wire(body['subscription']), body['transfer_id'], body['source_commit'],
        ReadingId.from_wire(body['reading_id']), body['before'], body['after'],
        body['predecessor'], tuple(commitments),
    )
    if encode_manifest(manifest) != encoded:
        raise ValueError('manifest encoding must be canonical')
    return manifest
