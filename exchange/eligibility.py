from dataclasses import dataclass

from exchange.subscriptions import FrozenSubscription
from lake.transfers import FormalTransfer


@dataclass(frozen=True)
class StationObligation:
    station: str
    direction: int


def scoped_directions(before: str, after: str, stations: tuple[str, ...]) -> tuple[StationObligation, ...]:
    obligations = [StationObligation(station, direction)
                   for station, direction in ((before, -1), (after, 1)) if station in stations]
    return tuple(sorted(obligations, key=lambda obligation: obligation.station))


def project_obligations(transfer: FormalTransfer,
                        subscription: FrozenSubscription) -> tuple[StationObligation, ...]:
    if transfer.day != subscription.day:
        raise ValueError('subscription and transfer have different accounting days')
    return scoped_directions(transfer.before, transfer.after, subscription.stations)
