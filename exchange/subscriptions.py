from dataclasses import dataclass
import json
import sqlite3

from audit.snapshots import atomic
from lake.readings import canonical_json, normalize_stations, require_day, require_name


@dataclass(frozen=True)
class FrozenSubscription:
    subscription_id: str
    day: str
    stations: tuple[str, ...]

    def __post_init__(self):
        require_name(self.subscription_id, 'subscription id')
        require_day(self.day)
        if type(self.stations) is not tuple or normalize_stations(self.stations) != self.stations:
            raise ValueError('frozen stations must be sorted and unique')

    def to_wire(self) -> dict:
        return {'id': self.subscription_id, 'day': self.day, 'stations': list(self.stations)}

    @classmethod
    def from_wire(cls, value):
        if type(value) is not dict or value.keys() != {'id', 'day', 'stations'}:
            raise ValueError('invalid subscription body')
        if type(value['stations']) is not list:
            raise ValueError('invalid subscription station list')
        return cls(value['id'], value['day'], tuple(value['stations']))


def create_subscriptions(connection: sqlite3.Connection) -> None:
    connection.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            subscription_id TEXT PRIMARY KEY,
            day TEXT NOT NULL,
            stations BLOB NOT NULL
        )
    ''')


def _find_subscription(connection: sqlite3.Connection, subscription_id: str):
    row = connection.execute('SELECT day, stations FROM subscriptions WHERE subscription_id = ?',
                             (subscription_id,)).fetchone()
    if row is None:
        return None
    day, stations = row
    return FrozenSubscription(subscription_id, day, tuple(json.loads(stations)))


def load_subscription(connection: sqlite3.Connection, subscription_id: str) -> FrozenSubscription:
    require_name(subscription_id, 'subscription id')
    subscription = _find_subscription(connection, subscription_id)
    if subscription is None:
        raise KeyError(f'unknown subscription {subscription_id}')
    return subscription


def store_subscription(connection: sqlite3.Connection,
                       subscription: FrozenSubscription) -> FrozenSubscription:
    previous = _find_subscription(connection, subscription.subscription_id)
    if previous is not None:
        if previous != subscription:
            raise ValueError('subscription is already frozen with different terms')
        return previous
    connection.execute('''
        INSERT INTO subscriptions (subscription_id, day, stations) VALUES (?, ?, ?)
    ''', (subscription.subscription_id, subscription.day, canonical_json(subscription.stations)))
    return subscription


def register_subscription(connection: sqlite3.Connection, subscription_id: str,
                          day: str, stations) -> FrozenSubscription:
    requested = FrozenSubscription(subscription_id, day, normalize_stations(stations))
    with atomic(connection):
        return store_subscription(connection, requested)
