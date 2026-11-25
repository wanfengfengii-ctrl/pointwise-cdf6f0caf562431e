from collections.abc import Iterable
from dataclasses import dataclass

from lake.readings import Reading


@dataclass(frozen=True)
class StationMeasure:
    kg: int = 0
    missing: int = 0

    def __add__(self, other):
        if not isinstance(other, StationMeasure):
            return NotImplemented
        return StationMeasure(self.kg + other.kg, self.missing + other.missing)


def measure_readings(readings: Iterable[Reading]) -> StationMeasure:
    kg = 0
    missing = 0
    for reading in readings:
        if reading.kg is None:
            missing += 1
        else:
            kg += reading.kg
    return StationMeasure(kg, missing)


def measure_change(reading: Reading, direction: int) -> StationMeasure:
    if type(direction) is not int or direction not in (-1, 1):
        raise ValueError('direction must be -1 or 1')
    if reading.kg is None:
        return StationMeasure(missing=direction)
    return StationMeasure(kg=direction * reading.kg)
