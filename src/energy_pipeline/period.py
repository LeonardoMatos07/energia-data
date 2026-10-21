"""Monthly reference period handled by each pipeline run."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

_PERIOD_PATTERN = re.compile(r"^(\d{4})-(\d{2})$")


@dataclass(frozen=True, order=True)
class Period:
    year: int
    month: int

    def __post_init__(self) -> None:
        if not 1 <= self.month <= 12:
            raise ValueError(f"Invalid month: {self.month}")

    @classmethod
    def parse(cls, value: str) -> Period:
        """Parse a period written as ``YYYY-MM``."""
        match = _PERIOD_PATTERN.match(value.strip())
        if not match:
            raise ValueError(f"Expected a period formatted as YYYY-MM, got {value!r}")
        return cls(int(match.group(1)), int(match.group(2)))

    @classmethod
    def from_datetime(cls, value: datetime) -> Period:
        return cls(value.year, value.month)

    @property
    def start(self) -> datetime:
        """First instant of the month (inclusive)."""
        return datetime(self.year, self.month, 1)

    @property
    def end(self) -> datetime:
        """First instant of the following month (exclusive)."""
        return self.next().start

    @property
    def hours(self) -> int:
        # Brazil has not observed daylight saving time since 2019, so every
        # day in the ONS series has exactly 24 hourly records.
        return int((self.end - self.start).total_seconds() // 3600)

    @property
    def half_hours(self) -> int:
        return self.hours * 2

    def next(self) -> Period:
        if self.month == 12:
            return Period(self.year + 1, 1)
        return Period(self.year, self.month + 1)

    def previous(self) -> Period:
        if self.month == 1:
            return Period(self.year - 1, 12)
        return Period(self.year, self.month - 1)

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def periods_between(first: Period, last: Period) -> list[Period]:
    """Every month from ``first`` to ``last``, inclusive."""
    if first > last:
        raise ValueError(f"{first} is after {last}")
    periods = [first]
    while periods[-1] < last:
        periods.append(periods[-1].next())
    return periods


def trailing_periods(last: Period, count: int) -> list[Period]:
    """The ``count`` months ending at ``last`` (the reprocessing window)."""
    first = last
    for _ in range(count - 1):
        first = first.previous()
    return periods_between(first, last)
