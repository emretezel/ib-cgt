"""Statement coverage — which days of a new statement an earlier one already owns.

Two statements for one account may overlap: IB lets you download any
date range, so a re-downloaded statement that runs a few weeks past
the one already on file, or a calendar-year PDF that ends on the day
a tax-year HTML file starts, both repeat facts the database already
holds. IB prints no per-row identifier, and genuinely distinct fills
can share every visible field (migration 006), so the facts cannot be
de-duplicated by content. They are de-duplicated by **date** instead:

    The first statement ingested for an account owns every day of
    its period. A later statement whose period overlaps it
    contributes only the facts dated on days the earlier one does
    not cover.

Two refinements make that rule exact for IB's files:

* Only statements whose periods actually **overlap** the new one take
  part. IB assigns a transaction to a statement by the exchange
  trading date, so a fill executed late on the evening before a
  period starts can be printed with the previous calendar date while
  belonging to the later statement (a 17:06 ET CME fill on 5 April
  sits in the statement that starts on 6 April). Two consecutive,
  non-overlapping statements never share a fact, so nothing of the
  later one may be skipped — whatever its rows are dated.
* When an overlapping statement **starts on the same day** as the new
  one, IB attached exactly the same evening-before rows to both, so
  the new statement's rows dated before its own period are skipped
  as well.

Open positions are never filtered (they are a snapshot as of the
period's last day, and the calculator reads the latest statement's),
and `ingest --replace` remains the explicit way to refresh a period.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True, slots=True, kw_only=True)
class Coverage:
    """What earlier statements already own of one new statement's period.

    Attributes:
        period_start: First day of the new statement's period.
        period_end: Last day of the new statement's period.
        overlapping: The `(start, end)` periods already on file for
            the account that overlap `[period_start, period_end]`,
            sorted. Non-overlapping periods are deliberately absent.
    """

    period_start: date
    period_end: date
    overlapping: tuple[tuple[date, date], ...]

    @classmethod
    def for_period(cls, existing: Iterable[tuple[date, date]], start: date, end: date) -> Coverage:
        """Keep the periods of `existing` that overlap `[start, end]`."""
        overlapping = sorted(
            (period_start, period_end)
            for period_start, period_end in existing
            if period_start <= end and period_end >= start
        )
        return cls(period_start=start, period_end=end, overlapping=tuple(overlapping))

    def owned_elsewhere(self, day: date) -> bool:
        """True iff a fact of the new statement dated `day` belongs to an earlier statement.

        Either `day` lies inside an overlapping period, or it precedes
        the new statement's period and an overlapping statement starts
        on the very same day (so IB attached the same pre-period rows
        to both).
        """
        for start, end in self.overlapping:
            if start <= day <= end:
                return True
            if day < self.period_start and start == self.period_start:
                return True
        return False

    @property
    def fully_covered(self) -> bool:
        """True iff every day of the new statement's period is owned elsewhere."""
        day = self.period_start
        for start, end in self.overlapping:
            if end < day:
                continue
            if start > day:
                return False
            if end >= self.period_end:
                return True
            day = end + timedelta(days=1)
        return False


__all__ = ["Coverage"]
