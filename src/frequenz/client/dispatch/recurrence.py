# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Types for recurrence rules."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum

from dateutil import rrule

# pylint: disable=no-name-in-module
from frequenz.api.dispatch.v1.dispatch_pb2 import RecurrenceRule as PBRecurrenceRule

from frequenz.client.base.conversion import to_datetime, to_timestamp

# pylint: enable=no-name-in-module


class Weekday(IntEnum):
    """Enum representing the day of the week."""

    UNSPECIFIED = PBRecurrenceRule.WEEKDAY_UNSPECIFIED
    MONDAY = PBRecurrenceRule.WEEKDAY_MONDAY
    TUESDAY = PBRecurrenceRule.WEEKDAY_TUESDAY
    WEDNESDAY = PBRecurrenceRule.WEEKDAY_WEDNESDAY
    THURSDAY = PBRecurrenceRule.WEEKDAY_THURSDAY
    FRIDAY = PBRecurrenceRule.WEEKDAY_FRIDAY
    SATURDAY = PBRecurrenceRule.WEEKDAY_SATURDAY
    SUNDAY = PBRecurrenceRule.WEEKDAY_SUNDAY


class Frequency(IntEnum):
    """Enum representing the frequency of the recurrence."""

    UNSPECIFIED = PBRecurrenceRule.FREQUENCY_UNSPECIFIED
    MINUTELY = PBRecurrenceRule.FREQUENCY_MINUTELY
    HOURLY = PBRecurrenceRule.FREQUENCY_HOURLY
    DAILY = PBRecurrenceRule.FREQUENCY_DAILY
    WEEKLY = PBRecurrenceRule.FREQUENCY_WEEKLY
    MONTHLY = PBRecurrenceRule.FREQUENCY_MONTHLY
    YEARLY = PBRecurrenceRule.FREQUENCY_YEARLY


_RRULE_FREQ_MAP = {
    Frequency.MINUTELY: rrule.MINUTELY,
    Frequency.HOURLY: rrule.HOURLY,
    Frequency.DAILY: rrule.DAILY,
    Frequency.WEEKLY: rrule.WEEKLY,
    Frequency.MONTHLY: rrule.MONTHLY,
}
"""To map from our Frequency enum to the dateutil library enum."""

_RRULE_WEEKDAY_MAP = {
    Weekday.MONDAY: rrule.MO,
    Weekday.TUESDAY: rrule.TU,
    Weekday.WEDNESDAY: rrule.WE,
    Weekday.THURSDAY: rrule.TH,
    Weekday.FRIDAY: rrule.FR,
    Weekday.SATURDAY: rrule.SA,
    Weekday.SUNDAY: rrule.SU,
}
"""To map from our Weekday enum to the dateutil library enum."""


@dataclass(kw_only=True)
class EndCriteria:
    """Controls when a recurring dispatch should end."""

    count: int | None = None
    """The number of times this dispatch should recur."""
    until: datetime | None = None
    """The end time of this dispatch in UTC."""

    @classmethod
    def from_protobuf(cls, pb_criteria: PBRecurrenceRule.EndCriteria) -> "EndCriteria":
        """Convert a protobuf end criteria to an end criteria.

        Args:
            pb_criteria: The protobuf end criteria to convert.

        Returns:
            The converted end criteria.
        """
        instance = cls()

        match pb_criteria.WhichOneof("count_or_until"):
            case "count":
                instance.count = pb_criteria.count
            case "until":
                instance.until = to_datetime(pb_criteria.until)
        return instance

    def to_protobuf(self) -> PBRecurrenceRule.EndCriteria:
        """Convert an end criteria to a protobuf end criteria.

        Returns:
            The converted protobuf end criteria.
        """
        pb_criteria = PBRecurrenceRule.EndCriteria()

        if self.count is not None:
            pb_criteria.count = self.count
        elif self.until is not None:
            pb_criteria.until.CopyFrom(to_timestamp(self.until))

        return pb_criteria


# pylint: disable=too-many-instance-attributes
@dataclass(kw_only=True)
class RecurrenceRule:
    """Ruleset governing when and how a dispatch should re-occur.

    Attributes follow the iCalendar specification (RFC5545) for recurrence rules.
    """

    frequency: Frequency = Frequency.UNSPECIFIED
    """The frequency specifier of this recurring dispatch."""

    interval: int = 0
    """How often this dispatch should recur, based on the frequency."""

    end_criteria: EndCriteria | None = None
    """When this dispatch should end.

    Can recur a fixed number of times or until a given timestamp."""

    byminutes: list[int] = field(default_factory=list)
    """On which minute(s) of the hour the event occurs."""

    byhours: list[int] = field(default_factory=list)
    """On which hour(s) of the day the event occurs."""

    byweekdays: list[Weekday] = field(default_factory=list)
    """On which day(s) of the week the event occurs."""

    bymonthdays: list[int] = field(default_factory=list)
    """On which day(s) of the month the event occurs."""

    bymonths: list[int] = field(default_factory=list)
    """On which month(s) of the year the event occurs."""

    @classmethod
    def from_protobuf(cls, pb_rule: PBRecurrenceRule) -> "RecurrenceRule":
        """Convert a protobuf recurrence rule to a recurrence rule.

        Args:
            pb_rule: The protobuf recurrence rule to convert.

        Returns:
            The converted recurrence rule.
        """
        return RecurrenceRule(
            frequency=Frequency(pb_rule.freq),
            interval=pb_rule.interval,
            end_criteria=(
                EndCriteria.from_protobuf(pb_rule.end_criteria)
                if pb_rule.HasField("end_criteria")
                else None
            ),
            byminutes=list(pb_rule.byminutes),
            byhours=list(pb_rule.byhours),
            byweekdays=[Weekday(day) for day in pb_rule.byweekdays],
            bymonthdays=list(pb_rule.bymonthdays),
            bymonths=list(pb_rule.bymonths),
        )

    def to_protobuf(self) -> PBRecurrenceRule:
        """Convert a recurrence rule to a protobuf recurrence rule.

        Returns:
            The converted protobuf recurrence rule.
        """
        pb_rule = PBRecurrenceRule()

        pb_rule.freq = self.frequency.value
        pb_rule.interval = self.interval
        if self.end_criteria is not None:
            pb_rule.end_criteria.CopyFrom(self.end_criteria.to_protobuf())
        pb_rule.byminutes.extend(self.byminutes)
        pb_rule.byhours.extend(self.byhours)
        pb_rule.byweekdays.extend([day.value for day in self.byweekdays])
        pb_rule.bymonthdays.extend(self.bymonthdays)
        pb_rule.bymonths.extend(self.bymonths)

        return pb_rule

    def _as_rrule(self, start_time: datetime) -> rrule.rrule:
        """Prepare the rrule object.

        Args:
            start_time: The start time of the dispatch.

        Returns:
            The rrule object.

        Raises:
            ValueError: If the interval is 0.
        """
        if self.interval == 0:
            raise ValueError("Interval must be greater than 0")

        count, until = (None, None)
        if end := self.end_criteria:
            count = end.count
            until = end.until

        rrule_obj = rrule.rrule(
            freq=_RRULE_FREQ_MAP[self.frequency],
            dtstart=start_time,
            count=count,
            until=until,
            byminute=self.byminutes or None,
            byhour=self.byhours or None,
            byweekday=[_RRULE_WEEKDAY_MAP[weekday] for weekday in self.byweekdays]
            or None,
            bymonthday=self.bymonthdays or None,
            bymonth=self.bymonths or None,
            interval=self.interval,
        )

        return rrule_obj
