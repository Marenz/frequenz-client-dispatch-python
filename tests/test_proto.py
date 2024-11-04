# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.client.dispatch types."""

from datetime import datetime, timedelta, timezone

from frequenz.client.common.microgrid.components import ComponentCategory
from frequenz.client.dispatch._internal_types import DispatchCreateRequest
from frequenz.client.dispatch.recurrence import (
    EndCriteria,
    Frequency,
    RecurrenceRule,
    Weekday,
)
from frequenz.client.dispatch.types import (
    Dispatch,
    _component_selector_from_protobuf,
    _component_selector_to_protobuf,
)


def test_component_selector() -> None:
    """Test the component selector."""
    for selector in (
        [1, 2, 3],
        [10, 20, 30],
        [ComponentCategory.BATTERY],
        [ComponentCategory.GRID],
        [ComponentCategory.METER],
        [ComponentCategory.EV_CHARGER, ComponentCategory.BATTERY],
    ):
        protobuf = _component_selector_to_protobuf(selector)
        assert _component_selector_from_protobuf(protobuf) == selector


def test_end_criteria() -> None:
    """Test the end criteria."""
    for end_criteria in (
        EndCriteria(
            until=datetime(2023, 1, 1, tzinfo=timezone.utc),
        ),
        EndCriteria(
            count=10,
        ),
    ):
        assert EndCriteria.from_protobuf(end_criteria.to_protobuf()) == end_criteria


def test_recurrence_rule() -> None:
    """Test the recurrence rule."""
    for recurrence_rule in (
        RecurrenceRule(
            frequency=Frequency.DAILY,
            interval=1,
            end_criteria=EndCriteria(until=datetime(2023, 1, 1, tzinfo=timezone.utc)),
            byminutes=[1, 20, 59],
        ),
        RecurrenceRule(
            frequency=Frequency.MONTHLY,
            interval=3,
            end_criteria=EndCriteria(count=20),
            byhours=[1, 12, 23],
            byweekdays=[Weekday.SUNDAY, Weekday.FRIDAY],
        ),
        RecurrenceRule(
            frequency=Frequency.HOURLY,
            interval=30,
            end_criteria=EndCriteria(until=datetime(2025, 1, 1, tzinfo=timezone.utc)),
            bymonthdays=[1, 15, 31],
        ),
        RecurrenceRule(
            frequency=Frequency.MONTHLY,
            interval=10,
            end_criteria=EndCriteria(count=5),
            bymonths=[1, 6, 12],
        ),
        RecurrenceRule(
            frequency=Frequency.WEEKLY,
            interval=2,
            end_criteria=EndCriteria(count=10),
            byweekdays=[Weekday.MONDAY, Weekday.TUESDAY],
        ),
    ):
        assert (
            RecurrenceRule.from_protobuf(recurrence_rule.to_protobuf())
            == recurrence_rule
        )


def test_dispatch() -> None:
    """Test the dispatch."""
    for dispatch in (
        Dispatch(
            id=123,
            type="test",
            create_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
            update_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
            start_time=datetime(2024, 10, 10, tzinfo=timezone.utc),
            duration=timedelta(days=10),
            selector=[1, 2, 3],
            active=True,
            dry_run=False,
            payload={"key": "value"},
            recurrence=RecurrenceRule(
                frequency=Frequency.DAILY,
                interval=1,
                end_criteria=EndCriteria(
                    until=datetime(2023, 1, 1, tzinfo=timezone.utc)
                ),
                byminutes=[1, 20, 59],
            ),
        ),
        Dispatch(
            id=124,
            type="test-2",
            create_time=datetime(2024, 3, 10, tzinfo=timezone.utc),
            update_time=datetime(2024, 3, 11, tzinfo=timezone.utc),
            start_time=datetime(2024, 11, 10, tzinfo=timezone.utc),
            duration=timedelta(seconds=20),
            selector=[ComponentCategory.BATTERY],
            active=False,
            dry_run=True,
            payload={"key": "value1"},
            recurrence=RecurrenceRule(
                frequency=Frequency.MONTHLY,
                interval=3,
                end_criteria=EndCriteria(count=20),
                byhours=[1, 12, 23],
                byweekdays=[Weekday.SUNDAY, Weekday.FRIDAY],
            ),
        ),
    ):
        assert Dispatch.from_protobuf(dispatch.to_protobuf()) == dispatch


def test_dispatch_create_request_with_no_recurrence() -> None:
    """Test the dispatch create request with no recurrence."""
    request = DispatchCreateRequest(
        microgrid_id=123,
        type="test",
        start_time=datetime(2024, 10, 10, tzinfo=timezone.utc),
        duration=timedelta(days=10),
        selector=[1, 2, 3],
        active=True,
        dry_run=False,
        payload={"key": "value"},
        recurrence=None,
    )

    assert request.to_protobuf().dispatch_data.HasField("recurrence") is False
