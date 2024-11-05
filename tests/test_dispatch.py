# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the Dispatch class methods using pytest parametrization."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
import time_machine

from frequenz.client.common.microgrid.components import ComponentCategory
from frequenz.client.dispatch.recurrence import Frequency, RecurrenceRule, Weekday
from frequenz.client.dispatch.types import Dispatch, RunningState

# Define a fixed current time for testing to avoid issues with datetime.now()
CURRENT_TIME = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def dispatch_base() -> Dispatch:
    """Fixture to create a base Dispatch instance."""
    return Dispatch(
        id=1,
        type="TypeA",
        start_time=CURRENT_TIME,
        duration=timedelta(minutes=20),
        selector=[ComponentCategory.BATTERY],
        active=True,
        dry_run=False,
        payload={},
        recurrence=RecurrenceRule(),
        create_time=CURRENT_TIME - timedelta(hours=1),
        update_time=CURRENT_TIME - timedelta(minutes=30),
    )


@time_machine.travel(CURRENT_TIME)
@pytest.mark.parametrize(
    "dispatch_type, requested_type, active, start_time_offset, duration, expected_state",
    [
        # Dispatch type does not match the requested type
        (
            "TypeA",
            "TypeB",
            True,
            timedelta(minutes=-10),
            timedelta(minutes=20),
            RunningState.DIFFERENT_TYPE,
        ),
        # Dispatch is inactive
        (
            "TypeA1",
            "TypeA1",
            False,
            timedelta(minutes=-10),
            timedelta(minutes=20),
            RunningState.STOPPED,
        ),
        # Current time is before the start time
        (
            "TypeA2",
            "TypeA2",
            True,
            timedelta(minutes=10),
            timedelta(minutes=20),
            RunningState.STOPPED,
        ),
        # Dispatch with infinite duration
        (
            "TypeA3",
            "TypeA3",
            True,
            timedelta(minutes=-10),
            None,
            RunningState.RUNNING,
        ),
        # Dispatch is currently running
        (
            "TypeA4",
            "TypeA4",
            True,
            timedelta(minutes=-10),
            timedelta(minutes=20),
            RunningState.RUNNING,
        ),
        # Dispatch duration has passed
        (
            "TypeA5",
            "TypeA5",
            True,
            timedelta(minutes=-30),
            timedelta(minutes=20),
            RunningState.STOPPED,
        ),
    ],
)
# pylint: disable=too-many-arguments,too-many-positional-arguments
def test_dispatch_running(
    dispatch_base: Dispatch,
    dispatch_type: str,
    requested_type: str,
    active: bool,
    start_time_offset: timedelta,
    duration: timedelta | None,
    expected_state: RunningState,
) -> None:
    """Test the running method of the Dispatch class."""
    dispatch = replace(
        dispatch_base,
        type=dispatch_type,
        start_time=CURRENT_TIME + start_time_offset,
        duration=duration,
        active=active,
    )

    assert dispatch.running(requested_type) == expected_state


@time_machine.travel(CURRENT_TIME)
@pytest.mark.parametrize(
    "active, duration, start_time_offset, expected_until_offset",
    [
        # Dispatch is inactive
        (False, timedelta(minutes=20), timedelta(minutes=-10), None),
        # Dispatch with infinite duration (no duration)
        (True, None, timedelta(minutes=-10), None),
        # Current time is before the start time
        (True, timedelta(minutes=20), timedelta(minutes=10), timedelta(minutes=30)),
        # Dispatch is currently running
        (
            True,
            timedelta(minutes=20),
            timedelta(minutes=-10),
            timedelta(minutes=10),
        ),
    ],
)
def test_dispatch_until(
    dispatch_base: Dispatch,
    active: bool,
    duration: timedelta | None,
    start_time_offset: timedelta,
    expected_until_offset: timedelta | None,
) -> None:
    """Test the until property of the Dispatch class."""
    start_time = CURRENT_TIME + start_time_offset
    dispatch = replace(
        dispatch_base,
        active=active,
        duration=duration,
        start_time=start_time,
    )

    if duration is None:
        with pytest.raises(ValueError):
            _ = dispatch.until
        return

    expected_until = (
        CURRENT_TIME + expected_until_offset
        if expected_until_offset is not None
        else None
    )

    assert dispatch.until == expected_until


@time_machine.travel(CURRENT_TIME)
@pytest.mark.parametrize(
    "recurrence, duration, start_time_offset, expected_next_run_offset",
    [
        # No recurrence and start time in the past
        (RecurrenceRule(), timedelta(minutes=20), timedelta(minutes=-10), None),
        # No recurrence and start time in the future
        (
            RecurrenceRule(),
            timedelta(minutes=20),
            timedelta(minutes=10),
            timedelta(minutes=10),
        ),
        # Daily recurrence
        (
            RecurrenceRule(frequency=Frequency.DAILY, interval=1),
            timedelta(minutes=20),
            timedelta(minutes=-10),
            timedelta(days=1, minutes=-10),
        ),
        # Weekly recurrence on Monday
        (
            RecurrenceRule(
                frequency=Frequency.WEEKLY, byweekdays=[Weekday.MONDAY], interval=1
            ),
            timedelta(minutes=20),
            timedelta(minutes=-10),
            None,  # We'll compute expected_next_run inside the test
        ),
    ],
)
def test_dispatch_next_run(
    dispatch_base: Dispatch,
    recurrence: RecurrenceRule,
    duration: timedelta | None,
    start_time_offset: timedelta,
    expected_next_run_offset: timedelta | None,
) -> None:
    """Test the next_run property of the Dispatch class."""
    start_time = CURRENT_TIME + start_time_offset
    dispatch = replace(
        dispatch_base,
        start_time=start_time,
        duration=duration,
        recurrence=recurrence,
    )

    if recurrence.frequency == Frequency.WEEKLY:
        # Compute the next run based on the recurrence rule
        expected_next_run = recurrence.prepare(start_time).after(
            CURRENT_TIME, inc=False
        )
    elif expected_next_run_offset is not None:
        expected_next_run = CURRENT_TIME + expected_next_run_offset
    else:
        expected_next_run = None

    assert dispatch.next_run == expected_next_run


@time_machine.travel(CURRENT_TIME)
@pytest.mark.parametrize(
    "after_offset, recurrence, duration, expected_next_run_after_offset",
    [
        # No recurrence
        (timedelta(minutes=10), RecurrenceRule(), timedelta(minutes=20), None),
        # Weekly recurrence, after current time
        (
            timedelta(days=2),
            RecurrenceRule(
                frequency=Frequency.WEEKLY, byweekdays=[Weekday.MONDAY], interval=1
            ),
            timedelta(minutes=20),
            None,  # We'll compute expected_next_run_after inside the test
        ),
        # Daily recurrence
        (
            timedelta(minutes=10),
            RecurrenceRule(frequency=Frequency.DAILY, interval=1),
            timedelta(minutes=20),
            timedelta(days=1),
        ),
    ],
)
def test_dispatch_next_run_after(
    dispatch_base: Dispatch,
    after_offset: timedelta,
    recurrence: RecurrenceRule,
    duration: timedelta | None,
    expected_next_run_after_offset: timedelta | None,
) -> None:
    """Test the next_run_after method of the Dispatch class."""
    after = CURRENT_TIME + after_offset
    dispatch = replace(
        dispatch_base,
        recurrence=recurrence,
        duration=duration,
    )

    if recurrence.frequency == Frequency.WEEKLY:
        expected_next_run_after = recurrence.prepare(dispatch.start_time).after(
            after, inc=True
        )
    elif expected_next_run_after_offset is not None:
        expected_next_run_after = CURRENT_TIME + expected_next_run_after_offset
    else:
        expected_next_run_after = None

    assert dispatch.next_run_after(after) == expected_next_run_after
