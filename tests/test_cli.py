# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Test the dispatch CLI."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Generator, Literal
from unittest.mock import patch

import pytest
from asyncclick.testing import CliRunner
from tzlocal import get_localzone

from frequenz.client.common.microgrid.components import ComponentCategory
from frequenz.client.dispatch.__main__ import cli
from frequenz.client.dispatch.recurrence import (
    EndCriteria,
    Frequency,
    RecurrenceRule,
    Weekday,
)
from frequenz.client.dispatch.test.client import ALL_KEY, FakeClient
from frequenz.client.dispatch.types import Dispatch

TEST_NOW = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
"""Arbitrary time used as NOW for testing."""

ENVIRONMENT_VARIABLES = {"DISPATCH_API_KEY": ALL_KEY}


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for CLI Runner."""
    return CliRunner()


@pytest.fixture
def fake_client() -> FakeClient:
    """Fixture for Fake Client."""
    return FakeClient()


@pytest.fixture(autouse=True)
def mock_client(fake_client: FakeClient) -> Generator[None, None, None]:
    """Fixture to mock get_client with FakeClient."""
    with patch("frequenz.client.dispatch.__main__.Client", return_value=fake_client):
        yield


# For test functions we want to disable some pylint checks, we need many (positional)
# arguments to pass fixtures, these functions are not meant to be called directly, and
# having too many locals in tests is not a problem either.
# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatches, microgrid_id, expected_output, expected_return_code",
    [
        (
            {
                1: [
                    Dispatch(
                        id=1,
                        type="test",
                        start_time=datetime(2023, 1, 1, 0, 0, 0),
                        duration=timedelta(seconds=3600),
                        target=[1, 2, 3],
                        active=True,
                        dry_run=False,
                        payload={},
                        recurrence=RecurrenceRule(),
                        create_time=datetime(2023, 1, 1, 0, 0, 0),
                        update_time=datetime(2023, 1, 1, 0, 0, 0),
                    )
                ]
            },
            1,
            "1 dispatches total.",
            0,
        ),
        ({}, 1, "0 dispatches total.", 0),
        (
            {
                2: [
                    Dispatch(
                        id=2,
                        type="test",
                        start_time=datetime(2023, 1, 1, 0, 0, 0),
                        duration=timedelta(seconds=3600),
                        target=[1, 2, 3],
                        active=True,
                        dry_run=False,
                        payload={},
                        recurrence=RecurrenceRule(),
                        create_time=datetime(2023, 1, 1, 0, 0, 0),
                        update_time=datetime(2023, 1, 1, 0, 0, 0),
                    )
                ]
            },
            1,
            "0 dispatches total.",
            0,
        ),
        (
            {
                1: [
                    Dispatch(
                        id=1,
                        type="test",
                        start_time=datetime(2023, 1, 1, 0, 0, 0),
                        duration=timedelta(seconds=3600),
                        target=[1, 2, 3],
                        active=True,
                        dry_run=False,
                        payload={},
                        recurrence=RecurrenceRule(),
                        create_time=datetime(2023, 1, 1, 0, 0, 0),
                        update_time=datetime(2023, 1, 1, 0, 0, 0),
                    )
                ],
                2: [
                    Dispatch(
                        id=2,
                        type="test",
                        start_time=datetime(2023, 1, 1, 0, 0, 0),
                        duration=timedelta(seconds=3600),
                        target=[1, 2, 3],
                        active=True,
                        dry_run=False,
                        payload={},
                        recurrence=RecurrenceRule(),
                        create_time=datetime(2023, 1, 1, 0, 0, 0),
                        update_time=datetime(2023, 1, 1, 0, 0, 0),
                    ),
                ],
            },
            1,
            "1 dispatches total.",
            0,
        ),
        (
            {},
            "x",
            "Error: Invalid value for 'MICROGRID_ID': 'x' is not a valid integer.",
            2,
        ),
    ],
)
async def test_list_command(
    runner: CliRunner,
    fake_client: FakeClient,
    dispatches: dict[int, list[Dispatch]],
    microgrid_id: int,
    expected_output: str,
    expected_return_code: int,
) -> None:
    """Test the list command."""
    for microgrid_id_, dispatch_list in dispatches.items():
        fake_client.set_dispatches(microgrid_id_, dispatch_list)

    result = await runner.invoke(
        cli, ["--raw", "list", str(microgrid_id)], env=ENVIRONMENT_VARIABLES
    )
    assert expected_output in result.output
    assert result.exit_code == expected_return_code


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args, expected_microgrid_id, expected_type, "
    "expected_start_time_delta, expected_duration, "
    "expected_target, expected_options, expected_reccurence, expected_return_code",
    [
        (
            [
                "create",
                "829",
                "test",
                "BATTERY",
                "in 1 hour",
                "1h",
                "--active",
                "False",
            ],
            829,
            "test",
            timedelta(hours=1),
            timedelta(seconds=3600),
            [ComponentCategory.BATTERY],
            {"active": False},
            RecurrenceRule(),
            0,
        ),
        (
            [
                "create",
                "1",
                "test",
                "1,2,3",
                "in 2 hours",
                "1 hour",
                "--dry-run",
                "true",
            ],
            1,
            "test",
            timedelta(hours=2),
            timedelta(seconds=3600),
            [1, 2, 3],
            {"dry_run": True},
            RecurrenceRule(),
            0,
        ),
        (
            ["create", "x"],
            0,
            "",
            timedelta(),
            timedelta(),
            [],
            {},
            RecurrenceRule(),
            2,
        ),
        (
            [
                "create",
                "1",
                "test",
                "CHP",
                "in 1 hour",
                "1h",
                "--frequency",
                "hourly",
                "--interval",
                "5",
                "--count",
                "10",
                "--by-minute",
                "0",
                "--by-minute",
                "30",
                "--by-hour",
                "5",
                "--by-weekday",
                "Monday",
                "--by-weekday",
                "WEDNESDAY",
                "--by-monthday",
                "15",
                "--by-monthday",
                "16",
                "--by-monthday",
                "17",
            ],
            1,
            "test",
            timedelta(hours=1),
            timedelta(seconds=3600),
            [ComponentCategory.CHP],
            {},
            RecurrenceRule(
                frequency=Frequency.HOURLY,
                interval=5,
                end_criteria=EndCriteria(
                    count=10,
                    until=None,
                ),
                byminutes=[0, 30],
                byhours=[5],
                byweekdays=[Weekday.MONDAY, Weekday.WEDNESDAY],
                bymonthdays=[15, 16, 17],
            ),
            0,
        ),
        (
            [
                "create",
                "50",
                "test50",
                "EV_CHARGER",
                "in 5 hours",
                "1h",
                "--frequency",
                "daily",
                "--until",
                "in 24h",
                "--by-minute",
                "5",
            ],
            50,
            "test50",
            timedelta(hours=5),
            timedelta(seconds=3600),
            [ComponentCategory.EV_CHARGER],
            {},
            RecurrenceRule(
                frequency=Frequency.DAILY,
                interval=1,
                end_criteria=EndCriteria(
                    count=None, until=(TEST_NOW + timedelta(days=1))
                ),
                byminutes=[5],
                byhours=[],
                byweekdays=[],
                bymonthdays=[],
            ),
            0,
        ),
        (
            [
                "create",
                "1",
                "test_start_immediately",
                "BATTERY",
                "now",
                "1h",
            ],
            1,
            "test_start_immediately",
            "NOW",
            timedelta(seconds=3600),
            [ComponentCategory.BATTERY],
            {},
            RecurrenceRule(),
            0,
        ),
    ],
)
async def test_create_command(
    runner: CliRunner,
    fake_client: FakeClient,
    args: list[str],
    expected_microgrid_id: int,
    expected_type: str,
    expected_start_time_delta: timedelta | Literal["NOW"],
    expected_duration: timedelta,
    expected_target: list[int] | list[ComponentCategory],
    expected_options: dict[str, Any],
    expected_reccurence: RecurrenceRule | None,
    expected_return_code: int,
) -> None:
    """Test the create command."""
    args.insert(0, "--raw")
    result = await runner.invoke(cli, args, env=ENVIRONMENT_VARIABLES)
    now = datetime.now(get_localzone())

    if (
        expected_reccurence is not None
        and expected_reccurence.end_criteria is not None
        and expected_reccurence.end_criteria.until is not None
    ):
        expected_reccurence = replace(
            expected_reccurence,
            end_criteria=replace(
                expected_reccurence.end_criteria,
                until=(now + (expected_reccurence.end_criteria.until - TEST_NOW))
                .astimezone(timezone.utc)
                .replace(microsecond=0),
            ),
        )

    assert result.exit_code == expected_return_code
    assert "id" in result.output

    dispatches = fake_client.dispatches(expected_microgrid_id)

    if expected_return_code != 0:
        assert len(dispatches) == 0
        return

    assert len(dispatches) == 1
    created_dispatch = dispatches[0]
    assert created_dispatch.type == expected_type

    if isinstance(expected_start_time_delta, timedelta):
        assert created_dispatch.start_time.timestamp() == pytest.approx(
            (now + expected_start_time_delta).astimezone(timezone.utc).timestamp(),
            abs=2,
        )
    else:
        assert created_dispatch.start_time.timestamp() == pytest.approx(
            now.astimezone(timezone.utc).timestamp(), abs=2
        )

    assert created_dispatch.duration and (
        created_dispatch.duration.total_seconds()
        == pytest.approx(expected_duration.total_seconds(), abs=2)
    )
    assert created_dispatch.target == expected_target
    assert created_dispatch.recurrence == expected_reccurence

    for key, value in expected_options.items():
        assert getattr(created_dispatch, key) == value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatches, args, fields, expected_return_code, expected_output",
    [
        (
            [
                Dispatch(
                    id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    target=[ComponentCategory.BATTERY],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            [
                "--duration",
                "7200",
            ],
            {"duration": timedelta(seconds=7200)},
            0,
            "duration=datetime.timedelta(seconds=7200)",
        ),
        (
            [
                Dispatch(
                    id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    target=[ComponentCategory.BATTERY],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            [
                "--active",
                "False",
            ],
            {
                "active": False,
            },
            0,
            "active=False",
        ),
        (
            [
                Dispatch(
                    id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    target=[ComponentCategory.BATTERY, ComponentCategory.EV_CHARGER],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            [
                "--target",
                "BATTERY, EV_CHARGER, CHP",
            ],
            {
                "target": [
                    ComponentCategory.BATTERY,
                    ComponentCategory.EV_CHARGER,
                    ComponentCategory.CHP,
                ],
            },
            0,
            "target=[<ComponentCategory.BATTERY: 5>,\n                 "
            + "<ComponentCategory.EV_CHARGER: 6>,\n                 "
            + "<ComponentCategory.CHP: 10>]",
        ),
        (
            [
                Dispatch(
                    id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    target=[500, 501],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            [
                "--target",
                "400, 401",
                "--frequency",
                "daily",
                "--interval",
                "5",
                "--count",
                "10",
                "--by-minute",
                "0",
                "--by-minute",
                "3",
                "--by-minute",
                "5",
                "--by-minute",
                "30",
                "--payload",
                '{"key": "value"}',
            ],
            {
                "target": [400, 401],
                "recurrence": RecurrenceRule(
                    frequency=Frequency.DAILY,
                    interval=5,
                    end_criteria=EndCriteria(
                        count=10,
                        until=None,
                    ),
                    byminutes=[0, 3, 5, 30],
                    byhours=[],
                    byweekdays=[],
                    bymonthdays=[],
                ),
                "payload": {"key": "value"},
            },
            0,
            """         target=[400, 401],
         active=True,
         dry_run=False,
         payload={'key': 'value'},
         recurrence=RecurrenceRule(frequency=<Frequency.DAILY: 3>,
                                   interval=5,
                                   end_criteria=EndCriteria(count=10,
                                                            until=None),
                                   byminutes=[0, 3, 5, 30],
                                   byhours=[],
                                   byweekdays=[],
                                   bymonthdays=[],
                                   bymonths=[]),""",
        ),
        (
            [],
            [
                "--duration",
                "frankly my dear, I don't give a damn",
            ],
            {},
            2,
            "Error: Invalid value for '--duration': Could not parse time expression",
        ),
    ],
)
async def test_update_command(
    runner: CliRunner,
    fake_client: FakeClient,
    dispatches: list[Dispatch],
    args: list[str],
    fields: dict[str, Any],
    expected_return_code: int,
    expected_output: str,
) -> None:
    """Test the update command."""
    fake_client.set_dispatches(1, dispatches)
    result = await runner.invoke(
        cli, ["--raw", "update", "1", "1", *args], env=ENVIRONMENT_VARIABLES
    )
    assert expected_output in result.output
    assert result.exit_code == expected_return_code
    if dispatches:
        assert len(fake_client.dispatches(1)) == 1
        for key, value in fields.items():
            assert getattr(fake_client.dispatches(1)[0], key) == value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatches, dispatch_id, expected_in_output",
    [
        (
            [
                Dispatch(
                    id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    target=[1, 2, 3],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            1,
            "Dispatch(id=1,",
        ),
        ([], 999, "Error"),
        (
            [],
            "x",
            "Error: Invalid value for '[DISPATCH_IDS]...': 'x' is not a valid integer.",
        ),
    ],
)
async def test_get_command(
    runner: CliRunner,
    fake_client: FakeClient,
    dispatches: list[Dispatch],
    dispatch_id: int,
    expected_in_output: str,
) -> None:
    """Test the get command."""
    fake_client.set_dispatches(1, dispatches)
    result = await runner.invoke(
        cli, ["--raw", "get", "1", str(dispatch_id)], env=ENVIRONMENT_VARIABLES
    )
    assert result.exit_code == 0 if dispatches else 1
    assert expected_in_output in result.output


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatches, dispatch_id, expected_output, expected_return_code",
    [
        (
            [
                Dispatch(
                    id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    target=[1, 2, 3],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            1,
            "Dispatches deleted: [1]",
            0,
        ),
        ([], 999, "Error", 1),
        (
            [],
            "x",
            "Error: Invalid value for '[DISPATCH_IDS]...': Invalid integer",
            2,
        ),
    ],
)
async def test_delete_command(
    runner: CliRunner,
    fake_client: FakeClient,
    dispatches: list[Dispatch],
    dispatch_id: int,
    expected_output: str,
    expected_return_code: int,
) -> None:
    """Test the delete command."""
    fake_client.set_dispatches(1, dispatches)
    result = await runner.invoke(
        cli, ["delete", "1", str(dispatch_id)], env=ENVIRONMENT_VARIABLES
    )
    assert result.exit_code == expected_return_code
    assert expected_output in result.output
    if dispatches:
        assert len(fake_client.dispatches(1)) == 0
