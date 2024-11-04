# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.client.dispatch package."""

import asyncio
import random
from dataclasses import replace
from datetime import timedelta

import grpc
from pytest import raises

from frequenz.client.dispatch.test.client import FakeClient, to_create_params
from frequenz.client.dispatch.test.fixtures import client, generator, sample
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.client.dispatch.types import Dispatch, Event

# Ignore flake8 error in the rest of the file to use the same fixture names
# flake8: noqa[811]


def _update_metadata(dispatch: Dispatch, created: Dispatch) -> Dispatch:
    """Update the dispatch.

    Updates the id, create_time and update_time of the local dispatch
    with the values from the created dispatch.

    Args:
        dispatch: The local dispatch.
        created: The created dispatch.

    Returns:
        The updated dispatch.
    """
    return replace(
        dispatch,
        id=created.id,
        create_time=created.create_time,
        update_time=created.update_time,
    )


async def test_create_dispatch(client: FakeClient, sample: Dispatch) -> None:
    """Test creating a dispatch."""
    microgrid_id = random.randint(1, 100)
    dispatch = await client.create(**to_create_params(microgrid_id, sample))

    sample = _update_metadata(sample, dispatch)
    assert dispatch == sample


async def test_create_return_dispatch(
    client: FakeClient, generator: DispatchGenerator
) -> None:
    """Test creating a dispatch and returning the created dispatch."""
    microgrid_id = random.randint(1, 100)
    for _ in range(100):
        sample = generator.generate_dispatch()

        dispatch = await client.create(**to_create_params(microgrid_id, sample))

        sample = _update_metadata(sample, dispatch)

        assert dispatch == sample


async def test_create_duration_none(client: FakeClient, sample: Dispatch) -> None:
    """Test creating a dispatch with a None duration."""
    microgrid_id = random.randint(1, 100)
    sample = replace(sample, duration=None)
    dispatch = await client.create(**to_create_params(microgrid_id, sample))
    sample = _update_metadata(sample, dispatch)
    assert dispatch == sample


async def test_list_dispatches(
    client: FakeClient, generator: DispatchGenerator
) -> None:
    """Test listing dispatches."""
    microgrid_id = random.randint(1, 100)

    client.set_dispatches(
        microgrid_id=microgrid_id,
        value=[generator.generate_dispatch() for _ in range(100)],
    )

    dispatches = client.list(microgrid_id=1)
    async for page in dispatches:
        for dispatch in page:
            assert dispatch in client.dispatches(microgrid_id=1)


async def test_list_dispatches_no_duration(
    client: FakeClient, generator: DispatchGenerator
) -> None:
    """Test listing dispatches with a None duration."""
    microgrid_id = random.randint(1, 100)

    client.set_dispatches(
        microgrid_id=microgrid_id,
        value=[
            replace(generator.generate_dispatch(), duration=None) for _ in range(100)
        ],
    )

    dispatches = client.list(microgrid_id=1)
    async for page in dispatches:
        for dispatch in page:
            assert dispatch in client.dispatches(microgrid_id=1)


async def test_list_create_dispatches(
    client: FakeClient, generator: DispatchGenerator
) -> None:
    """Test listing dispatches."""
    microgrid_id = random.randint(1, 100)

    # Test with empty list
    page = await anext(client.list(microgrid_id=microgrid_id))
    assert not any(page)

    for _ in range(100):
        sample = generator.generate_dispatch()
        await client.create(
            **to_create_params(microgrid_id=microgrid_id, dispatch=sample)
        )

        dispatch = None
        async for page in client.list(microgrid_id=microgrid_id):
            for dispatch in page:
                pass

        if dispatch is None:
            raise AssertionError("Dispatch not found")

        sample = _update_metadata(sample, dispatch)
        assert dispatch == sample


async def test_update_dispatch(client: FakeClient, sample: Dispatch) -> None:
    """Test updating a dispatch."""
    microgrid_id = random.randint(1, 100)
    dispatch = await client.create(**to_create_params(microgrid_id, sample))

    sample = _update_metadata(sample, dispatch)
    assert dispatch == sample

    await client.update(
        microgrid_id=microgrid_id,
        dispatch_id=dispatch.id,
        new_fields={"recurrence.interval": 4},
    )
    assert client.dispatches(microgrid_id)[0].recurrence.interval == 4


async def test_update_dispatch_to_no_duration(
    client: FakeClient, sample: Dispatch
) -> None:
    """Test updating the duration field of a dispatch to None."""
    microgrid_id = random.randint(1, 100)
    client.set_dispatches(
        microgrid_id=microgrid_id,
        value=[replace(sample, duration=timedelta(minutes=10))],
    )

    await client.update(
        microgrid_id=microgrid_id,
        dispatch_id=sample.id,
        new_fields={"duration": None},
    )
    assert client.dispatches(microgrid_id)[0].duration is None


async def test_update_dispatch_from_no_duration(
    client: FakeClient, sample: Dispatch
) -> None:
    """Test updating the duration field of a dispatch from None."""
    microgrid_id = random.randint(1, 100)
    client.set_dispatches(
        microgrid_id=microgrid_id, value=[replace(sample, duration=None)]
    )

    await client.update(
        microgrid_id=microgrid_id,
        dispatch_id=sample.id,
        new_fields={"duration": timedelta(minutes=10)},
    )
    assert client.dispatches(microgrid_id)[0].duration == timedelta(minutes=10)


async def test_update_dispatch_fail(client: FakeClient, sample: Dispatch) -> None:
    """Test updating the type and dry_run fields of a dispatch."""
    microgrid_id = random.randint(1, 100)
    dispatch = await client.create(**to_create_params(microgrid_id, sample))

    assert dispatch is not None

    sample = _update_metadata(sample, dispatch)
    assert dispatch == sample

    for field, value in [
        ("type", "new_type"),
        ("dry_run", True),
        ("is_dry_run", True),
    ]:
        with raises(ValueError):
            await client.update(
                microgrid_id=microgrid_id,
                dispatch_id=dispatch.id,
                new_fields={field: value},
            )


async def test_get_dispatch(client: FakeClient, sample: Dispatch) -> None:
    """Test getting a dispatch."""
    microgrid_id = random.randint(1, 100)
    dispatch = await client.create(**to_create_params(microgrid_id, sample))

    sample = _update_metadata(sample, dispatch)
    assert dispatch == sample

    assert (
        await client.get(microgrid_id=microgrid_id, dispatch_id=dispatch.id) == dispatch
    )


async def test_get_dispatch_no_duration(client: FakeClient, sample: Dispatch) -> None:
    """Test getting a dispatch with a None duration."""
    microgrid_id = random.randint(1, 100)
    sample = replace(sample, duration=None)
    dispatch = await client.create(**to_create_params(microgrid_id, sample))

    sample = _update_metadata(sample, dispatch)
    assert dispatch == sample

    assert (
        await client.get(microgrid_id=microgrid_id, dispatch_id=dispatch.id) == dispatch
    )


async def test_get_dispatch_fail(client: FakeClient) -> None:
    """Test getting a non-existent dispatch."""
    with raises(grpc.RpcError):
        await client.get(microgrid_id=1, dispatch_id=1)


async def test_delete_dispatch(client: FakeClient, sample: Dispatch) -> None:
    """Test deleting a dispatch."""
    microgrid_id = random.randint(1, 100)
    dispatch = await client.create(**to_create_params(microgrid_id, sample))

    sample = _update_metadata(sample, dispatch)
    assert dispatch == sample

    await client.delete(microgrid_id=microgrid_id, dispatch_id=dispatch.id)

    assert len(client.dispatches(microgrid_id)) == 0


async def test_delete_dispatch_fail(client: FakeClient) -> None:
    """Test deleting a non-existent dispatch."""
    with raises(grpc.RpcError):
        await client.delete(microgrid_id=1, dispatch_id=1)


async def test_dispatch_stream(client: FakeClient, sample: Dispatch) -> None:
    """Test dispatching a stream of dispatches."""
    microgrid_id = random.randint(1, 100)
    dispatches = [sample, sample, sample]

    stream = client.stream(microgrid_id)

    async def expect(dispatch: Dispatch, event: Event) -> None:
        message = await stream.receive()
        assert message.dispatch == dispatch
        assert message.event == event

    # Give stream some time to start
    await asyncio.sleep(0.1)

    # Add a new dispatch
    dispatches[0] = await client.create(**to_create_params(microgrid_id, dispatches[0]))
    # Expect the first dispatch event
    await expect(dispatches[0], Event.CREATED)

    # Add a new dispatch
    dispatches[1] = await client.create(**to_create_params(microgrid_id, dispatches[1]))
    # Expect the second dispatch
    await expect(dispatches[1], Event.CREATED)

    # Add a new dispatch
    dispatches[2] = await client.create(**to_create_params(microgrid_id, dispatches[2]))
    # Expect the third dispatch
    await expect(dispatches[2], Event.CREATED)

    # Update the first dispatch
    dispatches[0] = await client.update(
        microgrid_id=microgrid_id,
        dispatch_id=dispatches[0].id,
        new_fields={"start_time": dispatches[0].start_time + timedelta(minutes=1)},
    )

    # Expect the first dispatch update
    await expect(dispatches[0], Event.UPDATED)

    # Delete the first dispatch
    await client.delete(microgrid_id=microgrid_id, dispatch_id=dispatches[0].id)

    # Expect the first dispatch deletion
    await expect(dispatches[0], Event.DELETED)
