# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.client.dispatch.test._service package."""

from datetime import datetime, timedelta, timezone

from frequenz.api.common.v1alpha8.pagination.pagination_params_pb2 import (
    PaginationParams,
)

# pylint: disable=no-name-in-module
from frequenz.api.common.v1alpha8.types.interval_pb2 import Interval as PBInterval
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    DispatchFilter,
    ListMicrogridDispatchesRequest,
)

from frequenz.client.base.conversion import to_timestamp
from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.dispatch.recurrence import RecurrenceRule
from frequenz.client.dispatch.test._service import FakeService
from frequenz.client.dispatch.types import (
    Dispatch,
    DispatchId,
    TargetIds,
)


async def test_list_dispatches_filter_start_time() -> None:
    """Test listing dispatches with a start time filter."""
    service = FakeService()
    now = datetime.now(timezone.utc)
    dispatches = [
        Dispatch(
            id=DispatchId(i),
            start_time=now + timedelta(minutes=i),
            duration=timedelta(minutes=1),
            type="test",
            target=TargetIds(1),
            active=True,
            dry_run=False,
            payload={},
            recurrence=RecurrenceRule(),
            create_time=now,
            update_time=now,
        )
        for i in range(10)
    ]
    service.dispatches[MicrogridId(1)] = dispatches

    # Filter for dispatches starting after now + 5 minutes
    req = ListMicrogridDispatchesRequest(
        microgrid_id=1,
        filter=DispatchFilter(
            start_time_interval=PBInterval(
                start_time=to_timestamp(now + timedelta(minutes=5)),
                end_time=None,
            )
        ),
    )
    filtered_dispatches = [
        Dispatch.from_protobuf(dispatch)
        for dispatch in (await service.ListMicrogridDispatches(req)).dispatches
    ]
    assert len(filtered_dispatches) == 5
    assert all(d.start_time >= now + timedelta(minutes=5) for d in filtered_dispatches)

    # Filter for dispatches starting before now + 5 minutes
    req = ListMicrogridDispatchesRequest(
        microgrid_id=1,
        filter=DispatchFilter(
            start_time_interval=PBInterval(
                start_time=None,
                end_time=to_timestamp(now + timedelta(minutes=5)),
            )
        ),
        pagination_params=PaginationParams(page_size=100),
    )
    filtered_dispatches = [
        Dispatch.from_protobuf(dispatch)
        for dispatch in (await service.ListMicrogridDispatches(req)).dispatches
    ]
    assert len(filtered_dispatches) == 5
    assert all(d.start_time < now + timedelta(minutes=5) for d in filtered_dispatches)


async def test_list_dispatches_filter_end_time() -> None:
    """Test listing dispatches with an end time filter."""
    service = FakeService()
    now = datetime.now(timezone.utc)
    dispatches = [
        Dispatch(
            id=DispatchId(i),
            start_time=now,
            duration=timedelta(minutes=i),
            type="test",
            target=TargetIds(1),
            active=True,
            dry_run=False,
            payload={},
            recurrence=RecurrenceRule(),
            create_time=now,
            update_time=now,
        )
        for i in range(1, 11)
    ]
    service.dispatches[MicrogridId(1)] = dispatches

    # Filter for dispatches ending after now + 5 minutes
    req = ListMicrogridDispatchesRequest(
        microgrid_id=1,
        filter=DispatchFilter(
            end_time_interval=PBInterval(
                start_time=to_timestamp(now + timedelta(minutes=5)),
                end_time=None,
            )
        ),
        pagination_params=PaginationParams(page_size=100),
    )
    filtered_dispatches = [
        Dispatch.from_protobuf(dispatch)
        for dispatch in (await service.ListMicrogridDispatches(req)).dispatches
    ]
    assert len(filtered_dispatches) == 6
    assert all(
        d.start_time + d.duration >= now + timedelta(minutes=5)  # type: ignore[operator]
        for d in filtered_dispatches
    )

    # Filter for dispatches ending before now + 5 minutes
    req = ListMicrogridDispatchesRequest(
        microgrid_id=1,
        filter=DispatchFilter(
            end_time_interval=PBInterval(
                start_time=None,
                end_time=to_timestamp(now + timedelta(minutes=5)),
            )
        ),
    )
    filtered_dispatches = [
        Dispatch.from_protobuf(dispatch)
        for dispatch in (await service.ListMicrogridDispatches(req)).dispatches
    ]
    assert len(filtered_dispatches) == 4
    assert all(
        d.start_time + d.duration < now + timedelta(minutes=5)  # type: ignore[operator]
        for d in filtered_dispatches
    )
