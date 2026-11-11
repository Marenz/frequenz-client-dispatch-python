# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for Dispatch API client timeouts."""

import asyncio
from datetime import timedelta
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import grpc
import pytest
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    ListMicrogridDispatchesResponse,
    StreamMicrogridDispatchesResponse,
)
from pytest_mock import MockerFixture

from frequenz.client.common.microgrid import MicrogridId
from frequenz.client.dispatch import DispatchApiClient


@pytest.mark.parametrize(
    "stream_timeout, expected_timeout",
    [
        (None, None),
        (timedelta(seconds=12), 12.0),
    ],
)
def test_stream_timeout_reaches_grpc(
    mocker: MockerFixture,
    stream_timeout: timedelta | None,
    expected_timeout: float | None,
) -> None:
    """Test that omitted and explicit stream timeouts reach gRPC unchanged."""
    grpc_stream = object()
    stub = MagicMock()
    stub.StreamMicrogridDispatches.return_value = grpc_stream
    mocker.patch.object(
        DispatchApiClient, "stub", new_callable=PropertyMock, return_value=stub
    )
    broadcaster_class = mocker.patch(
        "frequenz.client.dispatch._client.GrpcStreamBroadcaster"
    )
    client = DispatchApiClient(
        server_url="grpc://example.com",
        auth_key="key",
        connect=False,
        stream_timeout=stream_timeout,
    )

    client.stream(MicrogridId(1))
    stream_method = broadcaster_class.call_args.kwargs["stream_method"]

    assert client.stream_timeout == stream_timeout
    assert stream_method() is grpc_stream
    stub.StreamMicrogridDispatches.assert_called_once()
    assert (
        stub.StreamMicrogridDispatches.call_args.kwargs["timeout"] == expected_timeout
    )


async def test_unary_timeout_is_unchanged(mocker: MockerFixture) -> None:
    """Test that the stream default does not alter unary call deadlines."""
    stub = MagicMock()
    stub.ListMicrogridDispatches = AsyncMock(
        return_value=ListMicrogridDispatchesResponse()
    )
    mocker.patch.object(
        DispatchApiClient, "stub", new_callable=PropertyMock, return_value=stub
    )
    client = DispatchApiClient(
        server_url="grpc://example.com", auth_key="key", connect=False
    )

    await anext(client.list(MicrogridId(1)))

    assert client.call_timeout == timedelta(seconds=60)
    assert stub.ListMicrogridDispatches.call_args.kwargs["timeout"] == 60.0


async def test_transient_stream_failure_retries(mocker: MockerFixture) -> None:
    """Test that a transient gRPC stream failure starts a new subscription."""
    retry_started = asyncio.Event()
    keep_open = asyncio.Event()

    async def failing_stream() -> AsyncIterator[StreamMicrogridDispatchesResponse]:
        raise grpc.aio.AioRpcError(
            grpc.StatusCode.UNAVAILABLE,
            grpc.aio.Metadata(),
            grpc.aio.Metadata(),
            "transient failure",
            None,
        )
        yield StreamMicrogridDispatchesResponse()  # pragma: no cover

    async def open_stream() -> AsyncIterator[StreamMicrogridDispatchesResponse]:
        retry_started.set()
        await keep_open.wait()
        yield StreamMicrogridDispatchesResponse()  # pragma: no cover

    stub = MagicMock()
    stub.StreamMicrogridDispatches.side_effect = [failing_stream(), open_stream()]
    mocker.patch.object(
        DispatchApiClient, "stub", new_callable=PropertyMock, return_value=stub
    )
    mocker.patch("frequenz.client.base.streaming.asyncio.sleep", new_callable=AsyncMock)
    client = DispatchApiClient(
        server_url="grpc://example.com", auth_key="key", connect=False
    )

    client.stream(MicrogridId(1))
    await asyncio.wait_for(retry_started.wait(), timeout=1)

    assert client.stream_timeout is None
    assert stub.StreamMicrogridDispatches.call_count == 2
    assert all(
        call.kwargs["timeout"] is None
        for call in stub.StreamMicrogridDispatches.call_args_list
    )

    broadcaster = client._streams[MicrogridId(1)]  # pylint: disable=protected-access
    await broadcaster.stop()
