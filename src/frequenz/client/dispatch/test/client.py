# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Fake client for testing."""

from typing import Any

from frequenz.client.common.microgrid import MicrogridId

from .. import DispatchApiClient
from ..types import Dispatch
from ._service import ALL_KEY, NONE_KEY, FakeService

__all__ = ["FakeClient", "to_create_params", "ALL_KEY", "NONE_KEY"]


class FakeClient(DispatchApiClient):
    """Fake client for testing.

    This client uses a fake service to simulate the dispatch api.
    """

    def __init__(
        self,
    ) -> None:
        """Initialize the mock client."""
        super().__init__(server_url="mock", key=ALL_KEY, connect=False)
        self._stuba: FakeService = FakeService()

    @property
    def stub(self) -> FakeService:  # type: ignore
        """The fake service.

        Returns:
            FakeService: The fake service.
        """
        return self._stuba

    def dispatches(self, microgrid_id: MicrogridId) -> list[Dispatch]:
        """List of dispatches.

        Args:
            microgrid_id: The microgrid id.

        Returns:
            list[Dispatch]: The list of dispatches
        """
        return self._service.dispatches.get(microgrid_id, [])

    def set_dispatches(self, microgrid_id: MicrogridId, value: list[Dispatch]) -> None:
        """Set the list of dispatches.

        Args:
            microgrid_id: The microgrid id.
            value: The list of dispatches to set.
        """
        self._service.dispatches[microgrid_id] = value
        self._service.refresh_last_id_for(microgrid_id)

    @property
    def _service(self) -> FakeService:
        """The fake service.

        Returns:
            FakeService: The fake service.
        """
        return self._stuba


def to_create_params(microgrid_id: MicrogridId, dispatch: Dispatch) -> dict[str, Any]:
    """Convert a dispatch to client.create parameters.

    Args:
        microgrid_id: The microgrid id.
        dispatch: The dispatch to convert.

    Returns:
        dict[str, Any]: The create parameters.
    """
    return {
        "microgrid_id": microgrid_id,
        "type": dispatch.type,
        "start_time": dispatch.start_time,
        "duration": dispatch.duration,
        "target": dispatch.target,
        "active": dispatch.active,
        "dry_run": dispatch.dry_run,
        "payload": dispatch.payload,
        "recurrence": dispatch.recurrence,
    }
