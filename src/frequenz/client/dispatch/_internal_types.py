# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Type wrappers for the generated protobuf messages."""


from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

# pylint: disable=no-name-in-module
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    CreateMicrogridDispatchRequest as PBDispatchCreateRequest,
)
from frequenz.api.dispatch.v1.dispatch_pb2 import DispatchData
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp

from frequenz.client.base.conversion import to_datetime, to_timestamp

from .recurrence import RecurrenceRule
from .types import (
    TargetComponents,
    _target_components_from_protobuf,
    _target_components_to_protobuf,
)

# pylint: enable=no-name-in-module


# pylint: disable=too-many-instance-attributes
@dataclass(kw_only=True)
class DispatchCreateRequest:
    """Request to create a new dispatch."""

    microgrid_id: int
    """The identifier of the microgrid to which this dispatch belongs."""

    type: str
    """User-defined information about the type of dispatch.

    This is understood and processed by downstream applications."""

    start_time: datetime | Literal["NOW"]
    """The start time of the dispatch in UTC."""

    duration: timedelta | None
    """The duration of the dispatch, represented as a timedelta.

    If None, the dispatch is considered to be "infinite" or "instantaneous",
    like a command to turn on a component.
    """

    target: TargetComponents
    """The target components of the dispatch."""

    active: bool
    """Indicates whether the dispatch is active and eligible for processing."""

    dry_run: bool
    """Indicates if the dispatch is a dry run.

    Executed for logging and monitoring without affecting actual component states."""

    payload: dict[str, Any]
    """The dispatch payload containing arbitrary data.

    It is structured as needed for the dispatch operation."""

    recurrence: RecurrenceRule | None
    """The recurrence rule for the dispatch.
    Defining any repeating patterns or schedules."""

    @classmethod
    def from_protobuf(
        cls, pb_object: PBDispatchCreateRequest
    ) -> "DispatchCreateRequest":
        """Convert a protobuf dispatch create request to a dispatch.

        Args:
            pb_object: The protobuf dispatch create request to convert.

        Returns:
            The converted dispatch.
        """
        duration = (
            timedelta(seconds=pb_object.dispatch_data.duration)
            if pb_object.dispatch_data.HasField("duration")
            else None
        )

        return DispatchCreateRequest(
            microgrid_id=pb_object.microgrid_id,
            type=pb_object.dispatch_data.type,
            start_time=(
                "NOW"
                if pb_object.start_immediately
                else rounded_start_time(to_datetime(pb_object.dispatch_data.start_time))
            ),
            duration=duration,
            target=_target_components_from_protobuf(pb_object.dispatch_data.target),
            active=pb_object.dispatch_data.is_active,
            dry_run=pb_object.dispatch_data.is_dry_run,
            payload=MessageToDict(pb_object.dispatch_data.payload),
            recurrence=RecurrenceRule.from_protobuf(pb_object.dispatch_data.recurrence),
        )

    def to_protobuf(self) -> PBDispatchCreateRequest:
        """Convert a dispatch to a protobuf dispatch create request.

        Returns:
            The converted protobuf dispatch create request.
        """
        payload = Struct()
        payload.update(self.payload)

        return PBDispatchCreateRequest(
            microgrid_id=self.microgrid_id,
            dispatch_data=DispatchData(
                type=self.type,
                start_time=(
                    to_timestamp(self.start_time)
                    if isinstance(self.start_time, datetime)
                    else Timestamp()
                ),
                duration=(
                    round(self.duration.total_seconds()) if self.duration else None
                ),
                target=_target_components_to_protobuf(self.target),
                is_active=self.active,
                is_dry_run=self.dry_run,
                payload=payload,
                recurrence=self.recurrence.to_protobuf() if self.recurrence else None,
            ),
            start_immediately=self.start_time == "NOW",
        )


def rounded_start_time(start_time: datetime) -> datetime:
    """Round the start time to the nearest second.

    Args:
        start_time: The start time to round.

    Returns:
        The rounded start time.
    """
    # Round start_time seconds to have the same behavior as the gRPC server
    # https://github.com/frequenz-io/frequenz-service-dispatch/issues/77
    new_seconds = start_time.second + start_time.microsecond / 1_000_000
    start_time = start_time.replace(microsecond=0, second=0)
    start_time += timedelta(seconds=round(new_seconds))
    return start_time
