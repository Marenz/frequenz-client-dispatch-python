# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Type wrappers for the generated protobuf messages."""


from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum, IntEnum
from typing import Any, cast

# pylint: disable=no-name-in-module
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    ComponentSelector as PBComponentSelector,
)
from frequenz.api.dispatch.v1.dispatch_pb2 import Dispatch as PBDispatch
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    DispatchData,
    DispatchMetadata,
    StreamMicrogridDispatchesResponse,
)
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

from frequenz.client.base.conversion import to_datetime, to_timestamp

# pylint: enable=no-name-in-module
from frequenz.client.common.microgrid.components import ComponentCategory

from .recurrence import Frequency, RecurrenceRule, Weekday

ComponentSelector = list[int] | list[ComponentCategory]
"""A component selector specifying which components a dispatch targets.

A component selector can be a list of component IDs or a list of categories.
"""


def _component_selector_from_protobuf(
    pb_selector: PBComponentSelector,
) -> ComponentSelector:
    """Convert a protobuf component selector to a component selector.

    Args:
        pb_selector: The protobuf component selector to convert.

    Raises:
        ValueError: If the protobuf component selector is invalid.

    Returns:
        The converted component selector.
    """
    match pb_selector.WhichOneof("selector"):
        case "component_ids":
            id_list: list[int] = list(pb_selector.component_ids.ids)
            return id_list
        case "component_categories":
            category_list: list[ComponentCategory] = list(
                map(
                    ComponentCategory.from_proto,
                    pb_selector.component_categories.categories,
                )
            )
            return category_list
        case _:
            raise ValueError("Invalid component selector")


def _component_selector_to_protobuf(
    selector: ComponentSelector,
) -> PBComponentSelector:
    """Convert a component selector to a protobuf component selector.

    Args:
        selector: The component selector to convert.

    Raises:
        ValueError: If the component selector is invalid.

    Returns:
        The converted protobuf component selector.
    """
    pb_selector = PBComponentSelector()
    match selector:
        case list(component_ids) if all(isinstance(id, int) for id in component_ids):
            pb_selector.component_ids.ids.extend(cast(list[int], component_ids))
        case list(categories) if all(
            isinstance(cat, ComponentCategory) for cat in categories
        ):
            pb_selector.component_categories.categories.extend(
                map(
                    lambda cat: cat.to_proto(),
                    cast(list[ComponentCategory], categories),
                )
            )
        case _:
            raise ValueError("Invalid component selector")
    return pb_selector


@dataclass(frozen=True, kw_only=True)
class TimeIntervalFilter:
    """Filter for a time interval."""

    start_from: datetime | None
    """Filter by start_time >= start_from."""

    start_to: datetime | None
    """Filter by start_time < start_to."""

    end_from: datetime | None
    """Filter by end_time >= end_from."""

    end_to: datetime | None
    """Filter by end_time < end_to."""


class RunningState(Enum):
    """The running state of a dispatch."""

    RUNNING = "RUNNING"
    """The dispatch is running."""

    STOPPED = "STOPPED"
    """The dispatch is stopped."""

    DIFFERENT_TYPE = "DIFFERENT_TYPE"
    """The dispatch is for a different type."""


@dataclass(kw_only=True, frozen=True)
class Dispatch:  # pylint: disable=too-many-instance-attributes
    """Represents a dispatch operation within a microgrid system."""

    id: int
    """The unique identifier for the dispatch."""

    type: str
    """User-defined information about the type of dispatch.

    This is understood and processed by downstream applications."""

    start_time: datetime
    """The start time of the dispatch in UTC."""

    duration: timedelta | None
    """The duration of the dispatch, represented as a timedelta."""

    selector: ComponentSelector
    """The component selector specifying which components the dispatch targets."""

    active: bool
    """Indicates whether the dispatch is active and eligible for processing."""

    dry_run: bool
    """Indicates if the dispatch is a dry run.

    Executed for logging and monitoring without affecting actual component states."""

    payload: dict[str, Any]
    """The dispatch payload containing arbitrary data.

    It is structured as needed for the dispatch operation."""

    recurrence: RecurrenceRule
    """The recurrence rule for the dispatch.

    Defining any repeating patterns or schedules."""

    create_time: datetime
    """The creation time of the dispatch in UTC. Set when a dispatch is created."""

    update_time: datetime
    """The last update time of the dispatch in UTC. Set when a dispatch is modified."""

    def running(self, type_: str) -> RunningState:
        """Check if the dispatch is currently supposed to be running.

        Args:
            type_: The type of the dispatch that should be running.

        Returns:
            RUNNING if the dispatch is running,
            STOPPED if it is stopped,
            DIFFERENT_TYPE if it is for a different type.
        """
        if self.type != type_:
            return RunningState.DIFFERENT_TYPE

        if not self.active:
            return RunningState.STOPPED

        now = datetime.now(tz=timezone.utc)

        if now < self.start_time:
            return RunningState.STOPPED

        # A dispatch without duration is always running, once it started
        if self.duration is None:
            return RunningState.RUNNING

        if until := self._until(now):
            return RunningState.RUNNING if now < until else RunningState.STOPPED

        return RunningState.STOPPED

    @property
    def until(self) -> datetime | None:
        """Time when the dispatch should end.

        Returns the time that a running dispatch should end.
        If the dispatch is not running, None is returned.

        Returns:
            The time when the dispatch should end or None if the dispatch is not running.
        """
        if not self.active:
            return None

        now = datetime.now(tz=timezone.utc)
        return self._until(now)

    @property
    def next_run(self) -> datetime | None:
        """Calculate the next run of a dispatch.

        Returns:
            The next run of the dispatch or None if the dispatch is finished.
        """
        return self.next_run_after(datetime.now(tz=timezone.utc))

    def next_run_after(self, after: datetime) -> datetime | None:
        """Calculate the next run of a dispatch.

        Args:
            after: The time to calculate the next run from.

        Returns:
            The next run of the dispatch or None if the dispatch is finished.
        """
        if (
            not self.recurrence.frequency
            or self.recurrence.frequency == Frequency.UNSPECIFIED
            or self.duration is None  # Infinite duration
        ):
            if after > self.start_time:
                return None
            return self.start_time

        # Make sure no weekday is UNSPECIFIED
        if Weekday.UNSPECIFIED in self.recurrence.byweekdays:
            return None

        # No type information for rrule, so we need to cast
        return cast(
            datetime | None,
            self.recurrence._as_rrule(  # pylint: disable=protected-access
                self.start_time
            ).after(after, inc=True),
        )

    def _until(self, now: datetime) -> datetime | None:
        """Calculate the time when the dispatch should end.

        If no previous run is found, None is returned.

        Args:
            now: The current time.

        Returns:
            The time when the dispatch should end or None if the dispatch is not running.

        Raises:
            ValueError: If the dispatch has no duration.
        """
        if self.duration is None:
            raise ValueError("_until: Dispatch has no duration")

        if (
            not self.recurrence.frequency
            or self.recurrence.frequency == Frequency.UNSPECIFIED
        ):
            return self.start_time + self.duration

        latest_past_start: datetime | None = (
            self.recurrence._as_rrule(  # pylint: disable=protected-access
                self.start_time
            ).before(now, inc=True)
        )

        if not latest_past_start:
            return None

        return latest_past_start + self.duration

    @classmethod
    def from_protobuf(cls, pb_object: PBDispatch) -> "Dispatch":
        """Convert a protobuf dispatch to a dispatch.

        Args:
            pb_object: The protobuf dispatch to convert.

        Returns:
            The converted dispatch.
        """
        return Dispatch(
            id=pb_object.metadata.dispatch_id,
            type=pb_object.data.type,
            create_time=to_datetime(pb_object.metadata.create_time),
            update_time=to_datetime(pb_object.metadata.modification_time),
            start_time=to_datetime(pb_object.data.start_time),
            duration=(
                timedelta(seconds=pb_object.data.duration)
                if pb_object.data.duration
                else None
            ),
            selector=_component_selector_from_protobuf(pb_object.data.selector),
            active=pb_object.data.is_active,
            dry_run=pb_object.data.is_dry_run,
            payload=MessageToDict(pb_object.data.payload),
            recurrence=RecurrenceRule.from_protobuf(pb_object.data.recurrence),
        )

    def to_protobuf(self) -> PBDispatch:
        """Convert a dispatch to a protobuf dispatch.

        Returns:
            The converted protobuf dispatch.
        """
        payload = Struct()
        payload.update(self.payload)

        return PBDispatch(
            metadata=DispatchMetadata(
                dispatch_id=self.id,
                create_time=to_timestamp(self.create_time),
                modification_time=to_timestamp(self.update_time),
            ),
            data=DispatchData(
                type=self.type,
                start_time=to_timestamp(self.start_time),
                duration=(
                    round(self.duration.total_seconds()) if self.duration else None
                ),
                selector=_component_selector_to_protobuf(self.selector),
                is_active=self.active,
                is_dry_run=self.dry_run,
                payload=payload,
                recurrence=self.recurrence.to_protobuf() if self.recurrence else None,
            ),
        )


class Event(IntEnum):
    """Enum representing the type of event that occurred during a dispatch operation."""

    UNSPECIFIED = StreamMicrogridDispatchesResponse.Event.EVENT_UNSPECIFIED
    CREATED = StreamMicrogridDispatchesResponse.Event.EVENT_CREATED
    UPDATED = StreamMicrogridDispatchesResponse.Event.EVENT_UPDATED
    DELETED = StreamMicrogridDispatchesResponse.Event.EVENT_DELETED


@dataclass(kw_only=True, frozen=True)
class DispatchEvent:
    """Represents an event that occurred during a dispatch operation."""

    dispatch: Dispatch
    """The dispatch associated with the event."""

    event: Event
    """The type of event that occurred."""

    @classmethod
    def from_protobuf(
        cls, pb_object: StreamMicrogridDispatchesResponse
    ) -> "DispatchEvent":
        """Convert a protobuf dispatch event to a dispatch event.

        Args:
            pb_object: The protobuf dispatch event to convert.

        Returns:
            The converted dispatch event.
        """
        return DispatchEvent(
            dispatch=Dispatch.from_protobuf(pb_object.dispatch),
            event=Event(pb_object.event),
        )
