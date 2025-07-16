# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Type wrappers for the generated protobuf messages."""


from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Self, SupportsInt, TypeAlias, cast, final

# pylint: enable=no-name-in-module
from frequenz.api.common.v1.microgrid.components.battery_pb2 import (
    BatteryType as PBBatteryType,
)
from frequenz.api.common.v1.microgrid.components.ev_charger_pb2 import (
    EvChargerType as PBEvChargerType,
)
from frequenz.api.common.v1.microgrid.components.inverter_pb2 import (
    InverterType as PBInverterType,
)

# pylint: disable=no-name-in-module
from frequenz.api.dispatch.v1.dispatch_pb2 import Dispatch as PBDispatch
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    DispatchData,
    DispatchMetadata,
    StreamMicrogridDispatchesResponse,
)
from frequenz.api.dispatch.v1.dispatch_pb2 import TargetComponents as PBTargetComponents
from frequenz.core.id import BaseId
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

from frequenz.client.base.conversion import to_datetime, to_timestamp
from frequenz.client.common.microgrid.components import ComponentCategory

from .recurrence import Frequency, RecurrenceRule, Weekday


@final
class DispatchId(BaseId, str_prefix="DID"):
    """A unique identifier for a dispatch."""


class EvChargerType(Enum):
    """Enum representing the type of EV charger."""

    UNSPECIFIED = PBEvChargerType.EV_CHARGER_TYPE_UNSPECIFIED
    """Unspecified type of EV charger."""

    AC = PBEvChargerType.EV_CHARGER_TYPE_AC
    """AC EV charger."""

    DC = PBEvChargerType.EV_CHARGER_TYPE_DC
    """DC EV charger."""

    HYBRID = PBEvChargerType.EV_CHARGER_TYPE_HYBRID
    """Hybrid EV charger."""


class BatteryType(Enum):
    """Enum representing the type of battery."""

    UNSPECIFIED = PBBatteryType.BATTERY_TYPE_UNSPECIFIED
    """Unspecified type of battery."""

    LI_ION = PBBatteryType.BATTERY_TYPE_LI_ION
    """Lithium-ion battery."""

    NA_ION = PBBatteryType.BATTERY_TYPE_NA_ION
    """Sodium-ion battery."""


class InverterType(Enum):
    """Enum representing the type of inverter."""

    UNSPECIFIED = PBInverterType.INVERTER_TYPE_UNSPECIFIED
    """Unspecified type of inverter."""

    BATTERY = PBInverterType.INVERTER_TYPE_BATTERY
    """Battery inverter."""

    SOLAR = PBInverterType.INVERTER_TYPE_SOLAR
    """Solar inverter."""

    HYBRID = PBInverterType.INVERTER_TYPE_HYBRID
    """Hybrid inverter."""


@dataclass(frozen=True)
class TargetCategory:
    """Represents a category and optionally a type."""

    target: ComponentCategory | BatteryType | EvChargerType | InverterType
    """The target category of the dispatch.

    Implicitly derived from the types.
    """

    @property
    def category(self) -> ComponentCategory:
        """Get the category of the target.

        Returns:
            The category of the target.
        """
        match self.target:
            case ComponentCategory():
                return self.target
            case BatteryType():
                return ComponentCategory.BATTERY
            case EvChargerType():
                return ComponentCategory.EV_CHARGER
            case InverterType():
                return ComponentCategory.INVERTER

    @property
    def type(self) -> BatteryType | EvChargerType | InverterType | None:
        """Get the type of the category.

        Returns:
            The type of the category.
        """
        match self.target:
            case BatteryType() | EvChargerType() | InverterType():
                return self.target
            case _:
                return None


class TargetIds(frozenset[int]):
    """A set of target component IDs.

    This is a frozen set, so it is immutable.
    """

    def __new__(cls, *ids: SupportsInt) -> Self:
        """Create a new TargetIds instance.

        Args:
            *ids: The target IDs to initialize.

        Returns:
            A new TargetIds instance.
        """
        # Convert all provided ids to integers before creating the frozenset
        processed_ids = tuple(int(id_val) for id_val in ids)
        return super().__new__(cls, processed_ids)


# Define the union of types that can be passed to TargetCategories constructor
TargetCategoryInputType = (
    TargetCategory | ComponentCategory | BatteryType | InverterType | EvChargerType
)
"""Type for the input to TargetCategories constructor."""


class TargetCategories(frozenset[TargetCategory]):
    """A set of target component categories and types.

    This is a frozen set, so it is immutable.
    """

    def __new__(cls, *categories_input: TargetCategoryInputType) -> Self:
        """Create a new TargetCategories instance.

        Args:
            *categories_input: TargetCategory instances or raw ComponentCategory/specific types
                               (BatteryType, InverterType, EvChargerType) to be wrapped.

        Returns:
            A new TargetCategories instance.

        Raises:
            TypeError: If an item in categories_input is not a TargetCategory
                       nor one of the wrappable types.
        """
        processed_elements = []
        for item in categories_input:
            if isinstance(item, TargetCategory):
                processed_elements.append(item)
            elif isinstance(
                item, (ComponentCategory, BatteryType, InverterType, EvChargerType)
            ):
                # Wrap raw categories/types into TargetCategory instances
                processed_elements.append(TargetCategory(target=item))
            else:
                # This case should ideally be caught by static type checkers
                # if call sites adhere to type hints.
                raise TypeError(
                    f"Invalid type for TargetCategories constructor: {type(item)}. "
                    f"Expected TargetCategory, ComponentCategory, BatteryType, "
                    f"InverterType, or EvChargerType."
                )
        # `super().__new__` for frozenset expects an iterable of elements for the set
        return super().__new__(cls, processed_elements)

    def __repr__(self) -> str:
        """Return an ordered string representation."""
        ordered = sorted(list(self), key=lambda cat: cat.target.value)
        return str([cat.target.name for cat in ordered])


TargetComponents: TypeAlias = TargetIds | TargetCategories
"""Target components.

Can be one of the following:

- A set of target component IDs (TargetIds)
- A set of target component categories with opt. types (TargetCategories)

This is a frozen set, so it is immutable.
The target components are used to specify the components that a dispatch
should target.
"""


def _target_components_from_protobuf(
    pb_target: PBTargetComponents,
) -> TargetComponents:
    """Convert protobuf target components to a more native type.

    Args:
        pb_target: The protobuf target components to convert.

    Raises:
        ValueError: If the protobuf target components are invalid.

    Returns:
        The converted target components.
    """
    match pb_target.WhichOneof("components"):
        case "component_ids":
            return TargetIds(*pb_target.component_ids.ids)
        case "component_categories":
            return TargetCategories(
                *map(
                    ComponentCategory.from_proto,
                    pb_target.component_categories.categories,
                )
            )
        case "component_categories_types":
            return TargetCategories(
                *map(
                    lambda cat_and_type: _extract_category_type(cat_and_type)
                    or ComponentCategory.from_proto(cat_and_type.category),
                    pb_target.component_categories_types.categories,
                )
            )
        case _:
            raise ValueError("Invalid target components")


def _extract_category_type(
    cat_and_type: PBTargetComponents.CategoryAndType,
) -> BatteryType | EvChargerType | InverterType | None:
    """Extract the category type from a protobuf CategoryAndType.

    Args:
        cat_and_type: The protobuf CategoryAndType to extract from.

    Returns:
        The extracted category type.
    """
    match cat_and_type.WhichOneof("type"):
        case "battery":
            return BatteryType(cat_and_type.battery)
        case "ev_charger":
            return EvChargerType(cat_and_type.ev_charger)
        case "inverter":
            return InverterType(cat_and_type.inverter)
        case _:
            return None


# Old, soon deprecated way to specify categories
def _target_components_to_protobuf(
    target: TargetComponents,
) -> PBTargetComponents:
    """Convert target components to protobuf.

    Args:
        target: The target components to convert.

    Raises:
        ValueError: If the target components are invalid.

    Returns:
        The converted protobuf target components.
    """
    pb_target = PBTargetComponents()
    match target:
        case TargetIds(component_ids):
            pb_target.component_ids.ids.extend(component_ids)
        case TargetCategories(categories):
            # Old, soon deprecated way to specify categories
            pb_target.component_categories.categories.extend(
                map(
                    lambda cat: cat.category.to_proto(),
                    categories,
                )
            )

        case _:
            raise ValueError(f"Invalid target components: {target}")
    return pb_target


# New, not yet supported way to specify categories and types
def _target_components_to_protobuf_new(
    target: TargetComponents,
) -> PBTargetComponents:
    """Convert target components to protobuf.

    Args:
        target: The target components to convert.

    Raises:
        ValueError: If the target components are invalid.

    Returns:
        The converted protobuf target components.
    """
    pb_target = PBTargetComponents()
    match target:
        case TargetIds(component_ids):
            pb_target.component_ids.ids.extend(component_ids)
        case TargetCategories(categories):
            for category in categories:
                pb_category = pb_target.component_categories_types.categories.add()
                pb_category.category = category.category.to_proto()

                match category.type:
                    case BatteryType():
                        pb_category.battery = category.type.value
                    case EvChargerType():
                        pb_category.ev_charger = category.type.value
                    case InverterType():
                        pb_category.inverter = category.type.value

        case _:
            raise ValueError(f"Invalid target components: {target}")
    return pb_target


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


@dataclass(kw_only=True, frozen=True)
class Dispatch:  # pylint: disable=too-many-instance-attributes
    """Represents a dispatch operation within a microgrid system."""

    id: DispatchId
    """The unique identifier for the dispatch."""

    type: str
    """User-defined information about the type of dispatch.

    This is understood and processed by downstream applications."""

    start_time: datetime
    """The start time of the dispatch in UTC."""

    duration: timedelta | None
    """The duration of the dispatch, represented as a timedelta."""

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

    recurrence: RecurrenceRule
    """The recurrence rule for the dispatch.

    Defining any repeating patterns or schedules."""

    create_time: datetime
    """The creation time of the dispatch in UTC. Set when a dispatch is created."""

    update_time: datetime
    """The last update time of the dispatch in UTC. Set when a dispatch is modified."""

    end_time: datetime | None = None
    """The end time of the dispatch in UTC.

    Calculated and sent by the backend service.
    """

    @property
    def started(self) -> bool:
        """Check if the dispatch has started.

        A dispatch is considered started if the current time is after the start
        time but before the end time.

        Recurring dispatches are considered started if the current time is after
        the start time of the last occurrence but before the end time of the
        last occurrence.
        """
        if not self.active:
            return False

        now = datetime.now(tz=timezone.utc)
        return self.started_at(now)

    def started_at(self, now: datetime) -> bool:
        """Check if the dispatch has started.

        A dispatch is considered started if the current time is after the start
        time but before the end time.

        Recurring dispatches are considered started if the current time is after
        the start time of the last occurrence but before the end time of the
        last occurrence.

        Args:
            now: time to use as now

        Returns:
            True if the dispatch is started
        """
        if not self.active:
            return False

        if now < self.start_time:
            return False

        # A dispatch without duration is always running, once it started
        if self.duration is None:
            return True

        if until := self._until(now):
            return now < until

        return False

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
            id=DispatchId(pb_object.metadata.dispatch_id),
            type=pb_object.data.type,
            create_time=to_datetime(pb_object.metadata.create_time),
            update_time=to_datetime(pb_object.metadata.update_time),
            end_time=(
                to_datetime(pb_object.metadata.end_time)
                if pb_object.metadata.HasField("end_time")
                else None
            ),
            start_time=to_datetime(pb_object.data.start_time),
            duration=(
                timedelta(seconds=pb_object.data.duration)
                if pb_object.data.HasField("duration")
                else None
            ),
            target=_target_components_from_protobuf(pb_object.data.target),
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
                dispatch_id=int(self.id),
                create_time=to_timestamp(self.create_time),
                update_time=to_timestamp(self.update_time),
                end_time=(
                    to_timestamp(self.end_time) if self.end_time is not None else None
                ),
            ),
            data=DispatchData(
                type=self.type,
                start_time=to_timestamp(self.start_time),
                duration=(
                    None
                    if self.duration is None
                    else round(self.duration.total_seconds())
                ),
                target=_target_components_to_protobuf(self.target),
                is_active=self.active,
                is_dry_run=self.dry_run,
                payload=payload,
                recurrence=self.recurrence.to_protobuf() if self.recurrence else None,
            ),
        )


class Event(Enum):
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
