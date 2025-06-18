# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""A sampler for dispatch messages."""

import random
from datetime import datetime, timedelta, timezone

from frequenz.client.common.microgrid.components import ComponentCategory

from .._internal_types import rounded_start_time
from ..recurrence import EndCriteria, Frequency, RecurrenceRule, Weekday
from ..types import (
    BatteryType,
    Dispatch,
    DispatchId,
    EvChargerType,
    InverterType,
    TargetCategories,
    TargetCategory,
    TargetComponents,
    TargetIds,
)


class DispatchGenerator:
    """Generates random dispatch messages."""

    def __init__(self, seed: int = 0) -> None:
        """Initialize the sampler.

        Args:
            seed: seed to initialize the rng with
        """
        self._rng = random.Random(seed)
        self._last_id: int = 0

    def generate_recurrence_rule(self) -> RecurrenceRule:
        """Generate a random recurrence rule.

        Returns:
            a random recurrence rule
        """
        return RecurrenceRule(
            frequency=self._rng.choice(list(Frequency)[1:]),
            interval=self._rng.randint(1, 100),
            end_criteria=self._rng.choice(
                [
                    None,
                    self._rng.choice(
                        [
                            EndCriteria(count=self._rng.randint(1, 1000)),
                            EndCriteria(
                                until=datetime.fromtimestamp(
                                    self._rng.randint(0, 1000000),
                                    tz=timezone.utc,
                                )
                            ),
                        ]
                    ),
                ]
            ),
            byminutes=[
                self._rng.randint(0, 59) for _ in range(self._rng.randint(0, 10))
            ],
            byhours=[self._rng.randint(0, 23) for _ in range(self._rng.randint(0, 10))],
            byweekdays=[
                self._rng.choice(list(Weekday)[1:])
                for _ in range(self._rng.randint(0, 7))
            ],
            bymonthdays=[
                self._rng.randint(1, 31) for _ in range(self._rng.randint(0, 10))
            ],
            bymonths=[
                self._rng.randint(1, 12) for _ in range(self._rng.randint(0, 12))
            ],
        )

    def generate_target_category_and_type(self) -> TargetCategory:
        """Generate a random category and type.

        Returns:
            a random category and type
        """
        category = self._rng.choice(list(ComponentCategory)[1:])
        category_type: BatteryType | InverterType | EvChargerType | None = None

        match category:
            case ComponentCategory.BATTERY:
                category_type = self._rng.choice(list(BatteryType)[1:])
            case ComponentCategory.INVERTER:
                category_type = self._rng.choice(list(InverterType)[1:])
            case ComponentCategory.EV_CHARGER:
                category_type = self._rng.choice(list(EvChargerType)[1:])
            case _:
                category_type = None

        return TargetCategory(category_type or category)

    def generate_dispatch(self) -> Dispatch:
        """Generate a random dispatch instance.

        Returns:
            a random dispatch instance
        """
        self._last_id += 1
        create_time = datetime.fromtimestamp(
            self._rng.randint(0, 1000000), tz=timezone.utc
        )

        target_choices: list[TargetComponents] = [
            TargetIds(
                *[self._rng.randint(1, 100) for _ in range(self._rng.randint(1, 10))]
            ),
            TargetCategories(
                *[
                    # Not yet used
                    # self.generate_target_category_and_type()
                    self._rng.choice(list(ComponentCategory)[1:])
                    for _ in range(self._rng.randint(1, 10))
                ]
            ),
        ]

        return Dispatch(
            id=DispatchId(self._last_id),
            create_time=create_time,
            update_time=create_time + timedelta(seconds=self._rng.randint(0, 1000000)),
            type=str(self._rng.randint(0, 100_000)),
            start_time=rounded_start_time(
                datetime.now(tz=timezone.utc)
                + timedelta(seconds=self._rng.randint(0, 1000000))
            ),
            duration=self._rng.choice(
                [
                    None,
                    timedelta(seconds=self._rng.randint(0, 1000000)),
                ]
            ),
            target=self._rng.choice(target_choices),
            active=self._rng.choice([True, False]),
            dry_run=self._rng.choice([True, False]),
            payload={
                f"key_{i}": self._rng.choice(
                    [
                        self._rng.randint(0, 100),
                        self._rng.uniform(0, 100),
                        self._rng.choice([True, False]),
                        self._rng.choice(["a", "b", "c"]),
                    ]
                )
                for i in range(self._rng.randint(0, 10))
            },
            recurrence=self.generate_recurrence_rule(),
        )
