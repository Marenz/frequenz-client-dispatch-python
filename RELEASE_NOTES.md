# Frequenz Dispatch Client Library Release Notes

## Summary

While the new TargetCategory class supports subtypes, only reading them is currently available; setting subtypes will be introduced in a future release.

## Upgrading

* `TargetComponents` was reworked. It now is a type alias for `TargetIds | TargetCategories`:
 * `TargetIds` can be used to specify one or more specific target IDs:
  * `TargetIds(1, 2, 3)` or
  * `TargetIds(ComponentIds(1), ComponentIds(2), ComponentIds(3))`
 * `TargetCategories` can be used to specify one or more target categories:
  * `TargetCategories(ComponentCategory.BATTERY, ComponentCategory.INVERTER)`
* `DispatchApiClient.stream()` now returns a streamer instead of a receiver. This allows you to use the new `new_receiver` method to create a receiver with more options, such as `max_size`, `warn_on_overflow` and `include_events`.

    ```python
    client = DispatchApiClient(
        key="key",
        server_url="grpc://dispatch.url.goes.here.example.com"
    )
    receiver = client.stream(microgrid_id=1).new_receiver()

    async for message in receiver:
        print(message.event, message.dispatch)
    ```


## New Features

* With the new `TargetCategory` class (providing `.category` and `.type`) we can now specify subtypes of the categories:
 * `ComponentCategory.BATTERY` uses `BatteryType` with possible values: `LI_ION`, `NA_ION`
 * `ComponentCategory.INVERTER` uses `InverterType` with possible values: `BATTERY`, `SOLAR`, `HYBRID`
 * `ComponentCategory.EV_CHARGER` uses `EvChargerType`: with possible values `AC`, `DC`, `HYBRID`
 * A few examples on how to use the new `TargetCategory`:
    * `TargetCategory(BatteryType.LI_ION)`
        * `category` is `ComponentCategory.BATTERY`
        * `type` is `BatteryType.LI_ION`
    * `TargetCategory(ComponentCategory.BATTERY)`
        * `category` is `ComponentCategory.BATTERY`
        * `type` is `None`
    * `TargetCategories(InverterType.SOLAR)`
        * `category` is `ComponentCategory.INVERTER`
        * `type` is `InverterType.SOLAR`


## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
