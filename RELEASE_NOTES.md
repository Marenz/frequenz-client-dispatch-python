# Frequenz Dispatch Client Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* `TargetComponents` was reworked. It now is a type alias for `TargetIds | TargetCategories`:
 * `TargetIds` can be used to specify one or more specific target IDs:
  * `TargetIds(1, 2, 3)` or
  * `TargetIds(ComponentIds(1), ComponentIds(2), ComponentIds(3))`
 * `TargetCategories` can be used to specify one or more target categories:
  * `TargetCategories(ComponentCategory.BATTERY, ComponentCategory.SOLAR)`

## New Features

* `dispatch-cli` supports now the parameter `--type` and `--running` to filter the list of running services by type and status, respectively.
* Every call now has a default timeout of 60 seconds, streams terminate after five minutes. This can be influenced by the two new parameters for`DispatchApiClient.__init__()`:
    * `default_timeout: timedelta` (default: 60 seconds)
    * `stream_timeout: timedelta` (default: 5 minutes)
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
