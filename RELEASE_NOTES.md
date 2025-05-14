# Frequenz Dispatch Client Library Release Notes

## Features

* The dispatch client now supports the official dispatch domain.

## Bug Fixes

* Fix that a user might see invalid values for dispatches without `end_time`. It was not correctly handling the `None` value when converted from protobuf to pythons `Dispatch` class.

## Upgrading

* Renamed `Client` class to `DispatchApiClient` for clarity.
