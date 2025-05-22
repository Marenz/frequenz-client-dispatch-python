# Frequenz Dispatch Client Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

* `dispatch-cli` supports now the parameter `--type` and `--running` to filter the list of running services by type and status, respectively.
* Every call now has a default timeout of 60 seconds, streams terminate after five minutes. This can be influenced by the two new parameters for`DispatchApiClient.__init__()`:
    * `default_timeout: timedelta` (default: 60 seconds)
    * `stream_timeout: timedelta` (default: 5 minutes)

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
