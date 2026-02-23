# Frequenz Dispatch Client Library Release Notes

## Summary

Raise `ValueError` when `start_time` passed to `create()` is neither a `datetime` nor `"NOW"`.

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- `DispatchApiClient.create()`: Passing an invalid `start_time` (not a `datetime` or `"NOW"`) previously silently created a dispatch with an epoch timestamp (1970-01-01). It now raises `ValueError` immediately.
