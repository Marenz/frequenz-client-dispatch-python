# Frequenz Dispatch Client Library Release Notes

## Summary

Dispatch streams now have no deadline by default and rely on gRPC keepalive to
detect dead connections.

## Upgrading

- `DispatchApiClient` no longer sets a five-minute stream deadline by default.
  Pass a `timedelta` as `stream_timeout` to keep a finite deadline. The public
  `stream_timeout` property now returns `None` when no deadline is configured.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Remove the default stream deadline that forced healthy subscriptions to reconnect
  every five minutes. Explicit stream timeouts remain supported.
