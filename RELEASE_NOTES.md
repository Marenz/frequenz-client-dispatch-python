# Frequenz Dispatch Client Library Release Notes

## New Features

* Update BaseApiClient to get the http2 keepalive feature.
* Some Methods from the high-level API have been moved to this repo: The dispatch class now offers: `until`, `running`, `next_run` and `next_run_after`.

## Bug Fixes

* Fix crash by adding the missing YEARLY frequency.
