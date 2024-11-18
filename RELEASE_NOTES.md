# Frequenz Dispatch Client Library Release Notes

## New Features

* Update BaseApiClient to get the http2 keepalive feature.
* Some Methods from the high-level API have been moved to this repo: The dispatch class now offers: `until`, `started`, `next_run` and `next_run_after`.
* Add `start_immediately` support to the `create` method. You can now specify "NOW" as the start time to trigger immediate dispatch. Note: While the dispatch CLI previously allowed this by converting "NOW" to a timestamp client-side before sending it to the server, this functionality is now supported directly on the server side!
