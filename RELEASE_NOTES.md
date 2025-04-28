# Frequenz Dispatch Client Library Release Notes

## Features

* `Dispatch.end_time` has been added to the `Dispatch` class, which is the time when the dispatch ended as calculated by the server. `dispatch-cli` will also print this time.

## Bug Fixes

* Fix that `dispatch-cli stream` would try to print an event as dispatch, causing an exception.
* Fix that `stream()` would not reconnect and just closes the channel upon disconnection.
