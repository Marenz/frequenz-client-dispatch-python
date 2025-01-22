# Frequenz Dispatch Client Library Release Notes

## Bug Fixes

* Fix missing dependency in last release.
* The `FakeClient.set_dispatches()` method now correctly updates `FakeService._last_id` which is used to generate unique dispatch IDs.
* Fix that streams were globally shared between all clients.

