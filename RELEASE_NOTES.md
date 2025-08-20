# Frequenz Dispatch Client Library Release Notes

## Summary

This release updates the `frequenz-api-dispatch` dependency to `v1.0.0-rc3` and adapts the client to the latest API changes. It introduces more specific component categories and new event names while maintaining backward compatibility.

## Upgrading

* `InverterType.SOLAR` is now deprecated in favor of `InverterType.PV`. The old name can still be used but will be removed in a future release.
* The following dependencies were updated:
  * `frequenz-api-dispatch` to `v1.0.0-rc3`
  * `frequenz-client-common` to `v0.3.6`
  * `grpcio` to `v1.72.1`

## New Features

* **`ElectricalComponentCategory`**: The client now uses the more specific `frequenz.client.common.microgrid.electrical_components.ElectricalComponentCategory` for targeting components.
  * The new property `TargetCategory.category2` will return an `ElectricalComponentCategory`.
  * The existing `TargetCategory.category` property is preserved for backward compatibility and returns the corresponding `ComponentCategory`.
* Support secrets for signing and verifying messages.
  * Use the new env variable `DISPATCH_API_SIGN_SECRET` to set the secret key.
  * Use the new `sign_secret` parameter in the `DispatchClient` constructor to set the secret key.
* Added `auth_key` parameter to the `dispatch-cli` and the new variable `DISPATCH_API_AUTH_KEY` to set the authentication key for the Dispatch API.


## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
