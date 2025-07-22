# Frequenz Dispatch Client Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

* Support secrets for signing and verifying messages.
  * Use the new env variable `DISPATCH_API_SIGN_SECRET` to set the secret key.
  * Use the new `sign_secret` parameter in the `DispatchClient` constructor to set the secret key.
* Added `auth_key` parameter to the `dispatch-cli` and thew env variable `DISPATCH_API_AUTH_KEY` to set the authentication key for the Dispatch API.


## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
