# Frequenz Dispatch Client Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

* Support secrets for signing and verifying messages.
  * Use the new env variable `DISPATCH_API_SECRET` to set the secret key.
  * Use the new `sign_secret` parameter in the `DispatchClient` constructor to set the secret key.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
