# Frequenz Dispatch Client Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

* Added support for `dispatch_ids` and `queries` filters in the `list` method
  - `dispatch_ids` parameter allows filtering by specific dispatch IDs
  - `filter_queries` parameter supports text-based filtering on dispatch `id` and `type` fields
      - Query format: IDs are prefixed with `#` (e.g., `#4`), types are matched as substrings (e.g., `bar` matches `foobar`)
      - Multiple queries are combined with logical OR

## Bug Fixes

