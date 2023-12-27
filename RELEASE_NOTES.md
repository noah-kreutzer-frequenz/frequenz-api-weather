# Frequenz Weather API Release Notes

## Summary
This API interface's client requires the SDK, which depends on
frequenz-api-common in the version range of 0.3.0 to < 0.4.0. However,
weather.proto necessitates the location and pagination protos from
frequenz-api-common, which are only available from version 0.5.0. This
discrepancy creates a dependency conflict. To resolve this, the current
PR incorporates the required protos directly from the frequenz-api-common
repository and stores them locally. This approach eliminates the dependency
on the common repository for the time being, until the SDK is updated to a
newer version of frequenz-api-common.

## Upgrading


## New Features


## Bug Fixes

