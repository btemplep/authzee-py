# Changelog

Changelog for `authzee`.
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- 
## [Unreleased] - YYYY-MM-DD

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security 
-->

## [0.1.0a6] - 2026-08-22

Support for Authzee spec 0.5.0.

### Added

- `ValidateBatchRequestResult` TypedDict type
- `validate_request_result_schema` - Return value schema for the `validate_request` function

### Changed

- `validate_batch_request` now returns `ValidateBatchRequestResult` with `{error, batch}` instead of `GenericResult`
    - `batch` contains per-item validation errors (or None for valid items)
- `validate_batch_request_result_schema` renamed `batch_errors` field to `batch`


## [0.1.0a5] - 2026-08-19

New revamp to support Authzee spec 0.4.0. 


## [0.1.0a4] - 2026-06-15

New revamp to support Authzee spec 0.3.0. 

### [0.1.0a3] - 2024-02-19

- Remove sync methods and only accepts async
- `AuthzeeSync` class as a sync wrapper for Authzee app
- Locality checks streamlined. Only specify current backends locality
- Parallel pagination skeleton
- general renaming
- rework ResourceAuthzs. They are now passed as instances to the Authzee app. Logic is streamlined

### [0.1.0a2] - 2023-07-23

- Initial Alpha.

### [0.1.0a1] - 2023-07-03

- Initial stub 

