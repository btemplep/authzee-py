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

## [0.1.0a6] - TBD

Support for Authzee spec 0.5.0.

### Added

- `ValidateBatchRequestResult` TypedDict type
- `validate_request_result_schema` - Return value schema for the `validate_request` function

### Changed

- Updated typing for Python 3.11+
    - `List[X]` → `list[X]`, `Dict[X, Y]` → `dict[X, Y]`, `Union[X, Y]` → `X | Y`
    - Removed `List`, `Dict`, `Union` from typing imports
- `validate_batch_request` now returns `ValidateBatchRequestResult` with `{error, batch}` instead of `GenericResult`
    - `batch` contains per-item validation errors (or None for valid items)
- `validate_batch_request_result_schema` renamed `batch_errors` field to `batch`
- `validate_request` in `InProcessCompute` now respects the full `ValidateRequestConfig`
    - Uses `use_list_context_defs`, `use_list_identity_defs`, `use_list_resource_defs` config options
- `validate_batch_request` in `InProcessCompute` now returns per-item errors in `batch` instead of failing fast
- `InProcessCompute`
    - Updated `validate_request` and `validate_batch_request` to be much more efficient, and fully support all config options for `get_*` vs `list_*`.
- Default config - parallel paging set to false by default.

### Removed

- `compute_storage_kwargs` parameter from `Authzee` and `AuthzeeAsync`
    - Compute module now receives `storage_kwargs` directly
    - If different storage kwargs are needed, create a separate Authzee instance with `InProcessCompute`


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

