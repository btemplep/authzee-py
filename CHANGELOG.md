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

## [Unreleased] - YYYY-MM-DD

### Added
- `SQLStorage` - SQL based storage module.
- `StorageModule` and `ComputeModule` now automatically translate exceptions raised in their methods into the method's expected result body.
    - Uses new `_StorageMeta` / `_ComputeMeta` metaclasses (built on a shared `_ModuleMeta`).
    - A raised exception is caught and returned as the correctly shaped result body with `error` populated and `error_type` set to `"storage"` or `"compute"` depending on where it originated.
- Full class and method docstrings for `StorageModule` and `ComputeModule`, including success and error return examples, call examples with the full config body, and notes on the automatic exception translation.
    - `ComputeModule` docstring notes that a compute module must handle all errors returned from storage.
- Class docstrings for `InProcessCompute` and `DictStorage`.

### Changed
- `ComputeModule` and `StorageModule` base classes now inherit from ABC. 
- `DictStorage` now stores storage latch `created_at` as an ISO 8601 string instead of a `datetime` object.

### Deprecated

### Removed
- `NotImplementedError` since base classes now use auto checks from ABC.

### Fixed
- `InProcessCompute` request and batch request validation
    - `get_context_def` / `get_resource_def` now use their own config instead of `get_identity_def`.
    - The non-list (`get_*`) identity lookup in `validate_request` now populates the identity lookup and returns the correct identity error message.
    - `validate_batch_request` no longer raises `KeyError` on the non-list identity lookup path and no longer silently succeeds for an unregistered root definition in the list path.

### Security 


## [0.1.0a6] - 2026-08-27

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

