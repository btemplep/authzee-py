"""Authzee config override types."""

from typing import TypedDict


class AuthzeeBaseConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Authzee base instance configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "raise_crits": True
    }
    ```

    Attributes
    ----------
    raise_crits : bool
        Whether to raise on critical errors.
    """
    raise_crits: bool


class ComputeStartConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Compute start configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class StorageStartConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Storage start configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class StartConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Start configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "compute_start": {},
        "storage_start": {}
    }
    ```

    Attributes
    ----------
    compute_start : ComputeStartConfigOverride
        Compute start configuration override.
    storage_start : StorageStartConfigOverride
        Storage start configuration override.
    """
    compute_start: ComputeStartConfigOverride
    storage_start: StorageStartConfigOverride


class ComputeShutdownConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Compute shutdown configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class StorageShutdownConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Storage shutdown configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class ShutdownConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Shutdown configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "compute_shutdown": {},
        "storage_shutdown": {}
    }
    ```

    Attributes
    ----------
    compute_shutdown : ComputeShutdownConfigOverride
        Compute shutdown configuration override.
    storage_shutdown : StorageShutdownConfigOverride
        Storage shutdown configuration override.
    """
    compute_shutdown: ComputeShutdownConfigOverride
    storage_shutdown: StorageShutdownConfigOverride


class ComputeConstructConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Compute construct configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class StorageConstructConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Storage construct configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class ConstructConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Construct configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "compute_construct": {},
        "storage_construct": {}
    }
    ```

    Attributes
    ----------
    compute_construct : ComputeConstructConfigOverride
        Compute construct configuration override.
    storage_construct : StorageConstructConfigOverride
        Storage construct configuration override.
    """
    compute_construct: ComputeConstructConfigOverride
    storage_construct: StorageConstructConfigOverride


class ComputeDestroyConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Compute destroy configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class StorageDestroyConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Storage destroy configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class DestroyConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Destroy configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "compute_destroy": {},
        "storage_destroy": {}
    }
    ```

    Attributes
    ----------
    compute_destroy : ComputeDestroyConfigOverride
        Compute destroy configuration override.
    storage_destroy : StorageDestroyConfigOverride
        Storage destroy configuration override.
    """
    compute_destroy: ComputeDestroyConfigOverride
    storage_destroy: StorageDestroyConfigOverride


class ListContextDefsConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    List context definitions configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "page_size": 100,
        "use_cache": False
    }
    ```

    Attributes
    ----------
    page_size : int
        Number of items per page.
    use_cache : bool
        Whether to use cache.
    """
    page_size: int
    use_cache: bool


class ListIdentityDefsConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    List identity definitions configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "page_size": 100,
        "use_cache": False
    }
    ```

    Attributes
    ----------
    page_size : int
        Number of items per page.
    use_cache : bool
        Whether to use cache.
    """
    page_size: int
    use_cache: bool


class ListResourceDefsConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    List resource definitions configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "page_size": 100,
        "use_cache": False
    }
    ```

    Attributes
    ----------
    page_size : int
        Number of items per page.
    use_cache : bool
        Whether to use cache.
    """
    page_size: int
    use_cache: bool


class ListGrantsConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    List grants configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "page_size": 100,
        "use_cache": False
    }
    ```

    Attributes
    ----------
    page_size : int
        Number of items per page.
    use_cache : bool
        Whether to use cache.
    """
    page_size: int
    use_cache: bool


class GetConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Get configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "use_cache": False
    }
    ```

    Attributes
    ----------
    use_cache : bool
        Whether to use cache.
    """
    use_cache: bool


class ListGrantRefsConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    List grant references configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "page_size": 10,
        "use_cache": False
    }
    ```

    Attributes
    ----------
    page_size : int
        Number of items per page.
    use_cache : bool
        Whether to use cache.
    """
    page_size: int
    use_cache: bool


class ValidateRequestConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Validate request configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "use_list_identity_defs": True,
        "list_identity_defs": {
            "page_size": 100,
            "use_cache": True
        }
    }
    ```

    Attributes
    ----------
    use_list_identity_defs : bool
        Whether to use list identity defs for validation.
    list_identity_defs : ListIdentityDefsConfigOverride
        Config for listing identity definitions during validation.
    """
    use_list_identity_defs: bool
    list_identity_defs: ListIdentityDefsConfigOverride


class ValidateBatchRequestConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Validate batch request configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "use_list_context_defs": True,
        "list_context_defs": {
            "page_size": 100,
            "use_cache": True
        },
        "use_list_identity_defs": True,
        "list_identity_defs": {
            "page_size": 100,
            "use_cache": True
        },
        "use_list_resource_defs": True,
        "list_resource_defs": {
            "page_size": 100,
            "use_cache": True
        }
    }
    ```

    Attributes
    ----------
    use_list_context_defs : bool
        Whether to use list context defs for validation.
    list_context_defs : ListContextDefsConfigOverride
        Config for listing context definitions during validation.
    use_list_identity_defs : bool
        Whether to use list identity defs for validation.
    list_identity_defs : ListIdentityDefsConfigOverride
        Config for listing identity definitions during validation.
    use_list_resource_defs : bool
        Whether to use list resource defs for validation.
    list_resource_defs : ListResourceDefsConfigOverride
        Config for listing resource definitions during validation.
    """
    use_list_context_defs: bool
    list_context_defs: ListContextDefsConfigOverride
    use_list_identity_defs: bool
    list_identity_defs: ListIdentityDefsConfigOverride
    use_list_resource_defs: bool
    list_resource_defs: ListResourceDefsConfigOverride


class AuditConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Audit configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "grants_page_size": 100,
        "use_grants_cache": True,
        "validate_request": {
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            }
        }
    }
    ```

    Attributes
    ----------
    grants_page_size : int
        Number of grants per page.
    use_grants_cache : bool
        Whether to use grants cache.
    validate_request : ValidateRequestConfigOverride
        Config for validating requests during audit.
    """
    grants_page_size: int
    use_grants_cache: bool
    validate_request: ValidateRequestConfigOverride


class BatchAuditConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Batch audit configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "grants_page_size": 100,
        "use_grants_cache": True,
        "validate_batch_request": {
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        }
    }
    ```

    Attributes
    ----------
    grants_page_size : int
        Number of grants per page.
    use_grants_cache : bool
        Whether to use grants cache.
    validate_batch_request : ValidateBatchRequestConfigOverride
        Config for validating batch requests during audit.
    """
    grants_page_size: int
    use_grants_cache: bool
    validate_batch_request: ValidateBatchRequestConfigOverride


class AuthorizeConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Authorize configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "grants_page_size": 100,
        "use_grants_cache": True,
        "grant_refs_page_size": 10,
        "use_grant_refs_cache": True,
        "parallel_paging": True,
        "validate_request": {
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            }
        }
    }
    ```

    Attributes
    ----------
    grants_page_size : int
        Number of grants per page.
    use_grants_cache : bool
        Whether to use grants cache.
    grant_refs_page_size : int
        Number of grant references per page.
    use_grant_refs_cache : bool
        Whether to use grant references cache.
    parallel_paging : bool
        Whether to use parallel paging.
    validate_request : ValidateRequestConfigOverride
        Config for validating requests during authorization.
    """
    grants_page_size: int
    use_grants_cache: bool
    grant_refs_page_size: int
    use_grant_refs_cache: bool
    parallel_paging: bool
    validate_request: ValidateRequestConfigOverride


class BatchAuthorizeConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Batch authorize configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "grants_page_size": 100,
        "use_grants_cache": True,
        "grant_refs_page_size": 10,
        "use_grant_refs_cache": True,
        "parallel_paging": True,
        "validate_batch_request": {
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        }
    }
    ```

    Attributes
    ----------
    grants_page_size : int
        Number of grants per page.
    use_grants_cache : bool
        Whether to use grants cache.
    grant_refs_page_size : int
        Number of grant references per page.
    use_grant_refs_cache : bool
        Whether to use grant references cache.
    parallel_paging : bool
        Whether to use parallel paging.
    validate_batch_request : ValidateBatchRequestConfigOverride
        Config for validating batch requests during authorization.
    """
    grants_page_size: int
    use_grants_cache: bool
    grant_refs_page_size: int
    use_grant_refs_cache: bool
    parallel_paging: bool
    validate_batch_request: ValidateBatchRequestConfigOverride


class AuthzeeConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Dict[str, Any]]
    ```
    Authzee configuration override Type. All keys are optional.

    Used to override specific configuration values at the Authzee class instance level
    or function/method call level without needing to specify all fields.

    Attributes
    ----------
    authzee : AuthzeeBaseConfigOverride
        Authzee class instance configuration override.
    start : StartConfigOverride
        Start configuration override.
    shutdown : ShutdownConfigOverride
        Shutdown configuration override.
    construct : ConstructConfigOverride
        Construct configuration override.
    destroy : DestroyConfigOverride
        Destroy configuration override.
    list_context_defs : ListContextDefsConfigOverride
        Config override for listing context definitions.
    list_identity_defs : ListIdentityDefsConfigOverride
        Config override for listing identity definitions.
    list_resource_defs : ListResourceDefsConfigOverride
        Config override for listing resource definitions.
    list_grants : ListGrantsConfigOverride
        Config override for listing grants.
    list_grant_refs : ListGrantRefsConfigOverride
        Config override for listing grant page references.
    validate_request : ValidateRequestConfigOverride
        Config override for validating requests.
    validate_batch_request : ValidateBatchRequestConfigOverride
        Config override for validating batch requests.
    audit : AuditConfigOverride
        Config override for the audit operation.
    batch_audit : BatchAuditConfigOverride
        Config override for the batch audit operation.
    authorize : AuthorizeConfigOverride
        Config override for the authorize operation.
    batch_authorize : BatchAuthorizeConfigOverride
        Config override for the batch authorize operation.
    """
    authzee: AuthzeeBaseConfigOverride
    start: StartConfigOverride
    shutdown: ShutdownConfigOverride
    construct: ConstructConfigOverride
    destroy: DestroyConfigOverride
    validate_context_def: dict
    list_context_defs: ListContextDefsConfigOverride
    get_context_def: GetConfigOverride
    put_context_def: dict
    delete_context_def: dict
    validate_identity_def: dict
    list_identity_defs: ListIdentityDefsConfigOverride
    get_identity_def: GetConfigOverride
    put_identity_def: dict
    delete_identity_def: dict
    validate_resource_def: dict
    list_resource_defs: ListResourceDefsConfigOverride
    get_resource_def: GetConfigOverride
    put_resource_def: dict
    delete_resource_def: dict
    validate_grant: dict
    list_grants: ListGrantsConfigOverride
    get_grant: GetConfigOverride
    enact: dict
    repeal: dict
    list_grant_refs: ListGrantRefsConfigOverride
    cleanup_latches: dict
    validate_request: ValidateRequestConfigOverride
    validate_batch_request: ValidateBatchRequestConfigOverride
    audit: AuditConfigOverride
    batch_audit: BatchAuditConfigOverride
    authorize: AuthorizeConfigOverride
    batch_authorize: BatchAuthorizeConfigOverride
