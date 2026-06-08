"""Authzee types."""

__all__ = [
    "AnyJSON",
    "AuthzeeBaseConfig",
    "StartConfig",
    "ComputeStartConfig",
    "StorageStartConfig",
    "ShutdownConfig",
    "ComputeShutdownConfig",
    "StorageShutdownConfig",
    "ConstructConfig",
    "ComputeConstructConfig",
    "StorageConstructConfig",
    "DestroyConfig",
    "ComputeDestroyConfig",
    "StorageDestroyConfig",
    "ListContextDefsConfig",
    "ListIdentityDefsConfig",
    "ListResourceDefsConfig",
    "ListGrantsConfig",
    "ListGrantRefsConfig",
    "GetConfig",
    "ValidateRequestConfig",
    "ValidateBatchRequestConfig",
    "AuditConfig",
    "BatchAuditConfig",
    "AuthorizeConfig",
    "BatchAuthorizeConfig",
    "AuthzeeBaseConfigOverride",
    "StartConfigOverride",
    "ComputeStartConfigOverride",
    "StorageStartConfigOverride",
    "ShutdownConfigOverride",
    "ComputeShutdownConfigOverride",
    "StorageShutdownConfigOverride",
    "ConstructConfigOverride",
    "ComputeConstructConfigOverride",
    "StorageConstructConfigOverride",
    "DestroyConfigOverride",
    "ComputeDestroyConfigOverride",
    "StorageDestroyConfigOverride",
    "ListContextDefsConfigOverride",
    "ListIdentityDefsConfigOverride",
    "ListResourceDefsConfigOverride",
    "ListGrantsConfigOverride",
    "GetConfigOverride",
    "ListGrantRefsConfigOverride",
    "ValidateRequestConfigOverride",
    "ValidateBatchRequestConfigOverride",
    "AuditConfigOverride",
    "BatchAuditConfigOverride",
    "AuthorizeConfigOverride",
    "BatchAuthorizeConfigOverride",
    "AuthzeeConfigOverride",
    "AuthzeeConfig",
    "GenericError",
    "ResultErrors",
    "GenericResult",
    "ContextDef",
    "ContextDefResult",
    "ContextDefsPage",
    "IdentityDef",
    "IdentityDefResult",
    "IdentityDefsPage",
    "ResourceDef",
    "ResourceDefResult",
    "ResourceDefsPage",
    "Grant",
    "GrantResult",
    "GrantsPage",
    "PageRefsPage",
    "StorageLatch",
    "StorageLatchResult",
    "AuthzeeRequest",
    "BatchItem",
    "AuthzeeBatchRequest",
    "ExecuteResult",
    "EvaluateResult",
    "AuditResultItem",
    "AuditResultPage",
    "AuthorizeResult",
    "BatchAuditResultItem",
    "BatchAuditResultPage",
    "BatchAuthorizeResult",
]
from typing import Any, Dict, List, Literal, NotRequired, TypedDict


AnyJSON = bool | str | int | float | None | list | dict


class AuthzeeBaseConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Authzee base instance configuration.

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


class ComputeStartConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Compute start configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class StorageStartConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Storage start configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class StartConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Start configuration.

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
    compute_start : ComputeStartConfig
        Compute start configuration.
    storage_start : StorageStartConfig
        Storage start configuration.
    """
    compute_start: ComputeStartConfig
    storage_start: StorageStartConfig


class ComputeShutdownConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Compute shutdown configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class StorageShutdownConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Storage shutdown configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class ShutdownConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Shutdown configuration.

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
    compute_shutdown : ComputeShutdownConfig
        Compute shutdown configuration.
    storage_shutdown : StorageShutdownConfig
        Storage shutdown configuration.
    """
    compute_shutdown: ComputeShutdownConfig
    storage_shutdown: StorageShutdownConfig


class ComputeConstructConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Compute construct configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class StorageConstructConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Storage construct configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class ConstructConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Construct configuration.

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
    compute_construct : ComputeConstructConfig
        Compute construct configuration.
    storage_construct : StorageConstructConfig
        Storage construct configuration.
    """
    compute_construct: ComputeConstructConfig
    storage_construct: StorageConstructConfig


class ComputeDestroyConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Compute destroy configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class StorageDestroyConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Storage destroy configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class DestroyConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Destroy configuration.

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
    compute_destroy : ComputeDestroyConfig
        Compute destroy configuration.
    storage_destroy : StorageDestroyConfig
        Storage destroy configuration.
    """
    compute_destroy: ComputeDestroyConfig
    storage_destroy: StorageDestroyConfig


class ListContextDefsConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    List context definitions configuration.

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


class ListIdentityDefsConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    List identity definitions configuration.

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


class ListResourceDefsConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    List resource definitions configuration.

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


class ListGrantsConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    List grants configuration.

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


class GetConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Get configuration.

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


class ListGrantRefsConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    List grant references configuration.

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


class ValidateRequestConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Validate request configuration.

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
    list_identity_defs : ListIdentityDefsConfig
        Config for listing identity definitions during validation.
    """
    use_list_identity_defs: bool
    list_identity_defs: ListIdentityDefsConfig


class ValidateBatchRequestConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Validate batch request configuration.

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
    list_context_defs : ListContextDefsConfig
        Config for listing context definitions during validation.
    use_list_identity_defs : bool
        Whether to use list identity defs for validation.
    list_identity_defs : ListIdentityDefsConfig
        Config for listing identity definitions during validation.
    use_list_resource_defs : bool
        Whether to use list resource defs for validation.
    list_resource_defs : ListResourceDefsConfig
        Config for listing resource definitions during validation.
    """
    use_list_context_defs: bool
    list_context_defs: ListContextDefsConfig
    use_list_identity_defs: bool
    list_identity_defs: ListIdentityDefsConfig
    use_list_resource_defs: bool
    list_resource_defs: ListResourceDefsConfig


class AuditConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Audit configuration.

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
    validate_request : ValidateRequestConfig
        Config for validating requests during audit.
    """
    grants_page_size: int
    use_grants_cache: bool
    validate_request: ValidateRequestConfig


class BatchAuditConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Batch audit configuration.

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
    validate_batch_request : ValidateBatchRequestConfig
        Config for validating batch requests during audit.
    """
    grants_page_size: int
    use_grants_cache: bool
    validate_batch_request: ValidateBatchRequestConfig


class AuthorizeConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Authorize configuration.

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
    validate_request : ValidateRequestConfig
        Config for validating requests during authorization.
    """
    grants_page_size: int
    use_grants_cache: bool
    grant_refs_page_size: int
    use_grant_refs_cache: bool
    parallel_paging: bool
    validate_request: ValidateRequestConfig


class BatchAuthorizeConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Batch authorize configuration.

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
    validate_batch_request : ValidateBatchRequestConfig
        Config for validating batch requests during authorization.
    """
    grants_page_size: int
    use_grants_cache: bool
    grant_refs_page_size: int
    use_grant_refs_cache: bool
    parallel_paging: bool
    validate_batch_request: ValidateBatchRequestConfig


class AuthzeeConfig(TypedDict):
    """```python
    Dict[str, Dict[str, Any]]
    ```
    Authzee configuration Type. Held in each Authzee class instance to feed configuration for everything. 

    The configuration can be set at several different levels where only the provided values override the previous levels values. 

    The order of least to most precedence is:
    - Default config values - None Set
    - Authzee class instances config
    - Function/Method call config

    Examples
    --------
    **All base and nested fields are required for this Dict so they are normalized when passed to storage and compute.**
    Example with all defaults:
    ```python
    {
        "authzee": {
            "raise_crits": True
        },
        "start": {
            "compute_start": {},
            "storage_start": {},
        },
        "shutdown": {
            "compute_shutdown": {},
            "storage_shutdown": {},
        },
        "construct": {
            "compute_construct": {},
            "storage_construct": {},
        },
        "destroy": {
            "compute_destroy": {},
            "storage_destroy": {},
        },
        "validate_context_def": {},
        "list_context_defs": {
            "page_size": 100,
            "use_cache": False
        },
        "get_context_def": {
            "use_cache": False
        },
        "put_context_def": {},
        "delete_context_def": {},
        "validate_identity_def": {},
        "list_identity_defs": {
            "page_size": 100,
            "use_cache": False
        },
        "get_identity_def": {
            "use_cache": False  
        },
        "put_identity_def": {},
        "delete_identity_def": {},
        "validate_resource_def": {},
        "list_resource_defs": {
            "page_size": 100,
            "use_cache": False
        },
        "get_resource_def": {
            "use_cache": False  
        },
        "put_resource_def": {},
        "delete_resource_def": {},
        "validate_grant": {},
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        },
        "get_grant": {
            "use_cache": False  
        },
        "enact": {},
        "repeal": {},
        "list_grant_refs": {
            "page_size": 10,
            "use_cache": False
        },
        "cleanup_latches": {},
        "validate_request": {
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
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
        },
        "audit": {
            "grants_page_size": 100,
            "use_grants_cache": True,
            "validate_request": {
                "use_list_identity_defs": True,
                "list_identity_defs": {
                    "page_size": 100,
                    "use_cache": True
                }
            }
        },
        "batch_audit": {
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
        },
        "authorize": {
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
        },
        "batch_authorize": {
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
    }
    ```

    Attributes
    ----------
    authzee : AuthzeeBaseConfig
        Authzee class instance configuration.
    list_context_defs : ListContextDefsConfig
        Config for listing context definitions.
    list_identity_defs : ListIdentityDefsConfig
        Config for listing identity definitions.
    list_resource_defs : ListResourceDefsConfig
        Config for listing resource definitions.
    list_grants : ListGrantsConfig
        Config for listing grants.
    list_grant_refs : ListGrantRefsConfig
        Config for listing grant page references.
    validate_request : ValidateRequestConfig
        Config for validating requests.
    validate_batch_request : ValidateBatchRequestConfig
        Config for validating batch requests.
    audit : AuditConfig
        Config for the audit operation.
    batch_audit : BatchAuditConfig
        Config for the batch audit operation.
    authorize : AuthorizeConfig
        Config for the authorize operation.
    batch_authorize : BatchAuthorizeConfig
        Config for the batch authorize operation.
    """
    authzee: AuthzeeBaseConfig
    start: StartConfig
    shutdown: ShutdownConfig
    construct: ConstructConfig
    destroy: DestroyConfig
    validate_context_def: dict
    list_context_defs: ListContextDefsConfig
    get_context_def: GetConfig
    put_context_def: dict
    delete_context_def: dict
    validate_identity_def: dict
    list_identity_defs: ListIdentityDefsConfig
    get_identity_def: GetConfig
    put_identity_def: dict
    delete_identity_def: dict
    validate_resource_def: dict
    list_resource_defs: ListResourceDefsConfig
    get_resource_def: GetConfig
    put_resource_def: dict
    delete_resource_def: dict
    validate_grant: dict
    list_grants: ListGrantsConfig
    get_grant: GetConfig
    enact: dict
    repeal: dict
    list_grant_refs: ListGrantRefsConfig
    cleanup_latches: dict
    validate_request: ValidateRequestConfig
    validate_batch_request: ValidateBatchRequestConfig
    audit: AuditConfig
    batch_audit: BatchAuditConfig
    authorize: AuthorizeConfig
    batch_authorize: BatchAuthorizeConfig


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

    Examples
    --------
    **All base and nested fields are optional.**
    Example with all defaults:
    ```python
    {
        "authzee": {
            "raise_crits": True
        },
        "start": {
            "compute_start": {},
            "storage_start": {},
        },
        "shutdown": {
            "compute_shutdown": {},
            "storage_shutdown": {},
        },
        "construct": {
            "compute_construct": {},
            "storage_construct": {},
        },
        "destroy": {
            "compute_destroy": {},
            "storage_destroy": {},
        },
        "validate_context_def": {},
        "list_context_defs": {
            "page_size": 100,
            "use_cache": False
        },
        "get_context_def": {
            "use_cache": False
        },
        "put_context_def": {},
        "delete_context_def": {},
        "validate_identity_def": {},
        "list_identity_defs": {
            "page_size": 100,
            "use_cache": False
        },
        "get_identity_def": {
            "use_cache": False  
        },
        "put_identity_def": {},
        "delete_identity_def": {},
        "validate_resource_def": {},
        "list_resource_defs": {
            "page_size": 100,
            "use_cache": False
        },
        "get_resource_def": {
            "use_cache": False  
        },
        "put_resource_def": {},
        "delete_resource_def": {},
        "validate_grant": {},
        "list_grants": {
            "page_size": 100,
            "use_cache": False
        },
        "get_grant": {
            "use_cache": False  
        },
        "enact": {},
        "repeal": {},
        "list_grant_refs": {
            "page_size": 10,
            "use_cache": False
        },
        "cleanup_latches": {},
        "validate_request": {
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
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
        },
        "audit": {
            "grants_page_size": 100,
            "use_grants_cache": True,
            "validate_request": {
                "use_list_identity_defs": True,
                "list_identity_defs": {
                    "page_size": 100,
                    "use_cache": True
                }
            }
        },
        "batch_audit": {
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
        },
        "authorize": {
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
        },
        "batch_authorize": {
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
    }
    ```

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


class GenericError(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Generic Error Type

    Examples
    --------
    ```python
    {
        "is_critical": False,
        "message": "Error message here"
    }
    ```
    """
    is_critical: bool
    message: str


ResultErrors = Dict[
    Literal[
        "definition",
        "grant",
        "request",
        "evaluation",
        "locality_incompatibility",
        "not_implemented",
        "parallel_pagination_not_supported",
        "page_reference",
        "resource_not_found",
        "start"
    ],
    List[GenericError]
]
"""Result errors for all responses

    Examples
    --------
    ```python
    {
        "<error_type>": [ 
            {
                "is_critical": False,
                "message": "Error message."
            }
        ],
        "<other_error_type>": [
            {
                "is_critical": False,
                "message": "Error message."
            }
        ]
    }
    ```
    """

class GenericResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    has_failed: bool
    errors: ResultErrors


class ContextDef(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "context_type": "MyContext",
        "schema": {
            "type": "object"    ,
            "properties": {
                "my_prop": {
                    "type": "string"
                }
            }
        }
    }
    ```
    """
    context_type: str
    schema: Dict[str: AnyJSON]


class ContextDefResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Result of 

    Examples
    --------
    ```python
    {
        "context_def": { # dict | None
            "context_type": "MyContext",
            "schema": {
                "type": "object"    ,
                "properties": {
                    "my_prop": {
                        "type": "string"
                    }
                }
            }
        },
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    context_def: ContextDef | None
    has_failed: bool
    errors: ResultErrors


class ContextDefsPage(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "context_defs": [
            {
                "context_type": "MyContext",
                "schema": {
                    "type": "object"    ,
                    "properties": {
                        "my_prop": {
                            "type": "string"
                        }
                    }
                }
            }
        ],
        "next_page_ref": "abc12": # str | None
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    context_defs: List[ContextDef]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors


class IdentityDef(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "identity_type": "MyIdentity",
        "schema": {
            "type": "object"    ,
            "properties": {
                "my_prop": {
                    "type": "string"
                }
            }
        }
    }
    ```
    """
    identity_type: str
    schema: dict


class IdentityDefResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "identity_def": { # dict | None
            "identity_type": "MyIdentity",
            "schema": {
                "type": "object"    ,
                "properties": {
                    "my_prop": {
                        "type": "string"
                    }
                }
            }
        },
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    identity_def: IdentityDef| None
    has_failed: bool
    errors: ResultErrors


class IdentityDefsPage(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "identity_defs": [
            {
                "identity_type": "MyIdentity",
                "schema": {
                    "type": "object"    ,
                    "properties": {
                        "my_prop": {
                            "type": "string"
                        }
                    }
                }
            }
        ],
        "next_page_ref": "abc12": # str | None
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    identity_defs: List[IdentityDef]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors


class ResourceDef(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "resource_type": "MyResource",
        "actions": [
            "MyResource.MyAction"
        ],
        "schema": {
            "type": "object"    ,
            "properties": {
                "my_prop": {
                    "type": "string"
                }
            }
        }
    }
    ```
    """
    resource_type: str
    actions: List[str]
    schema: dict


class ResourceDefResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "resource_def": { # dict | None
            "resource_type": "MyResource",
            "actions": [
                "MyResource.MyAction"
            ],
            "schema": {
                "type": "object"    ,
                "properties": {
                    "my_prop": {
                        "type": "string"
                    }
                }
            }
        },
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    resource_def: ResourceDef| None
    has_failed: bool
    errors: ResultErrors


class ResourceDefsPage(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "resource_defs": [
            {
                "resource_type": "MyResource",
                "actions": [
                    "MyResource.MyAction"
                ],
                "schema": {
                    "type": "object"    ,
                    "properties": {
                        "my_prop": {
                            "type": "string"
                        }
                    }
                }
            }
        ],
        "next_page_ref": "abc12": # str | None
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    resource_defs: List[ResourceDef]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors


class Grant(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
        "name": "People friendly name",
        "description": "Long description",
        "tags": {
            "my tag key": "my tag value"
        },
        "effect": "allow", # allow | deny 
        "actions": [
            "MyResource.MyAction"
        ],
        "query": "contains(request.identities, 'User')",
        "evaluation_handler": "evaluate", # evaluate | error | critical
        equality: True # AnyJSON
        data: { # top level dictionary with str keys, everything else is free form
            "str here": "anything else here
        }  
    }
    ```
    """
    grant_uuid: str
    name: str
    description: str
    tags: Dict[str, str]
    effect: Literal["allow", "deny"]
    actions: List[str]
    query: str
    evaluation_handler: Literal[
        "evaluate",
        "error",
        "critical"
    ]
    equality: AnyJSON
    data: Dict[str, Any]


class GrantResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "grant": { # dict | None
            "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            "name": "People friendly name",
            "description": "Long description",
            "tags": {
                "my tag key": "my tag value"
            },
            "effect": "allow", # allow | deny 
            "actions": [
                "MyResource.MyAction"
            ],
            "query": "contains(request.identities, 'User')",
            "evaluation_handler": "evaluate", # evaluate | error | critical
            equality: True # AnyJSON
            data: { # top level dictionary with str keys, everything else is free form
                "str here": "anything else here
            }  
        },
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    grant: Grant | None
    has_failed: bool
    errors: ResultErrors


class GrantsPage(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "grants": [
            { 
                "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                "name": "People friendly name",
                "description": "Long description",
                "tags": {
                    "my tag key": "my tag value"
                },
                "effect": "allow", # allow | deny 
                "actions": [
                    "MyResource.MyAction"
                ],
                "query": "contains(request.identities, 'User')",
                "evaluation_handler": "evaluate", # evaluate | error | critical
                equality: True # AnyJSON
                data: { # top level dictionary with str keys, everything else is free form
                    "str here": "anything else here
                }  
            }
        ],
        "next_page_ref": "abc123", # str | None
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    grants: List[Grant]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors


class PageRefsPage(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "page_refs": [
            abc123"
        ],
        "next_page_ref": "abc123", # str | None
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    page_refs: List[str]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors


class StorageLatch(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "storage_latch_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
        "is_set": False,
        "created": "2026-04-26T16:21:10.521220"
    }
    ```
    """
    storage_latch_uuid: str
    is_set: bool = False
    created_at: str


class StorageLatchResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "storage_latch": { # dict | None
            "storage_latch_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            "is_set": False,
            "created": "2026-04-26T16:21:10.521220"
        },
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    ```
    """
    storage_latch: StorageLatch | None
    has_failed: bool
    errors: ResultErrors


class AuthzeeRequest(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "identities": {
            "ADUser": [
                {
                    "cn": "authzee_user_1"
                }
            ]
        },
        "action": "Balloon.CreateBalloon",
        "resource_type": "Balloon",
        "resource": {
            "color": "blue",
            "size": 27.0
        },
        "evaluation_handler": "evaluate", # grant | evaluate | error | critical
        "context_type": "MyContext",
        "context": {
            "allowed_sizes": [20.0, 27.0]
        }
    }
    ```
    """
    identities: Dict[str, List[Dict[str, AnyJSON]]]
    action: str
    resource_type: str
    resource: Dict[str, AnyJSON]
    evaluation_handler: Literal[
        "grant",
        "evaluate",
        "error",
        "critical"
    ]
    context_type: str
    context: Dict[str, AnyJSON]


class BatchItem(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    **All base fields are not required.**
    ```python
    {
        "identities": { # dict | None
            "ADUser": [
                {"cn": "authzee_user_1"}
            ]
        },
        "resource_type": "Balloon", # str | None
        "resource": { # dict | None
            "color": "blue",
            "size": 27.0
        },
        "evaluation_handler": "evaluate", # evaluate | error | critical | None
        "context_type": "MyContext", # str | None
        "context": { # dict | None
            "allowed_sizes": [20.0, 27.0]
        }
    }
    ```
    """
    identities: Dict[str, List[Dict[str, AnyJSON]]] | None = None
    resource_type: str | None = None
    resource: Dict[str, AnyJSON] | None = None
    evaluation_handler: Literal[
        "grant",
        "evaluate",
        "error",
        "critical"
    ] | None = None
    context_type: str | None = None
    context: Dict[str, AnyJSON] | None = None


class AuthzeeBatchRequest(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "identities": {
            "ADUser": [
                {
                    "cn": "authzee_user_1"
                }
            ]
        },
        "action": "Balloon.CreateBalloon",
        "resource_type": "Balloon",
        "resource": {
            "color": "blue",
            "size": 27.0
        },
        "evaluation_handler": "evaluate", # grant | evaluate | error | critical
        "context_type": "MyContext",
        "context": {
            "allowed_sizes": [20.0, 27.0]
        },
        "batch": [
            {
                "resource": {
                    "color": "red",
                    "size": 100.8
                }
            }
        ]
    }
    ```
    """
    identities: Dict[str, List[Dict[str, AnyJSON]]]
    action: str
    resource_type: str
    resource: Dict[str, AnyJSON]
    evaluation_handler: Literal[
        "grant",
        "evaluate",
        "error",
        "critical"
    ]
    context_type: str
    context: Dict[str, AnyJSON]
    batch: List[BatchItem]

class ExecuteResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "result": True,
        "has_failed": False,
        "error_message": None # str | None
    }
    ```
    """
    result: Any
    has_failed: bool
    error_message: str | None


class EvaluateResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "is_applicable": True,
        "query_result": True,
        "has_failed": False,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    is_applicable: bool
    query_result: Any
    has_failed: bool
    errors: ResultErrors


class AuditResultItem(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "is_applicable": True,
        "query_result": True,
        "errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    is_applicable: bool
    query_result: AnyJSON
    errors: ResultErrors


class AuditResultPage(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "grants": [
            {
                "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                "name": "People friendly name",
                "description": "Long description",
                "tags": {"my tag key": "my tag value"},
                "effect": "allow",
                "actions": ["MyResource.MyAction"],
                "query": "contains(request.identities, 'User')",
                "evaluation_handler": "evaluate",
                "equality": True,
                "data": {}
            }
        ],
        "results": [
            {
                "is_applicable": True,
                "query_result": True,
                "errors": { # result errors
                    "<error_type>": [ 
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
        ],
        "next_page_ref": "abc123", # str | None
        "has_failed": False,
        "errors": { # request errors and propagated result errors
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    grants: List[Grant]
    results: List[AuditResultItem]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors


class AuthorizeResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "is_authorized": True,
        "grant": { # dict | None
            "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            "name": "People friendly name",
            "description": "Long description",
            "tags": {
                "my tag key": "my tag value"
            },
            "effect": "allow",
            "actions": ["MyResource.MyAction"],
            "query": "contains(request.identities, 'User')",
            "evaluation_handler": "evaluate",
            "equality": True,
            "data": {}
        },
        "message": "Authorized by grant.",
        "has_failed": False,
        "critical_errors": {
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    is_authorized: bool
    grant: Grant | None
    message: str
    has_failed: bool
    critical_errors: ResultErrors


class BatchAuditResultItem(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "results": [
            {
                "is_applicable": True,
                "query_result": True,
                "errors": { # result errors
                    "<error_type>": [ 
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
        ],
        "has_failed": False,
        "errors": { # request errors and propagated result errors
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    results: List[AuditResultItem]
    has_failed: bool
    errors: ResultErrors


class BatchAuditResultPage(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "grants": [
            {
                "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                "name": "People friendly name",
                "description": "Long description",
                "tags": {"my tag key": "my tag value"},
                "effect": "allow",
                "actions": ["MyResource.MyAction"],
                "query": "contains(request.identities, 'User')",
                "evaluation_handler": "evaluate",
                "equality": True,
                "data": {}
            }
        ],
        "batch_results": [
            {
                "results": [
                    {
                        "is_applicable": True,
                        "query_result": True,
                        "errors": { # result errors
                            "<error_type>": [ 
                                {
                                    "is_critical": False,
                                    "message": "Error message."
                                }
                            ]
                        }
                    }
                ],
                "has_failed": False,
                "errors": { # request errors and propagated result errors
                    "<error_type>": [ 
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
        ],
        "next_page_ref": "abc123", # str | None
        "has_failed": False,
        "errors": { # Batch request errors and propagated request errors
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    grants: List[Grant]
    batch_results: List[BatchAuditResultItem]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors


class BatchAuthorizeResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "batch_results": [
            {
                "is_authorized": True,
                "grant": {
                    "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                    "name": "People friendly name",
                    "description": "Long description",
                    "tags": {},
                    "effect": "allow",
                    "actions": ["MyResource.MyAction"],
                    "query": "contains(request.identities, 'User')",
                    "evaluation_handler": "evaluate",
                    "equality": True,
                    "data": {}
                },
                "message": "Authorized by grant.",
                "has_failed": False,
                "critical_errors": { # request errors
                    "<error_type>": [ 
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
        ],
        "has_failed": False,
        "critical_errors": { # batch errors and propagated request errors
            "<error_type>": [ 
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ]
        }
    }
    ```
    """
    batch_results: List[AuthorizeResult]
    has_failed: bool
    critical_errors: ResultErrors

