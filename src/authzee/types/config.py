"""Authzee config types."""

from typing import TypedDict


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
