"""Authzee config override types."""

__all__ = [
    "AuditConfigOverride",
    "AuthorizeConfigOverride",
    "AuthzeeBaseConfigOverride",
    "AuthzeeConfigOverride",
    "BatchAuditConfigOverride",
    "BatchAuthorizeConfigOverride",
    "CleanupLatchesConfigOverride",
    "ComputeConstructConfigOverride",
    "ComputeDestroyConfigOverride",
    "ComputeShutdownConfigOverride",
    "ComputeStartConfigOverride",
    "ConstructConfigOverride",
    "DeleteContextDefConfigOverride",
    "DeleteIdentityDefConfigOverride",
    "DeleteResourceDefConfigOverride",
    "DestroyConfigOverride",
    "EnactConfigOverride",
    "GetContextDefConfigOverride",
    "GetGrantConfigOverride",
    "GetIdentityDefConfigOverride",
    "GetResourceDefConfigOverride",
    "ListContextDefsConfigOverride",
    "ListGrantRefsConfigOverride",
    "ListGrantsConfigOverride",
    "ListIdentityDefsConfigOverride",
    "ListResourceDefsConfigOverride",
    "PutContextDefConfigOverride",
    "PutIdentityDefConfigOverride",
    "PutResourceDefConfigOverride",
    "RepealConfigOverride",
    "ShutdownConfigOverride",
    "StartConfigOverride",
    "StorageConstructConfigOverride",
    "StorageDestroyConfigOverride",
    "StorageShutdownConfigOverride",
    "StorageStartConfigOverride",
    "ValidateBatchRequestConfigOverride",
    "ValidateContextDefConfigOverride",
    "ValidateGrantConfigOverride",
    "ValidateIdentityDefConfigOverride",
    "ValidateRequestConfigOverride",
    "ValidateResourceDefConfigOverride"
]

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
        "raise_errors": True
    }
    ```

    Attributes
    ----------
    raise_errors : bool
        Whether to raise on critical errors.
    """
    raise_errors: bool


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


class ComputeStartConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Compute start configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "storage": {}
    }
    ```

    Attributes
    ----------
    storage : StorageStartConfigOverride
        Storage start configuration override.
    """
    storage: StorageStartConfigOverride


class StartConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Start configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "compute_start": {
            "storage": {}
        },
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


class ComputeShutdownConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Compute shutdown configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "storage": {}
    }
    ```

    Attributes
    ----------
    storage : StorageShutdownConfigOverride
        Storage shutdown configuration override.
    """
    storage: StorageShutdownConfigOverride


class ShutdownConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Shutdown configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "compute_shutdown": {
            "storage": {}
        },
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


class ValidateContextDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Validate context definition configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class GetContextDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Get context definition configuration override. All keys are optional.

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


class PutContextDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Put context definition configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class DeleteContextDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Delete context definition configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class ValidateIdentityDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Validate identity definition configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class GetIdentityDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Get identity definition configuration override. All keys are optional.

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


class PutIdentityDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Put identity definition configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class DeleteIdentityDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Delete identity definition configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class ValidateResourceDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Validate resource definition configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class GetResourceDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Get resource definition configuration override. All keys are optional.

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


class PutResourceDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Put resource definition configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class DeleteResourceDefConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Delete resource definition configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class ValidateGrantConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Validate grant configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class GetGrantConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Get grant configuration override. All keys are optional.

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


class EnactConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Enact grant configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class RepealConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Repeal grant configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class CleanupLatchesConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Cleanup latches configuration override. All keys are optional.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


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
        "get_context_def": {
            "use_cache": True
        },
        "use_list_context_defs": True,
        "list_context_defs": {
            "page_size": 100,
            "use_cache": True
        },
        "get_identity_def": {
            "use_cache": True
        },
        "use_list_identity_defs": True,
        "list_identity_defs": {
            "page_size": 100,
            "use_cache": True
        },
        "get_resource_def": {
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
    get_context_def : GetContextDefConfigOverride
        Config override for getting a context definition during validation.
    use_list_context_defs : bool
        Whether to use list context defs for validation.
    list_context_defs : ListContextDefsConfigOverride
        Config override for listing context definitions during validation.
    get_identity_def : GetIdentityDefConfigOverride
        Config override for getting an identity definition during validation.
    use_list_identity_defs : bool
        Whether to use list identity defs for validation.
    list_identity_defs : ListIdentityDefsConfigOverride
        Config override for listing identity definitions during validation.
    get_resource_def : GetResourceDefConfigOverride
        Config override for getting a resource definition during validation.
    use_list_resource_defs : bool
        Whether to use list resource defs for validation.
    list_resource_defs : ListResourceDefsConfigOverride
        Config override for listing resource definitions during validation.
    """
    get_context_def: GetContextDefConfigOverride
    use_list_context_defs: bool
    list_context_defs: ListContextDefsConfigOverride
    get_identity_def: GetIdentityDefConfigOverride
    use_list_identity_defs: bool
    list_identity_defs: ListIdentityDefsConfigOverride
    get_resource_def: GetResourceDefConfigOverride
    use_list_resource_defs: bool
    list_resource_defs: ListResourceDefsConfigOverride


class ValidateBatchRequestConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Validate batch request configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "get_context_def": {
            "use_cache": True
        },
        "use_list_context_defs": True,
        "list_context_defs": {
            "page_size": 100,
            "use_cache": True
        },
        "get_identity_def": {
            "use_cache": True
        },
        "use_list_identity_defs": True,
        "list_identity_defs": {
            "page_size": 100,
            "use_cache": True
        },
        "get_resource_def": {
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
    get_context_def : GetContextDefConfigOverride
        Config override for getting a context definition during validation.
    use_list_context_defs : bool
        Whether to use list context defs for validation.
    list_context_defs : ListContextDefsConfigOverride
        Config override for listing context definitions during validation.
    get_identity_def : GetIdentityDefConfigOverride
        Config override for getting an identity definition during validation.
    use_list_identity_defs : bool
        Whether to use list identity defs for validation.
    list_identity_defs : ListIdentityDefsConfigOverride
        Config override for listing identity definitions during validation.
    get_resource_def : GetResourceDefConfigOverride
        Config override for getting a resource definition during validation.
    use_list_resource_defs : bool
        Whether to use list resource defs for validation.
    list_resource_defs : ListResourceDefsConfigOverride
        Config override for listing resource definitions during validation.
    """
    get_context_def: GetContextDefConfigOverride
    use_list_context_defs: bool
    list_context_defs: ListContextDefsConfigOverride
    get_identity_def: GetIdentityDefConfigOverride
    use_list_identity_defs: bool
    list_identity_defs: ListIdentityDefsConfigOverride
    get_resource_def: GetResourceDefConfigOverride
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
        "validate_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": True
        }
    }
    ```

    Attributes
    ----------
    validate_request : ValidateRequestConfigOverride
        Config override for validating requests during audit.
    list_grants : ListGrantsConfigOverride
        Config override for listing grants during audit.
    """
    validate_request: ValidateRequestConfigOverride
    list_grants: ListGrantsConfigOverride


class BatchAuditConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Batch audit configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "validate_batch_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": True
        }
    }
    ```

    Attributes
    ----------
    validate_batch_request : ValidateBatchRequestConfigOverride
        Config override for validating batch requests during audit.
    list_grants : ListGrantsConfigOverride
        Config override for listing grants during audit.
    """
    validate_batch_request: ValidateBatchRequestConfigOverride
    list_grants: ListGrantsConfigOverride


class AuthorizeConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Authorize configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "validate_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": True
        },
        "parallel_paging": True,
        "list_grant_refs": {
            "page_size": 10,
            "use_cache": True
        }
    }
    ```

    Attributes
    ----------
    validate_request : ValidateRequestConfigOverride
        Config override for validating requests during authorization.
    list_grants : ListGrantsConfigOverride
        Config override for listing grants during authorization.
    parallel_paging : bool
        Whether to use parallel paging.
    list_grant_refs : ListGrantRefsConfigOverride
        Config override for listing grant references during authorization.
    """
    validate_request: ValidateRequestConfigOverride
    list_grants: ListGrantsConfigOverride
    parallel_paging: bool
    list_grant_refs: ListGrantRefsConfigOverride


class BatchAuthorizeConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```
    Batch authorize configuration override. All keys are optional.

    Examples
    --------
    ```python
    {
        "validate_batch_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "validate_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "list_grants": {
            "page_size": 100,
            "use_cache": True
        },
        "parallel_paging": True,
        "list_grant_refs": {
            "page_size": 10,
            "use_cache": True
        }
    }
    ```

    Attributes
    ----------
    validate_batch_request : ValidateBatchRequestConfigOverride
        Config override for validating batch requests during authorization.
    validate_request : ValidateRequestConfigOverride
        Config override for validating requests during authorization.
    list_grants : ListGrantsConfigOverride
        Config override for listing grants during authorization.
    parallel_paging : bool
        Whether to use parallel paging.
    list_grant_refs : ListGrantRefsConfigOverride
        Config override for listing grant references during authorization.
    """
    validate_batch_request: ValidateBatchRequestConfigOverride
    validate_request: ValidateRequestConfigOverride
    list_grants: ListGrantsConfigOverride
    parallel_paging: bool
    list_grant_refs: ListGrantRefsConfigOverride


class AuthzeeConfigOverride(TypedDict, total=False):
    """```python
    Dict[str, Dict[str, Any]]
    ```
    Authzee configuration override Type. All keys and nested keys are optional.

    The configuration can be set at several different levels where only the provided values override the previous levels values.

    The order of least to most precedence is:
    - Default config values - None Set
    - Authzee class instances config
    - Function/Method call config

    Examples
    --------
    **All base and nested fields are optional for this Dict.**
    Example with all defaults:
    ```python
    {
        "authzee": {
            "raise_errors": True
        },
        "start": {
            "compute_start": {
                "storage": {}
            },
            "storage_start": {}
        },
        "shutdown": {
            "compute_shutdown": {
                "storage": {}
            },
            "storage_shutdown": {}
        },
        "construct": {
            "compute_construct": {},
            "storage_construct": {}
        },
        "destroy": {
            "compute_destroy": {},
            "storage_destroy": {}
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
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "validate_batch_request": {
            "get_context_def": {
                "use_cache": True
            },
            "use_list_context_defs": True,
            "list_context_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_identity_def": {
                "use_cache": True
            },
            "use_list_identity_defs": True,
            "list_identity_defs": {
                "page_size": 100,
                "use_cache": True
            },
            "get_resource_def": {
                "use_cache": True
            },
            "use_list_resource_defs": True,
            "list_resource_defs": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "audit": {
            "validate_request": {
                "get_context_def": {
                    "use_cache": True
                },
                "use_list_context_defs": True,
                "list_context_defs": {
                    "page_size": 100,
                    "use_cache": True
                },
                "get_identity_def": {
                    "use_cache": True
                },
                "use_list_identity_defs": True,
                "list_identity_defs": {
                    "page_size": 100,
                    "use_cache": True
                },
                "get_resource_def": {
                    "use_cache": True
                },
                "use_list_resource_defs": True,
                "list_resource_defs": {
                    "page_size": 100,
                    "use_cache": True
                }
            },
            "list_grants": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "batch_audit": {
            "validate_batch_request": {
                "get_context_def": {
                    "use_cache": True
                },
                "use_list_context_defs": True,
                "list_context_defs": {
                    "page_size": 100,
                    "use_cache": True
                },
                "get_identity_def": {
                    "use_cache": True
                },
                "use_list_identity_defs": True,
                "list_identity_defs": {
                    "page_size": 100,
                    "use_cache": True
                },
                "get_resource_def": {
                    "use_cache": True
                },
                "use_list_resource_defs": True,
                "list_resource_defs": {
                    "page_size": 100,
                    "use_cache": True
                }
            },
            "list_grants": {
                "page_size": 100,
                "use_cache": True
            }
        },
        "authorize": {
            "validate_request": {
                "get_context_def": {
                    "use_cache": True
                },
                "use_list_context_defs": True,
                "list_context_defs": {
                    "page_size": 100,
                    "use_cache": True
                },
                "get_identity_def": {
                    "use_cache": True
                },
                "use_list_identity_defs": True,
                "list_identity_defs": {
                    "page_size": 100,
                    "use_cache": True
                },
                "get_resource_def": {
                    "use_cache": True
                },
                "use_list_resource_defs": True,
                "list_resource_defs": {
                    "page_size": 100,
                    "use_cache": True
                }
            },
            "list_grants": {
                "page_size": 100,
                "use_cache": True
            },
            "parallel_paging": True,
            "list_grant_refs": {
                "page_size": 10,
                "use_cache": True
            }
        },
        "batch_authorize": {
            "validate_batch_request": {
                "get_context_def": {
                    "use_cache": True
                },
                "use_list_context_defs": True,
                "list_context_defs": {
                    "page_size": 100,
                    "use_cache": True
                },
                "get_identity_def": {
                    "use_cache": True
                },
                "use_list_identity_defs": True,
                "list_identity_defs": {
                    "page_size": 100,
                    "use_cache": True
                },
                "get_resource_def": {
                    "use_cache": True
                },
                "use_list_resource_defs": True,
                "list_resource_defs": {
                    "page_size": 100,
                    "use_cache": True
                }
            },
            "validate_request": {
                "get_context_def": {
                    "use_cache": True
                },
                "use_list_context_defs": True,
                "list_context_defs": {
                    "page_size": 100,
                    "use_cache": True
                },
                "get_identity_def": {
                    "use_cache": True
                },
                "use_list_identity_defs": True,
                "list_identity_defs": {
                    "page_size": 100,
                    "use_cache": True
                },
                "get_resource_def": {
                    "use_cache": True
                },
                "use_list_resource_defs": True,
                "list_resource_defs": {
                    "page_size": 100,
                    "use_cache": True
                }
            },
            "list_grants": {
                "page_size": 100,
                "use_cache": True
            },
            "parallel_paging": True,
            "list_grant_refs": {
                "page_size": 10,
                "use_cache": True
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
    validate_context_def : ValidateContextDefConfigOverride
        Config override for validating context definitions.
    list_context_defs : ListContextDefsConfigOverride
        Config override for listing context definitions.
    get_context_def : GetContextDefConfigOverride
        Config override for getting a context definition.
    put_context_def : PutContextDefConfigOverride
        Config override for putting a context definition.
    delete_context_def : DeleteContextDefConfigOverride
        Config override for deleting a context definition.
    validate_identity_def : ValidateIdentityDefConfigOverride
        Config override for validating identity definitions.
    list_identity_defs : ListIdentityDefsConfigOverride
        Config override for listing identity definitions.
    get_identity_def : GetIdentityDefConfigOverride
        Config override for getting an identity definition.
    put_identity_def : PutIdentityDefConfigOverride
        Config override for putting an identity definition.
    delete_identity_def : DeleteIdentityDefConfigOverride
        Config override for deleting an identity definition.
    validate_resource_def : ValidateResourceDefConfigOverride
        Config override for validating resource definitions.
    list_resource_defs : ListResourceDefsConfigOverride
        Config override for listing resource definitions.
    get_resource_def : GetResourceDefConfigOverride
        Config override for getting a resource definition.
    put_resource_def : PutResourceDefConfigOverride
        Config override for putting a resource definition.
    delete_resource_def : DeleteResourceDefConfigOverride
        Config override for deleting a resource definition.
    validate_grant : ValidateGrantConfigOverride
        Config override for validating grants.
    list_grants : ListGrantsConfigOverride
        Config override for listing grants.
    get_grant : GetGrantConfigOverride
        Config override for getting a grant.
    enact : EnactConfigOverride
        Config override for enacting a grant.
    repeal : RepealConfigOverride
        Config override for repealing a grant.
    list_grant_refs : ListGrantRefsConfigOverride
        Config override for listing grant page references.
    cleanup_latches : CleanupLatchesConfigOverride
        Config override for cleaning up latches.
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
    validate_context_def: ValidateContextDefConfigOverride
    list_context_defs: ListContextDefsConfigOverride
    get_context_def: GetContextDefConfigOverride
    put_context_def: PutContextDefConfigOverride
    delete_context_def: DeleteContextDefConfigOverride
    validate_identity_def: ValidateIdentityDefConfigOverride
    list_identity_defs: ListIdentityDefsConfigOverride
    get_identity_def: GetIdentityDefConfigOverride
    put_identity_def: PutIdentityDefConfigOverride
    delete_identity_def: DeleteIdentityDefConfigOverride
    validate_resource_def: ValidateResourceDefConfigOverride
    list_resource_defs: ListResourceDefsConfigOverride
    get_resource_def: GetResourceDefConfigOverride
    put_resource_def: PutResourceDefConfigOverride
    delete_resource_def: DeleteResourceDefConfigOverride
    validate_grant: ValidateGrantConfigOverride
    list_grants: ListGrantsConfigOverride
    get_grant: GetGrantConfigOverride
    enact: EnactConfigOverride
    repeal: RepealConfigOverride
    list_grant_refs: ListGrantRefsConfigOverride
    cleanup_latches: CleanupLatchesConfigOverride
    validate_request: ValidateRequestConfigOverride
    validate_batch_request: ValidateBatchRequestConfigOverride
    audit: AuditConfigOverride
    batch_audit: BatchAuditConfigOverride
    authorize: AuthorizeConfigOverride
    batch_authorize: BatchAuthorizeConfigOverride
