"""Authzee config types."""

__all__ = [
    "AuditConfig",
    "AuthorizeConfig",
    "AuthzeeBaseConfig",
    "AuthzeeConfig",
    "BatchAuditConfig",
    "BatchAuthorizeConfig",
    "CleanupLatchesConfig",
    "ComputeConstructConfig",
    "ComputeDestroyConfig",
    "ComputeShutdownConfig",
    "ComputeStartConfig",
    "ConstructConfig",
    "CreateLatchConfig",
    "DeleteContextDefConfig",
    "DeleteIdentityDefConfig",
    "DeleteLatchConfig",
    "DeleteResourceDefConfig",
    "DestroyConfig",
    "EnactConfig",
    "GetContextDefConfig",
    "GetGrantConfig",
    "GetIdentityDefConfig",
    "GetLatchConfig",
    "GetResourceDefConfig",
    "ListContextDefsConfig",
    "ListGrantRefsConfig",
    "ListGrantsConfig",
    "ListIdentityDefsConfig",
    "ListResourceDefsConfig",
    "PutContextDefConfig",
    "PutIdentityDefConfig",
    "PutResourceDefConfig",
    "RepealConfig",
    "SetLatchConfig",
    "ShutdownConfig",
    "StartConfig",
    "StorageConstructConfig",
    "StorageDestroyConfig",
    "StorageShutdownConfig",
    "StorageStartConfig",
    "ValidateBatchRequestConfig",
    "ValidateContextDefConfig",
    "ValidateGrantConfig",
    "ValidateIdentityDefConfig",
    "ValidateRequestConfig",
    "ValidateResourceDefConfig"
]

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


class ComputeStartConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Compute start configuration.

    Examples
    --------
    ```python
    {
        "storage": {}
    }
    ```

    Attributes
    ----------
    storage : StorageStartConfig
        Storage start configuration.
    """
    storage: StorageStartConfig


class StartConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Start configuration.

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
    compute_start : ComputeStartConfig
        Compute start configuration.
    storage_start : StorageStartConfig
        Storage start configuration.
    """
    compute_start: ComputeStartConfig
    storage_start: StorageStartConfig


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


class ComputeShutdownConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Compute shutdown configuration.

    Examples
    --------
    ```python
    {
        "storage": {}
    }
    ```

    Attributes
    ----------
    storage : StorageShutdownConfig
        Storage shutdown configuration.
    """
    storage: StorageShutdownConfig


class ShutdownConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Shutdown configuration.

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


class ValidateContextDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Validate context definition configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class GetContextDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Get context definition configuration.

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


class PutContextDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Put context definition configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class DeleteContextDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Delete context definition configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class ValidateIdentityDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Validate identity definition configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class GetIdentityDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Get identity definition configuration.

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


class PutIdentityDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Put identity definition configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class DeleteIdentityDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Delete identity definition configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class ValidateResourceDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Validate resource definition configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class GetResourceDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Get resource definition configuration.

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


class PutResourceDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Put resource definition configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class DeleteResourceDefConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Delete resource definition configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class ValidateGrantConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Validate grant configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class GetGrantConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Get grant configuration.

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


class EnactConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Enact grant configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class RepealConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Repeal grant configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class CreateLatchConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Create latche configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class GetLatchConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Get Latch configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class SetLatchConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Set Latch configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class DeleteLatchConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Delete Latch configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


class CleanupLatchesConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Cleanup latches configuration.

    Examples
    --------
    ```python
    {}
    ```
    """
    pass


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
    get_context_def : GetContextDefConfig
        Config for getting a context definition during validation.
    use_list_context_defs : bool
        Whether to use list context defs for validation.
    list_context_defs : ListContextDefsConfig
        Config for listing context definitions during validation.
    get_identity_def : GetIdentityDefConfig
        Config for getting an identity definition during validation.
    use_list_identity_defs : bool
        Whether to use list identity defs for validation.
    list_identity_defs : ListIdentityDefsConfig
        Config for listing identity definitions during validation.
    get_resource_def : GetResourceDefConfig
        Config for getting a resource definition during validation.
    use_list_resource_defs : bool
        Whether to use list resource defs for validation.
    list_resource_defs : ListResourceDefsConfig
        Config for listing resource definitions during validation.
    """
    get_context_def: GetContextDefConfig
    use_list_context_defs: bool
    list_context_defs: ListContextDefsConfig
    get_identity_def: GetIdentityDefConfig
    use_list_identity_defs: bool
    list_identity_defs: ListIdentityDefsConfig
    get_resource_def: GetResourceDefConfig
    use_list_resource_defs: bool
    list_resource_defs: ListResourceDefsConfig


class ValidateBatchRequestConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Validate batch request configuration.

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
    get_context_def : GetContextDefConfig
        Config for getting a context definition during validation.
    use_list_context_defs : bool
        Whether to use list context defs for validation.
    list_context_defs : ListContextDefsConfig
        Config for listing context definitions during validation.
    get_identity_def : GetIdentityDefConfig
        Config for getting an identity definition during validation.
    use_list_identity_defs : bool
        Whether to use list identity defs for validation.
    list_identity_defs : ListIdentityDefsConfig
        Config for listing identity definitions during validation.
    get_resource_def : GetResourceDefConfig
        Config for getting a resource definition during validation.
    use_list_resource_defs : bool
        Whether to use list resource defs for validation.
    list_resource_defs : ListResourceDefsConfig
        Config for listing resource definitions during validation.
    """
    get_context_def: GetContextDefConfig
    use_list_context_defs: bool
    list_context_defs: ListContextDefsConfig
    get_identity_def: GetIdentityDefConfig
    use_list_identity_defs: bool
    list_identity_defs: ListIdentityDefsConfig
    get_resource_def: GetResourceDefConfig
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
    validate_request : ValidateRequestConfig
        Config for validating requests during audit.
    list_grants : ListGrantsConfig
        Config for listing grants during audit.
    """
    validate_request: ValidateRequestConfig
    list_grants: ListGrantsConfig


class BatchAuditConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Batch audit configuration.

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
    validate_batch_request : ValidateBatchRequestConfig
        Config for validating batch requests during audit.
    list_grants : ListGrantsConfig
        Config for listing grants during audit.
    """
    validate_batch_request: ValidateBatchRequestConfig
    list_grants: ListGrantsConfig


class AuthorizeConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Authorize configuration.

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
    validate_request : ValidateRequestConfig
        Config for validating requests during authorization.
    list_grants : ListGrantsConfig
        Config for listing grants during authorization.
    parallel_paging : bool
        Whether to use parallel paging.
    list_grant_refs : ListGrantRefsConfig
        Config for listing grant references during authorization.
    """
    validate_request: ValidateRequestConfig
    list_grants: ListGrantsConfig
    parallel_paging: bool
    list_grant_refs: ListGrantRefsConfig


class BatchAuthorizeConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Batch authorize configuration.

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
    validate_batch_request : ValidateBatchRequestConfig
        Config for validating batch requests during authorization.
    validate_request : ValidateRequestConfig
        Config for validating requests during authorization.
    list_grants : ListGrantsConfig
        Config for listing grants during authorization.
    parallel_paging : bool
        Whether to use parallel paging.
    list_grant_refs : ListGrantRefsConfig
        Config for listing grant references during authorization.
    """
    validate_batch_request: ValidateBatchRequestConfig
    validate_request: ValidateRequestConfig
    list_grants: ListGrantsConfig
    parallel_paging: bool
    list_grant_refs: ListGrantRefsConfig


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


    The root fields all represent the config that will be passed to the method in Authzee by name.
    The `authzee` root key is just for general Authzee instance level configuration.

    Examples
    --------
    **All base and nested fields are not required for this Dict.**
    Example with all defaults:
    ```python
    {
        "authzee": {
            "raise_crits": True
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
    authzee : AuthzeeBaseConfig
        Authzee class instance configuration.
    start : StartConfig
        Config for starting the authzee instance.
    shutdown : ShutdownConfig
        Config for shutting down the authzee instance.
    construct : ConstructConfig
        Config for constructing the authzee instance.
    destroy : DestroyConfig
        Config for destroying the authzee instance.
    validate_context_def : ValidateContextDefConfig
        Config for validating context definitions.
    list_context_defs : ListContextDefsConfig
        Config for listing context definitions.
    get_context_def : GetContextDefConfig
        Config for getting a context definition.
    put_context_def : PutContextDefConfig
        Config for putting a context definition.
    delete_context_def : DeleteContextDefConfig
        Config for deleting a context definition.
    validate_identity_def : ValidateIdentityDefConfig
        Config for validating identity definitions.
    list_identity_defs : ListIdentityDefsConfig
        Config for listing identity definitions.
    get_identity_def : GetIdentityDefConfig
        Config for getting an identity definition.
    put_identity_def : PutIdentityDefConfig
        Config for putting an identity definition.
    delete_identity_def : DeleteIdentityDefConfig
        Config for deleting an identity definition.
    validate_resource_def : ValidateResourceDefConfig
        Config for validating resource definitions.
    list_resource_defs : ListResourceDefsConfig
        Config for listing resource definitions.
    get_resource_def : GetResourceDefConfig
        Config for getting a resource definition.
    put_resource_def : PutResourceDefConfig
        Config for putting a resource definition.
    delete_resource_def : DeleteResourceDefConfig
        Config for deleting a resource definition.
    validate_grant : ValidateGrantConfig
        Config for validating grants.
    list_grants : ListGrantsConfig
        Config for listing grants.
    get_grant : GetGrantConfig
        Config for getting a grant.
    enact : EnactConfig
        Config for enacting a grant.
    repeal : RepealConfig
        Config for repealing a grant.
    list_grant_refs : ListGrantRefsConfig
        Config for listing grant page references.
    cleanup_latches : CleanupLatchesConfig
        Config for cleaning up latches.
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
    validate_context_def: ValidateContextDefConfig
    list_context_defs: ListContextDefsConfig
    get_context_def: GetContextDefConfig
    put_context_def: PutContextDefConfig
    delete_context_def: DeleteContextDefConfig
    validate_identity_def: ValidateIdentityDefConfig
    list_identity_defs: ListIdentityDefsConfig
    get_identity_def: GetIdentityDefConfig
    put_identity_def: PutIdentityDefConfig
    delete_identity_def: DeleteIdentityDefConfig
    validate_resource_def: ValidateResourceDefConfig
    list_resource_defs: ListResourceDefsConfig
    get_resource_def: GetResourceDefConfig
    put_resource_def: PutResourceDefConfig
    delete_resource_def: DeleteResourceDefConfig
    validate_grant: ValidateGrantConfig
    list_grants: ListGrantsConfig
    get_grant: GetGrantConfig
    enact: EnactConfig
    repeal: RepealConfig
    list_grant_refs: ListGrantRefsConfig
    cleanup_latches: CleanupLatchesConfig
    validate_request: ValidateRequestConfig
    validate_batch_request: ValidateBatchRequestConfig
    audit: AuditConfig
    batch_audit: BatchAuditConfig
    authorize: AuthorizeConfig
    batch_authorize: BatchAuthorizeConfig
