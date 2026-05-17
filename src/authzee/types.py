"""Authzee types."""

__all__ = [
    "AnyJSON",
    "AuthzeeConfig",
    "GenericError",
    "SDKError",
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


class AuthzeeConfig(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Authzee configuration Type

    The configuration can be set at several different levels where only the provided values override the previous levels values. 

    The order of least to most precedence is:
    - Default config values
    - Authzee class instances config
    - Authzee class instance compute config, for compute operations
    - Function/Method call config

    Examples
    --------
    **All base fields are not required.**
    Example with all defaults:
    ```python
    {
        "context_defs_page_size": 100,
        "identity_defs_page_size": 100,
        "resource_defs_page_size": 100,
        "grants_page_size": 100,
        "grant_refs_page_size": 10,
        "authorize_parallel_paging": True,
        "batch_authorize_parallel_paging": True,
        "raise_crits": True
    }
    ```

    Attributes
    ----------
    context_defs_page_size : int, default: 100
        Maximum number of context definitions to return per page. 
    identity_defs_page_size : int, default: 100
        Maximum number of identity definitions to return per page.
    resource_defs_page_size : int, default: 100
        Maximum number of resource definitions to return per page.
    grants_page_size : int, default: 100
        Maximum number of grants to return per page.
    grant_refs_page_size : int, default: 10
        Number of grants page references to return per page.
    authorize_parallel_paging: bool, default: True
        Use parallel pagination for the authorize operation if it is available.
    batch_authorize_parallel_paging: bool default: False
        Use parallel pagination for the batch_authorize operation if it is available.
    raise_crits: bool, default: True
        Raise critical errors as exceptions.
    """
    context_defs_page_size: NotRequired[int]
    identity_defs_page_size: NotRequired[int]
    resource_defs_page_size: NotRequired[int]
    grants_page_size: NotRequired[int]
    grant_refs_page_size: NotRequired[int]
    authorize_parallel_paging: NotRequired[bool]
    batch_authorize_parallel_paging: NotRequired[bool]
    raise_crits: NotRequired[bool]


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


class SDKError(TypedDict):
    """```python
    Dict[str, Any]
    ```

    SDK Error Type

    Examples
    --------
    ```python
    {
        "error_type": "sdk_error_type_string",
        "is_critical": False,
        "message": "Error message here"
    }
    ```
    """
    error_type: str
    is_critical: bool
    message: str


class ResultErrors(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    **All base fields are not required.**
    ```python
    {
        "definition": [ # not required
            {
                "is_critical": False,
                "message": "Error message."
            }
        ],
        "grant": [ # not required
            {
                "is_critical": False,
                "message": "Error message."
            }
        ],
        "request": [ # not required
            {
                "is_critical": False,
                "message": "Error message."
            }
        ],
        "evaluation": [ # not required
            {
                "is_critical": False,
                "message": "Error message."
            }
        ],
        "sdk": [ # not required
            {
                "error_type": "sdk_error_type_string",
                "is_critical": False,
                "message": "Error message."
            }
        ]
    }
    ```
    """
    definition: NotRequired[List[GenericError]]
    grant: NotRequired[List[GenericError]]
    request: NotRequired[List[GenericError]]
    evaluation: NotRequired[List[GenericError]]
    sdk: NotRequired[List[SDKError]]


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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "definition": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "grant": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "request": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "evaluation": [ # not required
                {
                    "is_critical": False,
                    "message": "Error message."
                }
            ],
            "sdk": [ # not required
                {
                    "error_type": "sdk_error_type_string",
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
            "evaluation": [ # not required
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
            "evaluation": [ # not required
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
                "errors": {}
            }
        ],
        "next_page_ref": "abc123", # str | None
        "has_failed": False,
        "errors": {
            "evaluation": [ # not required
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
            "evaluation": [ # not required
                {
                    "is_critical": True,
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
                "errors": {}
            },
            {
                "is_applicable": False,
                "query_result": False,
                "errors": {}
            }
        ],
        "has_failed": False,
        "errors": {
            "evaluation": [ # not required
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
                        "errors": {}
                    }
                ],
                "has_failed": False,
                "errors": {}
            }
        ],
        "next_page_ref": "abc123", # str | None
        "has_failed": False,
        "errors": {
            "evaluation": [ # not required
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
                "critical_errors": {}
            },
            {
                "is_authorized": False,
                "grant": None,
                "message": "No matching allow grants.",
                "has_failed": False,
                "critical_errors": {}
            }
        ],
        "has_failed": False,
        "critical_errors": {
            "evaluation": [ # not required
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

