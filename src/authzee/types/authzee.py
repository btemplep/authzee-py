"""Authzee core types."""

__all__ = [
    "AnyJSON",
    "AuditResultItem",
    "AuditResultPage",
    "AuthorizeResult",
    "AuthzeeBatchRequest",
    "AuthzeeError",
    "AuthzeeRequest",
    "BatchAuditResultItem",
    "BatchAuditResultPage",
    "BatchAuthorizeResult",
    "BatchItem",
    "ContextDef",
    "ContextDefResult",
    "ContextDefsPage",
    "EvaluateResult",
    "ExecuteResult",
    "GenericResult",
    "Grant",
    "GrantResult",
    "GrantsPage",
    "IdentityDef",
    "IdentityDefResult",
    "IdentityDefsPage",
    "PageRefsPage",
    "ResourceDef",
    "ResourceDefResult",
    "ResourceDefsPage",
    "StorageLatch",
    "StorageLatchResult",
    "ValidateBatchRequestResult"
]

from typing import Any, Dict, List, Literal, TypedDict


AnyJSON = (
    bool
    | str
    | int
    | float
    | None
    | list
    | dict
)


class AuthzeeError(TypedDict):
    """```python
    Dict[str, Any]
    ```
    Error from an Authzee operation.

    Examples
    --------
    ```python
    {
        "error_type": "evaluation",
        "message": "A JSON Query error has occurred."
    }
    ```
    """
    error_type: str
    message: str


class GenericResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    error: AuthzeeError | None


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
            "type": "object",
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
    schema: Dict[str, AnyJSON]


class ContextDefResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "context_def": { # Or None
            "context_type": "MyContext",
            "schema": {
                "type": "object",
                "properties": {
                    "my_prop": {
                        "type": "string"
                    }
                }
            }
        },
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    context_def: ContextDef | None
    error: AuthzeeError | None


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
                    "type": "object",
                    "properties": {
                        "my_prop": {
                            "type": "string"
                        }
                    }
                }
            }
        ],
        "next_page_ref": "abc123",
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    context_defs: List[ContextDef]
    next_page_ref: str | None
    error: AuthzeeError | None


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
            "type": "object",
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
        "identity_def": {
            "identity_type": "MyIdentity",
            "schema": {
                "type": "object",
                "properties": {
                    "my_prop": {
                        "type": "string"
                    }
                }
            }
        },
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    identity_def: IdentityDef | None
    error: AuthzeeError | None


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
                    "type": "object",
                    "properties": {
                        "my_prop": {
                            "type": "string"
                        }
                    }
                }
            }
        ],
        "next_page_ref": "abc123",
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    identity_defs: List[IdentityDef]
    next_page_ref: str | None
    error: AuthzeeError | None


class ResourceDef(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "resource_type": "Balloon",
        "actions": [
            "balloon:inflate"
        ],
        "schema": {
            "type": "object",
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
        "resource_def": {
            "resource_type": "Balloon",
            "actions": [
                "balloon:inflate"
            ],
            "schema": {
                "type": "object",
                "properties": {
                    "my_prop": {
                        "type": "string"
                    }
                }
            }
        },
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    resource_def: ResourceDef | None
    error: AuthzeeError | None


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
                "resource_type": "Balloon",
                "actions": [
                    "balloon:inflate"
                ],
                "schema": {
                    "type": "object",
                    "properties": {
                        "my_prop": {
                            "type": "string"
                        }
                    }
                }
            }
        ],
        "next_page_ref": "abc123",
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    resource_defs: List[ResourceDef]
    next_page_ref: str | None
    error: AuthzeeError | None


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
            "my_tag_key": "my tag value"
        },
        "effect": "allow",
        "actions": [
            "balloon:inflate"
        ],
        "query": "contains(request.identities, 'User')",
        "equality": True,
        "applicable_on_failure": False,
        "data": {
            "str_here": "anything else here"
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
    equality: AnyJSON
    applicable_on_failure: bool
    data: Dict[str, Any]


class GrantResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "grant": {
            "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            "name": "People friendly name",
            "description": "Long description",
            "tags": {
                "my_tag_key": "my tag value"
            },
            "effect": "allow",
            "actions": [
                "balloon:inflate"
            ],
            "query": "contains(request.identities, 'User')",
            "equality": True,
            "data": {}
        },
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    grant: Grant | None
    error: AuthzeeError | None


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
                    "my_tag_key": "my tag value"
                },
                "effect": "allow",
                "actions": [
                    "balloon:inflate"
                ],
                "query": "contains(request.identities, 'User')",
                "equality": True,
                "data": {}
            }
        ],
        "next_page_ref": "abc123",
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    grants: List[Grant]
    next_page_ref: str | None
    error: AuthzeeError | None


class PageRefsPage(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "page_refs": [
            "abc123"
        ],
        "next_page_ref": "abc123",
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    page_refs: List[str]
    next_page_ref: str | None
    error: AuthzeeError | None


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
        "created_at": "2026-04-26T16:21:10.521220"
    }
    ```
    """
    storage_latch_uuid: str
    is_set: bool
    created_at: str


class StorageLatchResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "storage_latch": {
            "storage_latch_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            "is_set": False,
            "created_at": "2026-04-26T16:21:10.521220"
        },
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    storage_latch: StorageLatch | None
    error: AuthzeeError | None


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
        "context_type": "MyContext",
        "context": {
            "allowed_sizes": [20.0, 27.0]
        }
    }
    ```
    """
    identities: Dict[
        str,
        List[Dict[str, AnyJSON]]
    ]
    action: str
    resource_type: str
    resource: Dict[str, AnyJSON]
    context_type: str
    context: Dict[str, AnyJSON]


class BatchItem(TypedDict, total=False):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    **All fields are optional.**
    ```python
    {
        "identities": {
            "ADUser": [
                {"cn": "authzee_user_1"}
            ]
        },
        "resource_type": "Balloon",
        "resource": {
            "color": "blue",
            "size": 27.0
        },
        "context_type": "MyContext",
        "context": {
            "allowed_sizes": [20.0, 27.0]
        }
    }
    ```
    """
    identities: Dict[
        str,
        List[Dict[str, AnyJSON]]
    ] | None
    resource_type: str | None
    resource: Dict[str, AnyJSON] | None
    context_type: str | None
    context: Dict[str, AnyJSON] | None


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
    identities: Dict[
        str,
        List[Dict[str, AnyJSON]]
    ]
    action: str
    resource_type: str
    resource: Dict[str, AnyJSON]
    context_type: str
    context: Dict[str, AnyJSON]
    batch: List[BatchItem]


class ValidateBatchRequestResult(TypedDict):
    """Result for validating a batch request.

    Examples
    --------
    ```python
    {
        "error": { # OR None
            "error_type": "request",
            "message": "This is a batch level error, and the whole think fails,
        },
        "batch": [
            None,
            { # OR None
                "error_type": "request",
                "message": "This is an error for the batch item."
            }
        ]
    }
    """
    error: AuthzeeError | None
    batch: List[GenericResult]


class ExecuteResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "result": True,
        "failure": "A JMESPath Query error has occurred: ..." # or None
    }
    ```
    """
    result: Any
    failure: str | None


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
        "failure": "A JSON Query error has occurred: ..." # or None
    }
    ```
    """
    is_applicable: bool
    query_result: Any
    failure: str | None


class AuditResultItem(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "grant": {
            "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            "name": "People friendly name",
            "description": "Long description",
            "tags": {},
            "effect": "allow",
            "actions": ["balloon:inflate"],
            "query": "contains(request.identities, 'User')",
            "equality": True,
            "data": {}
        },
        "is_applicable": True,
        "query_result": True,
        "failure": "A JSON Query error has occurred: ..." # Or None
    }
    ```
    """
    grant: Grant
    is_applicable: bool
    query_result: AnyJSON
    failure: str | None


class AuditResultPage(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "results": [
            {
                "grant": {
                    "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                    "name": "People friendly name",
                    "description": "Long description",
                    "tags": {},
                    "effect": "allow",
                    "actions": ["balloon:inflate"],
                    "query": "contains(request.identities, 'User')",
                    "equality": True,
                    "data": {}
                },
                "is_applicable": True,
                "query_result": True,
                "failure": "A JSON Query error has occurred: ..." # Or None
            }
        ],
        "next_page_ref": "abc123",
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    results: List[AuditResultItem]
    next_page_ref: str | None
    error: AuthzeeError | None


class AuthorizeResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "is_authorized": True,
        "grant": {
            "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            "name": "People friendly name",
            "description": "Long description",
            "tags": {
                "my tag key": "my tag value"
            },
            "effect": "allow",
            "actions": ["balloon:inflate"],
            "query": "contains(request.identities, 'User')",
            "equality": True,
            "data": {}
        },
        "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    is_authorized: bool
    grant: Grant | None
    message: str
    error: AuthzeeError | None


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
                "failure": "A JSON Query error has occurred: ..." # Or None
            }
        ],
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    results: List[EvaluateResult]
    error: AuthzeeError | None


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
                "tags": {},
                "effect": "allow",
                "actions": ["balloon:inflate"],
                "query": "contains(request.identities, 'User')",
                "equality": True,
                "data": {}
            }
        ],
        "batch": [
            {
                "results": [
                    {
                        "is_applicable": True,
                        "query_result": True,
                        "failure": "A JSON Query error has occurred: ..." # Or None
                    }
                ],
                "error": { # or None
                    "error_type": "<type>",
                    "message": "<description>"
                }
            }
        ],
        "next_page_ref": "abc123",
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    grants: List[Grant]
    batch: List[BatchAuditResultItem]
    next_page_ref: str | None
    error: AuthzeeError | None


class BatchAuthorizeResult(TypedDict):
    """```python
    Dict[str, Any]
    ```

    Examples
    --------
    ```python
    {
        "batch": [
            {
                "is_authorized": True,
                "grant": {
                    "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                    "name": "People friendly name",
                    "description": "Long description",
                    "tags": {},
                    "effect": "allow",
                    "actions": ["balloon:inflate"],
                    "query": "contains(request.identities, 'User')",
                    "equality": True,
                    "data": {}
                },
                "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
                "error": { # or None
                    "error_type": "<type>",
                    "message": "<description>"
                }
            }
        ],
        "error": { # or None
            "error_type": "<type>",
            "message": "<description>"
        }
    }
    ```
    """
    batch: List[AuthorizeResult]
    error: AuthzeeError | None
