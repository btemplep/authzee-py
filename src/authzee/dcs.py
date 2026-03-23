_all__ = [
    "AuthzeeConfig",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",

]
from dataclasses import dataclass
from typing import Any, Dict, List, Literal
from uuid import UUID


AnyJSON = bool | str | int | float | None | list | dict


@dataclass(kw_only=True)
class AuthzeeConfig:
    """Authzee configuration class. 

    Pass at the class instance level to set defaults.
    Pass at the method level to override instance defaults.

    Attributes
    ----------
    defs_page_size : int, default: 100
        Maximum number of definitions (context, identity, resource) to return per page. 
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
    definitions_page_size: int = 100
    grants_page_size: int = 100
    grant_refs_page_size: int = 10
    authorize_parallel_paging: bool = True
    batch_authorize_parallel_paging: bool = True
    raise_crits: bool = True


@dataclass(kw_only=True)
class GenericResult:
    has_failed: bool
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class ContextDef:
    context_type: str
    schema: str


@dataclass(kw_only=True)
class ContextDefResult:
    context_def: ContextDef
    has_failed: bool
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class ContextDefPage:
    context_defs: List[ContextDef]
    next_page_ref: str
    has_failed: bool
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class IdentityDef:
    identity_type: str
    schema: dict
    new_thing: str | None = None


@dataclass(kw_only=True)
class IdentityDefResult:
    identity_def: IdentityDef
    has_failed: bool
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class IdentityDefPage:
    identity_defs: List[IdentityDef]
    next_page_ref: str
    has_failed: bool
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class ResourceDef:
    resource_type: str
    actions: List[str]
    schema: dict


@dataclass(kw_only=True)
class ResourceDefResult:
    resource_def: ResourceDef
    has_failed: bool
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class ResourceDefPage:
    resource_defs: List[ResourceDef]
    next_page_ref: str
    has_failed: bool
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class Grant:
    grant_uuid: UUID
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
    equality: bool
    data: Dict[str, Any]



@dataclass(kw_only=True)
class GrantsPage:
    grants: List[Grant]
    next_page_ref: str
    has_failed: bool
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class AuthzeeRequest:
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


@dataclass(kw_only=True)
class BatchItem:
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


@dataclass(kw_only=True)
class AuthzeeBatchRequest:
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


@dataclass(kw_only=True)
class AuditResultItem:
    is_applicable: bool
    query_result: AnyJSON
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class AuditResult:
    grants: List[Grant]
    results: List[AuditResultItem]
    has_failed: bool
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class AuthorizeResult:
    is_authorized: bool
    grant: Grant | None
    message: str
    has_failed: bool
    errors: Dict[str, List[Dict[str, Any]]]


@dataclass(kw_only=True)
class BatchAuditResult:
    pass


@dataclass(kw_only=True)
class BatchAuthorizeResult:
    pass


