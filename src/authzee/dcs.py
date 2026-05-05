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
import datetime
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Literal
from uuid import UUID, uuid4


AnyJSON = bool | str | int | float | None | list | dict


def _asjsondict_inner(d: Any) -> dict:
    if type(d) is datetime.datetime:
        return d.isoformat()
    elif type(d) is UUID:
        return str(d)
    elif type(d) is dict:
        new_d = {}
        for k in d:
            new_d[k] = _asjsondict_inner(d[k])
        
        return new_d
    elif type(d) is list:
        return [_asjsondict_inner(i) for i in d]
    else:
        return d



def asjsondict(dc: object) -> dict:
    return _asjsondict_inner(asdict(dc))


@dataclass(kw_only=True)
class AuthzeeConfig:
    """Authzee configuration class. 

    Pass at the class instance level to set defaults.
    Pass at the method level to override instance defaults.

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
    context_defs_page_size: int = 100
    identity_defs_page_size: int = 100
    resource_defs_page_size: int = 100
    grants_page_size: int = 100
    grant_refs_page_size: int = 10
    authorize_parallel_paging: bool = True
    batch_authorize_parallel_paging: bool = True
    raise_crits: bool = True


@dataclass(kw_only=True)
class GenericError:
    is_critical: bool
    message: str


@dataclass(kw_only=True)
class SDKError:
    error_type: str
    is_critical: bool
    message: str


@dataclass(kw_only=True)
class ResultErrors:
    definition: List[GenericError] = field(default_factory=list)
    grant: List[GenericError] = field(default_factory=list)
    request: List[GenericError] = field(default_factory=list)
    evaluation: List[GenericError] = field(default_factory=list)
    sdk: List[SDKError] = field(default_factory=list)


@dataclass(kw_only=True)
class GenericResult:
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class ContextDef:
    context_type: str
    schema: dict


@dataclass(kw_only=True)
class ContextDefResult:
    context_def: ContextDef | None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class ContextDefsPage:
    context_defs: List[ContextDef]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class IdentityDef:
    identity_type: str
    schema: dict


@dataclass(kw_only=True)
class IdentityDefResult:
    identity_def: IdentityDef| None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class IdentityDefsPage:
    identity_defs: List[IdentityDef]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class ResourceDef:
    resource_type: str
    actions: List[str]
    schema: dict


@dataclass(kw_only=True)
class ResourceDefResult:
    resource_def: ResourceDef| None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class ResourceDefsPage:
    resource_defs: List[ResourceDef]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


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
class GrantResult:
    grant: Grant | None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class GrantsPage:
    grants: List[Grant]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class PageRefsPage:
    page_refs: List[str]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


@dataclass(kw_only=True)
class StorageLatch:
    storage_latch_uuid: UUID = field(default_factory=uuid4)
    is_set: bool = False
    created_at: datetime.datetime = field(default_factory=utc_now)


@dataclass(kw_only=True)
class StorageLatchResult:
    storage_latch: StorageLatch | None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


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
class ExecuteResult:
    result: Any
    has_failed: bool
    error_message: str | None


@dataclass(kw_only=True)
class EvaluateResult:
    is_applicable: bool
    query_result: Any
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class AuditResultItem:
    is_applicable: bool
    query_result: AnyJSON
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class AuditResultPage:
    grants: List[Grant]
    results: List[AuditResultItem]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class AuthorizeResult:
    is_authorized: bool
    grant: Grant | None
    message: str
    has_failed: bool
    critical_errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class BatchAuditResultItem:
    results: List[AuditResultItem]
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class BatchAuditResultPage:
    grants: List[Grant]
    batch_results: List[BatchAuditResultItem]
    next_page_ref: str | None
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)


@dataclass(kw_only=True)
class BatchAuthorizeResult:
    batch_results: List[AuthorizeResult]
    has_failed: bool
    errors: ResultErrors = field(default_factory=ResultErrors)

