"""Module for Authzee Exceptions
"""

__all__ = [
    "AuthzeeError",
    "AuthzeeSpecError",
    "DefinitionError",
    "GrantError",
    "AuthzeeOperationError",
    "EvaluationError",
    "RequestError",
    "AuthzeeSDKError",
    "LocalityIncompatibilityError",
    "GrantNotFoundError",
    "LatchNotFoundError",
    "StartError",
    "NotImplementedError",
    "ParallelPaginationNotSupported",
    "PageReferenceError"
]

from authzee.dcs import (
    GenericResult, 
    AuditResultPage, 
    AuthorizeResult, 
    BatchAuditResultPage, 
    BatchAuthorizeResult
)


class AuthzeeError(Exception):
    """Base Authzee Exception.
    """
    pass


class AuthzeeSpecError(AuthzeeError):
    """Base exception for errors defined in the Authzee Specification.
    """
    
    def __init__(
        self, 
        message: str, 
        is_critical: bool,
        result: GenericResult
    ):
        super().__init__(message)
        self.is_critical = is_critical
        self.result = result
      

class DefinitionError(AuthzeeSpecError):
    """Error when validating the identity and resource definitions."""
    pass


class GrantError(AuthzeeSpecError):
    """Error when validating grants."""
    pass


class AuthzeeOperationError(AuthzeeSpecError):
    """Errors specific to running Authzee operations. """

    def __init__(
        self, 
        message: str, 
        is_critical: bool,
        result: AuditResultPage | AuthorizeResult | BatchAuditResultPage | BatchAuthorizeResult
    ):
        super().__init__(message, is_critical, result)


class EvaluationError(AuthzeeOperationError):
    """Error when running an evaluation for a request."""
    pass


class RequestError(AuthzeeOperationError):
    """Error when validating a request or batch request."""
    pass


class AuthzeeSDKError(AuthzeeError):
    """Base exception for errors from the Authzee SDK that are **not** defined by the specification.
    """
    
    def __init__(
        self, 
        message: str, 
        is_critical: bool,
        result: GenericResult
    ):
        super().__init__(message)
        self.is_critical = is_critical
        self.result = result


class LocalityIncompatibilityError(AuthzeeSDKError):
    """The localities are not compatible.

    See `authzee.module_locality.ModuleLocality` for more info.
    """
    pass


class GrantNotFoundError(AuthzeeSDKError):
    """The Grant with a specific UUID was not found in the storage backend.
    """
    pass


class LatchNotFoundError(AuthzeeSDKError):
    """The storage latch with a specific UUID was not found in the storage backend.
    """
    pass


class StartError(AuthzeeSDKError):
    """There was an error during initialization of the Authzee App and modules.
    """
    pass


class NotImplementedError(AuthzeeSDKError):
    """The given method is not implemented for this class.
    """

    def __init__(self, msg: str = "This method is not implemented.", *args, **kwargs):
        super().__init__(msg, *args, **kwargs)


class ParallelPaginationNotSupported(AuthzeeSDKError):
    """Parallel pagination is not supported.
    """
    pass


class PageReferenceError(AuthzeeSDKError):
    """Error when processing a page reference.
    """
    pass
