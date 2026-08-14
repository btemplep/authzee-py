"""Module for Authzee Exceptions.
"""

__all__ = [
    "AuthzeeError",
    "AuthzeeSDKError",
    "AuthzeeSpecError",
    "DefinitionError",
    "EvaluationError",
    "GrantError",
    "LocalityIncompatibilityError",
    "NotImplementedError",
    "PageReferenceError",
    "ParallelPaginationNotSupported",
    "RequestError",
    "ResourceNotFoundError",
    "StartError"
]

from authzee.types import GenericResult


class AuthzeeError(Exception):
    """Base Authzee Exception.
    """
    pass


class AuthzeeSpecError(AuthzeeError):
    """Base exception for errors defined in the Authzee Specification.
    """


    def __init__(self, message: str, result: GenericResult):
        super().__init__(message)
        self.message = message
        self.result = result


class DefinitionError(AuthzeeSpecError):
    """Error when validating the identity and resource definitions."""
    pass


class EvaluationError(AuthzeeSpecError):
    """Error when running an evaluation for a request."""
    pass


class GrantError(AuthzeeSpecError):
    """Error when validating grants."""
    pass


class RequestError(AuthzeeSpecError):
    """Error when validating a request or batch request."""
    pass


class AuthzeeSDKError(AuthzeeError):
    """Base exception for errors from the Authzee SDK that are **not** defined by the specification.
    """


    def __init__(self, message: str, result: GenericResult):
        super().__init__(message)
        self.message = message
        self.result = result


class LocalityIncompatibilityError(AuthzeeSDKError):
    """The localities are not compatible.

    See `authzee.module_locality.ModuleLocality` for more info.
    """
    pass


class NotImplementedError(AuthzeeSDKError):
    """The given method is not implemented for this class.
    """


    def __init__(
        self,
        msg: str="This method is not implemented.",
        *args,
        **kwargs
    ):
        super().__init__(msg, *args, **kwargs)


class ParallelPaginationNotSupported(AuthzeeSDKError):
    """Parallel pagination is not supported.
    """
    pass


class PageReferenceError(AuthzeeSDKError):
    """Error when processing a page reference.
    """
    pass


class ResourceNotFoundError(AuthzeeSDKError):
    """The resource with a specific UUID or type was not found in the storage backend.
    """
    pass


class StartError(AuthzeeSDKError):
    """There was an error during initialization of the Authzee App and modules.
    """
    pass


_exception_map = {
    "definition": DefinitionError,
    "evaluation": EvaluationError,
    "grant": GrantError,
    "request": RequestError,
    "locality_incompatibility": LocalityIncompatibilityError,
    "not_implemented": NotImplementedError,
    "parallel_pagination_not_supported": ParallelPaginationNotSupported,
    "page_reference": PageReferenceError,
    "resource_not_found": ResourceNotFoundError,
    "start": StartError
}
"""Mapping of error type strings to Exception classes."""
