"""Module for Authzee Exceptions.
"""

__all__ = [
    "AuthzeeError",
    "AuthzeeSDKError",
    "AuthzeeSpecError",
    "ComputeError",
    "DefinitionError",
    "GrantError",
    "LocalityIncompatibilityError",
    "ParallelPaginationNotSupported",
    "RequestError",
    "ResourceNotFoundError",
    "StorageError"
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


class ParallelPaginationNotSupported(AuthzeeSDKError):
    """Parallel pagination is not supported.
    """
    pass


class ComputeError(AuthzeeSDKError):
    """Base exception for errors specific to compute modules.
    """
    pass


class StorageError(AuthzeeSDKError):
    """Base exception for errors specific to storage modules."""
    pass


class ResourceNotFoundError(StorageError):
    """The resource with a specific UUID or type was not found in the storage backend.
    """
    pass


_exception_map = {
    "definition": DefinitionError,
    "grant": GrantError,
    "request": RequestError,
    "locality_incompatibility": LocalityIncompatibilityError,
    "parallel_pagination_not_supported": ParallelPaginationNotSupported,
    "compute": ComputeError,
    "storage": StorageError,
    "resource_not_found": ResourceNotFoundError
}
"""Mapping of error type strings to Exception classes."""
