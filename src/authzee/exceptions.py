
"""Module for Authzee Exceptions
"""

from typing import List


class AuthzeeError(Exception):
    """Base Authzee Exception.
    """
    pass


class SpecificationError(AuthzeeError):
    """Base exception for errors defined in the Authzee specification.
    
    Includes a `response` field that always returns the currently running operations..
    """
    
    def __init__(self, message: str, response: dict):
        super().__init__(message)
        self.response = response


class ContextError(SpecificationError):
    """Critical error when evaluating context."""
    pass


class DefinitionError(SpecificationError):
    """Error when validating the identity and resource definitions."""
    pass


class GrantError(SpecificationError):
    """Error when validating grants."""
    pass


class JMESPathError(SpecificationError):
    """Critical error when evaluating JMESPath query."""
    pass


class RequestError(SpecificationError):
    """Error when validating the request."""
    pass


class SDKError(AuthzeeError):
    """Base exception for errors from the Authzee SDK that are **not** defined by the specification.
    """
    pass


class LocalityIncompatibility(SDKError):
    """The localities are not compatible.

    See ``authzee.module_locality.ModuleLocality`` for more info.
    """
    pass


class GrantNotFoundError(SDKError):
    """The Grant with a specific UUID was not found in the storage backend.
    """
    pass


class LatchNotFoundError(SDKError):
    """The storage latch with a specific UUID was not found in the storage backend.
    """
    pass


class StartError(SDKError):
    """There was an error during initialization of the Authzee App and modules.
    """
    pass


class MethodNotImplementedError(SDKError):
    """The given method is not implemented for this class.
    """

    def __init__(self, msg: str = "This method is not implemented.", *args, **kwargs):
        super().__init__(msg, *args, **kwargs)


class ParallelPaginationNotSupported(SDKError):
    """Parallel pagination is not supported.
    """
    pass

class PageReferenceError(SDKError):
    """The given page reference had an error when processing.
    """
    pass
