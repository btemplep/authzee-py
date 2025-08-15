"""Module for Authzee Exceptions
"""

__all__ = [
    "AuthzeeError",
    "SpecificationError",
    "ContextError",
    "DefinitionError",
    "GrantError",
    "JMESPathError",
    "RequestError",
    "SDKError",
    "LocalityIncompatibilityError",
    "GrantNotFoundError",
    "LatchNotFoundError",
    "StartError",
    "NotImplementedError",
    "ParallelPaginationNotSupported",
    "PageReferenceError"
]

from typing import List
from uuid import UUID


class AuthzeeError(Exception):
    """Base Authzee Exception.
    """
    pass


class SpecificationError(AuthzeeError):
    """Base exception for errors defined in the Authzee specification.
    
    Parameters
    ----------
    context : List[dict]
        Context errors from authzee spec.
    definition : List[dict]
        Definition errors from authzee spec.
    grant : List[dict]
        Grant errors from authzee spec.
    jmespath : List[dict]
        JMESPath errors from authzee spec.
    request : List[dict]
        Request errors from authzee spec.
    
    Attributes
    ---------------------
    context : List[dict]
        Context errors from authzee spec.
    definition : List[dict]
        Definition errors from authzee spec.
    grant : List[dict]
        Grant errors from authzee spec.
    jmespath : List[dict]
        JMESPath errors from authzee spec.
    request : List[dict]
        Request errors from authzee spec.
        
    Examples
    --------

    .. code-block:: python

        SpecificationError(
            message="A critical error occurred",
            context = [
                {
                    "message": "'request_source' is a required property",
                    "critical": false,
                    "grant": {
                        "grant_uuid": "c68cf016-1254-4c49-9f4d-3024dd6a937c",
                        "name": "thing",
                        "description": "thing",
                        "tags": {},
                        "effect": "allow",
                        "actions": [
                            "read"
                        ],
                        "query": "true",
                        "query_validation": "error",
                        "equality": true,
                        "data": {},
                        "context_schema": {
                            "type": "object",
                            "properties": {
                                "request_source": {
                                    "type": "string"
                                }
                            },
                            "required": [
                                "request_source"
                            ]
                        },
                        "context_validation": "error"
                    }
                }
            ],
            definition=[
                {
                    "message": "Identity types must be unique. 'User' is present more than once.",
                    "critical": true,
                    "definition_type": "identity",
                    "definition": { # this will match the definition given so it is free form.
                        "identity_type": "User",
                        "schema": {
                            "type": "object"
                        }
                    }
                }
            ],
            grant=[
                {
                    "message": "The grant is not valid. Schema Error: 'invalid_action' is not one of ['read', 'inflate', 'deflate', 'pop', 'tie']",
                    "critical": true,
                    "grant": {
                        "grant_uuid": "c68cf016-1254-4c49-9f4d-3024dd6a937c",
                        "name": "thing",
                        "description": "thing",
                        "tags": {},
                        "effect": "allow",
                        "actions": [
                            "invalid_action"
                        ],
                        "query": "true",
                        "query_validation": "error",
                        "equality": true,
                        "data": {},
                        "context_schema": {
                            "type": "object"
                        },
                        "context_validation": "none"
                    }
                }
            ],
            jmespath=[
                {
                    "message": "Invalid function name: invalid_function",
                    "critical": false,
                    "grant": {
                    "grant_uuid": "c68cf016-1254-4c49-9f4d-3024dd6a937c",
                    "name": "thing",
                    "description": "thing",
                    "tags": {},
                    "effect": "allow",
                    "actions": [
                        "read"
                    ],
                    "query": "invalid_function(request.identities.User[0].department)",
                    "query_validation": "error",
                    "equality": true,
                    "data": {},
                    "context_schema": {
                        "type": "object"
                    },
                    "context_validation": "none"
                }
            ],
            request=[
                {
                    "message": "The request is not valid for the request schema: 'invalid_action' is not one of ['read', 'inflate', 'deflate', 'pop', 'tie']",
                    "critical": true
                }
            ]
        )

    """
    
    def __init__(
        self, 
        message: str, 
        context: List[dict],
        definition: List[dict],
        grant: List[dict],
        jmespath: List[dict],
        request: List[dict]
    ):
        super().__init__(message)
        self.context = context
        self.definition = definition
        self.grant = grant
        self.jmespath = jmespath
        self.request = request


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


class LocalityIncompatibilityError(SDKError):
    """The localities are not compatible.

    See ``authzee.module_locality.ModuleLocality`` for more info.
    """
    pass


class GrantNotFoundError(SDKError):
    """The Grant with a specific UUID was not found in the storage backend.
    """
    
    def __init__(self, grant_uuid: UUID):
        super().__init__(f"Grant with UUID '{grant_uuid}' was not found.")
        self.grant_uuid = grant_uuid


class LatchNotFoundError(SDKError):
    """The storage latch with a specific UUID was not found in the storage backend.
    """
    pass


class StartError(SDKError):
    """There was an error during initialization of the Authzee App and modules.
    """
    pass


class NotImplementedError(SDKError):
    """The given method is not implemented for this class.
    """

    def __init__(self, msg: str = "This method is not implemented.", *args, **kwargs):
        super().__init__(msg, *args, **kwargs)


class ParallelPaginationNotSupported(SDKError):
    """Parallel pagination is not supported.
    """
    pass


class PageReferenceError(SDKError):
    """Error when processing a page reference.
    """
    pass
