"""Base compute module for Authzee.

See [](authzee.compute.compute_module.ComputeModule)
"""

__all__ = [
    "ComputeModule"
]

from abc import abstractmethod
from typing import Any, Callable, Type

from authzee.compute._compute_meta import _ComputeMeta
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule
from authzee.types.authzee import *
from authzee.types.config import (
    AuditConfig,
    AuthorizeConfig,
    BatchAuditConfig,
    BatchAuthorizeConfig,
    ComputeConstructConfig,
    ComputeDestroyConfig,
    ComputeShutdownConfig,
    ComputeStartConfig,
    ValidateBatchRequestConfig,
    ValidateContextDefConfig,
    ValidateGrantConfig,
    ValidateIdentityDefConfig,
    ValidateRequestConfig,
    ValidateResourceDefConfig
)


class ComputeModule(metaclass=_ComputeMeta):
    """Abstract base class for Authzee compute modules.

    A compute module processes authorization requests: it validates requests and
    definitions, and runs the audit/authorize operations by evaluating grants
    retrieved from a storage module.

    Subclass this to build a custom compute module. All methods are abstract and
    must be implemented, and all methods are asynchronous.

    Returning responses
    -------------------
    Every method returns a result body (a `dict`) rather than raising on failure.
    On success, populate the result fields and set `error` to `None`. On a handled
    failure, return the result body with its non-`error` fields set to safe
    defaults and `error` set to an `AuthzeeError` describing the problem.

    Automatic exception translation
    -------------------------------
    This class uses the [](authzee.compute._compute_meta._ComputeMeta) metaclass,
    which wraps every concrete (non-abstract) method in a try/except. Any exception
    that propagates out of a method implementation is automatically caught and
    translated into that method's expected result body, with `error` populated and
    `error_type` set to `"compute"` (since the failure originated in a compute
    module). Because of this, implementations may simply raise on unexpected
    failures and rely on the metaclass to produce a correctly shaped error
    response; there is no need to wrap every method body in your own try/except.

    Handling storage errors
    -----------------------
    A compute module calls into a storage module to retrieve definitions and
    grants. Storage methods do not raise; they return a result body with an
    `error` field. A compute module **must** check the `error` field of every
    storage result it receives and handle it, typically by short-circuiting and
    returning its own result body with that error propagated (its `error_type`
    will already be `"storage"`, identifying where the failure originated). Do not
    ignore storage errors or assume storage calls always succeed.

    Using per-call configuration
    ----------------------------
    Every method receives a per-call `config` (a `dict`). A compute module is not
    required to honor every config key that its config type allows - a config type
    describes every option that *could* apply to that call across all module
    implementations, and any key a given module does not understand may simply be
    ignored. However, a compute module **should** utilize the config keys that map
    to behavior it actually implements. In particular, when a compute config
    embeds storage-call sub-configs (for example the `get_*` / `use_list_*` /
    `list_*` sub-configs in `ValidateRequestConfig` and `ValidateBatchRequestConfig`,
    or `list_grants` / `list_grant_refs` / `parallel_paging` in the audit and
    authorize configs), the compute module should pass those through to the
    corresponding storage calls so callers can tune retrieval and paging.

    This base `ComputeModule` does **not** use the following config, and neither
    should subclasses, because a compute module has no corresponding operation -
    these are storage-only operations invoked through the storage module rather
    than implemented on compute:

    - The definition and grant persistence and retrieval configs:
    `GetContextDefConfig`, `PutContextDefConfig`, `DeleteContextDefConfig`,
    `GetIdentityDefConfig`, `PutIdentityDefConfig`, `DeleteIdentityDefConfig`,
    `GetResourceDefConfig`, `PutResourceDefConfig`, `DeleteResourceDefConfig`,
    `GetGrantConfig`, `EnactConfig`, and `RepealConfig`, along with the
    standalone `ListContextDefsConfig`, `ListIdentityDefsConfig`,
    `ListResourceDefsConfig`, `ListGrantsConfig`, and `ListGrantRefsConfig` as
    top-level (non-embedded) configs.
    - The storage latch configs: `CreateLatchConfig`, `GetLatchConfig`,
    `SetLatchConfig`, `DeleteLatchConfig`, and `CleanupLatchesConfig`.

    A compute module does still cause several of these storage calls to run (for
    example listing grants during an audit or authorize); when it does, it uses the
    versions of those sub-configs embedded in the compute config it received, not
    the standalone top-level configs above.

    Parameters
    ----------
    None

    Examples
    --------

    ```python
    from authzee import Authzee, DictStorage, InProcessCompute, jmespath_execute

    authz = Authzee(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": {}
        }
    )
    authz.construct()
    authz.start()
    ```
    """


    @abstractmethod
    async def start(
        self,
        execute: Callable[[str, Any], Any],
        storage_type: Type[StorageModule],
        storage_kwargs: dict[str, Any],
        config: ComputeStartConfig
    ) -> GenericResult:
        """Start up compute module.

        Run before use. After this method is complete these public instance vars or
        getters must be available and stable:

        - `locality` - Compute [Module Locality](#module-locality)
        - `has_parallel_paging` - if the compute module supports processing grants
        with parallel paging

        Parameters
        ----------
        execute : Callable[[str, Any], Any]
            The JSON query execute function used to evaluate grant queries.
        storage_type : Type[StorageModule]
            The storage module type the compute module will use to retrieve data.
        storage_kwargs : dict[str, Any]
            Keyword arguments used to instantiate the storage module.
        config : ComputeStartConfig
            The per-call configuration for starting the compute module.

        Examples
        --------

        ```python
        result = await compute.start(
            execute=jmespath_execute,
            storage_type=DictStorage,
            storage_kwargs={
                "storage_dict": {}
            },
            config={
                "storage": {}
            }
        )
        ```

        Returns
        -------

        GenericResult
            A result body with an `error` field that is `None` on success or an
            `AuthzeeError` describing the failure.

        Successful return:

        ```python
        {
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "error": {
                "error_type": "compute",
                "message": "Failed to start the compute module."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        self._execute = execute
        self._storage_type = storage_type
        self._storage_kwargs = storage_kwargs
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = False


    @abstractmethod
    async def shutdown(self, config: ComputeShutdownConfig) -> GenericResult:
        """Shutdown Compute module.

        Clean up runtime resources.

        Parameters
        ----------
        config : ComputeShutdownConfig
            The per-call configuration for shutting down the compute module.

        Examples
        --------

        ```python
        result = await compute.shutdown(
            config={
                "storage": {}
            }
        )
        ```

        Returns
        -------

        GenericResult
            A result body with an `error` field that is `None` on success or an
            `AuthzeeError` describing the failure.

        Successful return:

        ```python
        {
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "error": {
                "error_type": "compute",
                "message": "Failed to shut down the compute module."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def construct(self, config: ComputeConstructConfig) -> GenericResult:
        """Construct backend resources for compute.

        One time setup.

        Parameters
        ----------
        config : ComputeConstructConfig
            The per-call configuration for constructing compute resources.

        Examples
        --------

        ```python
        result = await compute.construct(
            config={}
        )
        ```

        Returns
        -------

        GenericResult
            A result body with an `error` field that is `None` on success or an
            `AuthzeeError` describing the failure.

        Successful return:

        ```python
        {
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "error": {
                "error_type": "compute",
                "message": "Failed to construct compute resources."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def destroy(self, config: ComputeDestroyConfig) -> GenericResult:
        """Tear down backend resources.

        Destructive - may lose all long lasting compute resources.

        Parameters
        ----------
        config : ComputeDestroyConfig
            The per-call configuration for destroying compute resources.

        Examples
        --------

        ```python
        result = await compute.destroy(
            config={}
        )
        ```

        Returns
        -------

        GenericResult
            A result body with an `error` field that is `None` on success or an
            `AuthzeeError` describing the failure.

        Successful return:

        ```python
        {
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "error": {
                "error_type": "compute",
                "message": "Failed to destroy compute resources."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def validate_context_def(
        self,
        context_def: ContextDef,
        config: ValidateContextDefConfig
    ) -> GenericResult:
        """Validate a context definition.

        Parameters
        ----------
        context_def : ContextDef
            The context definition to validate.
        config : ValidateContextDefConfig
            The per-call configuration for validating a context definition.

        Examples
        --------

        ```python
        result = await compute.validate_context_def(
            context_def={
                "context_type": "NONE",
                "schema": {
                    "type": "object",
                    "additionalProperties": False
                }
            },
            config={}
        )
        ```

        Returns
        -------

        GenericResult
            A result body with an `error` field that is `None` on success or an
            `AuthzeeError` describing the failure.

        Successful return:

        ```python
        {
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "error": {
                "error_type": "compute",
                "message": "The context definition is not valid."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def validate_identity_def(
        self,
        identity_def: IdentityDef,
        config: ValidateIdentityDefConfig
    ) -> GenericResult:
        """Validate an identity definition.

        Parameters
        ----------
        identity_def : IdentityDef
            The identity definition to validate.
        config : ValidateIdentityDefConfig
            The per-call configuration for validating an identity definition.

        Examples
        --------

        ```python
        result = await compute.validate_identity_def(
            identity_def={
                "identity_type": "user",
                "schema": {
                    "type": "object",
                    "required": [
                        "username",
                        "department"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "username": {
                            "type": "string"
                        },
                        "department": {
                            "type": "string"
                        }
                    }
                }
            },
            config={}
        )
        ```

        Returns
        -------

        GenericResult
            A result body with an `error` field that is `None` on success or an
            `AuthzeeError` describing the failure.

        Successful return:

        ```python
        {
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "error": {
                "error_type": "compute",
                "message": "The identity definition is not valid."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def validate_resource_def(
        self,
        resource_def: ResourceDef,
        config: ValidateResourceDefConfig
    ) -> GenericResult:
        """Validate a resource definition.

        Parameters
        ----------
        resource_def : ResourceDef
            The resource definition to validate.
        config : ValidateResourceDefConfig
            The per-call configuration for validating a resource definition.

        Examples
        --------

        ```python
        result = await compute.validate_resource_def(
            resource_def={
                "resource_type": "balloon",
                "actions": [
                    "balloon:read",
                    "balloon:inflate",
                    "balloon:pop"
                ],
                "schema": {
                    "type": "object",
                    "required": [
                        "color",
                        "is_inflated"
                    ],
                    "additionalProperties": False,
                    "properties": {
                        "color": {
                            "type": "string"
                        },
                        "is_inflated": {
                            "type": "boolean"
                        }
                    }
                }
            },
            config={}
        )
        ```

        Returns
        -------

        GenericResult
            A result body with an `error` field that is `None` on success or an
            `AuthzeeError` describing the failure.

        Successful return:

        ```python
        {
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "error": {
                "error_type": "compute",
                "message": "The resource definition is not valid."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def validate_grant(
        self,
        grant: Grant,
        config: ValidateGrantConfig
    ) -> GenericResult:
        """Validate a grant.

        Parameters
        ----------
        grant : Grant
            The grant to validate.
        config : ValidateGrantConfig
            The per-call configuration for validating a grant.

        Examples
        --------

        ```python
        result = await compute.validate_grant(
            grant={
                "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                "name": "Allow inflate for balloon department",
                "description": "Balloon department people are allowed to read and inflate all balloons.",
                "tags": {},
                "effect": "allow",
                "actions": [
                    "balloon:read",
                    "balloon:inflate"
                ],
                "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                "equality": True,
                "applicable_on_failure": False,
                "data": {}
            },
            config={}
        )
        ```

        Returns
        -------

        GenericResult
            A result body with an `error` field that is `None` on success or an
            `AuthzeeError` describing the failure.

        Successful return:

        ```python
        {
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "error": {
                "error_type": "compute",
                "message": "The grant is not valid."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def validate_request(
        self,
        request: AuthzeeRequest,
        config: ValidateRequestConfig
    ) -> GenericResult:
        """Validate a request.

        Parameters
        ----------
        request : AuthzeeRequest
            The authorization request to validate.
        config : ValidateRequestConfig
            The per-call configuration for validating a request.

        Examples
        --------

        ```python
        result = await compute.validate_request(
            request={
                "identities": {
                    "user": [
                        {
                            "username": "balloon_person",
                            "department": "Balloon Dept"
                        }
                    ]
                },
                "action": "balloon:inflate",
                "resource_type": "balloon",
                "resource": {
                    "color": "red",
                    "is_inflated": False
                },
                "context_type": "NONE",
                "context": {}
            },
            config={
                "get_context_def": {
                    "use_cache": True
                },
                "use_list_context_defs": False,
                "list_context_defs": {
                    "page_size": 1000,
                    "use_cache": True
                },
                "get_identity_def": {
                    "use_cache": True
                },
                "use_list_identity_defs": True,
                "list_identity_defs": {
                    "page_size": 1000,
                    "use_cache": True
                },
                "get_resource_def": {
                    "use_cache": True
                },
                "use_list_resource_defs": False,
                "list_resource_defs": {
                    "page_size": 1000,
                    "use_cache": True
                }
            }
        )
        ```

        Returns
        -------

        GenericResult
            A result body with an `error` field that is `None` on success or an
            `AuthzeeError` describing the failure.

        Successful return:

        ```python
        {
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "error": {
                "error_type": "compute",
                "message": "The request is not valid."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def validate_batch_request(
        self,
        batch_request: AuthzeeBatchRequest,
        config: ValidateBatchRequestConfig
    ) -> ValidateBatchRequestResult:
        """Validate a batch request.

        Parameters
        ----------
        batch_request : AuthzeeBatchRequest
            The batch authorization request to validate.
        config : ValidateBatchRequestConfig
            The per-call configuration for validating a batch request.

        Examples
        --------

        ```python
        result = await compute.validate_batch_request(
            batch_request=[
                {
                    "identities": {
                        "user": [
                            {
                                "username": "balloon_person",
                                "department": "Balloon Dept"
                            }
                        ]
                    },
                    "action": "balloon:inflate",
                    "resource_type": "balloon",
                    "resource": {
                        "color": "red",
                        "is_inflated": False
                    },
                    "context_type": "NONE",
                    "context": {}
                }
            ],
            config={
                "get_context_def": {
                    "use_cache": True
                },
                "use_list_context_defs": True,
                "list_context_defs": {
                    "page_size": 1000,
                    "use_cache": True
                },
                "get_identity_def": {
                    "use_cache": True
                },
                "use_list_identity_defs": True,
                "list_identity_defs": {
                    "page_size": 1000,
                    "use_cache": True
                },
                "get_resource_def": {
                    "use_cache": True
                },
                "use_list_resource_defs": True,
                "list_resource_defs": {
                    "page_size": 1000,
                    "use_cache": True
                }
            }
        )
        ```

        Returns
        -------

        ValidateBatchRequestResult
            A result with `error` (a batch level `AuthzeeError` or `None`) and
            `batch` (a list where each item is `None` when the corresponding batch
            item is valid or an `AuthzeeError` describing the item level failure).

        Successful return (each batch item is `None` when valid or an error object
        when that item is invalid):

        ```python
        {
            "error": None,
            "batch": [
                None,
                None
            ]
        }
        ```

        Error return (a batch level error fails the whole request):

        ```python
        {
            "error": {
                "error_type": "compute",
                "message": "The batch request is not valid."
            },
            "batch": []
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def audit(
        self,
        request: AuthzeeRequest,
        page_ref: str | None,
        config: AuditConfig
    ) -> AuditResultPage:
        """Run the Audit Operation for a page of results.

        Pass the returned page reference to get the next page until a null page reference is returned.

        Parameters
        ----------
        request : AuthzeeRequest
            The authorization request to audit against the stored grants.
        page_ref : str | None
            The page reference for the page to retrieve, or `None` for the first page.
        config : AuditConfig
            The per-call configuration for the audit operation.

        Examples
        --------

        ```python
        page = await compute.audit(
            request={
                "identities": {
                    "user": [
                        {
                            "username": "balloon_person",
                            "department": "Balloon Dept"
                        }
                    ]
                },
                "action": "balloon:inflate",
                "resource_type": "balloon",
                "resource": {
                    "color": "red",
                    "is_inflated": False
                },
                "context_type": "NONE",
                "context": {}
            },
            page_ref=None,
            config={
                "validate_request": {
                    "get_context_def": {
                        "use_cache": True
                    },
                    "use_list_context_defs": False,
                    "list_context_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    },
                    "get_identity_def": {
                        "use_cache": True
                    },
                    "use_list_identity_defs": True,
                    "list_identity_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    },
                    "get_resource_def": {
                        "use_cache": True
                    },
                    "use_list_resource_defs": False,
                    "list_resource_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    }
                },
                "list_grants": {
                    "page_size": 100,
                    "use_cache": True
                }
            }
        )
        ```

        Returns
        -------

        AuditResultPage
            A page result with `results` (a list of audit result items each with a
            `grant`, `is_applicable`, `query_result`, and `failure`),
            `next_page_ref` (the reference for the next page or `None` when there
            are no more pages), and `error` (`None` on success or an
            `AuthzeeError` describing the failure).

        Successful return:

        ```python
        {
            "results": [
                {
                    "grant": {
                        "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                        "name": "Allow inflate for balloon department",
                        "description": "Balloon department people are allowed to read and inflate all balloons.",
                        "tags": {},
                        "effect": "allow",
                        "actions": [
                            "balloon:read",
                            "balloon:inflate"
                        ],
                        "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                        "equality": True,
                        "applicable_on_failure": False,
                        "data": {}
                    },
                    "is_applicable": True,
                    "query_result": True,
                    "failure": None
                }
            ],
            "next_page_ref": "abc123",
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "results": [],
            "next_page_ref": None,
            "error": {
                "error_type": "compute",
                "message": "Failed to run the audit operation."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def authorize(
        self,
        request: AuthzeeRequest,
        config: AuthorizeConfig
    ) -> AuthorizeResult:
        """Run the Authorize Operation.

        Parameters
        ----------
        request : AuthzeeRequest
            The authorization request to evaluate against the stored grants.
        config : AuthorizeConfig
            The per-call configuration for the authorize operation.

        Examples
        --------

        ```python
        result = await compute.authorize(
            request={
                "identities": {
                    "user": [
                        {
                            "username": "balloon_person",
                            "department": "Balloon Dept"
                        }
                    ]
                },
                "action": "balloon:inflate",
                "resource_type": "balloon",
                "resource": {
                    "color": "red",
                    "is_inflated": False
                },
                "context_type": "NONE",
                "context": {}
            },
            config={
                "validate_request": {
                    "get_context_def": {
                        "use_cache": True
                    },
                    "use_list_context_defs": False,
                    "list_context_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    },
                    "get_identity_def": {
                        "use_cache": True
                    },
                    "use_list_identity_defs": True,
                    "list_identity_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    },
                    "get_resource_def": {
                        "use_cache": True
                    },
                    "use_list_resource_defs": False,
                    "list_resource_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    }
                },
                "list_grants": {
                    "page_size": 1000,
                    "use_cache": True
                },
                "parallel_paging": False,
                "list_grant_refs": {
                    "page_size": 10,
                    "use_cache": True
                }
            }
        )
        ```

        Returns
        -------

        AuthorizeResult
            A result with `is_authorized` (whether the request is authorized),
            `grant` (the grant responsible for the decision or `None`), `message`
            (one of the fixed enum strings describing the decision), and `error`
            (`None` on success or an `AuthzeeError` describing the failure). The
            `message` is one of:

            - `"An error has occurred. Therefore, the request is not authorized."`
            - `"A deny grant is applicable to the request. Therefore, the request is not authorized."`
            - `"An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized."`
            - `"No grants are applicable to the request. Therefore, the request is implicitly denied and is not authorized."`

        Successful return:

        ```python
        {
            "is_authorized": True,
            "grant": {
                "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                "name": "Allow inflate for balloon department",
                "description": "Balloon department people are allowed to read and inflate all balloons.",
                "tags": {},
                "effect": "allow",
                "actions": [
                    "balloon:read",
                    "balloon:inflate"
                ],
                "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                "equality": True,
                "applicable_on_failure": False,
                "data": {}
            },
            "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "is_authorized": False,
            "grant": None,
            "message": "An error has occurred. Therefore, the request is not authorized.",
            "error": {
                "error_type": "compute",
                "message": "Failed to run the authorize operation."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def batch_audit(
        self,
        batch_request: AuthzeeBatchRequest,
        page_ref: str | None,
        config: BatchAuditConfig
    ) -> BatchAuditResultPage:
        """Run the Batch Audit Operation for a page of results.

        Pass the returned page reference to get the next page until a null page reference is returned.

        Parameters
        ----------
        batch_request : AuthzeeBatchRequest
            The batch authorization request to audit against the stored grants.
        page_ref : str | None
            The page reference for the page to retrieve, or `None` for the first page.
        config : BatchAuditConfig
            The per-call configuration for the batch audit operation.

        Examples
        --------

        ```python
        page = await compute.batch_audit(
            batch_request=[
                {
                    "identities": {
                        "user": [
                            {
                                "username": "balloon_person",
                                "department": "Balloon Dept"
                            }
                        ]
                    },
                    "action": "balloon:inflate",
                    "resource_type": "balloon",
                    "resource": {
                        "color": "red",
                        "is_inflated": False
                    },
                    "context_type": "NONE",
                    "context": {}
                }
            ],
            page_ref=None,
            config={
                "validate_batch_request": {
                    "get_context_def": {
                        "use_cache": True
                    },
                    "use_list_context_defs": True,
                    "list_context_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    },
                    "get_identity_def": {
                        "use_cache": True
                    },
                    "use_list_identity_defs": True,
                    "list_identity_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    },
                    "get_resource_def": {
                        "use_cache": True
                    },
                    "use_list_resource_defs": True,
                    "list_resource_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    }
                },
                "list_grants": {
                    "page_size": 100,
                    "use_cache": True
                }
            }
        )
        ```

        Returns
        -------

        BatchAuditResultPage
            A page result with `grants` (the list of grants processed for this
            page), `batch` (a list of batch item results, each with `results` per
            grant index and an item level `error`), `next_page_ref` (the reference
            for the next page or `None` when there are no more pages), and `error`
            (`None` on success or an `AuthzeeError` describing the failure).

        Successful return:

        ```python
        {
            "grants": [
                {
                    "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                    "name": "Allow inflate for balloon department",
                    "description": "Balloon department people are allowed to read and inflate all balloons.",
                    "tags": {},
                    "effect": "allow",
                    "actions": [
                        "balloon:read",
                        "balloon:inflate"
                    ],
                    "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                    "equality": True,
                    "applicable_on_failure": False,
                    "data": {}
                }
            ],
            "batch": [
                {
                    "results": [
                        {
                            "is_applicable": True,
                            "query_result": True,
                            "failure": None
                        }
                    ],
                    "error": None
                }
            ],
            "next_page_ref": "abc123",
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "grants": [],
            "batch": [],
            "next_page_ref": None,
            "error": {
                "error_type": "compute",
                "message": "Failed to run the batch audit operation."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...


    @abstractmethod
    async def batch_authorize(
        self,
        batch_request: AuthzeeBatchRequest,
        config: BatchAuthorizeConfig
    ) -> BatchAuthorizeResult:
        """Run the Batch Authorize Operation.

        Parameters
        ----------
        batch_request : AuthzeeBatchRequest
            The batch authorization request to evaluate against the stored grants.
        config : BatchAuthorizeConfig
            The per-call configuration for the batch authorize operation.

        Examples
        --------

        ```python
        result = await compute.batch_authorize(
            batch_request=[
                {
                    "identities": {
                        "user": [
                            {
                                "username": "balloon_person",
                                "department": "Balloon Dept"
                            }
                        ]
                    },
                    "action": "balloon:inflate",
                    "resource_type": "balloon",
                    "resource": {
                        "color": "red",
                        "is_inflated": False
                    },
                    "context_type": "NONE",
                    "context": {}
                }
            ],
            config={
                "validate_batch_request": {
                    "get_context_def": {
                        "use_cache": True
                    },
                    "use_list_context_defs": True,
                    "list_context_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    },
                    "get_identity_def": {
                        "use_cache": True
                    },
                    "use_list_identity_defs": True,
                    "list_identity_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    },
                    "get_resource_def": {
                        "use_cache": True
                    },
                    "use_list_resource_defs": True,
                    "list_resource_defs": {
                        "page_size": 1000,
                        "use_cache": True
                    }
                },
                "list_grants": {
                    "page_size": 1000,
                    "use_cache": True
                },
                "parallel_paging": False,
                "list_grant_refs": {
                    "page_size": 10,
                    "use_cache": True
                }
            }
        )
        ```

        Returns
        -------

        BatchAuthorizeResult
            A result with `batch` (a list of authorize results, one per batch item,
            each with `is_authorized`, `grant`, `message` from the fixed enum, and
            `error`) and `error` (a batch level `AuthzeeError` or `None`).

        Successful return:

        ```python
        {
            "batch": [
                {
                    "is_authorized": True,
                    "grant": {
                        "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                        "name": "Allow inflate for balloon department",
                        "description": "Balloon department people are allowed to read and inflate all balloons.",
                        "tags": {},
                        "effect": "allow",
                        "actions": [
                            "balloon:read",
                            "balloon:inflate"
                        ],
                        "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                        "equality": True,
                        "applicable_on_failure": False,
                        "data": {}
                    },
                    "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
                    "error": None
                }
            ],
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "batch": [],
            "error": {
                "error_type": "compute",
                "message": "Failed to run the batch authorize operation."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_ComputeMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"compute"`.
        """
        ...
