"""Base storage module for Authzee.

See [](authzee.storage.storage_module.StorageModule)
"""

__all__ = [
    "StorageModule"
]

from abc import abstractmethod
import datetime

from authzee.module_locality import ModuleLocality
from authzee.storage._storage_meta import _StorageMeta
from authzee.types.authzee import *
from authzee.types.config import (
    CleanupLatchesConfig,
    CreateLatchConfig,
    DeleteContextDefConfig,
    DeleteIdentityDefConfig,
    DeleteLatchConfig,
    DeleteResourceDefConfig,
    EnactConfig,
    GetContextDefConfig,
    GetGrantConfig,
    GetIdentityDefConfig,
    GetLatchConfig,
    GetResourceDefConfig,
    ListContextDefsConfig,
    ListGrantRefsConfig,
    ListGrantsConfig,
    ListIdentityDefsConfig,
    ListResourceDefsConfig,
    PutContextDefConfig,
    PutIdentityDefConfig,
    PutResourceDefConfig,
    RepealConfig,
    SetLatchConfig,
    StorageConstructConfig,
    StorageDestroyConfig,
    StorageShutdownConfig,
    StorageStartConfig
)


class StorageModule(metaclass=_StorageMeta):
    """Abstract base class for Authzee storage modules.

    A storage module persists and retrieves Authzee data: context, identity, and
    resource definitions, grants, and storage latches.

    Subclass this to build a custom storage module. All methods are abstract and
    must be implemented, and all methods are asynchronous.

    Returning responses
    -------------------
    Every method returns a result body (a `dict`) rather than raising on failure.
    On success, populate the result fields and set `error` to `None`. On a handled
    failure, return the result body with its non-`error` fields set to safe
    defaults (for example `None` for a single item, `[]` and `next_page_ref` of
    `None` for a page) and `error` set to an `AuthzeeError` describing the problem.

    Automatic exception translation
    -------------------------------
    This class uses the [](authzee.storage._storage_meta._StorageMeta) metaclass,
    which wraps every concrete (non-abstract) method in a try/except. Any exception
    that propagates out of a method implementation is automatically caught and
    translated into that method's expected result body, with `error` populated and
    `error_type` set to `"storage"` (since the failure originated in a storage
    module). Because of this, implementations may simply raise on unexpected
    failures and rely on the metaclass to produce a correctly shaped error
    response; there is no need to wrap every method body in your own try/except.

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
    async def start(self, config: StorageStartConfig) -> GenericResult:
        """Start up storage module.

        Run before use. After this method is complete these public instance vars
        or getters must be available:

        - `locality` - Storage [Module Locality](#module-locality)
        - `has_parallel_paging` - if the storage module supports parallel paging
        (returning a page of grant page references).

        Parameters
        ----------
        config : StorageStartConfig
            The per-call configuration for starting the storage module.

        Examples
        --------

        ```python
        result = await storage.start(
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
                "error_type": "storage",
                "message": "Failed to start the storage module."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        self.locality = ModuleLocality.PROCESS
        self.has_parallel_paging = False

        return {
            "error": None
        }


    @abstractmethod
    async def shutdown(self, config: StorageShutdownConfig) -> GenericResult:
        """Shutdown storage module.

        Clean up runtime resources.

        Parameters
        ----------
        config : StorageShutdownConfig
            The per-call configuration for shutting down the storage module.

        Examples
        --------

        ```python
        result = await storage.shutdown(
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
                "error_type": "storage",
                "message": "Failed to shut down the storage module."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def construct(self, config: StorageConstructConfig) -> GenericResult:
        """Construct backend resources for storage.

        One time setup.

        Parameters
        ----------
        config : StorageConstructConfig
            The per-call configuration for constructing storage resources.

        Examples
        --------

        ```python
        result = await storage.construct(
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
                "error_type": "storage",
                "message": "Failed to construct storage resources."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def destroy(self, config: StorageDestroyConfig) -> GenericResult:
        """Tear down backend resources.

        Destructive - may lose all long lasting storage resources.

        Parameters
        ----------
        config : StorageDestroyConfig
            The per-call configuration for destroying storage resources.

        Examples
        --------

        ```python
        result = await storage.destroy(
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
                "error_type": "storage",
                "message": "Failed to destroy storage resources."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def list_context_defs(
        self,
        page_ref: str | None,
        config: ListContextDefsConfig
    ) -> ContextDefsPage:
        """Get a page of context definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.

        Parameters
        ----------
        page_ref : str | None
            The page reference for the page to retrieve, or `None` for the first page.
        config : ListContextDefsConfig
            The per-call configuration for listing context definitions.

        Examples
        --------

        ```python
        page = await storage.list_context_defs(
            page_ref=None,
            config={
                "page_size": 100,
                "use_cache": False
            }
        )
        ```

        Returns
        -------

        ContextDefsPage
            A page result with `context_defs` (a list of context definitions),
            `next_page_ref` (the reference for the next page or `None` when there
            are no more pages), and `error` (`None` on success or an
            `AuthzeeError` describing the failure).

        Successful return:

        ```python
        {
            "context_defs": [
                {
                    "context_type": "NONE",
                    "schema": {
                        "type": "object",
                        "additionalProperties": False
                    }
                }
            ],
            "next_page_ref": "abc123",
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "context_defs": [],
            "next_page_ref": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to list context definitions."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def get_context_def(
        self,
        context_type: str,
        config: GetContextDefConfig
    ) -> ContextDefResult:
        """Get a context definition by type.

        Parameters
        ----------
        context_type : str
            The unique context type to retrieve.
        config : GetContextDefConfig
            The per-call configuration for getting a context definition.

        Examples
        --------

        ```python
        result = await storage.get_context_def(
            context_type="NONE",
            config={
                "use_cache": False
            }
        )
        ```

        Returns
        -------

        ContextDefResult
            A result with `context_def` (the matching context definition or `None`
            when not found) and `error` (`None` on success or an `AuthzeeError`
            describing the failure).

        Successful return:

        ```python
        {
            "context_def": {
                "context_type": "NONE",
                "schema": {
                    "type": "object",
                    "additionalProperties": False
                }
            },
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "context_def": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to get the context definition."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def put_context_def(
        self,
        context_def: ContextDef,
        config: PutContextDefConfig
    ) -> GenericResult:
        """Add a new Context Definition or update an existing one.

        Parameters
        ----------
        context_def : ContextDef
            The context definition to add or update.
        config : PutContextDefConfig
            The per-call configuration for putting a context definition.

        Examples
        --------

        ```python
        result = await storage.put_context_def(
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
                "error_type": "storage",
                "message": "Failed to put the context definition."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def delete_context_def(
        self,
        context_type: str,
        config: DeleteContextDefConfig
    ) -> GenericResult:
        """Delete a context definition by type.

        Parameters
        ----------
        context_type : str
            The unique context type to delete.
        config : DeleteContextDefConfig
            The per-call configuration for deleting a context definition.

        Examples
        --------

        ```python
        result = await storage.delete_context_def(
            context_type="NONE",
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
                "error_type": "storage",
                "message": "Failed to delete the context definition."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def list_identity_defs(
        self,
        page_ref: str | None,
        config: ListIdentityDefsConfig
    ) -> IdentityDefsPage:
        """Get a page of identity definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.

        Parameters
        ----------
        page_ref : str | None
            The page reference for the page to retrieve, or `None` for the first page.
        config : ListIdentityDefsConfig
            The per-call configuration for listing identity definitions.

        Examples
        --------

        ```python
        page = await storage.list_identity_defs(
            page_ref=None,
            config={
                "page_size": 100,
                "use_cache": False
            }
        )
        ```

        Returns
        -------

        IdentityDefsPage
            A page result with `identity_defs` (a list of identity definitions),
            `next_page_ref` (the reference for the next page or `None` when there
            are no more pages), and `error` (`None` on success or an
            `AuthzeeError` describing the failure).

        Successful return:

        ```python
        {
            "identity_defs": [
                {
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
                }
            ],
            "next_page_ref": "abc123",
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "identity_defs": [],
            "next_page_ref": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to list identity definitions."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def get_identity_def(
        self,
        identity_type: str,
        config: GetIdentityDefConfig
    ) -> IdentityDefResult:
        """Get an identity definition by type.

        Parameters
        ----------
        identity_type : str
            The unique identity type to retrieve.
        config : GetIdentityDefConfig
            The per-call configuration for getting an identity definition.

        Examples
        --------

        ```python
        result = await storage.get_identity_def(
            identity_type="user",
            config={
                "use_cache": False
            }
        )
        ```

        Returns
        -------

        IdentityDefResult
            A result with `identity_def` (the matching identity definition or
            `None` when not found) and `error` (`None` on success or an
            `AuthzeeError` describing the failure).

        Successful return:

        ```python
        {
            "identity_def": {
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
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "identity_def": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to get the identity definition."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def put_identity_def(
        self,
        identity_def: IdentityDef,
        config: PutIdentityDefConfig
    ) -> GenericResult:
        """Add a new Identity Definition or update an existing one.

        Parameters
        ----------
        identity_def : IdentityDef
            The identity definition to add or update.
        config : PutIdentityDefConfig
            The per-call configuration for putting an identity definition.

        Examples
        --------

        ```python
        result = await storage.put_identity_def(
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
                "error_type": "storage",
                "message": "Failed to put the identity definition."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def delete_identity_def(
        self,
        identity_type: str,
        config: DeleteIdentityDefConfig
    ) -> GenericResult:
        """Delete an identity definition by type.

        Parameters
        ----------
        identity_type : str
            The unique identity type to delete.
        config : DeleteIdentityDefConfig
            The per-call configuration for deleting an identity definition.

        Examples
        --------

        ```python
        result = await storage.delete_identity_def(
            identity_type="user",
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
                "error_type": "storage",
                "message": "Failed to delete the identity definition."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def list_resource_defs(
        self,
        page_ref: str | None,
        config: ListResourceDefsConfig
    ) -> ResourceDefsPage:
        """Get a page of resource definitions.

        Pass the returned page reference to get the next page until a null page reference is returned.

        Parameters
        ----------
        page_ref : str | None
            The page reference for the page to retrieve, or `None` for the first page.
        config : ListResourceDefsConfig
            The per-call configuration for listing resource definitions.

        Examples
        --------

        ```python
        page = await storage.list_resource_defs(
            page_ref=None,
            config={
                "page_size": 100,
                "use_cache": False
            }
        )
        ```

        Returns
        -------

        ResourceDefsPage
            A page result with `resource_defs` (a list of resource definitions),
            `next_page_ref` (the reference for the next page or `None` when there
            are no more pages), and `error` (`None` on success or an
            `AuthzeeError` describing the failure).

        Successful return:

        ```python
        {
            "resource_defs": [
                {
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
                }
            ],
            "next_page_ref": "abc123",
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "resource_defs": [],
            "next_page_ref": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to list resource definitions."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def get_resource_def(
        self,
        resource_type: str,
        config: GetResourceDefConfig
    ) -> ResourceDefResult:
        """Get a resource definition by type.

        Parameters
        ----------
        resource_type : str
            The unique resource type to retrieve.
        config : GetResourceDefConfig
            The per-call configuration for getting a resource definition.

        Examples
        --------

        ```python
        result = await storage.get_resource_def(
            resource_type="balloon",
            config={
                "use_cache": False
            }
        )
        ```

        Returns
        -------

        ResourceDefResult
            A result with `resource_def` (the matching resource definition or
            `None` when not found) and `error` (`None` on success or an
            `AuthzeeError` describing the failure).

        Successful return:

        ```python
        {
            "resource_def": {
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
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "resource_def": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to get the resource definition."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def put_resource_def(
        self,
        resource_def: ResourceDef,
        config: PutResourceDefConfig
    ) -> GenericResult:
        """Add a new Resource Definition or update an existing one.

        Parameters
        ----------
        resource_def : ResourceDef
            The resource definition to add or update.
        config : PutResourceDefConfig
            The per-call configuration for putting a resource definition.

        Examples
        --------

        ```python
        result = await storage.put_resource_def(
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
                "error_type": "storage",
                "message": "Failed to put the resource definition."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def delete_resource_def(
        self,
        resource_type: str,
        config: DeleteResourceDefConfig
    ) -> GenericResult:
        """Delete a resource definition by type.

        Parameters
        ----------
        resource_type : str
            The unique resource type to delete.
        config : DeleteResourceDefConfig
            The per-call configuration for deleting a resource definition.

        Examples
        --------

        ```python
        result = await storage.delete_resource_def(
            resource_type="balloon",
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
                "error_type": "storage",
                "message": "Failed to delete the resource definition."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def enact(self, grant: Grant, config: EnactConfig) -> GenericResult:
        """Add a new grant.

        Parameters
        ----------
        grant : Grant
            The grant to add as a new authorization rule.
        config : EnactConfig
            The per-call configuration for enacting a grant.

        Examples
        --------

        ```python
        result = await storage.enact(
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
                "error_type": "storage",
                "message": "Failed to enact the grant."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def repeal(
        self,
        grant_uuid: str,
        purge: bool,
        config: RepealConfig
    ) -> GenericResult:
        """Delete a grant.

        Parameters
        ----------
        grant_uuid : str
            The UUID of the grant to delete.
        purge : bool
            If `True`, fully purge the grant from storage rather than performing a
            soft delete.
        config : RepealConfig
            The per-call configuration for repealing a grant.

        Examples
        --------

        ```python
        result = await storage.repeal(
            grant_uuid="0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            purge=False,
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
                "error_type": "storage",
                "message": "Failed to repeal the grant."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def get_grant(
        self,
        grant_uuid: str,
        config: GetGrantConfig
    ) -> GrantResult:
        """Get a grant by UUID.

        Parameters
        ----------
        grant_uuid : str
            The UUID of the grant to retrieve.
        config : GetGrantConfig
            The per-call configuration for getting a grant.

        Examples
        --------

        ```python
        result = await storage.get_grant(
            grant_uuid="0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            config={
                "use_cache": False
            }
        )
        ```

        Returns
        -------

        GrantResult
            A result with `grant` (the matching grant or `None` when not found) and
            `error` (`None` on success or an `AuthzeeError` describing the failure).

        Successful return:

        ```python
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
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "grant": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to get the grant."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def list_grants(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: ListGrantsConfig
    ) -> GrantsPage:
        """Retrieve a page of grants.

        Pass the returned page reference to get the next page until a null page reference is returned.

        Parameters
        ----------
        effect : str | None
            Filter grants by effect (`"allow"` or `"deny"`), or `None` to match any effect.
        action : str | None
            Filter grants by action (for example `"balloon:inflate"`), or `None` to match any action.
        page_ref : str | None
            The page reference for the page to retrieve, or `None` for the first page.
        config : ListGrantsConfig
            The per-call configuration for listing grants.

        Examples
        --------

        ```python
        page = await storage.list_grants(
            effect=None,
            action=None,
            page_ref=None,
            config={
                "page_size": 100,
                "use_cache": False
            }
        )
        ```

        Returns
        -------

        GrantsPage
            A page result with `grants` (a list of grants), `next_page_ref` (the
            reference for the next page or `None` when there are no more pages), and
            `error` (`None` on success or an `AuthzeeError` describing the failure).

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
            "next_page_ref": "abc123",
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "grants": [],
            "next_page_ref": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to list grants."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def list_grant_refs(
        self,
        effect: str | None,
        action: str | None,
        page_ref: str | None,
        config: ListGrantRefsConfig
    ) -> PageRefsPage:
        """Retrieve a page of grant page references for parallel pagination.

        Pass the returned page reference to get the next page until a null page reference is returned.

        For some storage modules this may not be possible.
        Check the `parallel_paging` attribute on the storage module after `start()` is complete.

        Parameters
        ----------
        effect : str | None
            Filter grants by effect (`"allow"` or `"deny"`), or `None` to match any effect.
        action : str | None
            Filter grants by action (for example `"balloon:inflate"`), or `None` to match any action.
        page_ref : str | None
            The page reference for the page to retrieve, or `None` for the first page.
        config : ListGrantRefsConfig
            The per-call configuration for listing grant page references.

        Examples
        --------

        ```python
        page = await storage.list_grant_refs(
            effect=None,
            action=None,
            page_ref=None,
            config={
                "page_size": 10,
                "use_cache": False
            }
        )
        ```

        Returns
        -------

        PageRefsPage
            A page result with `page_refs` (a list of grant page reference
            strings), `next_page_ref` (the reference for the next page or `None`
            when there are no more pages), and `error` (`None` on success or an
            `AuthzeeError` describing the failure).

        Successful return:

        ```python
        {
            "page_refs": [
                "abc123",
                "def456"
            ],
            "next_page_ref": "ghi789",
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "page_refs": [],
            "next_page_ref": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to list grant page references."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def create_latch(self, config: CreateLatchConfig) -> StorageLatchResult:
        """Create a new [storage latch](#storage-latches).

        Parameters
        ----------
        config : CreateLatchConfig
            The per-call configuration for creating a storage latch.

        Examples
        --------

        ```python
        result = await storage.create_latch(
            config={}
        )
        ```

        Returns
        -------

        StorageLatchResult
            A result with `storage_latch` (the created latch or `None` on failure)
            and `error` (`None` on success or an `AuthzeeError` describing the
            failure). The latch has `storage_latch_uuid`, `is_set`, and
            `created_at` fields.

        Successful return:

        ```python
        {
            "storage_latch": {
                "storage_latch_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                "is_set": False,
                "created_at": "2026-04-26T16:21:10.521220Z"
            },
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "storage_latch": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to create the storage latch."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def get_latch(
        self,
        storage_latch_uuid: str,
        config: GetLatchConfig
    ) -> StorageLatchResult:
        """Get a [storage latch](#storage-latches) by UUID.

        Parameters
        ----------
        storage_latch_uuid : str
            The UUID of the storage latch to retrieve.
        config : GetLatchConfig
            The per-call configuration for getting a storage latch.

        Examples
        --------

        ```python
        result = await storage.get_latch(
            storage_latch_uuid="0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            config={}
        )
        ```

        Returns
        -------

        StorageLatchResult
            A result with `storage_latch` (the matching latch or `None` when not
            found) and `error` (`None` on success or an `AuthzeeError` describing
            the failure). The latch has `storage_latch_uuid`, `is_set`, and
            `created_at` fields.

        Successful return:

        ```python
        {
            "storage_latch": {
                "storage_latch_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                "is_set": False,
                "created_at": "2026-04-26T16:21:10.521220Z"
            },
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "storage_latch": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to get the storage latch."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def set_latch(
        self,
        storage_latch_uuid: str,
        config: SetLatchConfig
    ) -> StorageLatchResult:
        """Set a [storage latch](#storage-latches) by UUID.

        Parameters
        ----------
        storage_latch_uuid : str
            The UUID of the storage latch to set.
        config : SetLatchConfig
            The per-call configuration for setting a storage latch.

        Examples
        --------

        ```python
        result = await storage.set_latch(
            storage_latch_uuid="0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            config={}
        )
        ```

        Returns
        -------

        StorageLatchResult
            A result with `storage_latch` (the updated latch with `is_set` set to
            `True`, or `None` on failure) and `error` (`None` on success or an
            `AuthzeeError` describing the failure). The latch has
            `storage_latch_uuid`, `is_set`, and `created_at` fields.

        Successful return:

        ```python
        {
            "storage_latch": {
                "storage_latch_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                "is_set": True,
                "created_at": "2026-04-26T16:21:10.521220Z"
            },
            "error": None
        }
        ```

        Error return:

        ```python
        {
            "storage_latch": None,
            "error": {
                "error_type": "storage",
                "message": "Failed to set the storage latch."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def delete_latch(
        self,
        storage_latch_uuid: str,
        config: DeleteLatchConfig
    ) -> GenericResult:
        """Delete a [storage latch](#storage-latches) by UUID.

        Parameters
        ----------
        storage_latch_uuid : str
            The UUID of the storage latch to delete.
        config : DeleteLatchConfig
            The per-call configuration for deleting a storage latch.

        Examples
        --------

        ```python
        result = await storage.delete_latch(
            storage_latch_uuid="0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
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
                "error_type": "storage",
                "message": "Failed to delete the storage latch."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...


    @abstractmethod
    async def cleanup_latches(
        self,
        before: datetime.datetime,
        config: CleanupLatchesConfig
    ) -> GenericResult:
        """Delete all latches before the specified datetime.

        Operations should clean up their own latches, but in case of a failure this
        can be used to clean up zombie latches.

        Parameters
        ----------
        before : datetime.datetime
            All storage latches created before this datetime are deleted.
        config : CleanupLatchesConfig
            The per-call configuration for cleaning up storage latches.

        Examples
        --------

        ```python
        result = await storage.cleanup_latches(
            before=datetime.datetime.now(tz=datetime.timezone.utc),
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
                "error_type": "storage",
                "message": "Failed to clean up storage latches."
            }
        }
        ```

        Raises
        ------
        None
            This method returns errors in the result body rather than raising. Any
            exception raised by the implementation is automatically caught by the
            `_StorageMeta` metaclass and translated into this method's result body
            with `error` populated and `error_type` set to `"storage"`.
        """
        ...
