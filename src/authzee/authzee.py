"""See {py:class}`authzee.authzee.Authzee`"""
__all__ = [
    "Authzee",
]

import asyncio
import datetime
from typing import Any, Callable, Dict, Type

from authzee.types import *
from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule
from authzee.authzee_async import AuthzeeAsync


class Authzee:
    """Authzee application.

    Parameters
    ----------
    execute : Callable[[str, Any], Any]
        JSON query function.
    compute_type : Type[ComputeModule]
        Compute Module Type.
    compute_kwargs : Dict[str, Any]
        Compute module KWArgs used to create instances.
    storage_type : Type[StorageModule]
        Storage Module Type. 
    storage_kwargs : Dict[str, Any]
        Storage module KWArgs used to create instances.
    compute_storage_kwargs : Dict[str, Any], optional
        Override storage module KWArgs that the compute module will use.  May only include KWArgs you want to override.
    config : AuthzeeConfig, optional
        Authzee configuration. May only include config keys you want to override.
    compute_config : AuthzeeConfig, optional
        Override default config values for calls that are passed to the compute backend. May only include config keys you want to override.
        - validate_context_def
        - validate_identity_def
        - validate_resource_def
        - validate_grant
        - audit
        - authorize
        - batch_audit
        - batch_authorize
    
    Examples
    --------
    Simple full example:
    ```python
    import json
    from uuid import uuid4

    from authzee import Authzee, DictStorage, InProcessCompute, jmespath_execute


    storage_dict = {}
    authz = Authzee(
        execute=jmespath_execute,
        compute_type=InProcessCompute,
        compute_kwargs={},
        storage_type=DictStorage,
        storage_kwargs={
            "storage_dict": storage_dict
        }
    )
    authz.construct() # one time setup for life of storage and compute
    authz.start() # initialize the authzee app - must be run once for every instance
    authz.put_context_def( # Context is used to pass structured data to authorization requests. Register them first as context definitions.
        {
            "context_type": "NONE", # unique
            "schema": { # JSON Schema
                "type": "object",
                "additionalProperties": False
            }
        }
    )
    authz.put_identity_def( # identities describe who is being authorized. Register them first as identity definitions.
        identity_def={
            "identity_type": "user", # unique
            "schema": { # JSON Schema
                "type":"object",
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
    )
    authz.put_resource_def( # resources define resource types and actions that can be taken on those resources.  Register them first as resource definitions.
        resource_def={
            "resource_type": "balloon", # unique
            "actions": [ # can be shared between resource types
                "balloon:read",
                "balloon:inflate",
                "balloon:pop"
            ],
            "schema": { # JSON Schema
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
    )
    authz.enact( # Enact grants to create authorization rules
        grant={
            "grant_uuid": str(uuid4()),
            "name": "Allow inflate for balloon department", # not unique
            "description": "Balloon department people are allowed to read and inflate all balloons.",
            "tags": {}, # tags for categorizing grants
            "effect": "allow", # allow or deny
            "actions": [ # list of actions to match
                "balloon:read",
                "balloon:inflate"
            ],
            "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`", # JSON Query for the request. JMESPath is preferred
            # query runs on {"request": <request>, "grant": <grant>}
            "evaluation_handler": "evaluate", 
            "equality": True, # expected result of the query
            "data": {} # data available to this grant
        }
    )
    result = authz.authorize(
        { # request for authorization runs on:
            "identities": { # identities
                "user": [ # identity_type with array of instances
                    {
                        "username": "balloon_person",
                        "department": "Balloon Dept"
                    }
                ]
            },
            "action": "balloon:inflate",
            "resource_type": "balloon",
            "resource": {
                "color": "inflated",
                "is_inflated": False
            },
            "evaluation_handler": "grant",
            "context_type": "NONE",
            "context": {}
        }
    )
    print(json.dumps(result, indent=4))
    ```
    Authorization response
    ```json
    {
        "is_authorized": true,
        "grant": {
            "grant_uuid": "49d5398a-cd5e-4944-bdb9-6543d061a53e",
            "name": "Allow inflate for balloon department",
            "description": "Balloon department people are allowed to read and inflate all balloons.",
            "tags": {},
            "effect": "allow",
            "actions": [
                "balloon:read",
                "balloon:inflate"
            ],
            "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
            "evaluation_handler": "evaluate",
            "equality": true,
            "data": {}
        },
        "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
        "has_failed": false,
        "critical_errors": {}
    }
    ```
    """

    def __init__(
        self, 
        execute: Callable[[str, Any], Any],
        compute_type: Type[ComputeModule],
        compute_kwargs: Dict[str, Any],
        storage_type: Type[StorageModule],
        storage_kwargs: Dict[str, Any],
        compute_storage_kwargs: Dict[str, Any] = None,
        config: AuthzeeConfig = None,
        compute_config: AuthzeeConfig = None
    ):
        self._authzee_async = AuthzeeAsync(
            execute=execute,
            compute_type=compute_type,
            compute_kwargs=compute_kwargs,
            storage_type=storage_type,
            storage_kwargs=storage_kwargs,
            compute_storage_kwargs=compute_storage_kwargs,
            config=config,
            compute_config=compute_config
        )


    def start(self, config: AuthzeeConfig | None = None) -> GenericResult:
        """Initialize the authzee app. Must be run once for every instance.

        Parameters
        ----------
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.start(
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "start": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        StartError
            If a critical error occurs during initialization.
        LocalityIncompatibilityError
            If the storage and compute localities are not compatible.
        """
        return asyncio.run(self._authzee_async.start(config))


    def shutdown(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Shutdown the authzee app.

        Parameters
        ----------
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.shutdown(
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "start": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        ShutdownError
            If a critical error occurs during shutdown.
        """
        return asyncio.run(self._authzee_async.shutdown(config))


    def construct(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """One time setup for the life of storage and compute. Creates DB tables, storage setup, etc.

        Parameters
        ----------
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.construct(
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "start": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        ConstructError
            If a critical error occurs during construction.
        """
        return asyncio.run(self._authzee_async.construct(config))


    def destroy(
        self, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Tear down everything that construct set up. Deletes DB tables, storage, etc.

        Parameters
        ----------
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.destroy(
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "start": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        DestroyError
            If a critical error occurs during destruction.
        """
        return asyncio.run(self._authzee_async.destroy(config))


    def validate_context_def(
        self,
        context_def: ContextDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a context definition without storing it.

        Parameters
        ----------
        context_def : ContextDef
            The context definition to validate.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.validate_context_def(
            context_def={
                "context_type": "NONE",
                "schema": {
                    "type": "object",
                    "additionalProperties": False
                }
            },
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "definition": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        DefinitionError
            If the context definition is invalid and raise_crits is True.
        """
        return asyncio.run(
            self._authzee_async.validate_context_def(
                context_def=context_def,
                config=config
            )
        )


    def list_context_defs(
        self, 
        page_ref: str | None = None,
        config: AuthzeeConfig | None = None
    ) -> ContextDefsPage:
        """Retrieve a single page of context definitions.

        Parameters
        ----------
        page_ref : str | None, optional
            Page reference for pagination. None for the first page.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.list_context_defs(
            page_ref="abc123",  # optional - str | None
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        With paginator:
        ```python
        from authzee import paginator

        # Assumes authz is an Authzee instance
        for page in paginator(authz.list_context_defs):
            for context_def in page['context_defs']:
                print(context_def['context_type'])
        ```

        Returns
        -------
        ContextDefsPage
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
                "next_page_ref": None,  # str | None - None means pagination is complete
                "has_failed": False,
                "errors": {
                    "page_reference": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        PageReferenceError
            If the page reference is invalid.
        """
        return asyncio.run(
            self._authzee_async.list_context_defs(
                page_ref=page_ref,
                config=config
            )
        )


    def get_context_def(
        self, 
        context_type: str, 
        config: AuthzeeConfig | None = None
    ) -> ContextDefResult:
        """Retrieve a single context definition by its context_type.

        Parameters
        ----------
        context_type : str
            The unique context type identifier.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.get_context_def(
            context_type="NONE",
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        ContextDefResult
            ```python
            {
                "context_def": {  # dict | None
                    "context_type": "NONE",
                    "schema": {
                        "type": "object",
                        "additionalProperties": False
                    }
                },
                "has_failed": False,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        ResourceNotFoundError
            If the context definition is not found.
        """
        return asyncio.run(
            self._authzee_async.get_context_def(
                context_type=context_type,
                config=config
            )
        )


    def put_context_def(
        self, 
        context_def: ContextDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Create or update a context definition.

        Parameters
        ----------
        context_def : ContextDef
            The context definition to store.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.put_context_def(
            context_def={
                "context_type": "NONE",
                "schema": {
                    "type": "object",
                    "additionalProperties": False
                }
            },
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "definition": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        DefinitionError
            If the context definition is invalid and raise_crits is True.
        """
        return asyncio.run(
            self._authzee_async.put_context_def(
                context_def=context_def,
                config=config
            )
        )


    def delete_context_def(
        self, 
        context_type: str, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Deletes the context definition if found.

        Parameters
        ----------
        context_type : str
            The unique context type identifier to delete.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.delete_context_def(
            context_type="NONE",
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        DeleteError
            If a critical error occurs during deletion.
        """
        return asyncio.run(
            self._authzee_async.delete_context_def(
                context_type=context_type,
                config=config
            )
        )


    def validate_identity_def(
        self,
        identity_def: IdentityDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate an identity definition without storing it.

        Parameters
        ----------
        identity_def : IdentityDef
            The identity definition to validate.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.validate_identity_def(
            identity_def={
                "identity_type": "user",
                "schema": {
                    "type": "object",
                    "required": [
                        "username"
                    ],
                    "properties": {
                        "username": {
                            "type": "string"
                        }
                    }
                }
            },
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "definition": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        DefinitionError
            If the identity definition is invalid and raise_crits is True.
        """
        return asyncio.run(
            self._authzee_async.validate_identity_def(
                identity_def=identity_def,
                config=config
            )
        )


    def list_identity_defs(
        self, 
        page_ref: str | None = None,
        config: AuthzeeConfig | None = None
    ) -> IdentityDefsPage:
        """Retrieve a single page of identity definitions.

        Parameters
        ----------
        page_ref : str | None, optional
            Page reference for pagination. None for the first page.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.list_identity_defs(
            page_ref="abc123",  # optional - str | None
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        With paginator:
        ```python
        from authzee import paginator

        # Assumes authz is an Authzee instance
        for page in paginator(authz.list_identity_defs):
            for identity_def in page['identity_defs']:
                print(identity_def['identity_type'])
        ```

        Returns
        -------
        IdentityDefsPage
            ```python
            {
                "identity_defs": [
                    {
                        "identity_type": "user",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "username": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                ],
                "next_page_ref": None,  # str | None - None means pagination is complete
                "has_failed": False,
                "errors": {
                    "page_reference": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        PageReferenceError
            If the page reference is invalid.
        """
        return asyncio.run(
            self._authzee_async.list_identity_defs(
                page_ref=page_ref,
                config=config
            )
        )


    def get_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig | None = None
    ) -> IdentityDefResult:
        """Retrieve a single identity definition by its identity_type.

        Parameters
        ----------
        identity_type : str
            The unique identity type identifier.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.get_identity_def(
            identity_type="user",
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        IdentityDefResult
            ```python
            {
                "identity_def": {  # dict | None
                    "identity_type": "user",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "username": {
                                "type": "string"
                            }
                        }
                    }
                },
                "has_failed": False,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        ResourceNotFoundError
            If the identity definition is not found.
        """
        return asyncio.run(
            self._authzee_async.get_identity_def(
                identity_type=identity_type,
                config=config
            )
        )


    def put_identity_def(
        self, 
        identity_def: IdentityDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Create or update an identity definition.

        Parameters
        ----------
        identity_def : IdentityDef
            The identity definition to store.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.put_identity_def(
            identity_def={
                "identity_type": "user",
                "schema": {
                    "type": "object",
                    "required": [
                        "username"
                    ],
                    "properties": {
                        "username": {
                            "type": "string"
                        }
                    }
                }
            },
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "definition": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        DefinitionError
            If the identity definition is invalid and raise_crits is True.
        """
        return asyncio.run(
            self._authzee_async.put_identity_def(
                identity_def=identity_def,
                config=config
            )
        )


    def delete_identity_def(
        self, 
        identity_type: str,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Deletes the identity definition if found.

        Parameters
        ----------
        identity_type : str
            The unique identity type identifier to delete.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.delete_identity_def(
            identity_type="user",
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        DeleteError
            If a critical error occurs during deletion.
        """
        return asyncio.run(
            self._authzee_async.delete_identity_def(
                identity_type=identity_type,
                config=config
            )
        )


    def validate_resource_def(
        self,
        resource_def: ResourceDef, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a resource definition without storing it.

        Parameters
        ----------
        resource_def : ResourceDef
            The resource definition to validate.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.validate_resource_def(
            resource_def={
                "resource_type": "balloon",
                "actions": [
                    "balloon:read",
                    "balloon:inflate",
                    "balloon:pop"
                ],
                "schema": {
                    "type": "object",
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
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "definition": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        DefinitionError
            If the resource definition is invalid and raise_crits is True.
        """
        return asyncio.run(
            self._authzee_async.validate_resource_def(
                resource_def=resource_def,
                config=config
            )
        )


    def list_resource_defs(
        self, 
        page_ref: str | None = None,
        config: AuthzeeConfig | None = None
    ) -> ResourceDefsPage:
        """Retrieve a single page of resource definitions.

        Parameters
        ----------
        page_ref : str | None, optional
            Page reference for pagination. None for the first page.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.list_resource_defs(
            page_ref="abc123",  # optional - str | None
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        With paginator:
        ```python
        from authzee import paginator

        # Assumes authz is an Authzee instance
        for page in paginator(authz.list_resource_defs):
            for resource_def in page['resource_defs']:
                print(resource_def['resource_type'])
        ```

        Returns
        -------
        ResourceDefsPage
            ```python
            {
                "resource_defs": [
                    {
                        "resource_type": "balloon",
                        "actions": [
                            "balloon:read",
                            "balloon:inflate"
                        ],
                        "schema": {
                            "type": "object",
                            "properties": {
                                "color": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                ],
                "next_page_ref": None,  # str | None - None means pagination is complete
                "has_failed": False,
                "errors": {
                    "page_reference": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        PageReferenceError
            If the page reference is invalid.
        """
        return asyncio.run(
            self._authzee_async.list_resource_defs(
                page_ref=page_ref,
                config=config
            )
        )


    def get_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig | None = None
    ) -> ResourceDefResult:
        """Retrieve a single resource definition by its resource_type.

        Parameters
        ----------
        resource_type : str
            The unique resource type identifier.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.get_resource_def(
            resource_type="balloon",
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        ResourceDefResult
            ```python
            {
                "resource_def": {  # dict | None
                    "resource_type": "balloon",
                    "actions": [
                        "balloon:read",
                        "balloon:inflate"
                    ],
                    "schema": {
                        "type": "object",
                        "properties": {
                            "color": {
                                "type": "string"
                            }
                        }
                    }
                },
                "has_failed": False,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        ResourceNotFoundError
            If the resource definition is not found.
        """
        return asyncio.run(
            self._authzee_async.get_resource_def(
                resource_type=resource_type,
                config=config
            )
        )


    def put_resource_def(
        self, 
        resource_def: ResourceDef,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Create or update a resource definition.

        Parameters
        ----------
        resource_def : ResourceDef
            The resource definition to store.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.put_resource_def(
            resource_def={
                "resource_type": "balloon",
                "actions": [
                    "balloon:read",
                    "balloon:inflate",
                    "balloon:pop"
                ],
                "schema": {
                    "type": "object",
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
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "definition": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        DefinitionError
            If the resource definition is invalid and raise_crits is True.
        """
        return asyncio.run(
            self._authzee_async.put_resource_def(
                resource_def=resource_def,
                config=config
            )
        )


    def delete_resource_def(
        self, 
        resource_type: str,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Deletes the resource definition if found.

        Parameters
        ----------
        resource_type : str
            The unique resource type identifier to delete.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.delete_resource_def(
            resource_type="balloon",
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        DeleteError
            If a critical error occurs during deletion.
        """
        return asyncio.run(
            self._authzee_async.delete_resource_def(
                resource_type=resource_type,
                config=config
            )
        )


    def validate_grant(
        self, 
        grant: Grant,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate a grant without storing it.

        Parameters
        ----------
        grant : Grant
            The grant to validate.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.validate_grant(
            grant={
                "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                "name": "Allow inflate",
                "description": "Allow balloon inflate for users.",
                "tags": {
                    "team": "balloon"
                },
                "effect": "allow",  # "allow" | "deny"
                "actions": [
                    "balloon:inflate"
                ],
                "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                "evaluation_handler": "evaluate",  # "evaluate" | "error" | "critical"
                "equality": True,  # bool | str | int | float | None | list | dict
                "data": {}
            },
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "grant": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        GrantError
            If the grant is invalid and raise_crits is True.
        """
        return asyncio.run(
            self._authzee_async.validate_grant(
                grant=grant,
                config=config
            )
        )


    def enact(
        self, 
        grant: Grant,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Enact (store) a grant to create an authorization rule.

        Parameters
        ----------
        grant : Grant
            The grant to enact.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.enact(
            grant={
                "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                "name": "Allow inflate",
                "description": "Allow balloon inflate for users.",
                "tags": {
                    "team": "balloon"
                },
                "effect": "allow",  # "allow" | "deny"
                "actions": [
                    "balloon:inflate"
                ],
                "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                "evaluation_handler": "evaluate",  # "evaluate" | "error" | "critical"
                "equality": True,  # bool | str | int | float | None | list | dict
                "data": {}
            },
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "grant": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        GrantError
            If the grant is invalid and raise_crits is True.
        """
        return asyncio.run(
            self._authzee_async.enact(
                grant=grant,
                config=config
            )
        )


    def repeal(
        self, 
        grant_uuid: str, 
        purge: bool = False,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Repeal (remove) a grant by its UUID.

        Parameters
        ----------
        grant_uuid : str
            The UUID of the grant to repeal.
        purge : bool, default=False
            If True, all grants and partitions may be scanned to completely remove. 
            Useful if corruption by update is suspected.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.repeal(
            grant_uuid="0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            purge=False, # optional
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": True,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": True,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        ResourceNotFoundError
            If the grant is not found.
        """
        return asyncio.run(
            self._authzee_async.repeal(
                grant_uuid=grant_uuid,
                purge=purge,
                config=config
            )
        )


    def get_grant(
        self, 
        grant_uuid: str,
        config: AuthzeeConfig | None = None
    ) -> GrantResult:
        """Retrieve a single grant by its UUID.

        Parameters
        ----------
        grant_uuid : str
            The UUID of the grant to retrieve.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.get_grant(
            grant_uuid="0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GrantResult
            ```python
            {
                "grant": {  # dict | None
                    "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                    "name": "Allow inflate",
                    "description": "Allow balloon inflate for users.",
                    "tags": {
                        "team": "balloon"
                    },
                    "effect": "allow",
                    "actions": [
                        "balloon:inflate"
                    ],
                    "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                    "evaluation_handler": "evaluate",
                    "equality": True,
                    "data": {}
                },
                "has_failed": False,
                "errors": {
                    "resource_not_found": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        ResourceNotFoundError
            If the grant is not found.
        """
        return asyncio.run(
            self._authzee_async.get_grant(
                grant_uuid=grant_uuid,
                config=config
            )
        )


    def list_grants(
        self,
        effect: str | None = None, 
        action: str | None = None, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> GrantsPage:
        """Retrieve a single page of grants with optional filtering.

        Parameters
        ----------
        effect : str | None, optional
            Filter by grant effect. "allow" or "deny".
        action : str | None, optional
            Filter by action string.
        page_ref : str | None, optional
            Page reference for pagination. None for the first page.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.list_grants(
            effect="allow",  # optional - str | None - "allow" | "deny"
            action="balloon:inflate",  # optional - str | None
            page_ref="abc123",  # optional - str | None
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        With paginator:
        ```python
        from authzee import paginator

        # Assumes authz is an Authzee instance
        for page in paginator(authz.list_grants, effect="allow"):
            for grant in page['grants']:
                print(grant['grant_uuid'])
        ```

        Returns
        -------
        GrantsPage
            ```python
            {
                "grants": [
                    {
                        "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                        "name": "Allow inflate",
                        "description": "Allow balloon inflate for users.",
                        "tags": {
                            "team": "balloon"
                        },
                        "effect": "allow",
                        "actions": [
                            "balloon:inflate"
                        ],
                        "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                        "evaluation_handler": "evaluate",
                        "equality": True,
                        "data": {}
                    }
                ],
                "next_page_ref": None,  # str | None - None means pagination is complete
                "has_failed": False,
                "errors": {
                    "page_reference": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        PageReferenceError
            If the page reference is invalid.
        """
        return asyncio.run(
            self._authzee_async.list_grants(
                effect=effect,
                action=action,
                page_ref=page_ref,
                config=config
            )
        )


    def list_grant_refs(
        self,
        effect: str | None = None, 
        action: str | None = None, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> PageRefsPage:
        """Retrieve a page of grant page references for parallel pagination.

        Parameters
        ----------
        effect : str | None, optional
            Filter by grant effect. "allow" or "deny".
        action : str | None, optional
            Filter by action string.
        page_ref : str | None, optional
            Page reference for pagination. None for the first page.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.list_grant_refs(
            effect="allow",  # optional - str | None - "allow" | "deny"
            action="balloon:inflate",  # optional - str | None
            page_ref="abc123",  # optional - str | None
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        With paginator:
        ```python
        from authzee import paginator

        # Assumes authz is an Authzee instance
        for page in paginator(authz.list_grant_refs, effect="allow"):
            for ref in page['page_refs']:
                print(ref)
        ```

        Returns
        -------
        PageRefsPage
            ```python
            {
                "page_refs": [
                    "page_ref_1",
                    "page_ref_2"
                ],
                "next_page_ref": None,  # str | None - None means pagination is complete
                "has_failed": False,
                "errors": {
                    "page_reference": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        PageReferenceError
            If the page reference is invalid.
        ParallelPaginationNotSupported
            If the storage backend does not support parallel pagination.
        """
        return asyncio.run(
            self._authzee_async.list_grant_refs(
                effect=effect,
                action=action,
                page_ref=page_ref,
                config=config
            )
        )


    def cleanup_latches(
        self, 
        before: datetime.datetime, 
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Clean up storage latches created before the given datetime.

        Operations should clean up their own latches, but in case of a failure
        this can be used to clean up zombie latches.

        Parameters
        ----------
        before : datetime.datetime
            Delete latches created before this datetime.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        import datetime

        # Assumes authz is an Authzee instance
        result = authz.cleanup_latches(
            before=datetime.datetime(2026, 1, 1),
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        GenericResult
            ```python
            {
                "has_failed": False,
                "errors": {
                    "start": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        StartError
            If a critical error occurs during cleanup.
        """
        return asyncio.run(
            self._authzee_async.cleanup_latches(
                before=before,
                config=config
            )
        )


    def audit(
        self,
        request: AuthzeeRequest, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> AuditResultPage:
        """Retrieve a page of audit results showing how each grant evaluated against the request.

        Parameters
        ----------
        request : AuthzeeRequest
            The authorization request to audit.
        page_ref : str | None, optional
            Page reference for pagination. None for the first page.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.audit(
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
                    "color": "blue",
                    "is_inflated": False
                },
                "evaluation_handler": "grant",  # "grant" | "evaluate" | "error" | "critical"
                "context_type": "NONE",
                "context": {}
            },
            page_ref="abc123",  # optional - str | None
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        With paginator:
        ```python
        from authzee import paginator

        for page in paginator(
        # Assumes authz is an Authzee instance
            authz.audit,
            request={ F
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
                    "color": "blue",
                    "is_inflated": False
                },
                "evaluation_handler": "grant",
                "context_type": "NONE",
                "context": {}
            }
        ):
            for grant, result in zip(page['grants'], page['results']):
                print(grant['grant_uuid'], result['is_applicable'])
        ```

        Returns
        -------
        AuditResultPage
            ```python
            {
                "grants": [
                    {
                        "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                        "name": "Allow inflate",
                        "description": "Allow balloon inflate for users.",
                        "tags": {},
                        "effect": "allow",
                        "actions": [
                            "balloon:inflate"
                        ],
                        "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                        "evaluation_handler": "evaluate",
                        "equality": True,
                        "data": {}
                    }
                ],
                "results": [
                    {
                        "is_applicable": True,
                        "query_result": True,
                        "errors": {}
                    }
                ],
                "next_page_ref": None,  # str | None - None means pagination is complete
                "has_failed": False,
                "errors": {
                    "request": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        RequestError
            If the request is invalid and raise_crits is True.
        EvaluationError
            If a critical evaluation error occurs and raise_crits is True.
        PageReferenceError
            If the page reference is invalid.
        """
        return asyncio.run(
            self._authzee_async.audit(
                request=request,
                page_ref=page_ref,
                config=config
            )
        )


    def authorize(
        self, 
        request: AuthzeeRequest,
        config: AuthzeeConfig | None = None
    ) -> AuthorizeResult:
        """Determine if the request is authorized.

        Parameters
        ----------
        request : AuthzeeRequest
            The authorization request to evaluate.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.authorize(
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
                    "color": "blue",
                    "is_inflated": False
                },
                "evaluation_handler": "grant",  # "grant" | "evaluate" | "error" | "critical"
                "context_type": "NONE",
                "context": {}
            },
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        AuthorizeResult
            ```python
            {
                "is_authorized": True,
                "grant": {  # dict | None
                    "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                    "name": "Allow inflate",
                    "description": "Allow balloon inflate for users.",
                    "tags": {},
                    "effect": "allow",
                    "actions": [
                        "balloon:inflate"
                    ],
                    "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                    "evaluation_handler": "evaluate",
                    "equality": True,
                    "data": {}
                },
                "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
                "has_failed": False,
                "critical_errors": {
                    "evaluation": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        RequestError
            If the request is invalid and raise_crits is True.
        EvaluationError
            If a critical evaluation error occurs and raise_crits is True.
        """
        return asyncio.run(
            self._authzee_async.authorize(
                request=request,
                config=config
            )
        )


    def batch_audit(
        self,
        batch_request: AuthzeeBatchRequest, 
        page_ref: str | None = None, 
        config: AuthzeeConfig | None = None
    ) -> BatchAuditResultPage:
        """Retrieve a page of batch audit results showing how each grant evaluated against the batch request.

        Parameters
        ----------
        batch_request : AuthzeeBatchRequest
            The batch authorization request to audit.
        page_ref : str | None, optional
            Page reference for pagination. None for the first page.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.batch_audit(
            batch_request={
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
                    "color": "blue",
                    "is_inflated": False
                },
                "evaluation_handler": "grant",  # "grant" | "evaluate" | "error" | "critical"
                "context_type": "NONE",
                "context": {},
                "batch": [
                    {
                        "resource": {  # optional - dict | None
                            "color": "red",
                            "is_inflated": True
                        }
                    }
                ]
            },
            page_ref="abc123",  # optional - str | None
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        With paginator:
        ```python
        from authzee import paginator

        for page in paginator(
        # Assumes authz is an Authzee instance
            authz.batch_audit,
            batch_request={
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
                    "color": "blue",
                    "is_inflated": False
                },
                "evaluation_handler": "grant",
                "context_type": "NONE",
                "context": {},
                "batch": [
                    {
                        "resource": {
                            "color": "red",
                            "is_inflated": True
                        }
                    }
                ]
            }
        ):
            for batch_result in page['batch_results']:
                for result_item in batch_result['results']:
                    print(result_item['is_applicable'])
        ```

        Returns
        -------
        BatchAuditResultPage
            ```python
            {
                "grants": [
                    {
                        "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                        "name": "Allow inflate",
                        "description": "Allow balloon inflate for users.",
                        "tags": {},
                        "effect": "allow",
                        "actions": [
                            "balloon:inflate"
                        ],
                        "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                        "evaluation_handler": "evaluate",
                        "equality": True,
                        "data": {}
                    }
                ],
                "batch_results": [
                    {
                        "results": [
                            {
                                "is_applicable": True,
                                "query_result": True,
                                "errors": {}
                            }
                        ],
                        "has_failed": False,
                        "errors": {}
                    }
                ],
                "next_page_ref": None,  # str | None - None means pagination is complete
                "has_failed": False,
                "errors": {
                    "request": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        RequestError
            If the batch request is invalid and raise_crits is True.
        EvaluationError
            If a critical evaluation error occurs and raise_crits is True.
        PageReferenceError
            If the page reference is invalid.
        """
        return asyncio.run(
            self._authzee_async.batch_audit(
                batch_request=batch_request,
                page_ref=page_ref,
                config=config
            )
        )


    def batch_authorize(
        self, 
        batch_request: AuthzeeBatchRequest,
        config: AuthzeeConfig | None = None
    ) -> BatchAuthorizeResult:
        """Determine if each item in the batch request is authorized.

        Parameters
        ----------
        batch_request : AuthzeeBatchRequest
            The batch authorization request to evaluate.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an Authzee instance
        result = authz.batch_authorize(
            batch_request={
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
                    "color": "blue",
                    "is_inflated": False
                },
                "evaluation_handler": "grant",  # "grant" | "evaluate" | "error" | "critical"
                "context_type": "NONE",
                "context": {},
                "batch": [
                    {
                        "resource": {  # optional - dict | None
                            "color": "red",
                            "is_inflated": True
                        }
                    }
                ]
            },
            config={  # optional - AuthzeeConfig | None - All keys are optional
                "context_defs_page_size": 100,
                "identity_defs_page_size": 100,
                "resource_defs_page_size": 100,
                "grants_page_size": 100,
                "grant_refs_page_size": 10,
                "authorize_parallel_paging": True,
                "batch_authorize_parallel_paging": True,
                "raise_crits": True
            }
        )
        ```

        Returns
        -------
        BatchAuthorizeResult
            ```python
            {
                "batch_results": [
                    {
                        "is_authorized": True,
                        "grant": {
                            "grant_uuid": "0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
                            "name": "Allow inflate",
                            "description": "Allow balloon inflate for users.",
                            "tags": {},
                            "effect": "allow",
                            "actions": [
                                "balloon:inflate"
                            ],
                            "query": "length(request.identities.user[?department == 'Balloon Dept']) > `0`",
                            "evaluation_handler": "evaluate",
                            "equality": True,
                            "data": {}
                        },
                        "message": "Authorized by grant.",
                        "has_failed": False,
                        "critical_errors": {}
                    },
                    {
                        "is_authorized": False,
                        "grant": None,
                        "message": "No matching allow grants.",
                        "has_failed": False,
                        "critical_errors": {}
                    }
                ],
                "has_failed": False,
                "critical_errors": {
                    "evaluation": [
                        {
                            "is_critical": False,
                            "message": "Error message."
                        }
                    ]
                }
            }
            ```

        Raises
        ------
        RequestError
            If the batch request is invalid and raise_crits is True.
        EvaluationError
            If a critical evaluation error occurs and raise_crits is True.
        """
        return asyncio.run(
            self._authzee_async.batch_authorize(
                batch_request=batch_request,
                config=config
            )
        )
