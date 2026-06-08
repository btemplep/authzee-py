"""See {py:class}`authzee.authzee_async.AuthzeeAsync`"""
__all__ = [
    "AuthzeeAsync",
]

from asyncio import gather
import datetime
from typing import Any, Callable, Dict, Type

from authzee.types import *
from authzee.exceptions import *
from authzee import core
from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule

from authzee.module_locality import locality_compatibility


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
_default_config: AuthzeeConfig = {
    "authzee": {
        "raise_crits": True
    },
    "list_context_defs": {
        "page_size": 100,
        "cache_ttl": 120
    },
    "list_identity_defs": {
        "page_size": 100
    },
    "list_resource_defs": {
        "page_size": 100
    },
    "list_grants": {
        "page_size": 100
    },
    "list_grant_refs": {
        "page_size": 100
    },
    "validate_request": {
        "list_context_defs": False,
        "context_defs_page_size": 100,
        "list_identity_defs": False,
        "identity_defs_page_size": 100,
        "list_resource_defs": False,
        "resource_defs_page_size": 100,
    },
    "validate_batch_request": {
        "list_context_defs": False,
        "context_defs_page_size": 100,
        "list_identity_defs": False,
        "identity_defs_page_size": 100,
        "list_resource_defs": False,
        "resource_defs_page_size": 100,
    },
    "audit": {
        "grants_page_size": 100,
    },
    "batch_audit": {
        "grants_page_size": 100
    },
    "authorize": {
        "grants_page_size": 100,
        "grant_refs_page_size": 10,
        "parallel_paging": True
    },
    "batch_authorize": {
        "grants_page_size": 100,
        "grant_refs_page_size": 10,
        "parallel_paging": True
    }
}


class AuthzeeAsync:
    """Authzee async application.

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
    import asyncio
    import json
    from uuid import uuid4

    from authzee import AuthzeeAsync, DictStorage, InProcessCompute, jmespath_execute


    async def main():
        storage_dict = {}
        authz = AuthzeeAsync(
            execute=jmespath_execute,
            compute_type=InProcessCompute,
            compute_kwargs={},
            storage_type=DictStorage,
            storage_kwargs={
                "storage_dict": storage_dict
            }
        )
        await authz.construct() # one time setup for life of storage and compute
        await authz.start() # initialize the authzee app - must be run once for every instance
        await authz.put_context_def(
            {
                "context_type": "NONE",
                "schema": {
                    "type": "object",
                    "additionalProperties": False
                }
            }
        )
        await authz.put_identity_def(
            identity_def={
                "identity_type": "user",
                "schema": {
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
        await authz.put_resource_def(
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
            }
        )
        await authz.enact(
            grant={
                "grant_uuid": str(uuid4()),
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
                "equality": True,
                "data": {}
            }
        )
        result = await authz.authorize(
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
                    "color": "inflated",
                    "is_inflated": False
                },
                "evaluation_handler": "grant",
                "context_type": "NONE",
                "context": {}
            }
        )
        print(json.dumps(result, indent=4))

    asyncio.run(main())
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
        self._execute = execute
        self._compute_type = compute_type
        self._compute_kwargs = compute_kwargs
        self._storage_type = storage_type
        self._storage_kwargs = storage_kwargs
        self._compute_storage_kwargs = storage_kwargs if compute_storage_kwargs is None else storage_kwargs | compute_storage_kwargs
        self._config = _default_config if config is None else _default_config | config
        self._compute_config = self._config if compute_config is None else self._config | compute_config
        self._compute: ComputeModule = None
        self._storage: StorageModule = None
    

    def _raise_result(self, result: GenericResult, config: AuthzeeConfig) -> None:
        if config['raise_crits'] is True and result['has_failed'] is True:
            if "critical_errors" in result:
                errors = result['critical_errors']
            else:
                errors = result['errors']

            for error_type in errors:
                for err in errors[error_type]:
                    if err['is_critical']:
                        raise _exception_map[error_type](
                            message=err['message'],
                            result=result
                        )


    def _combine_errors(self, result: GenericResult, *args: dict) ->  None:
        errors = result['errors']
        for new_result in args:
            if new_result['has_failed'] is True:
                result['has_failed'] = True

            new_errors = new_result['errors']
            for k in errors:
                if k in new_errors:
                    errors[k] += new_errors[k]
                
            for k in new_errors:
                if k not in errors:
                    errors[k] = new_errors[k]

    
    async def start(self, config: AuthzeeConfig | None = None) -> GenericResult:
        """Initialize the authzee app. Must be run once for every instance.

        Parameters
        ----------
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.start(
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
        config = self._config if config is None else self._config | config
        self._compute = self._compute_type(**self._compute_kwargs)
        self._storage = self._storage_type(**self._storage_kwargs)
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_results, storage_result = await gather(
            self._compute.start(
                execute=self._execute,
                storage_type=self._storage_type,
                storage_kwargs=self._compute_storage_kwargs,
                config=config
            ),
            self._storage.start(config)
        )
        core.combine_errors(result, compute_results, storage_result)
        self._raise_result(result, config)

        if self._storage.locality not in locality_compatibility[self._compute.locality]:
            result['errors']['locality_incompatibility'] = [
                {
                    "is_critical": False,
                    "message": f"The '{self._storage.locality}' storage locality is not compatible with the '{self._compute.locality}' compute locality."
                }
            ]

        return result


    async def shutdown(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.shutdown(
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
        config = self._config if config is None else self._config | config
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_result, storage_result = await gather(
            self._compute.shutdown(config),
            self._storage.shutdown(config)
        )
        core.combine_errors(result, compute_result, storage_result)
        self._raise_result(result, config)

        return result
        

    async def construct(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.construct(
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
        config = self._config if config is None else self._config | config
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute = self._compute_type(**self._compute_kwargs)
        storage = self._storage_type(**self._storage_kwargs)
        compute_result, storage_result = await gather(
            compute.construct(config),
            storage.construct(config)
        )
        core.combine_errors(result, compute_result, storage_result)
        self._raise_result(result, config)

        return result


    async def destroy(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.destroy(
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
        config = self._config if config is None else self._config | config
        result = {
            "has_failed": False,
            "errors": {}
        }
        compute_result, storage_result = await gather(
            self._compute.destroy(config),
            self._storage.destroy(config)
        )
        core.combine_errors(result, compute_result, storage_result)
        self._raise_result(result, config)

        return result


    async def validate_context_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.validate_context_def(
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
        config = self._compute_config if config is None else self._compute_config | config
        result = await self._compute.validate_context_def(
            context_def=context_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def list_context_defs(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.list_context_defs(
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

        With async paginator:
        ```python
        from authzee import async_paginator

        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        async for page in async_paginator(authz.list_context_defs):
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
        config = self._config if config is None else self._config | config
        result =  await self._storage.list_context_defs(
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result
    


    async def get_context_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.get_context_def(
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
        config = self._config if config is None else self._config | config
        result = await self._storage.get_context_def(
            context_type=context_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def put_context_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.put_context_def(
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
        valid_result = await self.validate_context_def(
            context_def=context_def,
            config=config
        )
        if valid_result['has_failed'] is True:
            return valid_result

        config = self._config if config is None else self._config | config
        result = await self._storage.put_context_def(
            context_def=context_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def delete_context_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.delete_context_def(
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
        config = self._config if config is None else self._config | config
        result = await self._storage.delete_context_def(
            context_type=context_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def validate_identity_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.validate_identity_def(
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
        config = self._compute_config if config is None else self._compute_config | config
        result = await self._compute.validate_identity_def(
            identity_def=identity_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def list_identity_defs(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.list_identity_defs(
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

        With async paginator:
        ```python
        from authzee import async_paginator

        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        async for page in async_paginator(authz.list_identity_defs):
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
        config = self._config if config is None else self._config | config
        result = await self._storage.list_identity_defs(
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_identity_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.get_identity_def(
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
        config = self._config if config is None else self._config | config
        result = await self._storage.get_identity_def(
            identity_type=identity_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def put_identity_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.put_identity_def(
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
        valid_result = await self.validate_identity_def(
            identity_def=identity_def,
            config=config
        )
        if valid_result['has_failed'] is True:
            return valid_result

        config = self._config if config is None else self._config | config
        result = await self._storage.put_identity_def(
            identity_def=identity_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def delete_identity_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.delete_identity_def(
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
        config = self._config if config is None else self._config | config
        result = await self._storage.delete_identity_def(
            identity_type=identity_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def validate_resource_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.validate_resource_def(
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
        config = self._compute_config if config is None else self._compute_config | config
        result = await self._compute.validate_resource_def(
            resource_def=resource_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def list_resource_defs(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.list_resource_defs(
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

        With async paginator:
        ```python
        from authzee import async_paginator

        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        async for page in async_paginator(authz.list_resource_defs):
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
        config = self._config if config is None else self._config | config
        result = await self._storage.list_resource_defs(
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_resource_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.get_resource_def(
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
        config = self._config if config is None else self._config | config
        result = await self._storage.get_resource_def(
            resource_type=resource_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result
    
    
    async def put_resource_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.put_resource_def(
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
        valid_result = await self.validate_resource_def(
            resource_def=resource_def,
            config=config
        )
        if valid_result['has_failed'] is True:
            return valid_result

        config = self._config if config is None else self._config | config
        result = await self._storage.put_resource_def(
            resource_def=resource_def,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def delete_resource_def(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.delete_resource_def(
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
        config = self._config if config is None else self._config | config
        result = await self._storage.delete_resource_def(
            resource_type=resource_type,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def validate_grant(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.validate_grant(
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
        config = self._compute_config if config is None else self._compute_config | config
        result = await self._compute.validate_grant(
            grant=grant,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def enact(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.enact(
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
        valid_result = await self.validate_grant(
            grant=grant,
            config=config
        )
        if valid_result['has_failed'] is True:
            return valid_result

        config = self._config if config is None else self._config | config
        result = await self._storage.enact(
            grant=grant,
            config=config
        )
        self._raise_result(result, config)
        
        return result

        
    async def repeal(
        self, 
        grant_uuid: str, 
        purge: bool,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Repeal (remove) a grant by its UUID.

        Parameters
        ----------
        grant_uuid : str
            The UUID of the grant to repeal.
        purge : bool
            If True, permanently delete the grant. If False, soft-delete.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.

        Examples
        --------
        ```python
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.repeal(
            grant_uuid="0da5dfc6-c919-4bd6-b80f-a351a9ac8d27",
            purge=True,
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
        ResourceNotFoundError
            If the grant is not found.
        """
        config = self._config if config is None else self._config | config
        result = await self._storage.repeal(
            grant_uuid=grant_uuid,
            purge=purge,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def get_grant(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.get_grant(
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
        config = self._config if config is None else self._config | config
        result = await self._storage.get_grant(
            grant_uuid=grant_uuid,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def list_grants(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.list_grants(
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

        With async paginator:
        ```python
        from authzee import async_paginator

        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        async for page in async_paginator(authz.list_grants, effect="allow"):
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
        config = self._config if config is None else self._config | config
        result = await self._storage.list_grants(
            effect=effect,
            action=action,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def list_grant_refs(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.list_grant_refs(
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

        With async paginator:
        ```python
        from authzee import async_paginator

        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        async for page in async_paginator(authz.list_grant_refs, effect="allow"):
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
        config = self._config if config is None else self._config | config
        result = await self._storage.list_grant_refs(
            effect=effect,
            action=action,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def cleanup_latches(
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

        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.cleanup_latches(
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
        config = self._config if config is None else self._config | config
        result = await self._storage.cleanup_latches(
            before=before,
            config=config
        )
        self._raise_result(result, config)
        
        return result
        
    
    async def validate_request(
        self,
        request: AuthzeeRequest,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate the Authzee Request.
        
         Parameters
        ----------
        request : AuthzeeRequest
            The request to validate.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.
        
        Examples
        --------
        ```python

        ```
        Returns
        -------
        GenericResult
             ```python
            {
                "has_failed": True,
                "errors": {
                    "start": [
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
        RequestError
            Error when validating the request.

    
        """
        config = self._config if config is None else self._config | config
        result = await self._compute.validate_request(
            request=request,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def audit(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.audit(
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

        With async paginator:
        ```python
        from authzee import async_paginator

        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        async for page in async_paginator(
            authz.audit,
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
        config = self._compute_config if config is None else self._compute_config | config
        valid_result = await self._compute.validate_request(
            request=request,
            config=config | {"raise_crits": False}
        )
        if valid_result['has_failed'] is True:
            result = {
                "grants": [],
                "results": [],
                "next_page_ref": None,
                "has_failed": True,
                "errors": valid_result['errors']
            }
            self._raise_result(result, config)
            
            return result

        result = await self._compute.audit(
            request=request,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    def _get_critical_errors(self, errors: ResultErrors) -> ResultErrors:
        critical_errors = {}
        for et in errors:
            for error in errors[et]:
                if error['is_critical']:
                    if et not in critical_errors:
                        critical_errors[et] = []
                    
                    critical_errors[et].append(error)
        
        return critical_errors


    async def authorize(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.authorize(
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
        config = self._compute_config if config is None else self._compute_config | config
        valid_result = await self._compute.validate_request(
            request=request,
            config=config | {"raise_crits": False}
        )
        
        if valid_result['has_failed'] is True:
            result = {
                "is_authorized": False,
                "grant": None,
                "message": "A critical error has occurred. Therefore, the request is not authorized.",
                "has_failed": valid_result['has_failed'],
                "critical_errors": self._get_critical_errors(valid_result['errors'])
            }
            self._raise_result(valid_result, config)
            
            return result

        result = await self._compute.authorize(
            request=request,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def validate_batch_request(
        self,
        batch_request: AuthzeeBatchRequest,
        config: AuthzeeConfig | None = None
    ) -> GenericResult:
        """Validate the Authzee Request.
        
         Parameters
        ----------
        batch_request : AuAuthzeeBatAuthzeeBatchRequestchRequestthzeeRequest
            The batch request to validate.
        config : AuthzeeConfig | None, optional
            Override configuration for this call. Only include keys to override.
        
        Examples
        --------
        ```python

        ```
        Returns
        -------
        GenericResult
             ```python
            {
                "has_failed": True,
                "errors": {
                    "start": [
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
        RequestError
            Error when validating the request.
        """
        config = self._config if config is None else self._config | config
        result = await self._compute.validate_batch_request(
            batch_request=batch_request,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def batch_audit(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.batch_audit(
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

        With async paginator:
        ```python
        from authzee import async_paginator

        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        async for page in async_paginator(
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
        config = self._compute_config if config is None else self._compute_config | config
        valid_result = await self._compute.validate_batch_request(
            batch_request=batch_request,
            config=config | {"raise_crits": False}
        )
        if valid_result['has_failed'] is True:
            result = {
                "grants": [],
                "batch_results": [],
                "next_page_ref": None,
                "has_failed": True,
                "errors": valid_result['errors']
            }
            self._raise_result(result, config)
            
            return result
    
        result = await self._compute.batch_audit(
            batch_request=batch_request,
            page_ref=page_ref,
            config=config
        )
        self._raise_result(result, config)
        
        return result


    async def batch_authorize(
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
        # Assumes authz is an AuthzeeAsync instance and this is in a running event loop
        result = await authz.batch_authorize(
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
        config = self._compute_config if config is None else self._compute_config | config
        valid_result = await self._compute.validate_batch_request(
            batch_request=batch_request,
            config=config | {"raise_crits": False}
        )
        if valid_result['has_failed'] is True:
            result = {
                "batch_results": [],
                "has_failed": True,
                "critical": self._get_critical_errors(valid_result['errors'])
            }
            self._raise_result(result, config)
            
            return result

        result = await self._compute.batch_authorize(
            batch_request=batch_request,
            config=config
        )
        self._raise_result(result, config)
        
        return result
        