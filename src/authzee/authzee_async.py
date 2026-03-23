
import copy
from typing import Any, AsyncIterable, Callable, Dict, List, Type
from uuid import UUID, uuid4

from authzee import core
from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule

from authzee import exceptions
from authzee.module_locality import locality_compatibility


class AuthzeeAsync:
    """Authzee application with asyncio.

    Parameters
    ----------
    identity_defs : List[dict[str]]
        Identity definitions to register.
    resource_defs : List[dict[str]]
        Resource Definitions to register.
    search : Callable[[str, Any], Any]
        JMESPath search function.
    compute_type : Type[ComputeModule]
        Compute Module Type.
    compute_kwargs : Dict[str, Any]
        Compute module KWArgs used to create instances.
    storage_type : Type[StorageModule]
        Storage Module Type. 
    storage_kwargs : Dict[str, Any]
        Storage module KWArgs used to create instances.
    grants_page_size : int
        Default page size to use for grants. 
    grant_refs_page_size : int
        Default page size for a page of grant page references.
    parallel_paging : bool
        Default setting to enable parallel pagination.
    
    Examples
    --------

    .. code-block:: python

        import asyncio

        from authzee import AuthzeeAsync, ComputeModule, StorageModule 
        # import real compute and store modules for use case
        import jmespath

        az = AuthzeeAsync(
            identity_defs=[
                {
                    "identity_type": "User", # unique identity type
                    "schema": { # JSON Schema 
                        "type": "object",
                        "properties": {
                            "id": {
                                "type": "string"
                            }
                        },
                        "required": [
                            "id"
                        ]
                    }
                }
            ],
            resource_defs=[
                {
                    "resource_type": "Balloon", # Resource types must be unique
                    "actions": [
                        "Balloon:Read", # Action types can be prefaced by a namespace - preferred so they are not shared across resources
                        "Balloon:inflate"
                    ],
                    "schema": { # JSON Schema
                        "type": "object", 
                        "properties": {
                            "color": {
                                "type": "string"
                            }
                        },
                        "required": [
                            "color"
                        ]
                    },
                    "parent_types": [], # parent resource types, if any
                    "child_types": [] # child resource types, if any
                }
            ],
            search=jmespath.search,
            compute_type=ComputeModule,
            compute_kwargs={},
            storage_type=StorageModule,
            storage_kwargs={},
            grants_page_size=100,
            grant_refs_page_size=10,
            parallel_paging=True
        )

        async def main():
            await new_grant = await az.enact(
                {
                    "name": "thing",
                    "description": "thing longer",
                    "tags": {},
                    "effect": "allow", # allow or deny
                    "actions": [ # any actions from your resources or empty to match all actions
                        "Balloon:Read",
                        "pop"
                    ],
                    "query": "request.resource.color == 'green'", # JMESPath query - Runs on {"request": <request obj>, "grant": <current grant>} and will return `true` if any of the calling entities, User type identities have the admin role
                    "query_validation": "validate",
                    "equality": True, # If the request action is in the grants actions and the query result matches this, then the grant is "applicable". 
                    "data": {},
                    "context_schema": {
                        "type": "object"
                    },
                    "context_validation": "none"
                }
            )
            request = {
                "identities": {
                    "User": [
                        {
                            "id": "user123"
                        }
                    ]
                },
                "resource_type": "Balloon",
                "action": "Balloon:Inflate",
                "resource": {
                    "color": "green"
                },
                "parents": {},
                "children": { },
                "query_validation": "error",
                "context": {
                    "timestamp": "2017-12-27T20:30:00Z",
                    "event_type": "birthday_party"
                },
                "context_validation": "grant"
            }
            resp = await az.authorize(
                request=request,
                grants_page_size=100,
                parallel_pagination=True,
                refs_page_size=100
            )
            if resp['authorized'] is True:
                print("I'm Authorized!!!")
            else:
                print("Not authorized :(")
            
            await az.shutdown() 
        
        asyncio.run(main())
    """

    def __init__(
        self, 
        execute: Callable[[str, Any], Any],
        compute_type: Type[ComputeModule],
        compute_kwargs: Dict[str, Any],
        storage_type: Type[StorageModule],
        storage_kwargs: Dict[str, Any]
    ):
        self.execute = execute
        self.compute_type = compute_type
        self.compute_kwargs = compute_kwargs
        self.storage_type = storage_type
        self.storage_kwargs= storage_kwargs


    async def start(self) -> None:
        """Initialize and start the Authzee app.

        Creates runtime resources for the compute and storage modules.

        Raises
        ------
        authzee.exceptions.DefinitionError
            The identity or resource definitions were invalid.
        authzee.exceptions.StartError
            An error occurred while initializing the app.
        """
        def_val = core.validate_definitions(
            identity_defs=self.identity_defs,
            resource_defs=self.resource_defs
        )
        if def_val['valid'] is False:
            raise exceptions.DefinitionError(
                message="Error when validating the identity or resource definitions",
                context=[],
                definition=def_val['errors'],
                grant=[],
                jmespath=[],
                request=[]
            )
        
        schemas = core.generate_schemas(
            identity_defs=self.identity_defs,
            resource_defs=self.resource_defs
        )
        self.grant_schema = schemas['grant']
        self.errors_schema = schemas['errors']
        self.request_schema = schemas['request']
        self.audit_schema = schemas['audit']
        self.authorize_schema = schemas['authorize']
        # create a storage and start
        self._storage: StorageModule = self.storage_type(**self.storage_kwargs)
        await self._storage.start(
            identity_defs=self.identity_defs,
            resource_defs=self.resource_defs
        )
        self._compute: ComputeModule = self.compute_type(**self.compute_kwargs)
        await self._compute.start(
            identity_defs=self.identity_defs,
            resource_defs=self.resource_defs,
            search=self.search,
            storage_type=self.storage_type,
            storage_kwargs=self.storage_kwargs
        )
        if self._storage.locality not in locality_compatibility[self._compute.locality]:
            raise exceptions.LocalityIncompatibility(
                (
                    f"The storage locality '{self._storage.locality}' is not compatible with the compute locality '{self._compute.locality}'. "
                    f"The compute locality is only compatible with {locality_compatibility[self._compute.locality]}."
                )
            )

        if (
            self.parallel_paging is True
            and self._storage.parallel_paging_supported is False
        ):
            raise exceptions.ParallelPaginationNotSupported("Parallel paging is set to default but the storage engine does not support it!")

    
    async def shutdown(self) -> None:
        """Clean up of resources for the authzee app.

        Shutdown and cleanup runtime resources for the compute and storage modules.

        Should be called on program shutdown.
        """
        await self._storage.shutdown()
        await self._compute.shutdown()
    

    async def construct(self) -> None:
        """One time setup for authzee app with the current configuration. 

        This method only has to be run once, and will standup resources for the compute and storage modules.
        """
        await self._storage.setup()
        await self._compute.setup()
    

    async def destroy(self) -> None:
        """Tear down all runtime resources for this instance and all storage. 

        Deletes all grants and definitions.
        """
        await self._storage.teardown()
        await self._compute.teardown()


    async def get_context_defs_page(
        self,
        page_ref: str | None, 
        page_size: int
    ) -> Any:
        pass


    async def get_context_def(self, context_type: str) -> Any:
        pass


    async def list_context_defs(self, page_size: int) -> AsyncIterable[Any]:
        pass


    async def put_context_def(self, context_def: Any) -> Any:
        pass


    async def delete_context_def(self, context_type: str) -> None:
        pass


    async def get_identity_defs_page(
        self,
        page_ref: str | None, 
        page_size: int
    ) -> Any:
        pass


    async def get_identity_def(self, identity_type: str) -> Any:
        pass


    async def list_identity_defs(self, page_size: int) -> AsyncIterable[Any]:
        pass


    async def put_identity_def(self, identity_def: Any) -> Any:
        pass


    async def delete_identity_def(self, identity_type: str) -> None:
        pass

    
    async def get_resource_defs_page(
        self,
        page_ref: str | None, 
        page_size: int
    ) -> Any:
        pass


    async def get_resource_def(self, resource_type: str) -> Any:
        pass


    async def list_resource_defs(self, page_size: int) -> AsyncIterable[Any]:
        pass


    async def put_resource_def(self, resource_def: Any) -> Any:
        pass


    async def delete_resource_def(self, resource_type: str) -> None:
        pass

    
    async def enact(self, new_grant: dict) -> Any:
        """Register an new grant with Authzee.

        Parameters
        ----------
        new_grant : dict
            New grant.

        Returns
        -------
        dict
            The new grant.

        Raises
        ------
        authzee.exceptions.GrantError
            Error when validating the new grant.
        """
        ng_copy = copy.deepcopy(new_grant)
        ng_copy['grant_uuid'] = str(uuid4())
        grant_val = core.validate_grants(
            grants=[ng_copy],
            schema=self.grant_schema
        )
        if grant_val['valid'] is False:
            raise exceptions.GrantError(
                message=f"Error validating grant. {grant_val}",
                context=[],
                definition=[],
                grant=grant_val['errors'],
                jmespath=[],
                request=[]
            )
        
        return await self._storage.enact(new_grant=new_grant)


    async def repeal(self, grant_uuid: str) -> None:
        """Delete a grant from Authzee.

        Parameters
        ----------
        grant_uuid : str
            UUID of the grant to delete.
        
        Raises
        ------
        authzee.exceptions.GrantNotFoundError
            The grant with the given UUID was not found.
        """
        await self._storage.repeal(grant_uuid=grant_uuid)
    

    async def get_grant(self, grant_uuid: str) -> dict:
        """Retrieve a grant from Authzee.

        Parameters
        ----------
        grant_uuid : str
            UUID if the grant to retrieve.
        
        Raises
        ------
        authzee.exceptions.GrantNotFoundError
            The grant with the given UUID was not found.
        """
        return await self._storage.get_grant(grant_uuid=grant_uuid)


    async def get_grants_page(
        self,
        effect: str | None,
        action: str | None, 
        page_ref: str | None, 
        page_size: int
    ) -> dict:
        """Get a page of grants.

        To get the next page pass the previous responses ``next_page_ref`` value 
        in the ``page_ref`` parameter with the other filter values being the same.
        Pagination is complete when ``next_page_ref`` is ``None``.

        Parameters
        ----------
        effect : str | None
            Filter by grant effect. None for no filter.
        action : str | None 
            Filter by grant action. None for no filter.
        page_ref : str | None
            Page reference of the page to retrieve. None to get the first page.
        grants_page_size : int | None
            Number of grants to return. Not exact. If ``None`` uses authzee default.

        Returns
        -------
        dict
            Page of grants with the next page reference. 
            
            .. code-block:: python

                {
                    "grants": [{<grant>}],
                    "next_page_ref": "some ref"
                }
        
        Raises
        ------
        authzee.exceptions.PageReferenceError
            Invalid page reference provided.

        Examples
        --------
        .. code-block:: python

            # assume az is already an Authzee instance
            resp = await az.get_grants_page(
                effect=None,
                action="ResourceA:ActionB",
                page_ref=None,
                grants_page_size=100
            )
            # get next page
            next_resp = await az.get_grants_page(
                effect=None,
                action="ResourceA:ActionB",
                page_ref=resp['next_page_ref'],
                grants_page_size=100
            )
        """
        return await self._storage.get_grants_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            grants_page_size=grants_page_size if grants_page_size is not None else self.grants_page_size
        )


    async def get_grant_page_refs_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        page_size: int,
        grants_page_size: int
    ) -> dict:
        """Retrieve a page of grant page references.

        To get the next page pass the previous responses ``next_page_ref`` value 
        in the ``page_ref`` parameter with the other filter values being the same.
        Pagination is complete when ``next_page_ref`` is ``None``.

        Parameters
        ----------
        effect : str | None
            Filter by grant effect. None for no filter.
        action : str | None 
            Filter by grant action. None for no filter.
        page_ref : str | None
            Page reference of the page to retrieve. None to get the first page.
        grants_page_size : int | None
            Number of grants per page. Not exact. If ``None`` uses authzee default.
        refs_page_size : int | None
            Number of page reference to return.  Not exact.  If ``None`` uses authzee default.

        Returns
        -------
        dict
            Page of page references with the next page reference. 
            
            .. code-block:: python

                {
                    "page_refs": ["some page ref"],
                    "next_page_ref": "some ref"
                }
        
        Raises
        ------
        authzee.exceptions.PageReferenceError
            Invalid page reference provided.

        Examples
        --------
        .. code-block:: python

            # assume az is already an Authzee instance
            resp = await az.get_grant_page_refs_page(
                effect=None,
                action="ResourceA:ActionB",
                page_ref=None,
                grants_page_size=100,
                refs_page_size=100
            )
            # get next page
            next_resp = await az.get_grant_page_refs_page(
                effect=None,
                action="ResourceA:ActionB",
                page_ref=resp['next_page_ref'],
                grants_page_size=100,
                refs_page_size=100
            )
            # get grants pages from the refs
            grants_page = await az.get_grants_page(
                effect=None,
                action="ResourceA:ActionB",
                page_ref=resp['page_refs'][0],
                grants_page_size=100
            )
        """
        return await self._storage.get_grant_page_refs_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            grants_page_size=grants_page_size if grants_page_size is not None else self.grants_page_size,
            refs_page_size=refs_page_size if refs_page_size is not None else self.grant_refs_page_size
        ) 


    async def audit_page(
        self, 
        request: dict, 
        page_ref: str | None, 
        page_size: int
    ) -> dict:
        """Process a page of grants that are applicable to an authorization request.

        To get the next page pass the previous responses ``next_page_ref`` value 
        in the ``page_ref`` parameter with the other filter values being the same.
        Pagination is complete when ``next_page_ref`` is ``None``.

        Parameters
        ----------
        request : dict
            Authzee request data.
        page_ref : str | None
            Page reference of the page to retrieve. None to get the first page.
        grants_page_size : int | None
            Number of grants per page to process. Not exact. If ``None`` uses authzee default.
        parallel_paging : bool
            Enable parallel pagination. May return many more results at once. If ``None`` uses authzee default.
        refs_page_size : int | None
            Number of page reference to process.  Not exact.  If ``None`` uses authzee default.

        Returns
        -------
        dict
            Audit response. 
            
            .. code-block:: python

                {
                    "completed": true,
                    "grants": [{<grant>}],
                    "errors": {
                        "context": [],
                        "definition": [],
                        "grant": [],
                        "jmespath": [],
                        "request": []
                    },
                    "next_page_ref": "page ref here"
                }
        
        Raises
        ------
        authzee.exceptions.ContextError
            Critical error when validating context.
        authzee.exceptions.JMESPathError
            Critical error when executing JMESPath query.
        authzee.exceptions.PageReferenceError
            Invalid page reference provided.
        authzee.exceptions.ParallelPaginationNotSupported
            Parallel pagination requested but not supported.
        authzee.exceptions.RequestError
            Invalid authorization request provided.
        
        Examples
        --------
        .. code-block:: python

            request = {
                "identities": {
                    "User": [
                        {
                            "id": "user123"
                        }
                    ]
                },
                "resource_type": "Balloon",
                "action": "Balloon:Inflate",
                "resource": {
                    "color": "green"
                },
                "parents": {
                    "BalloonStore": [
                        {
                            "id": "store123"
                        }
                    ]
                },
                "children": { },
                "query_validation": "error",
                "context": {
                    "timestamp": "2017-12-27T20:30:00Z",
                    "event_type": "birthday_party"
                },
                "context_validation": "grant"
            }
            # assume az is already an Authzee instance
            resp = await az.audit_page(
                request=request,
                page_ref=None,
                grants_page_size=100,
                parallel_pagination=True,
                refs_page_size=100
            )
            next_resp = await az.audit_page(
                request=request,
                page_ref=resp['next_page_ref'],
                grants_page_size=100,
                parallel_pagination=True,
                refs_page_size=100
            )
        """
        request_val = core.validate_request(request=request, schema=self.request_schema)
        if request_val['valid'] is False:
            raise exceptions.RequestError(
                message="Error validating request.",
                response=request_val
            )
        
        return await self._compute.audit_page(
            request=request,
            page_ref=page_ref,
            grants_page_size=grants_page_size if grants_page_size is not None else self.grants_page_size,
            parallel_paging=parallel_paging if parallel_paging is not None else self.parallel_paging,
            refs_page_size=refs_page_size if refs_page_size is not None else self.grant_refs_page_size
        )


    async def authorize(self, request: dict) -> dict:
        """Authorize a request.

        Parameters
        ----------
        request : dict
            Authzee request data.
        grants_page_size : int | None
            Number of grants per page to process. Not exact. If ``None`` uses authzee default.
        parallel_paging : bool
            Enable parallel pagination. Used to control how compute and storage process pages. If ``None`` uses authzee default.
        refs_page_size : int | None
            Number of page reference to process.  Not exact.  If ``None`` uses authzee default.

        Returns
        -------
        dict
            Authorize response. 
            
            .. code-block:: python

                {
                    "authorized": true,
                    "completed": true,
                    "grant": {
                        "effect": "allow",
                        "actions": [
                            "inflate"
                        ],
                        "query": "contains(request.identities.Role[*].permissions[], 'balloon:inflate') && request.identities.User[0].department == request.resource.owner_department",
                        "query_validation": "error",
                        "equality": true,
                        "data": {
                            "rule_name": "department_balloon_access",
                            "created_by": "party_team"
                        },
                        "context_schema": {
                            "type": "object"
                        },
                        "context_validation": "none"
                    },
                    "message": "An allow grant is applicable to the request, and there are no deny grants that are applicable to the request. Therefore, the request is authorized.",
                    "critical_errors": {
                        "context": [],
                        "definition": [],
                        "grant": [],
                        "jmespath": [],
                        "request": []
                    }
                }
        
        Raises
        ------
        authzee.exceptions.ContextError
            Critical error when validating context.
        authzee.exceptions.JMESPathError
            Critical error when executing JMESPath query.
        authzee.exceptions.ParallelPaginationNotSupported
            Parallel pagination requested but not supported.
        authzee.exceptions.RequestError
            Invalid authorization request provided.
        
        Examples
        --------
        .. code-block:: python

            request = {
                "identities": {
                    "User": [
                        {
                            "id": "user123"
                        }
                    ]
                },
                "resource_type": "Balloon",
                "action": "Balloon:Inflate",
                "resource": {
                    "color": "green"
                },
                "parents": {
                    "BalloonStore": [
                        {
                            "id": "store123"
                        }
                    ]
                },
                "children": { },
                "query_validation": "error",
                "context": {
                    "timestamp": "2017-12-27T20:30:00Z",
                    "event_type": "birthday_party"
                },
                "context_validation": "grant"
            }
            # assume az is already an Authzee instance
            resp = await az.authorize(
                request=request,
                grants_page_size=100,
                parallel_pagination=True,
                refs_page_size=100
            )
            if resp['authorized'] is True:
                print("I'm Authorized!!!")
        """
        request_val = core.validate_request(request=request, schema=self.request_schema)
        if request_val['valid'] is False:
            raise exceptions.RequestError(
                message="Error validating request.",
                response=request_val
            )

        return await self._compute.authorize(
            request=request,
            grants_page_size=grants_page_size if grants_page_size is not None else self.grants_page_size,
            parallel_paging=parallel_paging if parallel_paging is not None else self.parallel_paging,
            refs_page_size=refs_page_size if refs_page_size is not None else self.grant_refs_page_size
        )


    async def batch_audit_page(
        batch_request: Any, 
        page_ref: str | None, 
        page_size: int
    ) -> Any:
        pass


    async def batch_authorize(batch_request: Any) -> Any:
        pass