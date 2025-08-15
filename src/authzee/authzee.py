
import asyncio
import copy
from typing import Any, Callable, Dict, List,Type
from uuid import UUID, uuid4

from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule
from authzee.authzee_async import AuthzeeAsync


class Authzee:
    """Authzee application.

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

        from authzee import Authzee, ComputeModule, StorageModule 
        # import real compute and store modules for use case
        import jmespath

        az = Authzee(
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

        new_grant = az.enact(
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
        resp = az.authorize(
            request=request,
            grants_page_size=100,
            parallel_pagination=True,
            refs_page_size=100
        )
        if resp['authorized'] is True:
            print("I'm Authorized!!!")
    """

    def __init__(
        self, 
        identity_defs: List[dict[str]],
        resource_defs: List[dict[str]],
        search: Callable[[str, Any], Any],
        compute_type: Type[ComputeModule],
        compute_kwargs: Dict[str, Any],
        storage_type: Type[StorageModule],
        storage_kwargs: Dict[str, Any],
        grants_page_size: int,
        grant_refs_page_size: int,
        parallel_paging: bool
    ):
        self._authzee_async = AuthzeeAsync(
            identity_defs=identity_defs,
            resource_defs=resource_defs,
            search=search,
            compute_type=compute_type,
            compute_kwargs=compute_kwargs,
            storage_type=storage_type,
            storage_kwargs=storage_kwargs,
            grants_page_size=grants_page_size,
            grant_refs_page_size=grant_refs_page_size,
            parallel_paging=parallel_paging
        )


    def start(self) -> None:
        """Initialize and start the Authzee app.

        Creates runtime resources for the compute and storage modules.

        Raises
        ------
        authzee.exceptions.DefinitionError
            The identity or resource definitions were invalid.
        authzee.exceptions.StartError
            An error occurred while initializing the app.
        """
        asyncio.run(self._authzee_async.start())

    
    def shutdown(self) -> None:
        """Clean up of resources for the authzee app.

        Shutdown and cleanup runtime resources for the compute and storage modules.

        Should be called on program shutdown.
        """
        asyncio.run(self._authzee_async.shutdown())
    

    def setup(self) -> None:
        """One time setup for authzee app with the current configuration. 

        This method only has to be run once, and will standup resources for the compute and storage modules.
        """
        asyncio.run(self._authzee_async.setup())
    

    def teardown(self) -> None:
        """Tear down resources create for one time setup by ``setup()``.

        This may delete all storage for grants.
        """
        asyncio.run(self._authzee_async.teardown())


    def enact(self, new_grant: dict) -> dict:
        """Register an new grant with Authzee.

        Parameters
        ----------
        new_grant : dict
            New grant. Grant object without ``grant_uuid``.

        Returns
        -------
        dict
            The new grant.

        Raises
        ------
        authzee.exceptions.GrantError
            Error when validating the new grant.
        """
        return asyncio.run(self._authzee_async.enact(new_grant=new_grant))


    def repeal(self, grant_uuid: UUID) -> None:
        """Delete a grant from Authzee.

        Parameters
        ----------
        grant_uuid : UUID
            UUID if the grant to delete.
        
        Raises
        ------
        authzee.exceptions.GrantNotFoundError
            The grant with the given UUID was not found.
        """
        asyncio.run(self._authzee_async.repeal(grant_uuid=grant_uuid))


    def get_grant(self, grant_uuid: UUID) -> dict:
        """Retrieve a grant from Authzee.

        Parameters
        ----------
        grant_uuid : UUID
            UUID if the grant to retrieve.
        
        Raises
        ------
        authzee.exceptions.GrantNotFoundError
            The grant with the given UUID was not found.
        """
        return asyncio.run(self._authzee_async.get_grant(grant_uuid=grant_uuid))


    def get_grants_page(
        self,
        effect: str | None,
        action: str | None, 
        page_ref: str | None, 
        grants_page_size: int | None
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
            resp = az.get_grants_page(
                effect=None,
                action="ResourceA:ActionB",
                page_ref=None,
                grants_page_size=100
            )
            # get next page
            next_resp = az.get_grants_page(
                effect=None,
                action="ResourceA:ActionB",
                page_ref=resp['next_page_ref'],
                grants_page_size=100
            )
        """
        return asyncio.run(
            self._authzee_async.get_grants_page(
                effect=effect,
                action=action,
                page_ref=page_ref,
                grants_page_size=grants_page_size
            )
        )


    def get_grant_page_refs_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        grants_page_size: int | None,
        refs_page_size: int | None
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
            resp = az.get_grant_page_refs_page(
                effect=None,
                action="ResourceA:ActionB",
                page_ref=None,
                grants_page_size=100,
                refs_page_size=100
            )
            # get next page
            next_resp = az.get_grant_page_refs_page(
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
        return asyncio.run(
            self._authzee_async.get_grant_page_refs_page(
                effect=effect,
                action=action,
                page_ref=page_ref,
                grants_page_size=grants_page_size,
                refs_page_size=refs_page_size
            )
        )
        

    def audit_page(
        self, 
        request: dict, 
        page_ref: str | None, 
        grants_page_size: int | None, 
        parallel_paging: bool | None, 
        refs_page_size: int | None
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
            Page of page references with the next page reference. 
            
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
            resp = az.audit_page(
                request=request,
                page_ref=None,
                grants_page_size=100,
                parallel_pagination=True,
                refs_page_size=100
            )
            next_resp = az.audit_page(
                request=request,
                page_ref=resp['next_page_ref'],
                grants_page_size=100,
                parallel_pagination=True,
                refs_page_size=100
            )
        """
        return asyncio.run(
            self._authzee_async.audit_page(
                request=request,
                page_ref=page_ref,
                grants_page_size=grants_page_size,
                parallel_paging=parallel_paging,
                refs_page_size=refs_page_size
            )
        )


    def authorize(
        self, 
        request: dict, 
        grants_page_size: int | None, 
        parallel_paging: bool | None, 
        refs_page_size: int | None 
    ) -> dict:
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
            Page of page references with the next page reference. 
            
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
                    "errors": {
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
            resp = az.authorize(
                request=request,
                grants_page_size=100,
                parallel_pagination=True,
                refs_page_size=100
            )
            if resp['authorized'] is True:
                print("I'm Authorized!!!")
        """
        return asyncio.run(
            self._authzee_async.authorize(
                request=request,
                grants_page_size=grants_page_size,
                parallel_paging=parallel_paging,
                refs_page_size=refs_page_size
            )
        )