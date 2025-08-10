
import copy
from typing import Any, Callable, Dict, List,Type
from uuid import UUID, uuid4

from authzee import core
from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule

from authzee import exceptions
from authzee.module_locality import locality_compatibility


class AuthzeeAsync:
    """Authzee application with async.

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
        grant_refs_page_size: int
    ):
        self.identity_defs: List[dict[str]] = identity_defs
        self.resource_defs: List[dict[str]] = resource_defs
        self.search: Callable[[str, Any], Any] = search
        self.compute_type: Type[ComputeModule] = compute_type
        self.compute_kwargs: Dict[str, Any] = compute_kwargs
        self.storage_type: Type[StorageModule] = storage_type
        self.storage_kwargs: Dict[str, Any] = storage_kwargs
        self.grants_page_size: int = grants_page_size
        self.grant_refs_page_size: int = grant_refs_page_size
        self.grant_schema = {}
        self.error_schema = {}
        self.request_schema = {}
        self.audit_schema = {}
        self.authorize_schema = {}


    async def start(self) -> None:
        """Initialize and start the Authzee app.

        Raises
        ------
        authzee.exceptions.StartError
            An error occurred while initializing the app.
        
        Examples
        --------
        .. code-block:: python

            from authzee import Authzee

        """
        def_val = core.validate_definitions(
            identity_defs=self.identity_defs,
            resource_defs=self.resource_defs
        )
        if def_val['valid'] is False:
            raise exceptions.DefinitionError(
                message="Error when validating the identity or resource definitions",
                response=def_val
            )
        
        schemas = core.generate_schemas(
            identity_defs=self.identity_defs,
            resource_defs=self.resource_defs
        )
        self.grant_schema = schemas['grant']
        self.error_schema = schemas['error']
        self.request_schema = schemas['request']
        self.audit_schema = schemas['audit']
        self.authorize_schema = schemas['authorize']
        # create a storage and start
        self._storage: StorageModule = self.storage_type(**self.storage_kwargs)
        await self._storage.start(
            identity_defs=self.identity_defs,
            resource_defs=self.resource_defs
        )
        self._compute: StorageModule = self.compute_type(**self.compute_kwargs)
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

    
    async def shutdown(self) -> None:
        """Clean up of resources for the authzee app.

        Should be called on program shutdown to clean up async connections etc.

        Examples
        --------
        .. code-block:: python

            from authzee import Authzee

        """
        await self._storage.shutdown()
        await self._compute.shutdown()
    

    async def setup(self) -> None:
        """One time setup for authzee app with the current configuration. 

        This method only has to be run once.

        Examples
        --------
        .. code-block:: python

            from authzee import Authzee

        """
        await self._storage.setup()
        await self._compute.setup()
    

    async def teardown(self) -> None:
        """Tear down resources create for one time setup by ``setup()``.

        This may delete all storage for grants etc. 

        Examples
        --------
        .. code-block:: python

            from authzee import Authzee

        """
        await self._storage.teardown()
        await self._compute.teardown()


    async def enact(self, new_grant: dict) -> dict:
        ng_copy = copy.deepcopy(new_grant)
        ng_copy['grant_uuid'] = uuid4()
        grant_val = core.validate_grants(
            grants=[ng_copy],
            schema=self.grant_schema
        )
        if grant_val['valid'] is False:
            raise exceptions.GrantError(
                message="Error validating grant.",
                response=grant_val
            )
        
        return await self._storage.enact(new_grant=new_grant)


    async def repeal(self, grant_uuid: UUID) -> None:
        await self.storage.repeal(grant_uuid=grant_uuid)
    

    async def get_grant(self, grant_uuid: UUID) -> dict:
        return await self._storage.get_grant(grant_uuid=grant_uuid)


    async def get_grants_page(
        self,
        effect: str | None,
        action: str | None, 
        page_ref: str | None, 
        page_size: int | None
    ) -> dict:
        # check effect and action?
        return await self._storage.get_grants_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            page_size=page_size if page_size is not None else self.grants_page_size
        )


    async def get_grant_page_refs_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        page_size: int | None
    ) -> dict:
        return await self._storage.get_grant_page_refs_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            page_size=page_size if page_size is not None else self.grants_page_size
        ) 


    async def get_grants_page_parallel(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        page_size: int | None, 
        ref_page_size: int | None
    ) -> dict:
        return await self._compute.get_grant_page_refs_page(
            effect=effect,
            action=action,
            page_ref=page_ref,
            page_size=page_size if page_size is not None else self.grants_page_size,
            ref_page_size=ref_page_size if ref_page_size is not None else self.grant_refs_page_size
        )
        

    async def audit_page(
        self, 
        request: dict, 
        page_ref: str | None, 
        page_size: int | None, 
        parallel_paging: bool, 
        ref_page_size: int | None
    ) -> dict:
        request_val = core.validate_request(request=request, schema=self.request_schema)
        if request_val['valid'] is False:
            raise exceptions.RequestError(
                message="Error validating request.",
                response=request_val
            )
        
        return await self._compute_audit_page(
            request=request,
            page_ref=page_ref,
            page_size=page_size if page_size is not None else self.grants_page_size,
            parallel_paging=parallel_paging,
            ref_page_size=ref_page_size if ref_page_size is not None else self.grant_refs_page_size
        )


    async def authorize(
        self, 
        request: dict, 
        page_size: int | None, 
        parallel_paging: bool, 
        ref_page_size: int | None
    ) -> dict:
        request_val = core.validate_request(request=request, schema=self.request_schema)
        if request_val['valid'] is False:
            raise exceptions.RequestError(
                message="Error validating request.",
                response=request_val
            )

        return await self._compute_audit_page(
            request=request,
            page_size=page_size if page_size is not None else self.grants_page_size,
            parallel_paging=parallel_paging,
            ref_page_size=ref_page_size if ref_page_size is not None else self.grant_refs_page_size
        )