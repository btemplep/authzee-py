
import asyncio
import copy
from typing import Any, Callable, Dict, List,Type
from uuid import UUID, uuid4

from authzee.compute.compute_module import ComputeModule
from authzee.storage.storage_module import StorageModule
from authzee.authzee_async import AuthzeeAsync


class Authzee:
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
        self._authzee_async = AuthzeeAsync(
            identity_defs=identity_defs,
            resource_defs=resource_defs,
            search=search,
            compute_type=compute_type,
            compute_kwargs=compute_kwargs,
            storage_type=storage_type,
            storage_kwargs=storage_kwargs,
            grants_page_size=grants_page_size,
            grant_refs_page_size=grant_refs_page_size
        )


    def start(self) -> None:
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
        asyncio.run(self._authzee_async.start())

    
    def shutdown(self) -> None:
        """Clean up of resources for the authzee app.

        Should be called on program shutdown to clean up connections etc.

        Examples
        --------
        .. code-block:: python

            from authzee import Authzee

        """
        asyncio.run(self._authzee_async.shutdown())
    

    def setup(self) -> None:
        """One time setup for authzee app with the current configuration. 

        This method only has to be run once.

        Examples
        --------
        .. code-block:: python

            from authzee import Authzee

        """
        asyncio.run(self._authzee_async.setup())
    

    def teardown(self) -> None:
        """Tear down resources create for one time setup by ``setup()``.

        This may delete all storage for grants etc. 

        Examples
        --------
        .. code-block:: python

            from authzee import Authzee

        """
        asyncio.run(self._authzee_async.teardown())


    def enact(self, new_grant: dict) -> dict:
        return asyncio.run(self._authzee_async.enact(new_grant=new_grant))


    def repeal(self, grant_uuid: UUID) -> None:
        asyncio.run(self._authzee_async.repeal(grant_uuid=grant_uuid))
    

    def get_grant(self, grant_uuid: UUID) -> dict:
        return asyncio.run(self._authzee_async.get_grant(grant_uuid=grant_uuid))


    def get_grants_page(
        self,
        effect: str | None,
        action: str | None, 
        page_ref: str | None, 
        page_size: int | None
    ) -> dict:
        return asyncio.run(
            self._authzee_async.get_grants_page(
                effect=effect,
                action=action,
                page_ref=page_ref,
                page_size=page_size
            )
        )


    def get_grant_page_refs_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        page_size: int | None
    ) -> dict:
        return asyncio.run(
            self._authzee_async.get_grant_page_refs_page(
                effect=effect,
                action=action,
                page_ref=page_ref,
                page_size=page_size
            )
        )


    def get_grants_page_parallel(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        page_size: int | None, 
        ref_page_size: int | None
    ) -> dict:
        return asyncio.run(
            self._authzee_async.get_grants_page_parallel(
                effect=effect,
                action=action,
                page_ref=page_ref,
                page_size=page_size,
                ref_page_size=ref_page_size
            )
        )
        

    def audit_page(
        self, 
        request: dict, 
        page_ref: str | None, 
        page_size: int | None, 
        parallel_paging: bool, 
        ref_page_size: int | None
    ) -> dict:
        return asyncio.run(
            self._authzee_async.audit_page(
                request=request,
                page_ref=page_ref,
                page_size=page_size,
                parallel_paging=parallel_paging,
                ref_page_size=ref_page_size
            )
        )


    def authorize(
        self, 
        request: dict, 
        page_size: int | None, 
        parallel_paging: bool, 
        ref_page_size: int | None
    ) -> dict:
        return asyncio.run(
            self._authzee_async.authorize(
                request=request,
                page_size=page_size,
                parallel_paging=parallel_paging,
                ref_page_size=ref_page_size
            )
        )
