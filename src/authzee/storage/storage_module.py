

import datetime
from typing import Any, Dict, List
from uuid import UUID

from authzee import exceptions
from authzee.module_locality import ModuleLocality



class StorageModule:
    """Base class for Authzee Storage Modules. 
    
    The ``__init__`` method should take Storage module specific arguments and store them as necessary.
    """   


    async def start(
        self, 
        identity_defs: List[Dict[str, Any]],
        resource_defs: List[Dict[str, Any]]
    ) -> None:
        """Initialize the storage module. 

        Any initialization of runtime storage should take place here.
        Also update locality and parallel paging if needed after calling this super. 

        Parameters
        ----------
        identity_defs : List[dict[str]]
            Identity definitions registered and validated with Authzee.
        resource_defs : List[dict[str]]
            ``ResourceAuthz`` instances that have been registered with Authzee.
        """
        self.identity_defs = identity_defs
        self.resource_defs = resource_defs
        self.locality = ModuleLocality.PROCESS
        self.parallel_paging_supported = False
    

    async def shutdown(self) -> None:
        """Early clean up of storage module resources.
        """
        pass


    async def setup(self) -> None:
        """One time setup for storage module resources.
        """
        pass

    
    async def teardown(self) -> None:
        """Teardown and delete the results of ``setup()`` .
        """
        pass

    
    async def enact(self, new_grant: dict) -> dict:
        """Add a grant. 

        Parameters
        ----------
        new_grant : dict
            The new grant data.

        Returns
        -------
        dict
            The grant that has been added.

        Raises
        ------
        authzee.exceptions.MethodNotImplementedError
            ``StorageModule`` sub-classes must implement this method.
        """
        raise exceptions.MethodNotImplementedError()


    async def repeal(self, grant_uuid: UUID) -> None:
        """Delete a grant.

        Parameters
        ----------
        grant_uuid : UUID
            The UUID of the grant to delete.

        Raises
        ------
        authzee.exceptions.MethodNotImplementedError
            ``StorageModule`` sub-classes must implement this method.
        authzee.exceptions.GrantNotFoundError
            The grant with the given UUID could not be found.
        """
        raise exceptions.MethodNotImplementedError()
    

    async def get_grant(self, grant_uuid: UUID) -> dict:
        """Retrieve a grant by UUID.

        Parameters
        ----------
        grant_uuid : UUID
            Grant UUID.

        Returns
        -------
        dict
            The grant with the matching UUID.

        Raises
        ------
        authzee.exceptions.MethodNotImplementedError
            ``StorageModule`` sub-classes must implement this method.
        authzee.exceptions.GrantNotFoundError
            The grant with the given UUID could not be found.
        """
        raise exceptions.MethodNotImplementedError()
        

    async def get_grants_page(
        self,
        effect: str | None,
        action: str | None, 
        page_ref: str | None, 
        page_size: int
    ) -> dict:
        """Get a page of grants.

        Parameters
        ----------
        effect : str, optional
            Filter by grant effect. 
        action : str, optional
            Filter by grant action.
        page_ref : str, optional
            Reference to the page to retrieve.
        page_size : int
            Page size of grants.

        Returns
        -------
        dict
            Page of grants and next page ref.

        Raises
        ------
        authzee.exceptions.MethodNotImplementedError
            ``StorageModule`` sub-classes must implement this method.
        """
        raise exceptions.MethodNotImplementedError()
    

    async def get_grant_page_refs_page(
        self,
        effect: str | None, 
        action: str | None, 
        page_ref: str | None, 
        page_size: int | None
    ) -> dict:
        """Get a page of page references for parallel pagination. 

        Parameters
        ----------
        effect : str, optional
            Filter by grant effect. 
        action : str, optional
            Filter by grant action.
        page_ref : str, optional
            Reference to the page to retrieve.
        page_size : int
            Page size of grants.

        Returns
        -------
        dict
            Page of grants and next page ref.

        Raises
        ------
        authzee.exceptions.MethodNotImplementedError
            ``StorageModule`` sub-classes must implement this method if this storage module supports parallel pagination. 
            They must also set the ``supports_parallel_paging`` flag. 
        """
        if self.supports_parallel_paging is True:
            raise exceptions.MethodNotImplementedError(
                (
                    "There is an error in the storage module!"
                    "This storage module has marked supports_parallel_paging as true "
                    "but it has not implemented the required methods!"
                )
            )
        else:
            raise exceptions.ParallelPaginationNotSupported(
                "This storage module does not support parallel pagination."
            )
    
    
    async def create_latch(self) -> dict:
        """Create a new shared latch in the storage module.

        Returns
        -------
        dict
            New storage latch. 
        
        Raises
        ------
        authzee.exceptions.MethodNotImplementedError
            ``StorageModule`` sub-classes must implement this method.
        """
        pass


    async def get_latch(self, uuid: UUID) -> dict:
        """Retrieve latch by UUID.

        Parameters
        ----------
        uuid : UUID
            Storage latch UUID.

        Returns
        -------
        dict
            The storage latch with the given UUID.
        
        Raises
        ------
        authzee.exceptions.MethodNotImplementedError
            ``StorageModule`` sub-classes must implement this method.
        authzee.exceptions.LatchNotFoundError
            The latch with the given UUID could not be found.
        """
        pass


    async def set_latch(self, uuid: UUID) -> dict:
        """Set a latch for a given UUID. 

        Parameters
        ----------
        uuid : UUID
            Storage latch UUID.

        Returns
        -------
        dict
            The storage latch with the given UUID and the latch set.
        
        Raises
        ------
        authzee.exceptions.MethodNotImplementedError
            ``StorageModule`` sub-classes must implement this method.
        authzee.exceptions.LatchNotFoundError
            The latch with the given UUID could not be found.
        """
        pass


    async def delete_latch(self, latch_uuid: UUID) -> None:
        """Delete a storage latch by UUID.

        Parameters
        ----------
        uuid : UUID
            Storage latch UUID.
        
        Raises
        ------
        authzee.exceptions.MethodNotImplementedError
            ``StorageModule`` sub-classes must implement this method.
        authzee.exceptions.LatchNotFoundError
            The latch with the given UUID could not be found.
        """
        pass


    async def cleanup_latches(self, before: datetime.datetime) -> None:
        """Delete zombie storage latches created before a certain point in time.

        Parameters
        ----------
        before : datetime.datetime
            Delete latches created before this datetime.
        
        Raises
        ------
        authzee.exceptions.MethodNotImplementedError
            ``StorageModule`` sub-classes must implement this method.
        """
        pass

