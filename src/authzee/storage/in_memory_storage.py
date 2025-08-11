
import copy
import datetime
from typing import Any, Dict, List
from uuid import UUID, uuid4

from authzee import exceptions
from authzee.module_locality import ModuleLocality
from authzee.storage.storage_module import StorageModule



class InMemoryStorage(StorageModule):
    """Storage module that uses the Authzee apps memory to as the storage medium.

    Upon ``shutdown()`` or exit all storage is lost.
    """

    def __init__(self):
        self._grant_lut: Dict[UUID, dict] = {}
        self._grant_effect_lut: Dict[str, Dict[UUID, dict]] = {
            "allow": [],
            "deny": []
        }
        self._grant_action_lut: Dict[str, Dict[UUID, dict]] = {}
        self._grant_both_lut: Dict[str, Dict[str, Dict[UUID, dict]]] = {
            "allow": {},
            "deny": {}
        }


    async def start(
        self, 
        identity_defs: List[Dict[str, Any]],
        resource_defs: List[Dict[str, Any]]
    ) -> None:
        """Initialize the storage module. 

        Parameters
        ----------
        identity_defs : List[dict[str]]
            Identity definitions registered and validated with Authzee.
        resource_defs : List[dict[str]]
            ``ResourceAuthz`` instances that have been registered with Authzee.
        """
        await super().start(
            identity_defs=identity_defs,
            resource_defs=resource_defs
        )
        self.locality = ModuleLocality.PROCESS
        self.parallel_paging_supported = True
        for rd in resource_defs:
            for action in rd['actions']:
                self._grant_action_lut[action] = {}
                self._grant_both_lut['allow'][action] = {}
                self._grant_both_lut['deny'][action] = {}
        

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
        """
        grant = copy.deepcopy(new_grant)
        grant_uuid = uuid4()
        grant['grant_uuid'] = str(grant_uuid)
        self._grant_lut[grant_uuid] = grant
        self._grant_effect_lut[grant['effect']][grant_uuid] = grant
        for action in grant['actions']:
            self._grant_action_lut[action][grant_uuid] = grant
            self._grant_both_lut[grant['effect']][action][grant_uuid] = grant

        return copy.deepcopy(grant)


    async def repeal(self, grant_uuid: UUID) -> None:
        """Delete a grant.

        Parameters
        ----------
        grant_uuid : UUID
            The UUID of the grant to delete.

        Raises
        ------
        authzee.exceptions.GrantNotFoundError
            The grant with the given UUID could not be found.
        """
        if grant_uuid not in self._grant_lut:
            raise exceptions.GrantNotFoundError(grant_uuid=grant_uuid)
    
        grant = self._grant_lut.pop(grant_uuid)
        self._grant_effect_lut[grant['effect']].pop(grant_uuid)
        for action in grant['actions']:
            self._grant_action_lut[action].pop(grant_uuid)
            self._grant_both_lut[grant['effect']][action].pop(grant_uuid)
    

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
        if grant_uuid not in self._grant_lut:
            raise exceptions.GrantNotFoundError(grant_uuid=grant_uuid)

        return copy.deepcopy(self._grant_lut[grant_uuid])
        

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

